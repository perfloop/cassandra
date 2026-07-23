/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.test.microbench.partitions;

import java.nio.ByteBuffer;
import java.util.Iterator;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.BufferClusteringBound;
import org.apache.cassandra.db.BufferDecoratedKey;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionInfo;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.MutableDeletionInfo;
import org.apache.cassandra.db.RangeTombstone;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.Slice;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.partitions.AtomicBTreePartition;
import org.apache.cassandra.db.partitions.BTreePartitionData;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.dht.ByteOrderedPartitioner;
import org.apache.cassandra.index.transactions.UpdateTransaction;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.utils.btree.BTree;
import org.apache.cassandra.utils.concurrent.ImmediateFuture;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.memory.HeapCloner;
import org.apache.cassandra.utils.memory.HeapPool;
import org.apache.cassandra.utils.memory.MemtableAllocator;

/**
 * Shared immutable-prefix fixtures for deletion-info transition benchmarks and their correctness test.
 */
public final class DeletionInfoTransitionSupport
{
    public enum Operation
    {
        PARTITION_DELETE,
        OVERLAP,
        OUT_OF_ORDER,
        TWO_EXACT_BOUND
    }

    private static final int RANGE_STRIDE = 8;
    private static final long PREFIX_TIMESTAMP = 100;
    private static final long UPDATE_TIMESTAMP = 101;
    private static final long PREFIX_LOCAL_DELETION_TIME = 10;
    private static final long UPDATE_LOCAL_DELETION_TIME = 20;
    private static final ClusteringComparator COMPARATOR = new ClusteringComparator(Int32Type.instance);
    private static final HeapPool POOL = new HeapPool(Long.MAX_VALUE, 1.0f, () -> ImmediateFuture.success(Boolean.TRUE));

    static
    {
        DatabaseDescriptor.daemonInitialization();
        DatabaseDescriptor.setPartitionerUnsafe(ByteOrderedPartitioner.instance);
    }

    private DeletionInfoTransitionSupport()
    {
    }

    public static Fixture fixture(int rangeCount, String separation)
    {
        return new Fixture(rangeCount, separation);
    }

    public static void assertEquivalent(DeletionInfo expected, DeletionInfo actual)
    {
        if (!expected.getPartitionDeletion().equals(actual.getPartitionDeletion()))
            throw new AssertionError("partition deletion differs: expected=" + expected.getPartitionDeletion() + " actual=" + actual.getPartitionDeletion());

        if (expected.rangeCount() != actual.rangeCount())
            throw new AssertionError("range count differs: expected=" + expected.rangeCount() + " actual=" + actual.rangeCount());

        assertIteratorEquivalent(expected.rangeIterator(false), actual.rangeIterator(false));
    }

    public static void assertIteratorEquivalent(DeletionInfo expected, DeletionInfo actual, Slice slice, boolean reversed)
    {
        assertIteratorEquivalent(expected.rangeIterator(slice, reversed), actual.rangeIterator(slice, reversed));
    }

    public static void assertRangeEquivalent(RangeTombstone expected, RangeTombstone actual)
    {
        if (expected == null || actual == null)
        {
            if (expected != actual)
                throw new AssertionError("range coverage differs: expected=" + expected + " actual=" + actual);
            return;
        }

        if (COMPARATOR.compare(expected.deletedSlice().start(), actual.deletedSlice().start()) != 0
            || COMPARATOR.compare(expected.deletedSlice().end(), actual.deletedSlice().end()) != 0
            || !expected.deletionTime().equals(actual.deletionTime()))
        {
            throw new AssertionError("range differs: expected=" + expected.deletedSlice().toString(COMPARATOR)
                                     + '@' + expected.deletionTime()
                                     + " actual=" + actual.deletedSlice().toString(COMPARATOR)
                                     + '@' + actual.deletionTime());
        }
    }

    public static Slice sliceFor(int offset, int rangeCount, String position)
    {
        int first = offset;
        int middle = offset + (rangeCount / 2) * RANGE_STRIDE;
        int last = offset + (rangeCount - 1) * RANGE_STRIDE;

        switch (position)
        {
            case "EARLY":
                return Slice.make(BufferClusteringBound.inclusiveStartOf(integer(first + 1)),
                                  BufferClusteringBound.exclusiveEndOf(integer(first + 3)));
            case "MIDDLE":
                return Slice.make(BufferClusteringBound.exclusiveStartOf(integer(middle)),
                                  BufferClusteringBound.inclusiveEndOf(integer(middle + RANGE_STRIDE + 2)));
            case "LATE":
                return Slice.make(BufferClusteringBound.inclusiveStartOf(integer(last + 1)),
                                  BufferClusteringBound.exclusiveEndOf(integer(last + 3)));
            case "MISS":
                return Slice.make(BufferClusteringBound.inclusiveStartOf(integer(first + 4)),
                                  BufferClusteringBound.inclusiveEndOf(integer(first + 7)));
            default:
                throw new IllegalArgumentException("Unknown slice position " + position);
        }
    }

    public static Clustering<?>[] clusteringProbes(int offset, int rangeCount, String coverage)
    {
        int samples = Math.min(16, rangeCount);
        Clustering<?>[] probes = new Clustering<?>[samples];
        boolean covered;
        if ("COVERED".equals(coverage))
            covered = true;
        else if ("UNCOVERED".equals(coverage))
            covered = false;
        else
            throw new IllegalArgumentException("Unknown coverage " + coverage);

        for (int i = 0; i < samples; i++)
        {
            int range = i * rangeCount / samples;
            int value = offset + range * RANGE_STRIDE + (covered ? 1 : 5);
            probes[i] = Clustering.make(integer(value));
        }
        return probes;
    }

    public static void addDisjointRange(MutableDeletionInfo info, int offset, int rangeCount)
    {
        int start = offset + rangeCount * RANGE_STRIDE + 2;
        info.add(range(start, start + 2, UPDATE_TIMESTAMP + 1, UPDATE_LOCAL_DELETION_TIME + 1), COMPARATOR);
    }

    public static DeletionInfo deepClone(DeletionInfo info)
    {
        return info.clone(HeapCloner.instance);
    }

    public static ByteBuffer firstStartBuffer(DeletionInfo info)
    {
        return (ByteBuffer) info.rangeIterator(false).next().deletedSlice().start().get(0);
    }

    private static void assertIteratorEquivalent(Iterator<RangeTombstone> expected, Iterator<RangeTombstone> actual)
    {
        while (expected.hasNext())
        {
            if (!actual.hasNext())
                throw new AssertionError("actual iterator ended before expected iterator");
            assertRangeEquivalent(expected.next(), actual.next());
        }
        if (actual.hasNext())
            throw new AssertionError("actual iterator contains an unexpected range");
    }

    private static ByteBuffer integer(int value)
    {
        return Int32Type.instance.decompose(value);
    }

    private static RangeTombstone range(int start, int end, long timestamp, long localDeletionTime)
    {
        return new RangeTombstone(Slice.make(BufferClusteringBound.inclusiveStartOf(integer(start)),
                                             BufferClusteringBound.inclusiveEndOf(integer(end))),
                                  DeletionTime.build(timestamp, localDeletionTime));
    }

    public static final class Fixture
    {
        private final int rangeCount;
        private final String separation;
        private final TableMetadata metadata;
        private final DecoratedKey key;
        private final MemtableAllocator allocator;
        private final OpOrder opOrder = new OpOrder();
        private final AtomicBTreePartition partition;

        private Fixture(int rangeCount, String separation)
        {
            if (rangeCount < 2)
                throw new IllegalArgumentException("rangeCount must be at least two");

            this.rangeCount = rangeCount;
            this.separation = separation;
            this.metadata = TableMetadata.builder("deletion_info_bench", "transitions")
                                         .addPartitionKeyColumn("pk", Int32Type.instance)
                                         .addClusteringColumn("ck", Int32Type.instance)
                                         .partitioner(ByteOrderedPartitioner.instance)
                                         .offline()
                                         .build();
            ByteBuffer keyValue = integer(0);
            this.key = new BufferDecoratedKey(ByteOrderedPartitioner.instance.getToken(keyValue), keyValue);
            this.allocator = new HeapPool.Allocator(POOL);
            this.partition = new AtomicBTreePartition(TableMetadataRef.forOfflineTools(metadata), key, allocator);
        }

        public Transition[] transitions(Operation operation)
        {
            int gap = rangeCount * RANGE_STRIDE + RANGE_STRIDE;
            return new Transition[]{ buildTransition(operation, 0), buildTransition(operation, gap) };
        }

        public void prepare(Transition transition)
        {
            partition.unsafeSetHolder(transition.prefix);
        }

        public BTreePartitionData applyPrepared(Transition transition)
        {
            OpOrder.Group writeOp = opOrder.getCurrent();
            partition.addAll(transition.update, allocator.cloner(writeOp), writeOp, UpdateTransaction.NO_OP);
            return partition.unsafeGetHolder();
        }

        public DeletionInfo deletionInfo()
        {
            return partition.deletionInfo();
        }

        private Transition buildTransition(Operation operation, int offset)
        {
            MutableDeletionInfo prefixInfo = prefixInfo(offset);
            PartitionUpdate prefixUpdate = partitionUpdate(prefixInfo);
            partition.unsafeSetHolder(BTreePartitionData.unsafeGetEmpty());
            apply(prefixUpdate);
            BTreePartitionData prefix = partition.unsafeGetHolder();
            DeletionInfo published = partition.deletionInfo();

            MutableDeletionInfo updateInfo = updateInfo(operation, offset);
            MutableDeletionInfo expected = prefixInfo.mutableCopy();
            expected.add(updateInfo);
            return new Transition(offset,
                                  rangeCount,
                                  prefix,
                                  published,
                                  partitionUpdate(updateInfo),
                                  prefixInfo.mutableCopy(),
                                  expected);
        }

        private void apply(PartitionUpdate update)
        {
            OpOrder.Group writeOp = opOrder.getCurrent();
            partition.addAll(update, allocator.cloner(writeOp), writeOp, UpdateTransaction.NO_OP);
        }

        private MutableDeletionInfo prefixInfo(int offset)
        {
            MutableDeletionInfo info = MutableDeletionInfo.live();
            for (int i = 0; i < rangeCount; i++)
            {
                int start = offset + i * RANGE_STRIDE;
                info.add(range(start, start + 3, PREFIX_TIMESTAMP, PREFIX_LOCAL_DELETION_TIME), metadata.comparator);
            }
            return info;
        }

        private MutableDeletionInfo updateInfo(Operation operation, int offset)
        {
            switch (operation)
            {
                case PARTITION_DELETE:
                    return new MutableDeletionInfo(DeletionTime.build(UPDATE_TIMESTAMP, UPDATE_LOCAL_DELETION_TIME));
                case OVERLAP:
                {
                    int middle = rangeCount / 2;
                    int start = offset + middle * RANGE_STRIDE + 1;
                    int end = offset + (middle + 1) * RANGE_STRIDE + 2;
                    MutableDeletionInfo info = MutableDeletionInfo.live();
                    info.add(range(start, end, UPDATE_TIMESTAMP, UPDATE_LOCAL_DELETION_TIME), metadata.comparator);
                    return info;
                }
                case OUT_OF_ORDER:
                {
                    MutableDeletionInfo info = MutableDeletionInfo.live();
                    info.add(range(offset + 4, offset + 6, UPDATE_TIMESTAMP, UPDATE_LOCAL_DELETION_TIME), metadata.comparator);
                    return info;
                }
                case TWO_EXACT_BOUND:
                {
                    MutableDeletionInfo info = MutableDeletionInfo.live();
                    int first;
                    int second;
                    if ("ADJACENT".equals(separation))
                    {
                        first = rangeCount / 2;
                        second = first + 1;
                    }
                    else if ("OPPOSITE".equals(separation))
                    {
                        first = 0;
                        second = rangeCount - 1;
                    }
                    else
                    {
                        throw new IllegalArgumentException("Unknown separation " + separation);
                    }
                    addExactReplacement(info, offset, first);
                    addExactReplacement(info, offset, second);
                    return info;
                }
                default:
                    throw new AssertionError(operation);
            }
        }

        private void addExactReplacement(MutableDeletionInfo info, int offset, int range)
        {
            int start = offset + range * RANGE_STRIDE;
            info.add(DeletionInfoTransitionSupport.range(start, start + 3, UPDATE_TIMESTAMP, UPDATE_LOCAL_DELETION_TIME), metadata.comparator);
        }

        private PartitionUpdate partitionUpdate(MutableDeletionInfo deletionInfo)
        {
            BTreePartitionData holder = BTreePartitionData.unsafeConstruct(RegularAndStaticColumns.NONE,
                                                                            BTree.empty(),
                                                                            deletionInfo,
                                                                            Rows.EMPTY_STATIC_ROW,
                                                                            EncodingStats.NO_STATS);
            return PartitionUpdate.unsafeConstruct(metadata, key, holder, deletionInfo, false);
        }
    }

    public static final class Transition
    {
        private final int offset;
        private final int rangeCount;
        private final BTreePartitionData prefix;
        private final DeletionInfo published;
        private final PartitionUpdate update;
        private final MutableDeletionInfo expectedPrefix;
        private final MutableDeletionInfo expected;

        private Transition(int offset,
                           int rangeCount,
                           BTreePartitionData prefix,
                           DeletionInfo published,
                           PartitionUpdate update,
                           MutableDeletionInfo expectedPrefix,
                           MutableDeletionInfo expected)
        {
            this.offset = offset;
            this.rangeCount = rangeCount;
            this.prefix = prefix;
            this.published = published;
            this.update = update;
            this.expectedPrefix = expectedPrefix;
            this.expected = expected;
        }

        public int offset()
        {
            return offset;
        }

        public int rangeCount()
        {
            return rangeCount;
        }

        public DeletionInfo published()
        {
            return published;
        }

        public MutableDeletionInfo expectedPrefix()
        {
            return expectedPrefix;
        }

        public MutableDeletionInfo expected()
        {
            return expected;
        }
    }
}
