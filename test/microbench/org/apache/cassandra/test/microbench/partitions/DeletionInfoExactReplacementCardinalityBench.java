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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.cassandra.test.microbench.partitions;

import java.nio.ByteBuffer;
import java.util.Iterator;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.BufferClusteringBound;
import org.apache.cassandra.db.BufferDecoratedKey;
import org.apache.cassandra.db.DeletionInfo;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.MutableDeletionInfo;
import org.apache.cassandra.db.RangeTombstone;
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
import org.apache.cassandra.utils.memory.HeapPool;
import org.apache.cassandra.utils.memory.MemtableAllocator;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * Sweeps the cardinality of exact range replacements against immutable deletion prefixes.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(NANOSECONDS)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 1, time = 1)
@Fork(1)
@Threads(1)
public class DeletionInfoExactReplacementCardinalityBench
{
    private static final int RANGE_STRIDE = 8;
    private static final long PREFIX_TIMESTAMP = 100;
    private static final long UPDATE_TIMESTAMP = 101;
    private static final long PREFIX_LOCAL_DELETION_TIME = 10;
    private static final long UPDATE_LOCAL_DELETION_TIME = 20;
    private static final HeapPool POOL = new HeapPool(Long.MAX_VALUE, 1.0f, () -> ImmediateFuture.success(Boolean.TRUE));

    static
    {
        DatabaseDescriptor.daemonInitialization();
        DatabaseDescriptor.setPartitionerUnsafe(ByteOrderedPartitioner.instance);
    }

    @Benchmark
    public BTreePartitionData exactReplacements(ExactReplacementState state)
    {
        return state.apply();
    }

    @State(Scope.Benchmark)
    public static class ExactReplacementState
    {
        @Param({ "128", "512", "4096" })
        public int prefixRanges;

        @Param({ "1", "2", "32" })
        public int updateRanges;

        private TableMetadata metadata;
        private BufferDecoratedKey key;
        private MemtableAllocator allocator;
        private AtomicBTreePartition partition;
        private final OpOrder opOrder = new OpOrder();
        private BTreePartitionData[] prefixes;
        private PartitionUpdate[] updates;
        private int next;
        private int current;

        @Setup(Level.Trial)
        public void setup()
        {
            if (updateRanges > prefixRanges)
                throw new IllegalArgumentException("updateRanges must not exceed prefixRanges");

            metadata = TableMetadata.builder("deletion_info_bench", "exact_replacement_cardinality")
                                    .addPartitionKeyColumn("pk", Int32Type.instance)
                                    .addClusteringColumn("ck", Int32Type.instance)
                                    .partitioner(ByteOrderedPartitioner.instance)
                                    .offline()
                                    .build();
            ByteBuffer keyValue = integer(0);
            key = new BufferDecoratedKey(ByteOrderedPartitioner.instance.getToken(keyValue), keyValue);
            allocator = new HeapPool.Allocator(POOL);
            partition = new AtomicBTreePartition(TableMetadataRef.forOfflineTools(metadata), key, allocator);
            prefixes = new BTreePartitionData[2];
            updates = new PartitionUpdate[2];

            int gap = prefixRanges * RANGE_STRIDE + RANGE_STRIDE;
            for (int i = 0; i < prefixes.length; i++)
            {
                int offset = i * gap;
                MutableDeletionInfo prefix = ranges(offset, prefixRanges, PREFIX_TIMESTAMP, PREFIX_LOCAL_DELETION_TIME);
                partition.unsafeSetHolder(BTreePartitionData.unsafeGetEmpty());
                apply(partitionUpdate(prefix));
                prefixes[i] = partition.unsafeGetHolder();

                MutableDeletionInfo update = exactReplacements(offset);
                MutableDeletionInfo expected = prefix.mutableCopy();
                expected.add(update);
                updates[i] = partitionUpdate(update);
                partition.unsafeSetHolder(prefixes[i]);
                apply(updates[i]);
                assertEquivalent(expected, partition.deletionInfo());
            }
            next = 0;
        }

        @Setup(Level.Invocation)
        public void reset()
        {
            current = next++ & 1;
            partition.unsafeSetHolder(prefixes[current]);
        }

        BTreePartitionData apply()
        {
            apply(updates[current]);
            return partition.unsafeGetHolder();
        }

        private void apply(PartitionUpdate update)
        {
            OpOrder.Group writeOp = opOrder.getCurrent();
            partition.addAll(update, allocator.cloner(writeOp), writeOp, UpdateTransaction.NO_OP);
        }

        private MutableDeletionInfo exactReplacements(int offset)
        {
            MutableDeletionInfo update = MutableDeletionInfo.live();
            for (int i = 0; i < updateRanges; i++)
            {
                int range = updateRanges == 1 ? prefixRanges / 2 : i * (prefixRanges - 1) / (updateRanges - 1);
                int start = offset + range * RANGE_STRIDE;
                update.add(range(start, start + 3, UPDATE_TIMESTAMP, UPDATE_LOCAL_DELETION_TIME), metadata.comparator);
            }
            return update;
        }

        private MutableDeletionInfo ranges(int offset, int count, long timestamp, long localDeletionTime)
        {
            MutableDeletionInfo ranges = MutableDeletionInfo.live();
            for (int i = 0; i < count; i++)
            {
                int start = offset + i * RANGE_STRIDE;
                ranges.add(range(start, start + 3, timestamp, localDeletionTime), metadata.comparator);
            }
            return ranges;
        }

        private PartitionUpdate partitionUpdate(MutableDeletionInfo deletionInfo)
        {
            BTreePartitionData holder = BTreePartitionData.unsafeConstruct(metadata.regularAndStaticColumns(),
                                                                             BTree.empty(),
                                                                             deletionInfo,
                                                                             Rows.EMPTY_STATIC_ROW,
                                                                             EncodingStats.NO_STATS);
            return PartitionUpdate.unsafeConstruct(metadata, key, holder, deletionInfo, false);
        }
    }

    private static RangeTombstone range(int start, int end, long timestamp, long localDeletionTime)
    {
        return new RangeTombstone(Slice.make(BufferClusteringBound.inclusiveStartOf(integer(start)),
                                             BufferClusteringBound.inclusiveEndOf(integer(end))),
                                  DeletionTime.build(timestamp, localDeletionTime));
    }

    private static ByteBuffer integer(int value)
    {
        return Int32Type.instance.decompose(value);
    }

    private static void assertEquivalent(DeletionInfo expected, DeletionInfo actual)
    {
        if (expected.rangeCount() != actual.rangeCount())
            throw new AssertionError("range count differs");

        Iterator<RangeTombstone> expectedRanges = expected.rangeIterator(false);
        Iterator<RangeTombstone> actualRanges = actual.rangeIterator(false);
        while (expectedRanges.hasNext() && actualRanges.hasNext())
        {
            RangeTombstone left = expectedRanges.next();
            RangeTombstone right = actualRanges.next();
            if (!left.deletedSlice().equals(right.deletedSlice()) || !left.deletionTime().equals(right.deletionTime()))
                throw new AssertionError("range differs");
        }
        if (expectedRanges.hasNext() || actualRanges.hasNext())
            throw new AssertionError("iterator length differs");
    }
}
