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
package org.apache.cassandra.db.partitions;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.BufferDecoratedKey;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionInfo;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.MutableDeletionInfo;
import org.apache.cassandra.db.RangeTombstone;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.Slice;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.dht.ByteOrderedPartitioner;
import org.apache.cassandra.index.transactions.UpdateTransaction;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.utils.btree.BTree;
import org.apache.cassandra.utils.concurrent.ImmediateFuture;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.memory.ByteBufferCloner;
import org.apache.cassandra.utils.memory.HeapCloner;
import org.apache.cassandra.utils.memory.HeapPool;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;

public class DeletionInfoTransitionTest
{
    private static final int PREFIX_SIZE = 128;
    private static final long PREFIX_TIMESTAMP = 100L;
    private static final long UPDATE_TIMESTAMP = 200L;
    private static final long LOCAL_DELETION_TIME = 1L;
    private static final HeapPool POOL = new HeapPool(Long.MAX_VALUE,
                                                       1.0f,
                                                       () -> ImmediateFuture.success(Boolean.TRUE));
    private static final OpOrder ORDER = new OpOrder();

    private static TableMetadata metadata;
    private static DecoratedKey partitionKey;

    @BeforeClass
    public static void beforeClass()
    {
        DatabaseDescriptor.daemonInitialization();
        metadata = TableMetadata.builder("deletion_info_test", "transitions")
                                .addPartitionKeyColumn("pk", Int32Type.instance)
                                .addClusteringColumn("ck", Int32Type.instance)
                                .partitioner(ByteOrderedPartitioner.instance)
                                .build();

        ByteBuffer key = Int32Type.instance.decompose(0);
        partitionKey = new BufferDecoratedKey(ByteOrderedPartitioner.instance.getToken(key), key);
    }

    @Test
    public void exactBoundAdjacentAndOppositeTransitionsKeepPublishedSnapshot()
    {
        MutableDeletionInfo prefix = prefix(PREFIX_SIZE);
        for (boolean opposite : new boolean[]{ false, true })
        {
            Fixture fixture = fixture();
            apply(fixture, prefix);

            DeletionInfo published = fixture.partition.deletionInfo();
            List<RangeTombstone> publishedRanges = ranges(published, false);
            MutableDeletionInfo update = exactBoundUpdate(PREFIX_SIZE, opposite);
            DeletionInfo expected = mergeExpected(prefix, update);

            apply(fixture, update);

            assertDeletionEquals(expected, fixture.partition.deletionInfo());
            assertEquals(publishedRanges, ranges(published, false));
            assertTrue(fixture.allocator.onHeap().owns() >= 0L);
        }
    }

    @Test
    public void overlapAndOutOfOrderTransitionsUseCanonicalReconciliation()
    {
        MutableDeletionInfo prefix = prefix(PREFIX_SIZE);
        assertTransition(prefix, overlappingUpdate(PREFIX_SIZE));
        assertTransition(prefix, outOfOrderUpdate(PREFIX_SIZE));
    }

    @Test
    public void partitionDeletionAndBoundedSlicesMatchCanonicalState()
    {
        MutableDeletionInfo prefix = prefix(PREFIX_SIZE);
        Fixture fixture = fixture();
        apply(fixture, prefix);

        DeletionInfo actual = fixture.partition.deletionInfo();
        Slice[] slices = new Slice[]{
            Slice.ALL,
            sliceForRangeIndexes(0, 1),
            sliceForRangeIndexes(PREFIX_SIZE / 2, PREFIX_SIZE / 2 + 1),
            sliceForRangeIndexes(PREFIX_SIZE - 2, PREFIX_SIZE - 1),
            slice(rangeEnd(PREFIX_SIZE - 1) + 2, rangeEnd(PREFIX_SIZE - 1) + 8)
        };
        for (Slice slice : slices)
        {
            assertSliceEquals(prefix, actual, slice, false);
            assertSliceEquals(prefix, actual, slice, true);
        }

        MutableDeletionInfo partitionDelete = new MutableDeletionInfo(deletionTime(UPDATE_TIMESTAMP));
        DeletionInfo expected = mergeExpected(prefix, partitionDelete);
        apply(fixture, partitionDelete);

        assertDeletionEquals(expected, fixture.partition.deletionInfo());
        assertTrue(fixture.allocator.onHeap().owns() >= 0L);
    }

    @Test
    public void cloneMaterializesEveryRangeBoundary()
    {
        Fixture fixture = fixture();
        apply(fixture, prefix(PREFIX_SIZE));

        DeletionInfo actual = fixture.partition.deletionInfo();
        CountingCloner cloner = new CountingCloner();
        DeletionInfo cloned = actual.clone(cloner);

        assertDeletionEquals(actual, cloned);
        assertEquals(actual.rangeCount() * 2, cloner.allocations);

        RangeTombstone source = actual.rangeIterator(false).next();
        RangeTombstone copy = cloned.rangeIterator(false).next();
        assertNotSame(source.deletedSlice().start().get(0), copy.deletedSlice().start().get(0));
    }

    private static void assertTransition(MutableDeletionInfo prefix, MutableDeletionInfo update)
    {
        Fixture fixture = fixture();
        apply(fixture, prefix);
        DeletionInfo expected = mergeExpected(prefix, update);

        apply(fixture, update);

        assertDeletionEquals(expected, fixture.partition.deletionInfo());
        assertTrue(fixture.allocator.onHeap().owns() >= 0L);
    }

    private static void assertDeletionEquals(DeletionInfo expected, DeletionInfo actual)
    {
        assertEquals(expected.getPartitionDeletion(), actual.getPartitionDeletion());
        assertEquals(expected.rangeCount(), actual.rangeCount());
        assertEquals(ranges(expected, false), ranges(actual, false));

        int[] probes = { rangeStart(0), rangeEnd(0) + 2,
                         rangeStart(PREFIX_SIZE / 2), rangeEnd(PREFIX_SIZE - 1) + 2 };
        for (int i : probes)
            assertEquals(expected.rangeCovering(clustering(i)), actual.rangeCovering(clustering(i)));
    }

    private static void assertSliceEquals(DeletionInfo expected, DeletionInfo actual, Slice slice, boolean reversed)
    {
        assertEquals(ranges(expected, slice, reversed), ranges(actual, slice, reversed));
    }

    private static Fixture fixture()
    {
        HeapPool.Allocator allocator = new HeapPool.Allocator(POOL);
        AtomicBTreePartition partition = new AtomicBTreePartition(TableMetadataRef.forOfflineTools(metadata),
                                                                      partitionKey,
                                                                      allocator);
        return new Fixture(partition, allocator);
    }

    private static void apply(Fixture fixture, MutableDeletionInfo deletionInfo)
    {
        try (OpOrder.Group writeOp = ORDER.start())
        {
            fixture.partition.addAll(update(deletionInfo), HeapCloner.instance, writeOp, UpdateTransaction.NO_OP);
        }
    }

    private static PartitionUpdate update(MutableDeletionInfo deletionInfo)
    {
        BTreePartitionData holder = BTreePartitionData.unsafeConstruct(RegularAndStaticColumns.NONE,
                                                                         BTree.empty(),
                                                                         deletionInfo,
                                                                         Rows.EMPTY_STATIC_ROW,
                                                                         EncodingStats.NO_STATS);
        return PartitionUpdate.unsafeConstruct(metadata, partitionKey, holder, deletionInfo, false);
    }

    private static MutableDeletionInfo prefix(int count)
    {
        MutableDeletionInfo deletionInfo = new MutableDeletionInfo(DeletionTime.LIVE);
        for (int i = 0; i < count; i++)
            deletionInfo.add(range(i, PREFIX_TIMESTAMP), metadata.comparator);
        return deletionInfo;
    }

    private static MutableDeletionInfo exactBoundUpdate(int count, boolean opposite)
    {
        MutableDeletionInfo deletionInfo = new MutableDeletionInfo(DeletionTime.LIVE);
        int first = opposite ? 0 : count / 2;
        int second = opposite ? count - 1 : first + 1;
        deletionInfo.add(range(first, UPDATE_TIMESTAMP), metadata.comparator);
        deletionInfo.add(range(second, UPDATE_TIMESTAMP), metadata.comparator);
        return deletionInfo;
    }

    private static MutableDeletionInfo overlappingUpdate(int count)
    {
        int middle = count / 2;
        MutableDeletionInfo deletionInfo = new MutableDeletionInfo(DeletionTime.LIVE);
        deletionInfo.add(range(rangeStart(middle) - 1,
                               rangeEnd(middle + 2) + 1,
                               UPDATE_TIMESTAMP), metadata.comparator);
        return deletionInfo;
    }

    private static MutableDeletionInfo outOfOrderUpdate(int count)
    {
        MutableDeletionInfo deletionInfo = new MutableDeletionInfo(DeletionTime.LIVE);
        deletionInfo.add(range(count * 3 / 4, UPDATE_TIMESTAMP), metadata.comparator);
        deletionInfo.add(range(count / 4, UPDATE_TIMESTAMP + 1), metadata.comparator);
        return deletionInfo;
    }

    private static DeletionInfo mergeExpected(MutableDeletionInfo prefix, MutableDeletionInfo update)
    {
        return prefix.mutableCopy().add(update.clone(HeapCloner.instance));
    }

    private static RangeTombstone range(int index, long timestamp)
    {
        return range(rangeStart(index), rangeEnd(index), timestamp);
    }

    private static RangeTombstone range(int start, int end, long timestamp)
    {
        return new RangeTombstone(slice(start, end), deletionTime(timestamp));
    }

    private static DeletionTime deletionTime(long timestamp)
    {
        return DeletionTime.build(timestamp, LOCAL_DELETION_TIME);
    }

    private static Slice sliceForRangeIndexes(int first, int last)
    {
        return slice(rangeStart(first), rangeEnd(last));
    }

    private static Slice slice(int start, int end)
    {
        return Slice.make(clustering(start), clustering(end));
    }

    private static Clustering<?> clustering(int value)
    {
        return metadata.comparator.make(value);
    }

    private static int rangeStart(int index)
    {
        return index * 4;
    }

    private static int rangeEnd(int index)
    {
        return rangeStart(index) + 1;
    }

    private static List<RangeTombstone> ranges(DeletionInfo deletionInfo, boolean reversed)
    {
        List<RangeTombstone> ranges = new ArrayList<>();
        Iterator<RangeTombstone> iterator = deletionInfo.rangeIterator(reversed);
        iterator.forEachRemaining(ranges::add);
        return ranges;
    }

    private static List<RangeTombstone> ranges(DeletionInfo deletionInfo, Slice slice, boolean reversed)
    {
        List<RangeTombstone> ranges = new ArrayList<>();
        Iterator<RangeTombstone> iterator = deletionInfo.rangeIterator(slice, reversed);
        iterator.forEachRemaining(ranges::add);
        return ranges;
    }

    private static final class Fixture
    {
        final AtomicBTreePartition partition;
        final HeapPool.Allocator allocator;

        private Fixture(AtomicBTreePartition partition, HeapPool.Allocator allocator)
        {
            this.partition = partition;
            this.allocator = allocator;
        }
    }

    private static final class CountingCloner extends ByteBufferCloner
    {
        int allocations;

        @Override
        public ByteBuffer allocate(int size)
        {
            allocations++;
            return ByteBuffer.allocate(size);
        }
    }
}
