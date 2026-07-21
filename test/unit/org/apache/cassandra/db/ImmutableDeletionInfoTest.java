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
package org.apache.cassandra.db;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.partitions.AtomicBTreePartition;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.index.transactions.UpdateTransaction;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.btree.UpdateFunction;
import org.apache.cassandra.utils.concurrent.ImmediateFuture;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.memory.HeapCloner;
import org.apache.cassandra.utils.memory.HeapPool;
import org.apache.cassandra.utils.memory.MemtableAllocator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class ImmutableDeletionInfoTest
{
    private static final int RANGE_COUNT = 4_096;

    @BeforeClass
    public static void initializeDatabaseDescriptor()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Test
    public void usesBTreeForLargeAppendOnlyPartitions() throws Exception
    {
        TableMetadata metadata = metadata("immutable_ranges");
        HeapPool pool = new HeapPool(Long.MAX_VALUE, 1.0f, () -> ImmediateFuture.success(Boolean.TRUE));
        MemtableAllocator allocator = pool.newAllocator("immutable-ranges");
        AtomicBTreePartition partition = newPartition(metadata, allocator);
        OpOrder writeOrder = new OpOrder();
        try
        {
            for (int i = 0; i < RANGE_COUNT - 1; i++)
                merge(partition, rangeUpdate(metadata, i), writeOrder, UpdateTransaction.NO_OP);

            long beforeLastAppend = allocator.onHeap().owns();
            merge(partition, rangeUpdate(metadata, RANGE_COUNT - 1), writeOrder, UpdateTransaction.NO_OP);
            long lastAppendAllocation = allocator.onHeap().owns() - beforeLastAppend;
            assertTrue(lastAppendAllocation > 0L);

            DeletionInfo immutable = partition.deletionInfo();
            assertTrue(lastAppendAllocation < immutable.unsharedHeapSize());
            assertTrue(immutable instanceof ImmutableDeletionInfo);
            assertEquals(RANGE_COUNT, immutable.rangeCount());
            for (int i = 0; i < RANGE_COUNT; i++)
            {
                RangeTombstone range = immutable.rangeCovering(Clustering.make(ByteBufferUtil.bytes(i * 3)));
                assertNotNull(range);
                assertEquals(i + 1L, range.deletionTime().markedForDeleteAt());
            }

            MutableDeletionInfo mutable = immutable.mutableCopy();
            for (int i = 0; i < RANGE_COUNT; i++)
            {
                for (int value = i * 3; value <= i * 3 + 2; value++)
                    assertEquals(mutable.rangeCovering(Clustering.make(ByteBufferUtil.bytes(value))),
                                 immutable.rangeCovering(Clustering.make(ByteBufferUtil.bytes(value))));
            }

            Slice slice = Slice.make(BufferClusteringBound.inclusiveStartOf(ByteBufferUtil.bytes(3 * 1024 + 1)),
                                     BufferClusteringBound.inclusiveEndOf(ByteBufferUtil.bytes(3 * 2048)));
            List<RangeTombstone> forward = ranges(immutable.rangeIterator(slice, false));
            List<RangeTombstone> reverse = ranges(immutable.rangeIterator(slice, true));
            assertEquals(1025, forward.size());
            assertEquals(1025L, forward.get(0).deletionTime().markedForDeleteAt());
            assertEquals(2049L, forward.get(forward.size() - 1).deletionTime().markedForDeleteAt());
            Collections.reverse(reverse);
            assertEquals(forward, reverse);

            assertSliceMatches(mutable, immutable, Slice.ALL);
            assertSliceMatches(mutable, immutable,
                               Slice.make(BufferClusteringBound.exclusiveStartOf(ByteBufferUtil.bytes(3 * 1024)),
                                          BufferClusteringBound.exclusiveEndOf(ByteBufferUtil.bytes(3 * 2048 + 1))));
            assertSliceMatches(mutable, immutable,
                               Slice.make(BufferClusteringBound.inclusiveStartOf(ByteBufferUtil.bytes(3 * 1024 + 1)),
                                          BufferClusteringBound.inclusiveEndOf(ByteBufferUtil.bytes(3 * 2048))));
            assertSliceMatches(mutable, immutable, Slice.make(Clustering.make(ByteBufferUtil.bytes(3 * 1024))));
            assertSliceMatches(mutable, immutable, Slice.make(Clustering.make(ByteBufferUtil.bytes(3 * 1024 + 2))));

            assertEquals(immutable, mutable);
            assertEquals(mutable, immutable);
            assertEquals(immutable.hashCode(), mutable.hashCode());
            mutable.updateAllTimestamp(10_000L);
            assertAllTimestamps(mutable, 10_000L, 1);
            assertEquals(1L, immutable.rangeCovering(Clustering.make(ByteBufferUtil.bytes(0))).deletionTime().markedForDeleteAt());
            mutable.updateAllTimestampAndLocalDeletionTime(10_001L, 2_000L);
            assertAllTimestamps(mutable, 10_001L, 2_000);

            int overlapStart = 3 * 2048;
            PartitionUpdate overlap = new RowUpdateBuilder(metadata, 1, 20_000L, 0)
                                      .addRangeTombstone(overlapStart, overlapStart + 2)
                                      .buildUpdate();
            long beforeFallback = allocator.onHeap().owns();
            merge(partition, overlap, writeOrder, UpdateTransaction.NO_OP);
            DeletionInfo fallback = partition.deletionInfo();
            assertTrue(fallback instanceof MutableDeletionInfo);
            long fallbackAllocation = allocator.onHeap().owns() - beforeFallback;
            assertEquals(fallback.unsharedHeapSize() - immutable.unsharedHeapSize(), fallbackAllocation);
            assertEquals(20_000L, fallback.rangeCovering(Clustering.make(ByteBufferUtil.bytes(overlapStart))).deletionTime().markedForDeleteAt());
        }
        finally
        {
            allocator.setDiscarding();
            allocator.setDiscarded();
            pool.shutdownAndWait(1, TimeUnit.MINUTES);
        }
    }

    @Test
    public void preservesRangesAndDataSizeAcrossImmutablePublication() throws Exception
    {
        TableMetadata metadata = metadata("immutable_partition_delete");
        HeapPool pool = new HeapPool(Long.MAX_VALUE, 1.0f, () -> ImmediateFuture.success(Boolean.TRUE));
        MemtableAllocator allocator = pool.newAllocator("immutable-partition-delete");
        AtomicBTreePartition partition = newPartition(metadata, allocator);
        OpOrder writeOrder = new OpOrder();
        try
        {
            merge(partition, rangeUpdate(metadata, 0), writeOrder, UpdateTransaction.NO_OP);
            DeletionInfo oneRange = partition.deletionInfo();
            assertTrue(oneRange instanceof ImmutableDeletionInfo);
            assertDataSizeMatches(oneRange);

            PartitionUpdate twoRanges = new RowUpdateBuilder(metadata, 1, 2L, 0)
                                        .addRangeTombstone(3, 4)
                                        .addRangeTombstone(6, 7)
                                        .buildUpdate();
            merge(partition, twoRanges, writeOrder, UpdateTransaction.NO_OP);
            DeletionInfo rangesBeforePartitionDelete = partition.deletionInfo();
            assertTrue(rangesBeforePartitionDelete instanceof ImmutableDeletionInfo);
            assertDataSizeMatches(rangesBeforePartitionDelete);
            List<RangeTombstone> expectedRanges = ranges(rangesBeforePartitionDelete.rangeIterator(false));

            PartitionUpdate partitionDelete = PartitionUpdate.fullPartitionDelete(metadata,
                                                                                  metadata.partitioner.decorateKey(ByteBufferUtil.bytes(0)),
                                                                                  10_000L,
                                                                                  5);
            merge(partition, partitionDelete, writeOrder, UpdateTransaction.NO_OP);

            DeletionInfo withPartitionDelete = partition.deletionInfo();
            assertTrue(withPartitionDelete instanceof ImmutableDeletionInfo);
            assertEquals(10_000L, withPartitionDelete.getPartitionDeletion().markedForDeleteAt());
            assertEquals(expectedRanges, ranges(withPartitionDelete.rangeIterator(false)));
            long[] timestamps = { 1L, 2L, 2L };
            for (int i = 0; i < timestamps.length; i++)
            {
                RangeTombstone range = withPartitionDelete.rangeCovering(Clustering.make(ByteBufferUtil.bytes(i * 3)));
                assertNotNull(range);
                assertEquals(timestamps[i], range.deletionTime().markedForDeleteAt());
            }
            assertDataSizeMatches(withPartitionDelete);
        }
        finally
        {
            allocator.setDiscarding();
            allocator.setDiscarded();
            pool.shutdownAndWait(1, TimeUnit.MINUTES);
        }
    }

    @Test
    public void clonesCallerRangesAndReconcilesPartitionDeletion()
    {
        ClusteringComparator comparator = new ClusteringComparator(Int32Type.instance);
        ByteBuffer start = ByteBufferUtil.bytes(1);
        DeletionTime.ReusableDeletionTime sourceRangeDeletion = DeletionTime.ReusableDeletionTime.copy(DeletionTime.build(1L, 1L));
        MutableDeletionInfo source = singleRange(comparator, DeletionTime.LIVE, start, ByteBufferUtil.bytes(2), sourceRangeDeletion);
        DeletionTime.ReusableDeletionTime sourcePartitionDeletion = DeletionTime.ReusableDeletionTime.copy(DeletionTime.build(0L, 1L));
        ImmutableDeletionInfo immutable = ImmutableDeletionInfo.copyOf(source, sourcePartitionDeletion, UpdateFunction.noOp());
        start.putInt(0, 100);
        sourceRangeDeletion.resetLive();
        sourcePartitionDeletion.resetLive();
        RangeTombstone originalRange = immutable.rangeCovering(Clustering.make(ByteBufferUtil.bytes(1)));
        assertNotNull(originalRange);
        assertEquals(1L, originalRange.deletionTime().markedForDeleteAt());
        assertEquals(0L, immutable.getPartitionDeletion().markedForDeleteAt());
        assertNull(immutable.rangeCovering(Clustering.make(ByteBufferUtil.bytes(100))));

        MutableDeletionInfo materialized = MutableDeletionInfo.live();
        materialized.add(immutable);
        assertEquals(immutable, materialized);
        assertEquals(materialized, immutable);

        MutableDeletionInfo newerSource = singleRange(comparator,
                                                       DeletionTime.build(10L, 10L),
                                                       ByteBufferUtil.bytes(10),
                                                       ByteBufferUtil.bytes(11),
                                                       DeletionTime.build(10L, 10L));
        assertEquals(10L, ImmutableDeletionInfo.copyOf(newerSource, DeletionTime.build(9L, 9L), UpdateFunction.noOp())
                                                   .getPartitionDeletion().markedForDeleteAt());
        MutableDeletionInfo olderSource = singleRange(comparator,
                                                       DeletionTime.build(9L, 9L),
                                                       ByteBufferUtil.bytes(12),
                                                       ByteBufferUtil.bytes(13),
                                                       DeletionTime.build(9L, 9L));
        assertEquals(10L, ImmutableDeletionInfo.copyOf(olderSource, DeletionTime.build(10L, 10L), UpdateFunction.noOp())
                                                   .getPartitionDeletion().markedForDeleteAt());

        ByteBuffer appendedStart = ByteBufferUtil.bytes(4);
        DeletionTime.ReusableDeletionTime appendedRangeDeletion = DeletionTime.ReusableDeletionTime.copy(DeletionTime.build(2L, 2L));
        DeletionTime.ReusableDeletionTime appendedPartitionDeletion = DeletionTime.ReusableDeletionTime.copy(DeletionTime.build(3L, 3L));
        MutableDeletionInfo update = singleRange(comparator, appendedPartitionDeletion, appendedStart, ByteBufferUtil.bytes(5), appendedRangeDeletion);
        ImmutableDeletionInfo appended = immutable.tryAppend(update, UpdateFunction.noOp());
        assertNotNull(appended);
        appendedStart.putInt(0, 100);
        appendedRangeDeletion.resetLive();
        appendedPartitionDeletion.resetLive();
        RangeTombstone appendedRange = appended.rangeCovering(Clustering.make(ByteBufferUtil.bytes(4)));
        assertNotNull(appendedRange);
        assertEquals(2L, appendedRange.deletionTime().markedForDeleteAt());
        assertEquals(3L, appended.getPartitionDeletion().markedForDeleteAt());
        assertNull(appended.rangeCovering(Clustering.make(ByteBufferUtil.bytes(100))));

        MutableDeletionInfo incompatible = singleRange(new ClusteringComparator(Int32Type.instance, Int32Type.instance),
                                                        DeletionTime.build(10L, 10L),
                                                        ByteBufferUtil.bytes(6), ByteBufferUtil.bytes(7),
                                                        DeletionTime.build(1L, 1L));
        try
        {
            appended.tryAppend(incompatible, UpdateFunction.noOp());
            throw new AssertionError("Expected incompatible clustering comparator to be rejected");
        }
        catch (IllegalArgumentException expected)
        {
        }

        ImmutableDeletionInfo incompatibleImmutable = ImmutableDeletionInfo.copyOf(incompatible,
                                                                                     incompatible.getPartitionDeletion(),
                                                                                     UpdateFunction.noOp());
        MutableDeletionInfo mutable = singleRange(comparator, ByteBufferUtil.bytes(8), ByteBufferUtil.bytes(9));
        try
        {
            mutable.add(incompatibleImmutable);
            throw new AssertionError("Expected incompatible immutable deletion info to be rejected");
        }
        catch (IllegalArgumentException expected)
        {
        }
        assertTrue(mutable.getPartitionDeletion().isLive());
    }

    @Test
    public void doesNotMutateSnapshotWhenCasLoses() throws Exception
    {
        TableMetadata metadata = metadata("immutable_cas");
        HeapPool pool = new HeapPool(Long.MAX_VALUE, 1.0f, () -> ImmediateFuture.success(Boolean.TRUE));
        MemtableAllocator allocator = pool.newAllocator("immutable-cas");
        AtomicBTreePartition partition = newPartition(metadata, allocator);
        OpOrder writeOrder = new OpOrder();
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try
        {
            for (int i = 0; i < RANGE_COUNT; i++)
                merge(partition, rangeUpdate(metadata, i), writeOrder, UpdateTransaction.NO_OP);

            DeletionInfo snapshot = partition.deletionInfo();
            assertTrue(snapshot instanceof ImmutableDeletionInfo);
            int firstNewRange = RANGE_COUNT * 3;
            PartitionUpdate left = new RowUpdateBuilder(metadata, 1, 30_000L, 0)
                                   .addRangeTombstone(firstNewRange, firstNewRange + 1)
                                   .buildUpdate();
            PartitionUpdate right = new RowUpdateBuilder(metadata, 1, 30_001L, 0)
                                    .addRangeTombstone(firstNewRange + 3, firstNewRange + 4)
                                    .buildUpdate();
            CyclicBarrier beforeFirstCas = new CyclicBarrier(2);
            Future<?> leftFuture = executor.submit(() -> merge(partition, left, writeOrder, new FirstRangeBarrierTransaction(beforeFirstCas)));
            Future<?> rightFuture = executor.submit(() -> merge(partition, right, writeOrder, new FirstRangeBarrierTransaction(beforeFirstCas)));
            leftFuture.get();
            rightFuture.get();

            assertEquals(RANGE_COUNT, snapshot.rangeCount());
            assertNull(snapshot.rangeCovering(Clustering.make(ByteBufferUtil.bytes(firstNewRange))));
            assertNull(snapshot.rangeCovering(Clustering.make(ByteBufferUtil.bytes(firstNewRange + 3))));
            DeletionInfo current = partition.deletionInfo();
            assertEquals(RANGE_COUNT + 2, current.rangeCount());
            assertNotNull(current.rangeCovering(Clustering.make(ByteBufferUtil.bytes(firstNewRange))));
            assertNotNull(current.rangeCovering(Clustering.make(ByteBufferUtil.bytes(firstNewRange + 3))));
            assertTrue(allocator.onHeap().owns() > 0L);
        }
        finally
        {
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(1, TimeUnit.MINUTES));
            allocator.setDiscarding();
            allocator.setDiscarded();
            pool.shutdownAndWait(1, TimeUnit.MINUTES);
        }
    }

    private static TableMetadata metadata(String table)
    {
        return TableMetadata.builder("immutable_deletion_info", table)
                            .addPartitionKeyColumn("pk", Int32Type.instance)
                            .addClusteringColumn("ck", Int32Type.instance)
                            .build();
    }

    private static AtomicBTreePartition newPartition(TableMetadata metadata, MemtableAllocator allocator)
    {
        DecoratedKey key = metadata.partitioner.decorateKey(ByteBufferUtil.bytes(0));
        return new AtomicBTreePartition(TableMetadataRef.forOfflineTools(metadata), key, allocator);
    }

    private static PartitionUpdate rangeUpdate(TableMetadata metadata, int index)
    {
        return new RowUpdateBuilder(metadata, 1, index + 1L, 0)
               .addRangeTombstone(index * 3, index * 3 + 1)
               .buildUpdate();
    }

    private static MutableDeletionInfo singleRange(ClusteringComparator comparator, ByteBuffer start, ByteBuffer end)
    {
        return singleRange(comparator, DeletionTime.LIVE, start, end, DeletionTime.build(1L, 1L));
    }

    private static MutableDeletionInfo singleRange(ClusteringComparator comparator,
                                                   DeletionTime partitionDeletion,
                                                   ByteBuffer start,
                                                   ByteBuffer end,
                                                   DeletionTime rangeDeletion)
    {
        MutableDeletionInfo deletionInfo = new MutableDeletionInfo(partitionDeletion);
        deletionInfo.add(new RangeTombstone(Slice.make(BufferClusteringBound.inclusiveStartOf(start),
                                                        BufferClusteringBound.inclusiveEndOf(end)),
                                            rangeDeletion),
                         comparator);
        return deletionInfo;
    }

    private static void merge(AtomicBTreePartition partition, PartitionUpdate update, OpOrder writeOrder, UpdateTransaction transaction)
    {
        try (OpOrder.Group group = writeOrder.start())
        {
            partition.addAll(update, HeapCloner.instance, group, transaction);
        }
    }

    private static List<RangeTombstone> ranges(Iterator<RangeTombstone> iterator)
    {
        List<RangeTombstone> ranges = new ArrayList<>();
        iterator.forEachRemaining(ranges::add);
        return ranges;
    }

    private static void assertSliceMatches(DeletionInfo expected, DeletionInfo actual, Slice slice)
    {
        assertEquals(ranges(expected.rangeIterator(slice, false)), ranges(actual.rangeIterator(slice, false)));
        assertEquals(ranges(expected.rangeIterator(slice, true)), ranges(actual.rangeIterator(slice, true)));
    }

    private static void assertDataSizeMatches(DeletionInfo deletionInfo)
    {
        assertEquals(deletionInfo.mutableCopy().dataSize(), deletionInfo.dataSize());
    }

    private static void assertAllTimestamps(DeletionInfo deletionInfo, long timestamp, int localDeletionTime)
    {
        Iterator<RangeTombstone> iterator = deletionInfo.rangeIterator(false);
        while (iterator.hasNext())
        {
            DeletionTime deletion = iterator.next().deletionTime();
            assertEquals(timestamp, deletion.markedForDeleteAt());
            assertEquals(localDeletionTime, deletion.localDeletionTimeUnsignedInteger());
        }
    }

    private static class FirstRangeBarrierTransaction implements UpdateTransaction
    {
        private final CyclicBarrier barrier;
        private boolean waited;

        private FirstRangeBarrierTransaction(CyclicBarrier barrier)
        {
            this.barrier = barrier;
        }

        @Override
        public void start()
        {
        }

        @Override
        public void onPartitionDeletion(DeletionTime deletionTime)
        {
        }

        @Override
        public void onRangeTombstone(RangeTombstone rangeTombstone)
        {
            if (!waited)
            {
                waited = true;
                try
                {
                    barrier.await(1, TimeUnit.MINUTES);
                }
                catch (InterruptedException e)
                {
                    Thread.currentThread().interrupt();
                    throw new AssertionError(e);
                }
                catch (Exception e)
                {
                    throw new AssertionError(e);
                }
            }
        }

        @Override
        public void onInserted(org.apache.cassandra.db.rows.Row row)
        {
        }

        @Override
        public void onUpdated(org.apache.cassandra.db.rows.Row existing, org.apache.cassandra.db.rows.Row updated)
        {
        }

        @Override
        public void commit()
        {
        }
    }
}
