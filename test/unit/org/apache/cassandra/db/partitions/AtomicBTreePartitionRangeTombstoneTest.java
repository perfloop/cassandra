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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DeletionInfo;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.MutableDeletionInfo;
import org.apache.cassandra.db.RangeTombstone;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.Slice;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.index.transactions.UpdateTransaction;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.utils.concurrent.ImmediateFuture;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.memory.Cloner;
import org.apache.cassandra.utils.memory.HeapPool;
import org.apache.cassandra.utils.memory.MemtableAllocator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class AtomicBTreePartitionRangeTombstoneTest
{
    private static final int RANGE_COUNT = 4096;
    private static final int RANGE_WIDTH = 4;
    private static final OpOrder ORDER = new OpOrder();
    private static final HeapPool POOL = new HeapPool(Long.MAX_VALUE, 1.0f, () -> ImmediateFuture.success(Boolean.TRUE));

    private static TableMetadata metadata;
    private static org.apache.cassandra.db.DecoratedKey key;
    private static PartitionUpdate[] appendUpdates;

    @BeforeClass
    public static void beforeClass()
    {
        DatabaseDescriptor.daemonInitialization();
        metadata = TableMetadata.builder("range_tombstone_test", "updates")
                                .addPartitionKeyColumn("pk", Int32Type.instance)
                                .addClusteringColumn("ck", Int32Type.instance)
                                .partitioner(DatabaseDescriptor.getPartitioner())
                                .build();
        key = metadata.partitioner.decorateKey(Int32Type.instance.decompose(0));
        appendUpdates = new PartitionUpdate[RANGE_COUNT];
        for (int i = 0; i < RANGE_COUNT; i++)
            appendUpdates[i] = rangeUpdate(i, i + 1L);
    }

    @Test
    public void testAppendRangesPreserveSemanticsAndAccounting()
    {
        MemtableAllocator allocator = new HeapPool.Allocator(POOL);
        AtomicBTreePartition partition = newPartition(allocator);
        for (PartitionUpdate update : appendUpdates)
            add(partition, allocator, update, UpdateTransaction.NO_OP);

        DeletionInfo info = partition.deletionInfo();
        assertEquals(RANGE_COUNT, info.rangeCount());
        for (int i = 0; i < RANGE_COUNT; i++)
        {
            assertRange(info, i, i + 1L);
            assertNull(info.rangeCovering(clustering(i * RANGE_WIDTH + 2)));
        }

        Slice slice = Slice.make(clustering(8 * RANGE_WIDTH), clustering(12 * RANGE_WIDTH + 1));
        assertRangeOrder(collect(info.rangeIterator(slice, false)), 8, 9, 10, 11, 12);
        assertRangeOrder(collect(info.rangeIterator(slice, true)), 12, 11, 10, 9, 8);

        MutableDeletionInfo canonical = canonical(RANGE_COUNT);
        assertEquals(canonical, info);
        assertEquals(info, canonical);
        assertEquals(canonical.hashCode(), info.hashCode());
        assertEquals(canonical.dataSize(), info.dataSize());

        MutableDeletionInfo copy = info.mutableCopy();
        copy.updateAllTimestamp(RANGE_COUNT + 100L);
        assertRange(info, 0, 1L);
        assertRange(copy, 0, RANGE_COUNT + 100L);

        PartitionUpdate partitionDelete = PartitionUpdate.fullPartitionDelete(metadata, key, RANGE_COUNT + 200L, RANGE_COUNT + 200L);
        add(partition, allocator, partitionDelete, UpdateTransaction.NO_OP);
        info = partition.deletionInfo();
        canonical.add(partitionDelete.deletionInfo());
        assertEquals(RANGE_COUNT + 200L, info.getPartitionDeletion().markedForDeleteAt());
        assertEquals(RANGE_COUNT, info.rangeCount());
        assertEquals(canonical, info);
        assertEquals(canonical.dataSize(), info.dataSize());

        BTreePartitionData holder = partition.unsafeGetHolder();
        long expected = holder.deletionInfo.unsharedHeapSize() - BTreePartitionData.EMPTY.deletionInfo.unsharedHeapSize()
                        + holder.columns.unsharedHeapSize() - BTreePartitionData.EMPTY.columns.unsharedHeapSize()
                        + holder.stats.unsharedHeapSize() - EncodingStats.NO_STATS.unsharedHeapSize();
        assertEquals(expected, allocator.onHeap().owns());
    }

    @Test
    public void testOverlappingRangeUsesCanonicalReconciliation()
    {
        MemtableAllocator allocator = new HeapPool.Allocator(POOL);
        AtomicBTreePartition partition = newPartition(allocator);
        final int prefix = 32;
        for (int i = 0; i < prefix; i++)
            add(partition, allocator, appendUpdates[i], UpdateTransaction.NO_OP);

        PartitionUpdate overlap = rangeUpdate(16, 10_000L);
        add(partition, allocator, overlap, UpdateTransaction.NO_OP);

        MutableDeletionInfo expected = canonical(prefix);
        expected.add(overlap.deletionInfo());
        assertEquals(expected, partition.deletionInfo());
        assertEquals(expected.hashCode(), partition.deletionInfo().hashCode());
        assertRange(partition.deletionInfo(), 16, 10_000L);
    }

    @Test
    public void testCasLoserDoesNotMutatePublishedDeletionSnapshot() throws Exception
    {
        MemtableAllocator allocator = new HeapPool.Allocator(POOL);
        AtomicBTreePartition partition = newPartition(allocator);
        PartitionUpdate delayed = rangeUpdate(0, 1L);
        PartitionUpdate winner = rangeUpdate(1, 2L);
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicBoolean block = new AtomicBoolean(true);
        AtomicReference<Throwable> failure = new AtomicReference<>();

        UpdateTransaction blocker = new UpdateTransaction()
        {
            public void start() {}
            public void onPartitionDeletion(DeletionTime deletionTime) {}
            public void onRangeTombstone(RangeTombstone rangeTombstone)
            {
                if (block.compareAndSet(true, false))
                {
                    entered.countDown();
                    try
                    {
                        if (!release.await(30, TimeUnit.SECONDS))
                            throw new AssertionError("timed out waiting to release delayed merge");
                    }
                    catch (InterruptedException e)
                    {
                        Thread.currentThread().interrupt();
                        throw new AssertionError(e);
                    }
                }
            }
            public void onInserted(org.apache.cassandra.db.rows.Row row) {}
            public void onUpdated(org.apache.cassandra.db.rows.Row existing, org.apache.cassandra.db.rows.Row updated) {}
            public void commit() {}
        };

        Thread loser = new Thread(() -> {
            try
            {
                add(partition, allocator, delayed, blocker);
            }
            catch (Throwable t)
            {
                failure.set(t);
            }
        }, "range-tombstone-cas-loser");
        loser.start();
        try
        {
            assertTrue("delayed merge did not reach the range-tombstone callback", entered.await(30, TimeUnit.SECONDS));
            add(partition, allocator, winner, UpdateTransaction.NO_OP);
            DeletionInfo snapshot = partition.unsafeGetHolder().deletionInfo;
            assertRange(snapshot, 1, 2L);
            assertNull(snapshot.rangeCovering(clustering(0)));

            release.countDown();
            loser.join(TimeUnit.SECONDS.toMillis(30));
            assertFalse("delayed merge did not finish", loser.isAlive());
            if (failure.get() != null)
                throw new AssertionError("delayed merge failed", failure.get());

            assertRange(partition.deletionInfo(), 0, 1L);
            assertRange(partition.deletionInfo(), 1, 2L);
            assertRange(snapshot, 1, 2L);
            assertNull(snapshot.rangeCovering(clustering(0)));
        }
        finally
        {
            release.countDown();
            loser.join(TimeUnit.SECONDS.toMillis(30));
        }
    }

    private static AtomicBTreePartition newPartition(MemtableAllocator allocator)
    {
        return new AtomicBTreePartition(TableMetadataRef.forOfflineTools(metadata), key, allocator);
    }

    private static void add(AtomicBTreePartition partition, MemtableAllocator allocator, PartitionUpdate update, UpdateTransaction indexer)
    {
        OpOrder.Group writeOp = ORDER.getCurrent();
        Cloner cloner = allocator.cloner(writeOp);
        partition.addAll(update, cloner, writeOp, indexer);
    }

    private static MutableDeletionInfo canonical(int count)
    {
        MutableDeletionInfo result = MutableDeletionInfo.live();
        for (int i = 0; i < count; i++)
            result.add(range(i, i + 1L), metadata.comparator);
        return result;
    }

    private static PartitionUpdate rangeUpdate(int index, long timestamp)
    {
        PartitionUpdate.Builder builder = new PartitionUpdate.Builder(metadata, key, RegularAndStaticColumns.NONE, 0, false);
        builder.add(range(index, timestamp));
        return builder.build();
    }

    private static RangeTombstone range(int index, long timestamp)
    {
        return new RangeTombstone(Slice.make(clustering(index * RANGE_WIDTH), clustering(index * RANGE_WIDTH + 1)),
                                  DeletionTime.build(timestamp, timestamp));
    }

    private static Clustering<ByteBuffer> clustering(int value)
    {
        return Clustering.make(Int32Type.instance.decompose(value));
    }

    private static void assertRange(DeletionInfo info, int index, long timestamp)
    {
        RangeTombstone tombstone = info.rangeCovering(clustering(index * RANGE_WIDTH));
        assertNotNull("missing range " + index, tombstone);
        assertEquals(timestamp, tombstone.deletionTime().markedForDeleteAt());
    }

    private static List<RangeTombstone> collect(Iterator<RangeTombstone> iterator)
    {
        List<RangeTombstone> result = new ArrayList<>();
        iterator.forEachRemaining(result::add);
        return result;
    }

    private static void assertRangeOrder(List<RangeTombstone> ranges, int... indexes)
    {
        assertEquals(indexes.length, ranges.size());
        for (int i = 0; i < indexes.length; i++)
            assertEquals(range(indexes[i], indexes[i] + 1L), ranges.get(i));
    }
}
