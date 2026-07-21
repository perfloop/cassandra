/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.db.partitions;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

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
import org.apache.cassandra.db.Slice;
import org.apache.cassandra.db.marshal.Int32Type;
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class AtomicBTreePartitionRangeTombstoneTest
{
    private static final int RANGE_COUNT = 4096;
    private static final OpOrder NO_ORDER = new OpOrder();
    private static final HeapPool POOL = new HeapPool(Long.MAX_VALUE, 1.0f, () -> ImmediateFuture.success(Boolean.TRUE));

    private static TableMetadata metadata;
    private static TableMetadataRef metadataRef;
    private static DecoratedKey partitionKey;

    @BeforeClass
    public static void setup()
    {
        DatabaseDescriptor.daemonInitialization();
        DatabaseDescriptor.setPartitionerUnsafe(ByteOrderedPartitioner.instance);
        metadata = TableMetadata.builder("range_tombstone_test", "atomic_partition")
                                .addPartitionKeyColumn("pk", Int32Type.instance)
                                .addClusteringColumn("ck", Int32Type.instance)
                                .build();
        metadataRef = TableMetadataRef.forOfflineTools(metadata);
        partitionKey = new BufferDecoratedKey(ByteOrderedPartitioner.instance.getToken(Int32Type.instance.decompose(0)),
                                              Int32Type.instance.decompose(0));
    }

    @Test
    public void appendPathPreservesLookupIterationCopyAndAccounting()
    {
        State state = new State();
        try
        {
            for (int i = 0; i < RANGE_COUNT; i++)
                state.apply(rangeUpdate(i, i + 1L), UpdateTransaction.NO_OP);

            DeletionInfo published = state.partition.deletionInfo();
            assertEquals(RANGE_COUNT, published.rangeCount());
            assertRangeOnlyAccounting(state);

            for (int i : new int[] { 0, 1, 123, RANGE_COUNT - 1 })
            {
                RangeTombstone covered = published.rangeCovering(clustering(i * 4 + 1));
                assertNotNull(covered);
                assertEquals(i + 1L, covered.deletionTime().markedForDeleteAt());
                assertNull(published.rangeCovering(clustering(i * 4 + 3)));
            }

            Slice slice = Slice.make(clustering(400), clustering(798));
            assertSliceStarts(published.rangeIterator(slice, false), 100, 199, false);
            assertSliceStarts(published.rangeIterator(slice, true), 100, 199, true);

            MutableDeletionInfo mutableCopy = published.mutableCopy();
            assertEquals(published, mutableCopy);
            assertEquals(mutableCopy, published);
            assertEquals(published.hashCode(), mutableCopy.hashCode());
            assertEquals(published.dataSize(), mutableCopy.dataSize());

            mutableCopy.updateAllTimestampAndLocalDeletionTime(10_000, 10_001);
            assertEquals(1L, published.rangeIterator(false).next().deletionTime().markedForDeleteAt());
            assertEquals(10_000L, mutableCopy.rangeIterator(false).next().deletionTime().markedForDeleteAt());

            state.apply(partitionDelete(RANGE_COUNT + 1L), UpdateTransaction.NO_OP);
            DeletionInfo withPartitionDelete = state.partition.deletionInfo();
            assertEquals(RANGE_COUNT + 1L, withPartitionDelete.getPartitionDeletion().markedForDeleteAt());
            assertEquals(RANGE_COUNT, withPartitionDelete.rangeCount());
            assertRangeOnlyAccounting(state);
        }
        finally
        {
            state.close();
        }
    }

    @Test
    public void overlappingAndMultiRangeUpdatesUseCanonicalFallback()
    {
        State state = new State();
        try
        {
            List<PartitionUpdate> updates = new ArrayList<>();
            updates.add(rangeUpdate(0, 1));
            updates.add(rangeUpdate(2, 2));
            updates.add(rangeUpdate(1, 3));
            updates.add(multiRangeUpdate(4, 5, 4));

            MutableDeletionInfo expected = MutableDeletionInfo.live();
            for (PartitionUpdate update : updates)
            {
                state.apply(update, UpdateTransaction.NO_OP);
                expected = (MutableDeletionInfo) expected.mutableCopy().add(update.deletionInfo().clone(HeapCloner.instance));
            }

            DeletionInfo actual = state.partition.deletionInfo();
            assertEquals(expected, actual);
            assertEquals(actual, expected);
            assertEquals(expected.hashCode(), actual.hashCode());
            assertEquals(expected.dataSize(), actual.dataSize());
        }
        finally
        {
            state.close();
        }
    }

    @Test
    public void casLoserCannotMutatePublishedDeletionSnapshot() throws Exception
    {
        State state = new State();
        try
        {
            CountDownLatch loserEnteredMerge = new CountDownLatch(1);
            CountDownLatch releaseLoser = new CountDownLatch(1);
            AtomicBoolean blockOnce = new AtomicBoolean(true);
            AtomicReference<Throwable> failure = new AtomicReference<>();
            UpdateTransaction blocker = new BlockingUpdateTransaction(loserEnteredMerge, releaseLoser, blockOnce);

            Thread loser = new Thread(() -> {
                try
                {
                    state.apply(rangeUpdate(0, 10), blocker);
                }
                catch (Throwable t)
                {
                    failure.set(t);
                }
            }, "range-tombstone-cas-loser");
            loser.start();
            loserEnteredMerge.await();

            state.apply(rangeUpdate(1, 20), UpdateTransaction.NO_OP);
            DeletionInfo publishedWinner = state.partition.deletionInfo();
            assertEquals(1, publishedWinner.rangeCount());
            assertRange(publishedWinner.rangeIterator(false).next(), 4, 6, 20);

            releaseLoser.countDown();
            loser.join();
            if (failure.get() != null)
                throw new AssertionError("losing writer failed", failure.get());

            DeletionInfo finalInfo = state.partition.deletionInfo();
            assertEquals(2, finalInfo.rangeCount());
            Iterator<RangeTombstone> finalRanges = finalInfo.rangeIterator(false);
            assertRange(finalRanges.next(), 0, 2, 10);
            assertRange(finalRanges.next(), 4, 6, 20);
            assertFalse(finalRanges.hasNext());

            assertEquals(1, publishedWinner.rangeCount());
            assertRange(publishedWinner.rangeIterator(false).next(), 4, 6, 20);
        }
        finally
        {
            state.close();
        }
    }

    private static void assertRangeOnlyAccounting(State state)
    {
        BTreePartitionData holder = state.partition.unsafeGetHolder();
        long expected = holder.deletionInfo.unsharedHeapSize()
                        + holder.columns.unsharedHeapSize()
                        + BTree.sizeOnHeapOf(holder.tree)
                        + holder.stats.unsharedHeapSize();
        assertEquals(expected, state.allocator.onHeap().owns());
    }

    private static void assertSliceStarts(Iterator<RangeTombstone> ranges, int first, int last, boolean reversed)
    {
        int expected = reversed ? last : first;
        int step = reversed ? -1 : 1;
        while (ranges.hasNext())
        {
            assertEquals(expected * 4, start(ranges.next()));
            expected += step;
        }
        assertEquals(reversed ? first - 1 : last + 1, expected);
    }

    private static void assertRange(RangeTombstone range, int start, int end, long timestamp)
    {
        assertEquals(start, start(range));
        assertEquals(end, Int32Type.instance.compose((ByteBuffer) range.deletedSlice().end().get(0)).intValue());
        assertEquals(timestamp, range.deletionTime().markedForDeleteAt());
    }

    private static int start(RangeTombstone range)
    {
        return Int32Type.instance.compose((ByteBuffer) range.deletedSlice().start().get(0));
    }

    private static Clustering<?> clustering(int value)
    {
        return metadata.comparator.make(value);
    }

    private static PartitionUpdate rangeUpdate(int range, long timestamp)
    {
        int start = range * 4;
        PartitionUpdate.SimpleBuilder builder = PartitionUpdate.simpleBuilder(metadata, 0)
                                                              .timestamp(timestamp)
                                                              .nowInSec(1);
        builder.addRangeTombstone().start(start).end(start + 2);
        return builder.build();
    }

    private static PartitionUpdate multiRangeUpdate(int firstRange, int secondRange, long timestamp)
    {
        PartitionUpdate.SimpleBuilder builder = PartitionUpdate.simpleBuilder(metadata, 0)
                                                              .timestamp(timestamp)
                                                              .nowInSec(1);
        builder.addRangeTombstone().start(firstRange * 4).end(firstRange * 4 + 2);
        builder.addRangeTombstone().start(secondRange * 4).end(secondRange * 4 + 2);
        return builder.build();
    }

    private static PartitionUpdate partitionDelete(long timestamp)
    {
        return PartitionUpdate.simpleBuilder(metadata, 0).timestamp(timestamp).nowInSec(1).delete().build();
    }

    private static class State
    {
        private final MemtableAllocator allocator = new HeapPool.Allocator(POOL);
        private final AtomicBTreePartition partition = new AtomicBTreePartition(metadataRef, partitionKey, allocator);

        private void apply(PartitionUpdate update, UpdateTransaction indexer)
        {
            OpOrder.Group writeOp = NO_ORDER.getCurrent();
            partition.addAll(update, allocator.cloner(writeOp), writeOp, indexer);
        }

        private void close()
        {
            allocator.setDiscarding();
            allocator.setDiscarded();
        }
    }

    private static class BlockingUpdateTransaction implements UpdateTransaction
    {
        private final CountDownLatch entered;
        private final CountDownLatch release;
        private final AtomicBoolean blockOnce;

        private BlockingUpdateTransaction(CountDownLatch entered, CountDownLatch release, AtomicBoolean blockOnce)
        {
            this.entered = entered;
            this.release = release;
            this.blockOnce = blockOnce;
        }

        public void start()
        {
        }

        public void onPartitionDeletion(DeletionTime deletionTime)
        {
        }

        public void onRangeTombstone(RangeTombstone rangeTombstone)
        {
            if (!blockOnce.compareAndSet(true, false))
                return;

            entered.countDown();
            try
            {
                release.await();
            }
            catch (InterruptedException e)
            {
                Thread.currentThread().interrupt();
                throw new AssertionError("interrupted while holding the first merge", e);
            }
        }

        public void onInserted(org.apache.cassandra.db.rows.Row row)
        {
        }

        public void onUpdated(org.apache.cassandra.db.rows.Row existing, org.apache.cassandra.db.rows.Row updated)
        {
        }

        public void commit()
        {
        }
    }
}
