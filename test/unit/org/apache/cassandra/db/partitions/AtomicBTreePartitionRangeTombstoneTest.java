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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringBound;
import org.apache.cassandra.db.ClusteringComparator;
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
import org.apache.cassandra.utils.concurrent.ImmediateFuture;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.memory.HeapPool;
import org.apache.cassandra.utils.memory.MemtableAllocator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class AtomicBTreePartitionRangeTombstoneTest
{
    private static final int RANGE_COUNT = 4096;
    private static final long LOCAL_DELETION_TIME = 1;

    @BeforeClass
    public static void setup()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Test
    public void orderedRangeMergesSupportLookupsAndSlicedIteration()
    {
        Fixture fixture = new Fixture();
        try
        {
            for (int i = 0; i < RANGE_COUNT; i++)
                fixture.apply(fixture.rangeUpdate(i, i + 1L), UpdateTransaction.NO_OP);

            DeletionInfo info = fixture.partition.deletionInfo();
            assertEquals(RANGE_COUNT, info.rangeCount());
            assertEquals(1, timestamp(info, 1));
            assertEquals(1024, timestamp(info, 1023 * 4 + 1));
            assertEquals(RANGE_COUNT, timestamp(info, (RANGE_COUNT - 1) * 4 + 1));
            assertNull(info.rangeCovering(clustering(3)));
            assertNull(info.rangeCovering(clustering(RANGE_COUNT * 4)));

            Slice slice = Slice.make(ClusteringBound.create(fixture.metadata.comparator, true, true, 100 * 4),
                                     ClusteringBound.create(fixture.metadata.comparator, false, true, 109 * 4 + 2));
            List<RangeTombstone> forward = collect(info.rangeIterator(slice, false));
            List<RangeTombstone> reverse = collect(info.rangeIterator(slice, true));
            Collections.reverse(reverse);

            assertEquals(10, forward.size());
            assertEquals(forward, reverse);
        }
        finally
        {
            fixture.close();
        }
    }

    @Test
    public void overlapFallbackMatchesCanonicalMutableReconciliationAndMaterialization()
    {
        Fixture fixture = new Fixture();
        try
        {
            MutableDeletionInfo expected = MutableDeletionInfo.live();
            for (int i = 0; i < 4; i++)
            {
                PartitionUpdate update = fixture.rangeUpdate(i, i + 1L);
                fixture.apply(update, UpdateTransaction.NO_OP);
                expected.add(range(fixture.metadata.comparator, i * 4, i * 4 + 2, i + 1L), fixture.metadata.comparator);
            }

            PartitionUpdate overlap = fixture.rangeUpdate(5, 12, 100);
            fixture.apply(overlap, UpdateTransaction.NO_OP);
            expected.add(range(fixture.metadata.comparator, 5, 12, 100), fixture.metadata.comparator);

            DeletionInfo actual = fixture.partition.deletionInfo();
            assertEquals(collect(expected.rangeIterator(false)), collect(actual.rangeIterator(false)));

            MutableDeletionInfo materialized = actual.mutableCopy();
            assertEquals(actual, materialized);
            assertEquals(materialized, actual);
            assertEquals(actual.hashCode(), materialized.hashCode());
            assertEquals(actual.dataSize(), materialized.dataSize());

            materialized.updateAllTimestampAndLocalDeletionTime(1000, 9);
            assertEquals(100, timestamp(actual, 6));
            assertEquals(1000, timestamp(materialized, 6));
        }
        finally
        {
            fixture.close();
        }
    }

    @Test
    public void partitionDeletionPreservesRangesAndReportsTheActualUpdaterLedger()
    {
        Fixture fixture = new Fixture();
        try
        {
            assertLedger(fixture, fixture.rangeUpdate(0, 1L));
            assertLedger(fixture, fixture.rangeUpdate(1, 2L));
            assertLedger(fixture, fixture.partitionDelete(100L));

            DeletionInfo info = fixture.partition.deletionInfo();
            assertEquals(100, info.getPartitionDeletion().markedForDeleteAt());
            assertEquals(2, info.rangeCount());
            assertEquals(1, timestamp(info, 1));
            assertEquals(2, timestamp(info, 5));
        }
        finally
        {
            fixture.close();
        }
    }

    @Test
    public void casLoserCannotMutateThePublishedRangeSnapshot()
    {
        Fixture fixture = new Fixture();
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicReference<Throwable> writerFailure = new AtomicReference<>();
        UpdateTransaction blocker = new BlockingRangeTransaction(entered, release);
        PartitionUpdate delayed = fixture.rangeUpdate(20, 22, 200);
        Thread loser = new Thread(() -> {
            try
            {
                fixture.apply(delayed, blocker);
            }
            catch (Throwable t)
            {
                writerFailure.set(t);
            }
        }, "delayed-range-writer");

        BTreePartitionData snapshot = null;
        Throwable mainFailure = null;
        loser.start();
        try
        {
            await(entered);
            fixture.apply(fixture.rangeUpdate(10, 12, 100), UpdateTransaction.NO_OP);
            snapshot = fixture.partition.unsafeGetHolder();
        }
        catch (Throwable t)
        {
            mainFailure = t;
        }
        finally
        {
            release.countDown();
        }

        join(loser);
        try
        {
            if (mainFailure != null)
                throw new AssertionError(mainFailure);
            if (writerFailure.get() != null)
                throw new AssertionError(writerFailure.get());

            assertEquals(1, snapshot.deletionInfo.rangeCount());
            assertEquals(100, timestamp(snapshot.deletionInfo, 11));
            assertNull(snapshot.deletionInfo.rangeCovering(clustering(21)));

            DeletionInfo finalInfo = fixture.partition.deletionInfo();
            assertEquals(2, finalInfo.rangeCount());
            assertEquals(100, timestamp(finalInfo, 11));
            assertEquals(200, timestamp(finalInfo, 21));
        }
        finally
        {
            fixture.close();
        }
    }

    private static void assertLedger(Fixture fixture, PartitionUpdate update)
    {
        long before = fixture.allocator.onHeap().owns();
        BTreePartitionUpdater updater = fixture.apply(update, UpdateTransaction.NO_OP);
        assertEquals(updater.heapSize, fixture.allocator.onHeap().owns() - before);
    }

    private static List<RangeTombstone> collect(Iterator<RangeTombstone> iterator)
    {
        List<RangeTombstone> result = new ArrayList<>();
        iterator.forEachRemaining(result::add);
        return result;
    }

    private static long timestamp(DeletionInfo info, int key)
    {
        RangeTombstone tombstone = info.rangeCovering(clustering(key));
        assertTrue("expected a range tombstone covering " + key, tombstone != null);
        return tombstone.deletionTime().markedForDeleteAt();
    }

    private static Clustering<?> clustering(int value)
    {
        return Clustering.make(Int32Type.instance.decompose(value));
    }

    private static RangeTombstone range(ClusteringComparator comparator, int start, int end, long timestamp)
    {
        return new RangeTombstone(Slice.make(ClusteringBound.create(comparator, true, true, start),
                                             ClusteringBound.create(comparator, false, true, end)),
                                  DeletionTime.build(timestamp, LOCAL_DELETION_TIME));
    }

    private static void await(CountDownLatch latch)
    {
        try
        {
            latch.await();
        }
        catch (InterruptedException e)
        {
            Thread.currentThread().interrupt();
            throw new AssertionError(e);
        }
    }

    private static void join(Thread thread)
    {
        try
        {
            thread.join();
        }
        catch (InterruptedException e)
        {
            Thread.currentThread().interrupt();
            throw new AssertionError(e);
        }
    }

    private static final class BlockingRangeTransaction implements UpdateTransaction
    {
        private final CountDownLatch entered;
        private final CountDownLatch release;
        private final AtomicBoolean firstRange = new AtomicBoolean(true);

        private BlockingRangeTransaction(CountDownLatch entered, CountDownLatch release)
        {
            this.entered = entered;
            this.release = release;
        }

        public void start()
        {
        }

        public void onPartitionDeletion(DeletionTime deletionTime)
        {
        }

        public void onRangeTombstone(RangeTombstone rangeTombstone)
        {
            if (firstRange.compareAndSet(true, false))
            {
                entered.countDown();
                await(release);
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

    private static final class Fixture
    {
        private final TableMetadata metadata;
        private final HeapPool pool;
        private final MemtableAllocator allocator;
        private final AtomicBTreePartition partition;
        private final OpOrder order = new OpOrder();

        private Fixture()
        {
            metadata = TableMetadata.builder("range_tombstone_test", "partition")
                                    .addPartitionKeyColumn("pk", Int32Type.instance)
                                    .addClusteringColumn("ck", Int32Type.instance)
                                    .partitioner(ByteOrderedPartitioner.instance)
                                    .build();
            TableMetadataRef metadataRef = TableMetadataRef.forOfflineTools(metadata);
            pool = new HeapPool(Long.MAX_VALUE, 1.0f, () -> ImmediateFuture.success(Boolean.TRUE));
            allocator = pool.newAllocator("test");
            partition = new AtomicBTreePartition(metadataRef,
                                                 metadata.partitioner.decorateKey(Int32Type.instance.decompose(0)),
                                                 allocator);
        }

        private BTreePartitionUpdater apply(PartitionUpdate update, UpdateTransaction transaction)
        {
            OpOrder.Group writeOp = order.getCurrent();
            return partition.addAll(update, allocator.cloner(writeOp), writeOp, transaction);
        }

        private PartitionUpdate rangeUpdate(int index, long timestamp)
        {
            return rangeUpdate(index * 4, index * 4 + 2, timestamp);
        }

        private PartitionUpdate rangeUpdate(int start, int end, long timestamp)
        {
            PartitionUpdate.SimpleBuilder builder = PartitionUpdate.simpleBuilder(metadata, 0);
            builder.timestamp(timestamp).nowInSec(LOCAL_DELETION_TIME);
            builder.addRangeTombstone().start(start).end(end);
            return builder.build();
        }

        private PartitionUpdate partitionDelete(long timestamp)
        {
            PartitionUpdate.SimpleBuilder builder = PartitionUpdate.simpleBuilder(metadata, 0);
            builder.timestamp(timestamp).nowInSec(LOCAL_DELETION_TIME).delete();
            return builder.build();
        }

        private void close()
        {
            allocator.setDiscarding();
            allocator.setDiscarded();
        }
    }
}
