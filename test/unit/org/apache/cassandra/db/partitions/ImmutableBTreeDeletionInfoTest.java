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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class ImmutableBTreeDeletionInfoTest
{
    private static final long LOCAL_DELETION_TIME = 1;

    @BeforeClass
    public static void setup()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Test
    public void orderedRangesPublishIndependentPersistentSnapshots()
    {
        Fixture fixture = new Fixture();
        try
        {
            assertLedger(fixture, fixture.rangeUpdate(0, 2, 1));
            assertLedger(fixture, fixture.rangeUpdate(4, 6, 2));
            assertLedger(fixture, fixture.rangeUpdate(8, 10, 3));

            DeletionInfo published = fixture.partition.deletionInfo();
            assertTrue(published instanceof ImmutableBTreeDeletionInfo);
            assertEquals(3, published.rangeCount());
            assertEquals(2, timestamp(published, 5));

            Slice slice = Slice.make(ClusteringBound.create(fixture.metadata.comparator, true, true, 1),
                                     ClusteringBound.create(fixture.metadata.comparator, false, true, 9));
            List<RangeTombstone> forward = collect(published.rangeIterator(slice, false));
            List<RangeTombstone> reverse = collect(published.rangeIterator(slice, true));
            Collections.reverse(reverse);
            assertEquals(forward, reverse);
            assertEquals(3, forward.size());

            MutableDeletionInfo expected = MutableDeletionInfo.live();
            expected.add(range(fixture.metadata.comparator, 0, 2, 1), fixture.metadata.comparator);
            expected.add(range(fixture.metadata.comparator, 4, 6, 2), fixture.metadata.comparator);
            expected.add(range(fixture.metadata.comparator, 8, 10, 3), fixture.metadata.comparator);
            assertEquals(expected.dataSize(), published.dataSize());
            assertSameRangeIteration(expected, published, Slice.ALL);
            assertSameRangeIteration(expected, published, slice);
            assertSameRangeIteration(expected, published,
                                     Slice.make(ClusteringBound.create(fixture.metadata.comparator, true, false, 2),
                                                ClusteringBound.create(fixture.metadata.comparator, false, false, 8)));
            assertSameRangeIteration(expected, published,
                                     Slice.make(ClusteringBound.create(fixture.metadata.comparator, true, true, 3),
                                                ClusteringBound.create(fixture.metadata.comparator, false, true, 7)));

            MutableDeletionInfo composed = MutableDeletionInfo.live();
            composed.add(published);
            assertEquals(published, composed);
            assertEquals(composed, published);
            assertEquals(published.hashCode(), composed.hashCode());

            composed.updateAllTimestampAndLocalDeletionTime(100, 9);
            assertEquals(2, timestamp(published, 5));
            assertEquals(100, timestamp(composed, 5));

            assertLedger(fixture, fixture.partitionDelete(10));
            DeletionInfo withPartitionDeletion = fixture.partition.deletionInfo();
            assertTrue(withPartitionDeletion instanceof ImmutableBTreeDeletionInfo);
            assertEquals(10, withPartitionDeletion.getPartitionDeletion().markedForDeleteAt());
            assertEquals(3, withPartitionDeletion.rangeCount());
            expected.add(DeletionTime.build(10, LOCAL_DELETION_TIME));
            assertEquals(expected.dataSize(), withPartitionDeletion.dataSize());

            MutableDeletionInfo partitionDeleteMaterialized = withPartitionDeletion.mutableCopy();
            assertEquals(withPartitionDeletion, partitionDeleteMaterialized);
            assertEquals(partitionDeleteMaterialized, withPartitionDeletion);
            assertEquals(withPartitionDeletion.hashCode(), partitionDeleteMaterialized.hashCode());
        }
        finally
        {
            fixture.close();
        }
    }

    @Test
    public void supersedingPartitionDeletionsChargeOnlyTheNewStateDelta()
    {
        Fixture fixture = new Fixture();
        Fixture rebuilt = new Fixture();
        try
        {
            for (int i = 0; i < 2; i++)
            {
                assertLedger(fixture, fixture.rangeUpdate(i * 4, i * 4 + 2, i + 1L));
                assertLedger(rebuilt, rebuilt.rangeUpdate(i * 4, i * 4 + 2, i + 1L));
            }

            BTreePartitionUpdater first = assertLedger(fixture, fixture.partitionDelete(100L));
            BTreePartitionUpdater second = assertLedger(fixture, fixture.partitionDelete(200L));
            assertEquals(DeletionTime.build(100L, LOCAL_DELETION_TIME).unsharedHeapSize(), first.heapSize);
            assertEquals(0, second.heapSize);

            assertLedger(rebuilt, rebuilt.partitionDelete(200L));
            assertEquals(rebuilt.partition.deletionInfo().unsharedHeapSize(), fixture.partition.deletionInfo().unsharedHeapSize());
            assertEquals(200L, fixture.partition.deletionInfo().getPartitionDeletion().markedForDeleteAt());
        }
        finally
        {
            fixture.close();
            rebuilt.close();
        }
    }

    @Test
    public void overlappingRangeFallsBackToCanonicalMutableReconciliation()
    {
        Fixture fixture = new Fixture();
        try
        {
            MutableDeletionInfo expected = MutableDeletionInfo.live();
            RangeTombstone first = range(fixture.metadata.comparator, 0, 4, 1);
            RangeTombstone second = range(fixture.metadata.comparator, 8, 12, 2);
            RangeTombstone overlap = range(fixture.metadata.comparator, 2, 10, 3);

            fixture.apply(fixture.rangeUpdate(0, 4, 1));
            fixture.apply(fixture.rangeUpdate(8, 12, 2));
            fixture.apply(fixture.rangeUpdate(2, 10, 3));
            expected.add(first, fixture.metadata.comparator);
            expected.add(second, fixture.metadata.comparator);
            expected.add(overlap, fixture.metadata.comparator);

            DeletionInfo actual = fixture.partition.deletionInfo();
            assertFalse(actual instanceof ImmutableBTreeDeletionInfo);
            assertEquals(collect(expected.rangeIterator(false)), collect(actual.rangeIterator(false)));
        }
        finally
        {
            fixture.close();
        }
    }

    @Test
    public void multiRangeFallbackMatchesCanonicalMutableReconciliation()
    {
        Fixture fixture = new Fixture();
        try
        {
            MutableDeletionInfo expected = MutableDeletionInfo.live();
            for (int i = 0; i < 4; i++)
            {
                fixture.apply(fixture.rangeUpdate(i * 4, i * 4 + 2, i + 1));
                expected.add(range(fixture.metadata.comparator, i * 4, i * 4 + 2, i + 1), fixture.metadata.comparator);
            }

            fixture.apply(fixture.multiRangeUpdate(5, 12, 16, 18, 100));
            expected.add(range(fixture.metadata.comparator, 5, 12, 100), fixture.metadata.comparator);
            expected.add(range(fixture.metadata.comparator, 16, 18, 100), fixture.metadata.comparator);

            DeletionInfo actual = fixture.partition.deletionInfo();
            assertFalse(actual instanceof ImmutableBTreeDeletionInfo);
            assertSameRangeIteration(expected, actual, Slice.ALL);
            assertSameRangeIteration(expected, actual,
                                     Slice.make(ClusteringBound.create(fixture.metadata.comparator, true, true, 3),
                                                ClusteringBound.create(fixture.metadata.comparator, false, true, 17)));
            assertEquals(timestamp(expected, 6), timestamp(actual, 6));
            assertEquals(timestamp(expected, 17), timestamp(actual, 17));
            assertNull(actual.rangeCovering(Clustering.make(Int32Type.instance.decompose(3))));
        }
        finally
        {
            fixture.close();
        }
    }

    private static BTreePartitionUpdater assertLedger(Fixture fixture, PartitionUpdate update)
    {
        long before = fixture.allocator.onHeap().owns();
        BTreePartitionUpdater updater = fixture.apply(update);
        assertEquals(updater.heapSize, fixture.allocator.onHeap().owns() - before);
        return updater;
    }

    private static List<RangeTombstone> collect(Iterator<RangeTombstone> iterator)
    {
        List<RangeTombstone> ranges = new ArrayList<>();
        iterator.forEachRemaining(ranges::add);
        return ranges;
    }

    private static void assertSameRangeIteration(DeletionInfo expected, DeletionInfo actual, Slice slice)
    {
        assertEquals(collect(expected.rangeIterator(slice, false)), collect(actual.rangeIterator(slice, false)));
        assertEquals(collect(expected.rangeIterator(slice, true)), collect(actual.rangeIterator(slice, true)));
    }

    private static long timestamp(DeletionInfo info, int key)
    {
        RangeTombstone range = info.rangeCovering(Clustering.make(Int32Type.instance.decompose(key)));
        assertNotNull(range);
        return range.deletionTime().markedForDeleteAt();
    }

    private static RangeTombstone range(ClusteringComparator comparator, int start, int end, long timestamp)
    {
        return new RangeTombstone(Slice.make(ClusteringBound.create(comparator, true, true, start),
                                             ClusteringBound.create(comparator, false, true, end)),
                                  DeletionTime.build(timestamp, LOCAL_DELETION_TIME));
    }

    private static final class Fixture
    {
        private final TableMetadata metadata;
        private final MemtableAllocator allocator;
        private final AtomicBTreePartition partition;
        private final OpOrder order = new OpOrder();

        private Fixture()
        {
            metadata = TableMetadata.builder("immutable_deletion_info_test", "partition")
                                    .addPartitionKeyColumn("pk", Int32Type.instance)
                                    .addClusteringColumn("ck", Int32Type.instance)
                                    .partitioner(ByteOrderedPartitioner.instance)
                                    .build();
            TableMetadataRef metadataRef = TableMetadataRef.forOfflineTools(metadata);
            HeapPool pool = new HeapPool(Long.MAX_VALUE, 1.0f, () -> ImmediateFuture.success(Boolean.TRUE));
            allocator = pool.newAllocator("test");
            partition = new AtomicBTreePartition(metadataRef,
                                                 metadata.partitioner.decorateKey(Int32Type.instance.decompose(0)),
                                                 allocator);
        }

        private BTreePartitionUpdater apply(PartitionUpdate update)
        {
            OpOrder.Group writeOp = order.getCurrent();
            return partition.addAll(update, allocator.cloner(writeOp), writeOp, UpdateTransaction.NO_OP);
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

        private PartitionUpdate multiRangeUpdate(int firstStart, int firstEnd, int secondStart, int secondEnd, long timestamp)
        {
            PartitionUpdate.SimpleBuilder builder = PartitionUpdate.simpleBuilder(metadata, 0);
            builder.timestamp(timestamp).nowInSec(LOCAL_DELETION_TIME);
            builder.addRangeTombstone().start(firstStart).end(firstEnd);
            builder.addRangeTombstone().start(secondStart).end(secondEnd);
            return builder.build();
        }

        private void close()
        {
            allocator.setDiscarding();
            allocator.setDiscarded();
        }
    }
}
