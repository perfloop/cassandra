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

package org.apache.cassandra.db.partitions;

import java.nio.ByteBuffer;
import java.nio.ReadOnlyBufferException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.BTreeDeletionInfo;
import org.apache.cassandra.db.BufferClusteringBound;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringBound;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.DeletionInfo;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.MutableDeletionInfo;
import org.apache.cassandra.db.RangeTombstone;
import org.apache.cassandra.db.Slice;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.dht.ByteOrderedPartitioner;
import org.apache.cassandra.index.transactions.UpdateTransaction;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.concurrent.ImmediateFuture;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.memory.HeapCloner;
import org.apache.cassandra.utils.memory.HeapPool;
import org.apache.cassandra.utils.memory.MemtableAllocator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

public class BTreeDeletionInfoLifecycleTest
{
    private static final int RANGE_STRIDE = 10;
    private static final HeapPool POOL = new HeapPool(Long.MAX_VALUE, 1.0f, () -> ImmediateFuture.success(Boolean.TRUE));

    private static TableMetadata metadata;
    private static org.apache.cassandra.db.DecoratedKey key;

    @BeforeClass
    public static void beforeClass()
    {
        DatabaseDescriptor.clientInitialization(false);
        DatabaseDescriptor.setPartitionerUnsafe(ByteOrderedPartitioner.instance);
        metadata = TableMetadata.builder("btree_deletion_info_test", "partitions")
                                .addPartitionKeyColumn("pk", Int32Type.instance)
                                .addClusteringColumn("ck", Int32Type.instance)
                                .partitioner(ByteOrderedPartitioner.instance)
                                .build();
        key = metadata.partitioner.decorateKey(ByteBufferUtil.bytes(0));
    }

    @Test
    public void initialPersistentVersionClonesMutableBounds()
    {
        ByteBuffer start = ByteBufferUtil.bytes(10);
        ByteBuffer end = ByteBufferUtil.bytes(20);
        MutableDeletionInfo existing = deletionInfo(range(start, true, end, true, 1));
        DeletionInfo deletionInfo = BTreeDeletionInfo.merge(existing,
                                                            new MutableDeletionInfo(DeletionTime.build(2, 2)),
                                                            metadata.comparator);

        start.putInt(0, 7_777);
        end.putInt(0, 7_778);

        assertNotNull(deletionInfo.rangeCovering(clustering(15)));
        assertNull(deletionInfo.rangeCovering(clustering(7_777)));
        assertEquals(List.of(range(10, true, 20, true, 1)), ranges(deletionInfo, false));
    }

    @Test
    public void publishedRangesDoNotExposeMutableBounds()
    {
        AtomicBTreePartition partition = newPartition();
        apply(partition, update(deletionInfo(range(10, true, 20, true, 1))));
        DeletionInfo snapshot = partition.deletionInfo();

        assertBoundIsReadOnly(snapshot.rangeIterator(false).next());
        assertBoundIsReadOnly(snapshot.rangeCovering(clustering(15)));
        assertEquals(List.of(range(10, true, 20, true, 1)), ranges(snapshot, false));
        assertNull(snapshot.rangeCovering(clustering(7_777)));
        assertEquals(ranges(snapshot, false), ranges(partition.deletionInfo(), false));
    }

    @Test
    public void persistentVersionsCanBecomeMutableWithoutAliasing()
    {
        MutableDeletionInfo source = prefix(4);
        source.add(DeletionTime.build(10, 1));
        DeletionInfo persistent = BTreeDeletionInfo.merge(source, MutableDeletionInfo.live(), metadata.comparator);

        MutableDeletionInfo target = MutableDeletionInfo.live();
        target.add(persistent);
        assertDeletionInfoEquals(persistent, target);

        List<RangeTombstone> publishedRanges = ranges(persistent, false);
        target.add(range(100, true, 110, true, 20), metadata.comparator);
        assertEquals(publishedRanges, ranges(persistent, false));
    }

    @Test
    public void persistentVersionsRejectMismatchedComparators()
    {
        ClusteringComparator incompatible = new ClusteringComparator(LongType.instance);
        DeletionInfo persistent = btreeInfo(metadata.comparator, range(10, true, 20, true, 2));
        DeletionInfo incompatiblePersistent = btreeInfo(incompatible, range(10L, true, 20L, true, 2));
        MutableDeletionInfo incompatibleMutable = deletionInfo(incompatible, range(10L, true, 20L, true, 2));

        assertComparatorMismatch(() -> BTreeDeletionInfo.merge(persistent, incompatiblePersistent, metadata.comparator));
        assertComparatorMismatch(() -> BTreeDeletionInfo.merge(persistent, incompatibleMutable, metadata.comparator));
        assertComparatorMismatch(() -> BTreeDeletionInfo.merge(incompatibleMutable,
                                                                new MutableDeletionInfo(DeletionTime.build(2, 2)),
                                                                metadata.comparator));

        MutableDeletionInfo target = deletionInfo(incompatible, range(10L, true, 20L, true, 1));
        List<RangeTombstone> targetRanges = ranges(target, false);
        assertComparatorMismatch(() -> target.add(persistent));
        assertEquals(DeletionTime.LIVE, target.getPartitionDeletion());
        assertEquals(targetRanges, ranges(target, false));
    }

    @Test
    public void randomizedPersistentTransitionsMatchMutableDeletionInfo()
    {
        Random random = new Random(0xB7EED);
        AtomicBTreePartition partition = newPartition();
        MutableDeletionInfo expected = new MutableDeletionInfo(DeletionTime.LIVE);
        for (int update = 0; update < 128; update++)
        {
            int start = random.nextInt(128);
            int end = start + 1 + random.nextInt(24);
            RangeTombstone tombstone = range(start,
                                             random.nextBoolean(),
                                             end,
                                             random.nextBoolean(),
                                             1 + random.nextInt(16));
            MutableDeletionInfo incoming = deletionInfo(tombstone);
            expected.add(incoming.clone(HeapCloner.instance));
            apply(partition, update(incoming));

            DeletionInfo actual = partition.deletionInfo();
            assertDeletionInfoEquals(expected, actual);
            for (int point = 0; point < 160; point++)
                assertEquals(expected.rangeCovering(clustering(point)), actual.rangeCovering(clustering(point)));

            for (int slice = 0; slice < 8; slice++)
            {
                int sliceStart = random.nextInt(152);
                int sliceEnd = sliceStart + 1 + random.nextInt(16);
                Slice requested = slice(sliceStart, random.nextBoolean(), sliceEnd, random.nextBoolean());
                assertEquals(ranges(expected.rangeIterator(requested, false)), ranges(actual.rangeIterator(requested, false)));
                assertEquals(ranges(expected.rangeIterator(requested, true)), ranges(actual.rangeIterator(requested, true)));
            }
        }
    }

    @Test
    public void persistentVersionsAccountForOwnedBytes()
    {
        MemtableAllocator allocator = POOL.newAllocator("BTreeDeletionInfoAccounting");
        AtomicBTreePartition partition = newPartition(allocator);
        apply(partition, update(prefix(64)));
        apply(partition, update(deletionInfo(range(135, true, 185, true, 20))));
        apply(partition, update(deletionInfo(range(5, true, 22, true, 30))));

        MemtableAllocator recreatedAllocator = POOL.newAllocator("BTreeDeletionInfoRecreated");
        AtomicBTreePartition recreated = newPartition(recreatedAllocator);
        apply(recreated, update(partition.deletionInfo().mutableCopy()));

        assertDeletionInfoEquals(partition.deletionInfo(), recreated.deletionInfo());
        assertEquals(partition.deletionInfo().unsharedHeapSize(), allocator.onHeap().owns());
        assertEquals(recreated.deletionInfo().unsharedHeapSize(), recreatedAllocator.onHeap().owns());
    }

    private static AtomicBTreePartition newPartition()
    {
        return newPartition(POOL.newAllocator("BTreeDeletionInfoLifecycleTest"));
    }

    private static AtomicBTreePartition newPartition(MemtableAllocator allocator)
    {
        return new AtomicBTreePartition(TableMetadataRef.forOfflineTools(metadata), key, allocator);
    }

    private static void apply(AtomicBTreePartition partition, PartitionUpdate update)
    {
        OpOrder order = new OpOrder();
        try (OpOrder.Group writeOp = order.start())
        {
            partition.addAll(update, HeapCloner.instance, writeOp, UpdateTransaction.NO_OP);
        }
    }

    private static PartitionUpdate update(MutableDeletionInfo deletionInfo)
    {
        return PartitionUpdate.unsafeConstruct(metadata,
                                               key,
                                               BTreePartitionData.unsafeGetEmpty(),
                                               deletionInfo,
                                               false);
    }

    private static MutableDeletionInfo prefix(int count)
    {
        MutableDeletionInfo deletionInfo = new MutableDeletionInfo(DeletionTime.LIVE);
        for (int i = 0; i < count; i++)
            deletionInfo.add(range(i * RANGE_STRIDE, true, i * RANGE_STRIDE + 4, true, 1), metadata.comparator);
        return deletionInfo;
    }

    private static MutableDeletionInfo deletionInfo(RangeTombstone... tombstones)
    {
        return deletionInfo(metadata.comparator, tombstones);
    }

    private static MutableDeletionInfo deletionInfo(ClusteringComparator comparator, RangeTombstone... tombstones)
    {
        MutableDeletionInfo deletionInfo = new MutableDeletionInfo(DeletionTime.LIVE);
        for (RangeTombstone tombstone : tombstones)
            deletionInfo.add(tombstone, comparator);
        return deletionInfo;
    }

    private static DeletionInfo btreeInfo(ClusteringComparator comparator, RangeTombstone tombstone)
    {
        return BTreeDeletionInfo.merge(deletionInfo(comparator, tombstone),
                                       new MutableDeletionInfo(DeletionTime.build(2, 2)),
                                       comparator);
    }

    private static void assertDeletionInfoEquals(DeletionInfo expected, DeletionInfo actual)
    {
        assertEquals(expected.getPartitionDeletion(), actual.getPartitionDeletion());
        assertEquals(ranges(expected, false), ranges(actual, false));
        assertEquals(ranges(expected, true), ranges(actual, true));
    }

    private static List<RangeTombstone> ranges(DeletionInfo deletionInfo, boolean reversed)
    {
        return ranges(deletionInfo.rangeIterator(reversed));
    }

    private static List<RangeTombstone> ranges(Iterator<RangeTombstone> iterator)
    {
        List<RangeTombstone> ranges = new ArrayList<>();
        iterator.forEachRemaining(ranges::add);
        return ranges;
    }

    private static Clustering<?> clustering(int value)
    {
        return Clustering.make(ByteBufferUtil.bytes(value));
    }

    private static RangeTombstone range(int start, boolean startInclusive, int end, boolean endInclusive, long timestamp)
    {
        return range(ByteBufferUtil.bytes(start), startInclusive, ByteBufferUtil.bytes(end), endInclusive, timestamp);
    }

    private static RangeTombstone range(long start, boolean startInclusive, long end, boolean endInclusive, long timestamp)
    {
        return range(ByteBufferUtil.bytes(start), startInclusive, ByteBufferUtil.bytes(end), endInclusive, timestamp);
    }

    private static RangeTombstone range(ByteBuffer start, boolean startInclusive, ByteBuffer end, boolean endInclusive, long timestamp)
    {
        return new RangeTombstone(Slice.make(startInclusive ? BufferClusteringBound.inclusiveStartOf(start)
                                                            : BufferClusteringBound.exclusiveStartOf(start),
                                            endInclusive ? BufferClusteringBound.inclusiveEndOf(end)
                                                         : BufferClusteringBound.exclusiveEndOf(end)),
                                  DeletionTime.build(timestamp, 1));
    }

    private static Slice slice(int start, boolean startInclusive, int end, boolean endInclusive)
    {
        return Slice.make(startInclusive ? BufferClusteringBound.inclusiveStartOf(ByteBufferUtil.bytes(start))
                                         : BufferClusteringBound.exclusiveStartOf(ByteBufferUtil.bytes(start)),
                          endInclusive ? BufferClusteringBound.inclusiveEndOf(ByteBufferUtil.bytes(end))
                                       : BufferClusteringBound.exclusiveEndOf(ByteBufferUtil.bytes(end)));
    }

    private static void assertBoundIsReadOnly(RangeTombstone tombstone)
    {
        ClusteringBound<?> start = tombstone.deletedSlice().start();
        ByteBuffer value = (ByteBuffer) start.get(0);
        try
        {
            value.putInt(0, 7_777);
            fail("Expected a published range bound to be read-only");
        }
        catch (ReadOnlyBufferException e)
        {
            // expected
        }

        value.position(value.limit());
        Object[] values = start.getRawValues();
        values[0] = ByteBufferUtil.bytes(7_777);
    }

    private static void assertComparatorMismatch(Runnable merge)
    {
        try
        {
            merge.run();
            fail("Expected merge to reject a different clustering comparator");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("Cannot merge deletion infos with different clustering comparators", e.getMessage());
        }
    }
}
