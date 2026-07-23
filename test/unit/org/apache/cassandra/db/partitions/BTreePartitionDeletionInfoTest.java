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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.BufferClusteringBound;
import org.apache.cassandra.db.Clustering;
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
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.concurrent.ImmediateFuture;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.memory.HeapCloner;
import org.apache.cassandra.utils.memory.HeapPool;
import org.apache.cassandra.utils.memory.MemtableAllocator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class BTreePartitionDeletionInfoTest
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
        metadata = TableMetadata.builder("deletion_info_test", "partitions")
                                .addPartitionKeyColumn("pk", Int32Type.instance)
                                .addClusteringColumn("ck", Int32Type.instance)
                                .partitioner(ByteOrderedPartitioner.instance)
                                .build();
        key = metadata.partitioner.decorateKey(ByteBufferUtil.bytes(0));
    }

    @Test
    public void transitionsRetainPublishedSnapshotAndCloneIncomingBounds()
    {
        AtomicBTreePartition partition = newPartition();
        MutableDeletionInfo reference = prefix(32);
        apply(partition, update(reference.clone(HeapCloner.instance)));

        DeletionInfo published = partition.unsafeGetHolder().deletionInfo;
        List<RangeTombstone> publishedRanges = ranges(published, false);
        DeletionTime publishedPartitionDeletion = published.getPartitionDeletion();

        MutableDeletionInfo partitionDelete = new MutableDeletionInfo(DeletionTime.build(10, 1));
        applyAndCompare(partition, reference, partitionDelete);

        ByteBuffer mutableStart = ByteBufferUtil.bytes(15);
        ByteBuffer mutableEnd = ByteBufferUtil.bytes(35);
        MutableDeletionInfo overlap = deletionInfo(range(mutableStart, true, mutableEnd, true, 20));
        applyAndCompare(partition, reference, overlap);

        // The published deletion must own clone materializations, not the update's mutable buffers.
        mutableStart.putInt(0, 7_777);
        mutableEnd.putInt(0, 7_778);
        assertNotNull(partition.deletionInfo().rangeCovering(clustering(20)));
        assertNull(partition.deletionInfo().rangeCovering(clustering(7_777)));

        MutableDeletionInfo outOfOrder = deletionInfo(range(5, true, 12, true, 30));
        applyAndCompare(partition, reference, outOfOrder);

        MutableDeletionInfo twoRanges = deletionInfo(range(117, true, 128, true, 40),
                                                     range(147, true, 158, true, 40));
        applyAndCompare(partition, reference, twoRanges);

        assertEquals(publishedPartitionDeletion, published.getPartitionDeletion());
        assertEquals(publishedRanges, ranges(published, false));
    }

    @Test
    public void rangeSlicesAndPointReadsPreserveBoundsInBothDirections()
    {
        AtomicBTreePartition partition = newPartition();
        MutableDeletionInfo input = deletionInfo(range(0, true, 100, true, 1),
                                                range(200, true, 300, true, 2));
        apply(partition, update(input));
        DeletionInfo deletionInfo = partition.deletionInfo();

        assertSlice(deletionInfo,
                    slice(0, false, 100, false),
                    false,
                    range(0, false, 100, false, 1));
        assertSlice(deletionInfo,
                    slice(25, true, 250, true),
                    false,
                    range(25, true, 100, true, 1),
                    range(200, true, 250, true, 2));
        assertSlice(deletionInfo,
                    slice(25, true, 250, true),
                    true,
                    range(200, true, 250, true, 2),
                    range(25, true, 100, true, 1));
        assertSlice(deletionInfo,
                    slice(120, true, 180, true),
                    false);

        for (int i = 0; i < 32; i++)
        {
            assertNotNull(deletionInfo.rangeCovering(clustering(25)));
            assertNotNull(deletionInfo.rangeCovering(clustering(250)));
            assertNull(deletionInfo.rangeCovering(clustering(150)));
        }
    }

    private static AtomicBTreePartition newPartition()
    {
        MemtableAllocator allocator = POOL.newAllocator("BTreePartitionDeletionInfoTest");
        return new AtomicBTreePartition(TableMetadataRef.forOfflineTools(metadata), key, allocator);
    }

    private static void applyAndCompare(AtomicBTreePartition partition, MutableDeletionInfo reference, MutableDeletionInfo update)
    {
        reference.add(update.clone(HeapCloner.instance));
        apply(partition, update(update));
        assertDeletionInfoEquals(reference, partition.deletionInfo());
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
        MutableDeletionInfo deletionInfo = new MutableDeletionInfo(DeletionTime.LIVE);
        for (RangeTombstone tombstone : tombstones)
            deletionInfo.add(tombstone, metadata.comparator);
        return deletionInfo;
    }

    private static void assertDeletionInfoEquals(DeletionInfo expected, DeletionInfo actual)
    {
        assertEquals(expected.getPartitionDeletion(), actual.getPartitionDeletion());
        assertEquals(ranges(expected, false), ranges(actual, false));
        assertEquals(ranges(expected, true), ranges(actual, true));
    }

    private static void assertSlice(DeletionInfo deletionInfo, Slice slice, boolean reversed, RangeTombstone... expected)
    {
        assertEquals(Arrays.asList(expected), ranges(deletionInfo.rangeIterator(slice, reversed)));
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
}
