/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.db;

import java.util.Iterator;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.partitions.AtomicBTreePartition;
import org.apache.cassandra.db.partitions.BTreePartitionData;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.index.transactions.UpdateTransaction;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.btree.BTree;
import org.apache.cassandra.utils.concurrent.ImmediateFuture;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.memory.Cloner;
import org.apache.cassandra.utils.memory.HeapCloner;
import org.apache.cassandra.utils.memory.HeapPool;
import org.apache.cassandra.utils.memory.MemtableAllocator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class RangeTombstoneListCopyTest
{
    private static final ClusteringComparator comparator = new ClusteringComparator(Int32Type.instance);
    private static final OpOrder NO_ORDER = new OpOrder();

    @BeforeClass
    public static void initializeDatabaseDescriptor()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Test
    public void memtableSnapshotDoesNotAliasParentOrUpdate()
    {
        MutableDeletionInfo original = new MutableDeletionInfo(DeletionTime.LIVE, list(2));
        MutableDeletionInfo update = deletion(tombstone(10, 11, 3));
        MutableDeletionInfo copy = original.mutableCopyForMemtable(update);
        copy.add(update);

        original.add(tombstone(14, 15, 5), comparator);
        update.add(tombstone(12, 13, 4), comparator);

        assertTimestamps(original.rangeIterator(false), 1, 2, 5);
        assertTimestamps(update.rangeIterator(false), 3, 4);
        assertTimestamps(copy.rangeIterator(false), 1, 2, 3);
    }

    @Test
    public void persistentSnapshotMatchesFlatRangeQueries()
    {
        RangeTombstoneList flat = list(8);
        RangeTombstoneList snapshot = flat.copyForMemtable();

        assertSameRangeQueries(flat, snapshot);

        flat.add(tombstone(16, 17, 9));
        snapshot.add(tombstone(16, 17, 9));
        assertSameRangeQueries(flat, snapshot);
    }

    @Test
    public void persistentSnapshotSupportsIterationSearchCloneAndTimestampUpdates()
    {
        MutableDeletionInfo merged = MutableDeletionInfo.live();
        for (int i = 0; i < 8; i++)
        {
            MutableDeletionInfo update = deletion(tombstone(i * 2, i * 2 + 1, i + 1));
            MutableDeletionInfo next = merged.mutableCopyForMemtable(update);
            next.add(update);
            merged = next;
        }

        assertTimestamps(merged.rangeIterator(false), 1, 2, 3, 4, 5, 6, 7, 8);
        assertTimestamps(merged.rangeIterator(true), 8, 7, 6, 5, 4, 3, 2, 1);
        assertTimestamps(merged.rangeIterator(slice(4, 11), false), 3, 4, 5, 6);
        assertTimestamps(merged.rangeIterator(slice(4, 11), true), 6, 5, 4, 3);
        assertTimestamp(merged.rangeCovering(clustering(0)), 1);
        assertTimestamp(merged.rangeCovering(clustering(15)), 8);
        assertNull(merged.rangeCovering(clustering(16)));

        MutableDeletionInfo clone = merged.clone(HeapCloner.instance);
        clone.updateAllTimestamp(99);
        assertTimestamps(merged.rangeIterator(false), 1, 2, 3, 4, 5, 6, 7, 8);
        assertTimestamps(clone.rangeIterator(false), 99, 99, 99, 99, 99, 99, 99, 99);
    }

    @Test
    public void atomicPartitionPreservesRangeReadsForOrderedUpdates()
    {
        Fixture fixture = fixture();
        for (int i = 0; i < 8; i++)
            fixture.add(i);

        DeletionInfo deletionInfo = fixture.partition.deletionInfo();
        assertTimestamps(deletionInfo.rangeIterator(false), 1, 2, 3, 4, 5, 6, 7, 8);
        assertTimestamps(deletionInfo.rangeIterator(true), 8, 7, 6, 5, 4, 3, 2, 1);
        assertTimestamps(deletionInfo.rangeIterator(slice(4, 11), false), 3, 4, 5, 6);
        assertTimestamps(deletionInfo.rangeIterator(slice(4, 11), true), 6, 5, 4, 3);
        assertTimestamp(deletionInfo.rangeCovering(clustering(0)), 1);
        assertTimestamp(deletionInfo.rangeCovering(clustering(15)), 8);
        assertNull(deletionInfo.rangeCovering(clustering(16)));
    }

    @Test
    public void atomicPartitionPreservesRangeReadsForPrefixUpdates()
    {
        Fixture fixture = fixture();
        for (int i = 7; i >= 0; i--)
            fixture.add(i);

        DeletionInfo deletionInfo = fixture.partition.deletionInfo();
        assertTimestamps(deletionInfo.rangeIterator(false), 1, 2, 3, 4, 5, 6, 7, 8);
        assertTimestamps(deletionInfo.rangeIterator(true), 8, 7, 6, 5, 4, 3, 2, 1);
        assertTimestamps(deletionInfo.rangeIterator(slice(4, 11), false), 3, 4, 5, 6);
        assertTimestamp(deletionInfo.rangeCovering(clustering(0)), 1);
        assertTimestamp(deletionInfo.rangeCovering(clustering(15)), 8);
    }

    @Test
    public void orderedSnapshotAccountingMatchesTheAtomicMemtableDelta()
    {
        Fixture fixture = fixture();
        fixture.add(0);

        assertDeletionAccountingForNextRange(fixture, 1);
        assertDeletionAccountingForNextRange(fixture, 2);
        assertDeletionAccountingForNextRange(fixture, 3);
    }

    @Test
    public void persistentTreeAccountingStaysInSyncAcrossGrowthAndReadCaching()
    {
        Fixture fixture = fixture();
        fixture.add(0);

        for (int i = 1; i <= 256; i++)
        {
            // Force the lazily materialized read cache before the next persistent successor is built. The memtable
            // delta must charge only newly allocated tree nodes, not charge shared nodes or this read-side cache again.
            fixture.partition.deletionInfo().rangeCovering(clustering((i - 1) * 2));
            assertDeletionAccountingForNextRange(fixture, i);
        }
    }

    @Test
    public void persistentTreeAccountsEveryRangeInAnOrderedBatch()
    {
        Fixture fixture = fixture();
        fixture.add(0);

        assertDeletionAccounting(fixture, () -> fixture.addAll(1, 3));
        assertTimestamps(fixture.partition.deletionInfo().rangeIterator(false), 1, 2, 3, 4);
    }

    private static void assertDeletionAccountingForNextRange(Fixture fixture, int nextRange)
    {
        assertDeletionAccounting(fixture, () -> fixture.add(nextRange));
    }

    private static void assertDeletionAccounting(Fixture fixture, Runnable update)
    {
        long beforeOwned = fixture.allocator.onHeap().owns();
        MutableDeletionInfo beforeDeletionInfo = (MutableDeletionInfo) fixture.partition.deletionInfo();

        update.run();

        long afterOwned = fixture.allocator.onHeap().owns();
        MutableDeletionInfo afterDeletionInfo = (MutableDeletionInfo) fixture.partition.deletionInfo();
        long expectedDelta = afterDeletionInfo.memtableUnsharedHeapSize()
                           - beforeDeletionInfo.memtableUnsharedHeapSize()
                           + afterDeletionInfo.treeAllocatedOnHeap();
        assertEquals(expectedDelta, afterOwned - beforeOwned);
    }

    private static Fixture fixture()
    {
        TableMetadata metadata = TableMetadata.builder("test", "range_tombstone_copy")
                                              .addPartitionKeyColumn("pk", Int32Type.instance)
                                              .addClusteringColumn("ck", Int32Type.instance)
                                              .build();
        DecoratedKey key = metadata.partitioner.decorateKey(ByteBufferUtil.bytes(0));
        HeapPool pool = new HeapPool(Long.MAX_VALUE, 1.0f, () -> ImmediateFuture.success(Boolean.TRUE));
        MemtableAllocator allocator = new HeapPool.Allocator(pool);
        OpOrder.Group writeOp = NO_ORDER.getCurrent();
        Cloner cloner = allocator.cloner(writeOp);
        AtomicBTreePartition partition = new AtomicBTreePartition(TableMetadataRef.forOfflineTools(metadata), key, allocator);
        return new Fixture(metadata, key, allocator, writeOp, cloner, partition);
    }

    private static PartitionUpdate update(TableMetadata metadata, DecoratedKey key, int firstIndex, int count)
    {
        MutableDeletionInfo deletionInfo = MutableDeletionInfo.live();
        for (int i = firstIndex; i < firstIndex + count; i++)
            deletionInfo.add(tombstone(i * 2, i * 2 + 1, i + 1), comparator);

        BTreePartitionData holder = BTreePartitionData.unsafeConstruct(RegularAndStaticColumns.NONE,
                                                                        BTree.empty(),
                                                                        deletionInfo,
                                                                        Rows.EMPTY_STATIC_ROW,
                                                                        EncodingStats.NO_STATS);
        return PartitionUpdate.unsafeConstruct(metadata, key, holder, deletionInfo, false);
    }

    private static MutableDeletionInfo deletion(RangeTombstone tombstone)
    {
        MutableDeletionInfo deletion = MutableDeletionInfo.live();
        deletion.add(tombstone, comparator);
        return deletion;
    }

    private static RangeTombstoneList list(int count)
    {
        RangeTombstoneList list = new RangeTombstoneList(comparator, 8);
        for (int i = 0; i < count; i++)
            list.add(tombstone(2 * i + 2, 2 * i + 3, i + 1));
        return list;
    }

    private static Slice slice(int start, int end)
    {
        return Slice.make(clustering(start), clustering(end));
    }

    private static Clustering<?> clustering(int value)
    {
        return Clustering.make(ByteBufferUtil.bytes(value));
    }

    private static RangeTombstone tombstone(int start, int end, long timestamp)
    {
        return new RangeTombstone(Slice.make(BufferClusteringBound.inclusiveStartOf(ByteBufferUtil.bytes(start)),
                                             BufferClusteringBound.inclusiveEndOf(ByteBufferUtil.bytes(end))),
                                  DeletionTime.build(timestamp, 1));
    }

    private static void assertSameRangeQueries(RangeTombstoneList expected, RangeTombstoneList actual)
    {
        assertSameTombstones(expected.iterator(false), actual.iterator(false));
        assertSameTombstones(expected.iterator(true), actual.iterator(true));

        for (int point = 0; point <= 18; point++)
            assertEquals(expected.search(clustering(point)), actual.search(clustering(point)));

        for (int start = 0; start <= 18; start++)
        {
            for (int end = start; end <= 18; end++)
            {
                Slice slice = slice(start, end);
                assertSameTombstones(expected.iterator(slice, false), actual.iterator(slice, false));
                assertSameTombstones(expected.iterator(slice, true), actual.iterator(slice, true));
            }
        }
    }

    private static void assertSameTombstones(Iterator<RangeTombstone> expected, Iterator<RangeTombstone> actual)
    {
        while (expected.hasNext())
        {
            assertTrue(actual.hasNext());
            assertEquals(expected.next(), actual.next());
        }
        assertFalse(actual.hasNext());
    }

    private static void assertTimestamp(RangeTombstone tombstone, long timestamp)
    {
        assertEquals(timestamp, tombstone.deletionTime().markedForDeleteAt());
    }

    private static void assertTimestamps(Iterator<RangeTombstone> tombstones, long... timestamps)
    {
        for (long timestamp : timestamps)
        {
            assertTrue(tombstones.hasNext());
            assertTimestamp(tombstones.next(), timestamp);
        }
        assertFalse(tombstones.hasNext());
    }

    private static final class Fixture
    {
        private final TableMetadata metadata;
        private final DecoratedKey key;
        private final MemtableAllocator allocator;
        private final OpOrder.Group writeOp;
        private final Cloner cloner;
        private final AtomicBTreePartition partition;

        private Fixture(TableMetadata metadata,
                        DecoratedKey key,
                        MemtableAllocator allocator,
                        OpOrder.Group writeOp,
                        Cloner cloner,
                        AtomicBTreePartition partition)
        {
            this.metadata = metadata;
            this.key = key;
            this.allocator = allocator;
            this.writeOp = writeOp;
            this.cloner = cloner;
            this.partition = partition;
        }

        private void add(int index)
        {
            addAll(index, 1);
        }

        private void addAll(int firstIndex, int count)
        {
            partition.addAll(update(metadata, key, firstIndex, count), cloner, writeOp, UpdateTransaction.NO_OP);
        }
    }
}
