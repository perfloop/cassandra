/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.cassandra.db.partitions;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.Iterator;

import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.BufferClusteringBound;
import org.apache.cassandra.db.BufferDecoratedKey;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringBound;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.DeletionInfo;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.MutableDeletionInfo;
import org.apache.cassandra.db.RangeTombstone;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.Slice;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.ReversedType;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.dht.ByteOrderedPartitioner;
import org.apache.cassandra.index.transactions.UpdateTransaction;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.utils.concurrent.ImmediateFuture;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.memory.HeapCloner;
import org.apache.cassandra.utils.memory.HeapPool;
import org.apache.cassandra.utils.memory.MemtableAllocator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Differential coverage for the package-private BTree deletion snapshot used by BTreePartitionUpdater.
 */
public class BTreeDeletionInfoContractTest
{
    @BeforeClass
    public static void initialize()
    {
        DatabaseDescriptor.daemonInitialization();
        DatabaseDescriptor.setPartitionerUnsafe(ByteOrderedPartitioner.instance);
    }

    @Test
    public void btreeSnapshotMatchesMutableOracleAfterEachBoundaryTransition()
    {
        for (boolean startInclusive : new boolean[] { false, true })
        {
            for (boolean endInclusive : new boolean[] { false, true })
            {
                BoundaryState state = new BoundaryState();
                MutableDeletionInfo expected = state.installPrefix();
                assertBoundaryState(expected, state, "boundary-kind prefix");

                PartitionUpdate update = state.sharedEndpointOverlap(startInclusive, endInclusive);
                expected.add(update.deletionInfo());
                state.apply(update);
                assertBoundaryState(expected, state, "shared endpoint " + startInclusive + '/' + endInclusive);
            }
        }

        BoundaryState state = new BoundaryState();
        MutableDeletionInfo expected = state.installPrefix();
        for (PartitionUpdate update : new PartitionUpdate[]
        {
            state.higherTimestampOverlap(),
            state.lowerTimestampOverlap(),
            state.equalTimestampExactReplacement()
        })
        {
            expected.add(update.deletionInfo());
            state.apply(update);
            assertBoundaryState(expected, state, "timestamp transition " + update.deletionInfo().maxTimestamp());
        }

        assertDeletionInfoContract(expected,
                                   state.partition.deletionInfo(),
                                   state,
                                   state.furtherRangeUpdate());
    }

    @Test
    public void liveAndPartitionOnlyDeletionStatesPreserveDeletionInfoContracts()
    {
        BoundaryState state = new BoundaryState();
        MutableDeletionInfo live = MutableDeletionInfo.live();
        assertDeletionInfo(live, state.partition.deletionInfo(), state.metadata.comparator, "live state");
        assertDeletionInfoContract(live, state.partition.deletionInfo(), state, state.furtherRangeUpdate());

        PartitionUpdate partitionDelete = PartitionUpdate.fullPartitionDelete(state.metadata, state.key, 200, 42);
        MutableDeletionInfo expected = MutableDeletionInfo.live();
        expected.add(partitionDelete.deletionInfo());
        state.apply(partitionDelete);

        DeletionInfo actual = state.partition.deletionInfo();
        assertDeletionInfo(expected, actual, state.metadata.comparator, "partition-only state");
        assertDeletionInfoContract(expected, actual, state, state.furtherRangeUpdate());
    }

    @Test
    public void directMergeRejectsAnIncompatibleClusteringComparator()
    {
        Assume.assumeTrue(btreeDeletionInfoIsAvailable());
        BoundaryState state = new BoundaryState();
        state.installPrefix();

        try
        {
            directMerge(state.partition.deletionInfo(),
                        MutableDeletionInfo.live(),
                        new ClusteringComparator(ReversedType.getInstance(Int32Type.instance), Int32Type.instance));
            throw new AssertionError("direct merge accepted an incompatible clustering comparator");
        }
        catch (IllegalArgumentException expected)
        {
        }
    }

    @Test
    public void directMergeClonesRangeBoundsBeforePublishingTheSnapshot()
    {
        Assume.assumeTrue(btreeDeletionInfoIsAvailable());
        BoundaryState state = new BoundaryState();
        ByteBuffer startFirst = Int32Type.instance.decompose(4);
        ByteBuffer startSecond = Int32Type.instance.decompose(1);
        ByteBuffer endFirst = Int32Type.instance.decompose(4);
        ByteBuffer endSecond = Int32Type.instance.decompose(3);
        RangeTombstone supplied = new RangeTombstone(Slice.make(BufferClusteringBound.inclusiveStartOf(startFirst, startSecond),
                                                                BufferClusteringBound.exclusiveEndOf(endFirst, endSecond)),
                                                     DeletionTime.build(300, 40));
        MutableDeletionInfo update = MutableDeletionInfo.live();
        update.add(supplied, state.metadata.comparator);
        MutableDeletionInfo expected = update.clone(HeapCloner.instance);
        DeletionInfo actual = directMerge(DeletionInfo.LIVE, update, state.metadata.comparator);

        startFirst.putInt(0, 99);
        startSecond.putInt(0, 99);
        endFirst.putInt(0, 99);
        endSecond.putInt(0, 99);

        assertDeletionInfo(expected, actual, state.metadata.comparator, "directly merged update bounds");
        assertNotNull("published range no longer covers its original clustering", actual.rangeCovering(state.clustering(4, 2)));
        assertNull("published range followed a caller-owned buffer mutation", actual.rangeCovering(state.clustering(99, 2)));
    }

    @Test
    public void directMergeClonesExistingRangeBoundsBeforePublishingTheSnapshot()
    {
        Assume.assumeTrue(btreeDeletionInfoIsAvailable());
        BoundaryState state = new BoundaryState();
        ByteBuffer startFirst = Int32Type.instance.decompose(4);
        ByteBuffer startSecond = Int32Type.instance.decompose(1);
        ByteBuffer endFirst = Int32Type.instance.decompose(4);
        ByteBuffer endSecond = Int32Type.instance.decompose(3);
        MutableDeletionInfo existing = MutableDeletionInfo.live();
        existing.add(new RangeTombstone(Slice.make(BufferClusteringBound.inclusiveStartOf(startFirst, startSecond),
                                                   BufferClusteringBound.exclusiveEndOf(endFirst, endSecond)),
                                        DeletionTime.build(300, 40)),
                     state.metadata.comparator);
        MutableDeletionInfo update = new MutableDeletionInfo(DeletionTime.build(400, 50));
        MutableDeletionInfo expected = existing.clone(HeapCloner.instance);
        expected.add(update);
        DeletionInfo actual = directMerge(existing, update, state.metadata.comparator);

        startFirst.putInt(0, 99);
        startSecond.putInt(0, 99);
        endFirst.putInt(0, 99);
        endSecond.putInt(0, 99);

        assertDeletionInfo(expected, actual, state.metadata.comparator, "directly merged existing bounds");
        assertNotNull("published range no longer covers its original clustering", actual.rangeCovering(state.clustering(4, 2)));
        assertNull("published range followed a caller-owned existing buffer mutation", actual.rangeCovering(state.clustering(99, 2)));
    }

    @Test
    public void directMergeRejectsRangesNotOrderedByTheSuppliedComparator()
    {
        Assume.assumeTrue(btreeDeletionInfoIsAvailable());
        BoundaryState state = new BoundaryState();
        MutableDeletionInfo update = MutableDeletionInfo.live();
        update.add(state.range(true, new int[] { 0, 0 }, false, new int[] { 0, 1 }, 300), state.metadata.comparator);
        update.add(state.range(true, new int[] { 1, 0 }, false, new int[] { 1, 1 }, 301), state.metadata.comparator);

        try
        {
            directMerge(DeletionInfo.LIVE,
                        update,
                        new ClusteringComparator(ReversedType.getInstance(Int32Type.instance), Int32Type.instance));
            throw new AssertionError("direct merge accepted ranges ordered by a different comparator");
        }
        catch (IllegalArgumentException expected)
        {
        }
    }

    private static void assertBoundaryState(MutableDeletionInfo expected, BoundaryState state, String description)
    {
        DeletionInfo actual = state.partition.deletionInfo();
        assertDeletionInfo(expected, actual, state.metadata.comparator, description);
        assertSlicesAndSearches(expected, actual, state.metadata.comparator, state.slices(), state.probes());
    }

    private static void assertDeletionInfoContract(MutableDeletionInfo expected,
                                                   DeletionInfo actual,
                                                   BoundaryState state,
                                                   PartitionUpdate furtherUpdate)
    {
        assertEquals("data size", expected.dataSize(), actual.dataSize());
        assertEquals("maximum timestamp", expected.maxTimestamp(), actual.maxTimestamp());
        assertEquals("mayModify live", expected.mayModify(DeletionInfo.LIVE), actual.mayModify(DeletionInfo.LIVE));
        assertEquals("mayModify expected", expected.mayModify(expected), actual.mayModify(expected));
        assertTrue("deletion info must not report a negative heap size", actual.unsharedHeapSize() >= 0);

        EncodingStats.Collector expectedStats = new EncodingStats.Collector();
        expected.collectStats(expectedStats);
        EncodingStats.Collector actualStats = new EncodingStats.Collector();
        actual.collectStats(actualStats);
        assertEquals("encoding statistics", expectedStats.get(), actualStats.get());

        MutableDeletionInfo expectedCopy = expected.mutableCopy();
        MutableDeletionInfo actualCopy = actual.mutableCopy();
        assertDeletionInfo(expectedCopy, actualCopy, state.metadata.comparator, "mutable copy");

        expectedCopy.add(furtherUpdate.deletionInfo());
        actualCopy.add(furtherUpdate.deletionInfo());
        assertDeletionInfo(expectedCopy, actualCopy, state.metadata.comparator, "mutable copy followed by merge");
    }

    private static void assertSlicesAndSearches(DeletionInfo expected,
                                                DeletionInfo actual,
                                                ClusteringComparator comparator,
                                                Slice[] slices,
                                                Clustering<?>[] probes)
    {
        for (Slice slice : slices)
        {
            assertRangeIterators(expected.rangeIterator(slice, false), actual.rangeIterator(slice, false), comparator, "forward slice");
            assertRangeIterators(expected.rangeIterator(slice, true), actual.rangeIterator(slice, true), comparator, "reverse slice");
        }

        for (Clustering<?> probe : probes)
        {
            RangeTombstone expectedRange = expected.rangeCovering(probe);
            RangeTombstone actualRange = actual.rangeCovering(probe);
            if (expectedRange == null)
                assertNull("unexpected range covering " + probe, actualRange);
            else
            {
                assertNotNull("missing range covering " + probe, actualRange);
                assertRange(expectedRange, actualRange, comparator, "range covering " + probe);
            }
        }
    }

    private static boolean btreeDeletionInfoIsAvailable()
    {
        try
        {
            Class.forName("org.apache.cassandra.db.partitions.BTreeDeletionInfo");
            return true;
        }
        catch (ClassNotFoundException e)
        {
            return false;
        }
    }

    private static DeletionInfo directMerge(DeletionInfo existing, DeletionInfo update, ClusteringComparator comparator)
    {
        try
        {
            Class<?> deletionInfoClass = Class.forName("org.apache.cassandra.db.partitions.BTreeDeletionInfo");
            Method merge = deletionInfoClass.getDeclaredMethod("merge", DeletionInfo.class, DeletionInfo.class, ClusteringComparator.class);
            merge.setAccessible(true);
            return (DeletionInfo) merge.invoke(null, existing, update, comparator);
        }
        catch (InvocationTargetException e)
        {
            Throwable cause = e.getCause();
            if (cause instanceof RuntimeException)
                throw (RuntimeException) cause;
            throw new AssertionError("direct merge failed", cause);
        }
        catch (ReflectiveOperationException e)
        {
            throw new AssertionError("could not invoke direct merge", e);
        }
    }

    private static void assertDeletionInfo(DeletionInfo expected,
                                           DeletionInfo actual,
                                           ClusteringComparator comparator,
                                           String description)
    {
        assertEquals(description + " partition deletion", expected.getPartitionDeletion(), actual.getPartitionDeletion());
        assertEquals(description + " range count", expected.rangeCount(), actual.rangeCount());
        assertRangeIterators(expected.rangeIterator(false), actual.rangeIterator(false), comparator, description + " forward");
        assertRangeIterators(expected.rangeIterator(true), actual.rangeIterator(true), comparator, description + " reverse");
    }

    private static void assertRangeIterators(Iterator<RangeTombstone> expected,
                                             Iterator<RangeTombstone> actual,
                                             ClusteringComparator comparator,
                                             String description)
    {
        while (expected.hasNext())
        {
            assertTrue(description + " ended early", actual.hasNext());
            assertRange(expected.next(), actual.next(), comparator, description);
        }
        assertFalse(description + " retained an extra range", actual.hasNext());
    }

    private static void assertRange(RangeTombstone expected,
                                    RangeTombstone actual,
                                    ClusteringComparator comparator,
                                    String description)
    {
        assertEquals(description + " start", 0, comparator.compare(expected.deletedSlice().start(), actual.deletedSlice().start()));
        assertEquals(description + " end", 0, comparator.compare(expected.deletedSlice().end(), actual.deletedSlice().end()));
        assertEquals(description + " deletion time", expected.deletionTime(), actual.deletionTime());
    }

    private static final class BoundaryState
    {
        private static final HeapPool POOL = new HeapPool(Long.MAX_VALUE, 1.0f, () -> ImmediateFuture.success(Boolean.TRUE));

        private final TableMetadata metadata;
        private final BufferDecoratedKey key;
        private final MemtableAllocator allocator;
        private final AtomicBTreePartition partition;
        private final OpOrder order = new OpOrder();

        private BoundaryState()
        {
            metadata = TableMetadata.builder("deletion_info_transition", "boundary_ranges")
                                    .addPartitionKeyColumn("pk", Int32Type.instance)
                                    .addClusteringColumn("ck1", Int32Type.instance)
                                    .addClusteringColumn("ck2", Int32Type.instance)
                                    .partitioner(ByteOrderedPartitioner.instance)
                                    .build();
            ByteBuffer keyBytes = Int32Type.instance.decompose(0);
            key = new BufferDecoratedKey(ByteOrderedPartitioner.instance.getToken(keyBytes), keyBytes);
            allocator = POOL.newAllocator("deletion-info-boundary-transition");
            partition = new AtomicBTreePartition(TableMetadataRef.forOfflineTools(metadata), key, allocator);
        }

        private MutableDeletionInfo installPrefix()
        {
            RangeTombstone[] ranges = new RangeTombstone[]
            {
                range(BufferClusteringBound.BOTTOM, end(false, 0, 0), 10),
                range(start(true, 0, 0), end(false, 0, 1), 11),
                range(start(true, 0, 1), end(true, 1, 0), 12),
                range(start(false, 1, 0), end(true, 1, 1), 13),
                range(start(false, 1, 1), end(false, 1, 2), 14),
                range(start(true, 2), end(false, 2, 1), 15),
                range(start(false, 3, 0), BufferClusteringBound.TOP, 16)
            };
            apply(update(ranges));

            MutableDeletionInfo expected = MutableDeletionInfo.live();
            for (RangeTombstone range : ranges)
                expected.add(range, metadata.comparator);
            return expected;
        }

        private PartitionUpdate sharedEndpointOverlap(boolean startInclusive, boolean endInclusive)
        {
            return update(range(startInclusive, new int[] { 0, 1 }, endInclusive, new int[] { 1, 1 }, 100));
        }

        private PartitionUpdate higherTimestampOverlap()
        {
            return sharedEndpointOverlap(true, false);
        }

        private PartitionUpdate lowerTimestampOverlap()
        {
            return update(range(false, new int[] { 0, 1 }, true, new int[] { 1, 2 }, 5));
        }

        private PartitionUpdate equalTimestampExactReplacement()
        {
            return update(range(false, new int[] { 1, 0 }, true, new int[] { 1, 1 }, 100));
        }

        private PartitionUpdate furtherRangeUpdate()
        {
            return update(range(true, new int[] { 2 }, false, new int[] { 2, 2 }, 150));
        }

        private PartitionUpdate update(RangeTombstone... ranges)
        {
            PartitionUpdate.Builder builder = new PartitionUpdate.Builder(metadata, key, RegularAndStaticColumns.NONE, ranges.length);
            for (RangeTombstone range : ranges)
                builder.add(range);
            return builder.build();
        }

        private void apply(PartitionUpdate update)
        {
            partition.addAll(update, HeapCloner.instance, order.getCurrent(), UpdateTransaction.NO_OP);
        }

        private Slice[] slices()
        {
            return new Slice[]
            {
                slice(true, new int[] { 0, 1 }, true, new int[] { 1, 1 }),
                slice(true, new int[] { 0, 1 }, false, new int[] { 1, 1 }),
                slice(false, new int[] { 0, 1 }, true, new int[] { 1, 1 }),
                slice(false, new int[] { 0, 1 }, false, new int[] { 1, 1 }),
                Slice.make(BufferClusteringBound.BOTTOM, end(false, 0, 0)),
                slice(true, new int[] { 0, 0 }, true, new int[] { 0, 1 }),
                slice(true, new int[] { 0, 1 }, true, new int[] { 0, 2 }),
                slice(true, new int[] { 0, 2 }, true, new int[] { 1, 0 }),
                slice(true, new int[] { 1, 0 }, true, new int[] { 1, 1 }),
                slice(true, new int[] { 1, 1 }, true, new int[] { 1, 2 }),
                slice(true, new int[] { 1, 2 }, true, new int[] { 2, 0 }),
                slice(true, new int[] { 2 }, true, new int[] { 2 }),
                slice(true, new int[] { 2, 0 }, true, new int[] { 2, 1 }),
                slice(true, new int[] { 2, 1 }, true, new int[] { 2, 2 }),
                slice(true, new int[] { 2, 2 }, true, new int[] { 3, 0 }),
                Slice.make(start(false, 2), BufferClusteringBound.TOP)
            };
        }

        private Clustering<?>[] probes()
        {
            return new Clustering[]
            {
                clustering(-1, 0),
                clustering(0, 0),
                clustering(0, 1),
                clustering(0, 2),
                clustering(1, 0),
                clustering(1, 1),
                clustering(1, 2),
                clustering(2, 0),
                clustering(2, 1),
                clustering(2, 2),
                clustering(3, 0),
                clustering(3, 1)
            };
        }

        private RangeTombstone range(boolean startInclusive, int[] start, boolean endInclusive, int[] end, long timestamp)
        {
            return range(start(startInclusive, start), end(endInclusive, end), timestamp);
        }

        private RangeTombstone range(ClusteringBound<?> start, ClusteringBound<?> end, long timestamp)
        {
            return new RangeTombstone(Slice.make(start, end), DeletionTime.build(timestamp, 40));
        }

        private Slice slice(boolean startInclusive, int[] start, boolean endInclusive, int[] end)
        {
            return Slice.make(start(startInclusive, start), end(endInclusive, end));
        }

        private BufferClusteringBound start(boolean inclusive, int... values)
        {
            return inclusive ? BufferClusteringBound.inclusiveStartOf(buffers(values))
                             : BufferClusteringBound.exclusiveStartOf(buffers(values));
        }

        private BufferClusteringBound end(boolean inclusive, int... values)
        {
            return inclusive ? BufferClusteringBound.inclusiveEndOf(buffers(values))
                             : BufferClusteringBound.exclusiveEndOf(buffers(values));
        }

        private Clustering<?> clustering(int... values)
        {
            return Clustering.make(buffers(values));
        }

        private ByteBuffer[] buffers(int... values)
        {
            ByteBuffer[] buffers = new ByteBuffer[values.length];
            for (int i = 0; i < values.length; i++)
                buffers[i] = Int32Type.instance.decompose(values[i]);
            return buffers;
        }
    }
}
