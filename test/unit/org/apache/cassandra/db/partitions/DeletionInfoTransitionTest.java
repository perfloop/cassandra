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

import java.nio.ByteBuffer;
import java.util.Iterator;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.DeletionInfo;
import org.apache.cassandra.db.MutableDeletionInfo;
import org.apache.cassandra.db.RangeTombstone;
import org.apache.cassandra.db.Slice;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.index.transactions.UpdateTransaction;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.memory.ByteBufferCloner;
import org.apache.cassandra.utils.memory.HeapCloner;
import org.apache.cassandra.utils.memory.MemtableAllocator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class DeletionInfoTransitionTest
{
    private static final int BTREE_PREFIX = 4096;

    @BeforeClass
    public static void initialize()
    {
        DeletionInfoTransitionFixture.initialize();
    }

    /**
     * This intentionally exceeds the BTree deletion-prefix threshold. Each transition is checked
     * against an independently materialized MutableDeletionInfo oracle and against the retained,
     * published prefix from which the transition began.
     */
    @Test
    public void transitionsPreservePublishedBTreePrefixAt4096Ranges()
    {
        DeletionInfoTransitionFixture.State state = new DeletionInfoTransitionFixture.State(BTREE_PREFIX);
        assertTransition(state, "exact-bound adjacent",
                         state.exactBoundUpdate(DeletionInfoTransitionFixture.ExactBoundSeparation.ADJACENT, 0));
        assertTransition(state, "exact-bound opposite",
                         state.exactBoundUpdate(DeletionInfoTransitionFixture.ExactBoundSeparation.OPPOSITE, 1));
        assertTransition(state, "overlap", state.overlapUpdate(2));
        assertTransition(state, "out of order", state.outOfOrderUpdate(3));
        assertTransition(state, "partition delete", state.partitionDeleteUpdate(4));
    }

    @Test
    public void exactBoundOppositeTransitionPreservesSlicesSearchesAndCloneAt4096Ranges()
    {
        DeletionInfoTransitionFixture.State state = new DeletionInfoTransitionFixture.State(BTREE_PREFIX);
        state.reset();

        DeletionInfo published = state.partition.deletionInfo();
        MutableDeletionInfo expected = expectedAfter(state,
                                                      state.exactBoundUpdate(DeletionInfoTransitionFixture.ExactBoundSeparation.OPPOSITE, 0));
        state.apply(state.exactBoundUpdate(DeletionInfoTransitionFixture.ExactBoundSeparation.OPPOSITE, 0));

        DeletionInfo actual = state.partition.deletionInfo();
        assertDeletionInfo(expected, actual, state.metadata.comparator, "exact-bound opposite");
        assertDeletionInfo(state.canonicalPrefix(), published, state.metadata.comparator, "published prefix");
        assertSlicesAndSearches(expected, actual, state);

        TrackingCloner cloner = new TrackingCloner();
        DeletionInfo cloned = actual.clone(cloner);
        assertTrue("clone did not materialize any range boundaries", cloner.allocations > 0);
        assertDeletionInfo(expected, cloned, state.metadata.comparator, "ByteBufferCloner materialization");
    }

    @Test
    public void rangeTransitionAccountsAndRoundTripsThroughUnfilteredIteratorAt4096Ranges()
    {
        DeletionInfoTransitionFixture.State state = new DeletionInfoTransitionFixture.State(BTREE_PREFIX);
        state.reset();
        long before = state.allocator.onHeap().owns();
        state.apply(state.outOfOrderUpdate(0));
        long after = state.allocator.onHeap().owns();
        long expectedDeletionInfoDelta = state.partition.deletionInfo().unsharedHeapSize()
                                         - state.prefixDeletionInfo().unsharedHeapSize();
        assertTrue("range transition must retain non-negative heap accounting", after >= 0);
        assertEquals("range transition must account for its deletion-info heap delta",
                     expectedDeletionInfoDelta,
                     after - before);

        MemtableAllocator replayAllocator = state.newAllocator("deletion-info-transition-replay");
        AtomicBTreePartition replay = new AtomicBTreePartition(TableMetadataRef.forOfflineTools(state.metadata),
                                                                state.key,
                                                                replayAllocator);
        OpOrder order = new OpOrder();
        try (UnfilteredRowIterator iterator = state.partition.unfilteredIterator())
        {
            PartitionUpdate patch = PartitionUpdate.fromIterator(iterator, ColumnFilter.NONE);
            replay.addAll(patch, HeapCloner.instance, order.getCurrent(), UpdateTransaction.NO_OP);
        }

        assertDeletionInfo(state.partition.deletionInfo(),
                           replay.deletionInfo(),
                           state.metadata.comparator,
                           "unfiltered iterator round trip");
        assertTrue("unfiltered iterator replay must account for live partition data", replayAllocator.onHeap().owns() > 0);
    }

    private static void assertTransition(DeletionInfoTransitionFixture.State state, String description, PartitionUpdate update)
    {
        state.reset();
        DeletionInfo published = state.partition.deletionInfo();
        MutableDeletionInfo expected = expectedAfter(state, update);

        state.apply(update);

        assertDeletionInfo(expected, state.partition.deletionInfo(), state.metadata.comparator, description);
        assertDeletionInfo(state.canonicalPrefix(), published, state.metadata.comparator, description + " changed the published prefix");
    }

    private static MutableDeletionInfo expectedAfter(DeletionInfoTransitionFixture.State state, PartitionUpdate update)
    {
        MutableDeletionInfo expected = state.canonicalPrefix();
        expected.add(update.deletionInfo());
        return expected;
    }

    private static void assertSlicesAndSearches(DeletionInfo expected,
                                                DeletionInfo actual,
                                                DeletionInfoTransitionFixture.State state)
    {
        for (Slice slice : state.slices())
        {
            assertRangeIterators(expected.rangeIterator(slice, false), actual.rangeIterator(slice, false), state.metadata.comparator, "forward slice");
            assertRangeIterators(expected.rangeIterator(slice, true), actual.rangeIterator(slice, true), state.metadata.comparator, "reverse slice");
        }

        for (Clustering<?> probe : state.probes())
        {
            RangeTombstone expectedRange = expected.rangeCovering(probe);
            RangeTombstone actualRange = actual.rangeCovering(probe);
            if (expectedRange == null)
                assertNull("unexpected range covering " + probe, actualRange);
            else
            {
                assertNotNull("missing range covering " + probe, actualRange);
                assertRange(expectedRange, actualRange, state.metadata.comparator, "range covering " + probe);
            }
        }
    }

    private static void assertDeletionInfo(DeletionInfo expected,
                                           DeletionInfo actual,
                                           ClusteringComparator comparator,
                                           String description)
    {
        assertEquals(description + " partition deletion", expected.getPartitionDeletion(), actual.getPartitionDeletion());
        assertEquals(description + " range count", expected.rangeCount(), actual.rangeCount());
        assertRangeIterators(expected.rangeIterator(false), actual.rangeIterator(false), comparator, description);
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

    private static final class TrackingCloner extends ByteBufferCloner
    {
        private int allocations;

        @Override
        public ByteBuffer allocate(int size)
        {
            allocations++;
            return ByteBuffer.allocate(size);
        }

        @Override
        public boolean isContextAwareCloningSupported()
        {
            return false;
        }
    }
}
