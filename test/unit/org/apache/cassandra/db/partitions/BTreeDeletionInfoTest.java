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
import java.util.Iterator;

import org.junit.Test;

import org.apache.cassandra.db.BTreeDeletionInfo;
import org.apache.cassandra.db.BufferClusteringBound;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.DeletionInfo;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.MutableDeletionInfo;
import org.apache.cassandra.db.RangeTombstone;
import org.apache.cassandra.db.Slice;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.ReversedType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.test.microbench.partitions.DeletionInfoTransitionSupport;
import org.apache.cassandra.utils.memory.ByteBufferCloner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.fail;

public class BTreeDeletionInfoTest
{
    private static final ClusteringComparator COMPARATOR = new ClusteringComparator(Int32Type.instance);
    private static final ClusteringComparator OTHER_COMPARATOR = new ClusteringComparator(UTF8Type.instance);
    private static final ClusteringComparator REVERSED_COMPARATOR = new ClusteringComparator(ReversedType.getInstance(Int32Type.instance));

    @Test(expected = IllegalArgumentException.class)
    public void mergeRejectsAMutableInputWithADifferentClusteringComparator()
    {
        DeletionInfoTransitionSupport.Transition transition = DeletionInfoTransitionSupport.fixture(128, "ADJACENT")
                                                                                  .transitions(DeletionInfoTransitionSupport.Operation.TWO_EXACT_BOUND)[0];
        BTreeDeletionInfo.merge(transition.expectedPrefix().mutableCopy(),
                                MutableDeletionInfo.live(),
                                OTHER_COMPARATOR);
    }

    @Test(expected = IllegalArgumentException.class)
    public void mergeRejectsAMutableUpdateWithADifferentClusteringComparator()
    {
        DeletionInfoTransitionSupport.Transition transition = DeletionInfoTransitionSupport.fixture(128, "ADJACENT")
                                                                                  .transitions(DeletionInfoTransitionSupport.Operation.TWO_EXACT_BOUND)[0];
        BTreeDeletionInfo existing = BTreeDeletionInfo.merge(transition.expectedPrefix(), MutableDeletionInfo.live(), COMPARATOR);
        MutableDeletionInfo update = MutableDeletionInfo.live();
        RangeTombstone range = transition.expectedPrefix().rangeIterator(false).next();
        update.add(range, OTHER_COMPARATOR);
        BTreeDeletionInfo.merge(existing, update, COMPARATOR);
    }

    @Test
    public void mergeRejectsUnknownRangeBearingInputsBeforeIteration()
    {
        DeletionInfoTransitionSupport.Transition transition = DeletionInfoTransitionSupport.fixture(128, "ADJACENT")
                                                                                  .transitions(DeletionInfoTransitionSupport.Operation.TWO_EXACT_BOUND)[0];
        MutableDeletionInfo mutable = MutableDeletionInfo.live();
        mutable.add(transition.expectedPrefix().rangeIterator(false).next(), OTHER_COMPARATOR);

        ForwardingDeletionInfo existing = new ForwardingDeletionInfo(mutable);
        try
        {
            BTreeDeletionInfo.merge(existing, MutableDeletionInfo.live(), COMPARATOR);
            fail("expected an unknown range-bearing existing input to be rejected");
        }
        catch (IllegalArgumentException expected)
        {
        }
        assertFalse(existing.rangeIteratorCalled);

        BTreeDeletionInfo base = BTreeDeletionInfo.merge(transition.expectedPrefix(), MutableDeletionInfo.live(), COMPARATOR);
        ForwardingDeletionInfo update = new ForwardingDeletionInfo(mutable);
        try
        {
            BTreeDeletionInfo.merge(base, update, COMPARATOR);
            fail("expected an unknown range-bearing update input to be rejected");
        }
        catch (IllegalArgumentException expected)
        {
        }
        assertFalse(update.rangeIteratorCalled);
    }

    @Test(expected = IllegalArgumentException.class)
    public void repeatedMergeRejectsADifferentClusteringComparator()
    {
        DeletionInfoTransitionSupport.Transition transition = DeletionInfoTransitionSupport.fixture(128, "ADJACENT")
                                                                                  .transitions(DeletionInfoTransitionSupport.Operation.TWO_EXACT_BOUND)[0];
        BTreeDeletionInfo materialized = BTreeDeletionInfo.merge(transition.expectedPrefix(), MutableDeletionInfo.live(), COMPARATOR);
        BTreeDeletionInfo.merge(materialized,
                                MutableDeletionInfo.live(),
                                OTHER_COMPARATOR);
    }

    @Test(expected = IllegalArgumentException.class)
    public void mergeRejectsABTreeUpdateWithADifferentClusteringComparator()
    {
        DeletionInfoTransitionSupport.Transition transition = DeletionInfoTransitionSupport.fixture(128, "ADJACENT")
                                                                                  .transitions(DeletionInfoTransitionSupport.Operation.TWO_EXACT_BOUND)[0];
        BTreeDeletionInfo existing = BTreeDeletionInfo.merge(transition.expectedPrefix(), MutableDeletionInfo.live(), COMPARATOR);
        MutableDeletionInfo otherInput = MutableDeletionInfo.live();
        otherInput.add(transition.expectedPrefix().rangeIterator(false).next(), OTHER_COMPARATOR);
        BTreeDeletionInfo update = BTreeDeletionInfo.merge(otherInput, MutableDeletionInfo.live(), OTHER_COMPARATOR);
        BTreeDeletionInfo.merge(existing, update, COMPARATOR);
    }

    @Test
    public void mutableDeletionInfoRejectsARangeWithADifferentClusteringComparator()
    {
        DeletionInfoTransitionSupport.Transition transition = DeletionInfoTransitionSupport.fixture(128, "ADJACENT")
                                                                                  .transitions(DeletionInfoTransitionSupport.Operation.TWO_EXACT_BOUND)[0];
        MutableDeletionInfo receiver = transition.expectedPrefix().mutableCopy();
        DeletionInfo before = receiver.mutableCopy();
        RangeTombstone range = transition.expectedPrefix().rangeIterator(false).next();

        try
        {
            receiver.add(range, REVERSED_COMPARATOR);
            fail("expected a range with another clustering comparator to be rejected");
        }
        catch (IllegalArgumentException expected)
        {
        }
        DeletionInfoTransitionSupport.assertEquivalent(before, receiver);
    }

    @Test
    public void mutableDeletionInfoRejectsABTreeWithADifferentClusteringComparator()
    {
        DeletionInfoTransitionSupport.Transition transition = DeletionInfoTransitionSupport.fixture(128, "ADJACENT")
                                                                                  .transitions(DeletionInfoTransitionSupport.Operation.TWO_EXACT_BOUND)[0];
        MutableDeletionInfo receiver = transition.expectedPrefix().mutableCopy();
        MutableDeletionInfo reversed = MutableDeletionInfo.live();
        Iterator<RangeTombstone> ranges = transition.expectedPrefix().rangeIterator(false);
        reversed.add(ranges.next(), REVERSED_COMPARATOR);
        reversed.add(ranges.next(), REVERSED_COMPARATOR);
        BTreeDeletionInfo other = BTreeDeletionInfo.merge(reversed, MutableDeletionInfo.live(), REVERSED_COMPARATOR);
        DeletionInfo before = receiver.mutableCopy();

        try
        {
            receiver.add(other);
            fail("expected a BTree with another clustering comparator to be rejected");
        }
        catch (IllegalArgumentException expected)
        {
        }
        DeletionInfoTransitionSupport.assertEquivalent(before, receiver);
    }

    @Test
    public void materializingGenericDeletionInfoDoesNotRetainMutableState()
    {
        DeletionInfoTransitionSupport.Transition transition = DeletionInfoTransitionSupport.fixture(4096, "OPPOSITE")
                                                                                  .transitions(DeletionInfoTransitionSupport.Operation.TWO_EXACT_BOUND)[0];
        MutableDeletionInfo generic = transition.expectedPrefix().mutableCopy();
        DeletionInfo materialized = BTreeDeletionInfo.merge(generic, MutableDeletionInfo.live(), COMPARATOR);

        DeletionInfoTransitionSupport.assertEquivalent(generic, materialized);
        DeletionInfoTransitionSupport.addDisjointRange(generic, transition.offset(), transition.rangeCount());
        DeletionInfoTransitionSupport.assertEquivalent(transition.expectedPrefix(), materialized);

        DeletionInfo cloned = DeletionInfoTransitionSupport.deepClone(materialized);
        DeletionInfoTransitionSupport.assertEquivalent(materialized, cloned);
        assertNotSame("clone must own generic-materialization clustering-bound bytes",
                      DeletionInfoTransitionSupport.firstStartBuffer(materialized),
                      DeletionInfoTransitionSupport.firstStartBuffer(cloned));
    }

    @Test
    public void materializingGenericDeletionInfoOwnsRangeBoundBuffers()
    {
        ByteBuffer start = integer(10);
        ByteBuffer end = integer(20);
        MutableDeletionInfo generic = MutableDeletionInfo.live();
        generic.add(new RangeTombstone(Slice.make(BufferClusteringBound.inclusiveStartOf(start),
                                                  BufferClusteringBound.inclusiveEndOf(end)),
                                       DeletionTime.build(1, 1)), COMPARATOR);

        BTreeDeletionInfo materialized = BTreeDeletionInfo.merge(generic, MutableDeletionInfo.live(), COMPARATOR);
        RangeTombstone snapshot = materialized.rangeIterator(false).next();
        ByteBuffer snapshotStart = (ByteBuffer) snapshot.deletedSlice().start().get(0);
        assertNotSame(start, snapshotStart);

        start.putInt(0, 11);
        assertEquals(10, snapshotStart.getInt(0));
    }

    @Test
    public void mutableDeletionInfoMaterializesABTreeWithoutRetainingItsSnapshot()
    {
        DeletionInfoTransitionSupport.Transition transition = DeletionInfoTransitionSupport.fixture(128, "ADJACENT")
                                                                                  .transitions(DeletionInfoTransitionSupport.Operation.TWO_EXACT_BOUND)[0];
        BTreeDeletionInfo persistent = BTreeDeletionInfo.merge(transition.expectedPrefix(), MutableDeletionInfo.live(), COMPARATOR);
        MutableDeletionInfo materialized = MutableDeletionInfo.live();
        materialized.add(persistent);

        DeletionInfoTransitionSupport.assertEquivalent(persistent, materialized);
        DeletionInfoTransitionSupport.addDisjointRange(materialized, transition.offset(), transition.rangeCount());
        DeletionInfoTransitionSupport.assertEquivalent(transition.expectedPrefix(), persistent);
    }

    @Test
    public void pointLookupPatchesRemainCorrectAcrossCompaction()
    {
        DeletionInfoTransitionSupport.Transition transition = DeletionInfoTransitionSupport.fixture(4096, "OPPOSITE")
                                                                                   .transitions(DeletionInfoTransitionSupport.Operation.TWO_EXACT_BOUND)[0];
        MutableDeletionInfo expected = transition.expectedPrefix().mutableCopy();
        BTreeDeletionInfo actual = BTreeDeletionInfo.merge(expected, MutableDeletionInfo.live(), COMPARATOR);
        Iterator<RangeTombstone> ranges = transition.expectedPrefix().rangeIterator(false);
        RangeTombstone first = ranges.next();
        for (int i = 0; i < 33; i++)
        {
            RangeTombstone range = i < 2 ? first : ranges.next();
            MutableDeletionInfo update = MutableDeletionInfo.live();
            update.add(new RangeTombstone(range.deletedSlice(), DeletionTime.build(200 + i, 20 + i)), COMPARATOR);
            expected.add(update);
            actual = BTreeDeletionInfo.merge(actual, update, COMPARATOR);
            assertPointLookupsEquivalent(expected, actual, 33);
        }

        DeletionInfoTransitionSupport.assertEquivalent(expected, actual);
    }

    @Test
    public void arbitraryMultiRangeSlicesPreserveClippingAndDirection()
    {
        DeletionInfoTransitionSupport.Fixture fixture = DeletionInfoTransitionSupport.fixture(4096, "OPPOSITE");
        DeletionInfoTransitionSupport.Transition transition = fixture.transitions(DeletionInfoTransitionSupport.Operation.TWO_EXACT_BOUND)[0];
        fixture.prepare(transition);
        fixture.applyPrepared(transition);
        DeletionInfo actual = fixture.deletionInfo();
        int first = transition.offset();

        Slice[] slices = new Slice[]{
        slice(first + 1, true, first + 18, false),
        slice(first + 3, false, first + 19, true),
        slice(first + 4, true, first + 27, false),
        slice(first + 8, true, first + 26, true)
        };
        for (Slice slice : slices)
        {
            DeletionInfoTransitionSupport.assertIteratorEquivalent(transition.expected(), actual, slice, false);
            DeletionInfoTransitionSupport.assertIteratorEquivalent(transition.expected(), actual, slice, true);
        }
    }

    private static void assertPointLookupsEquivalent(DeletionInfo expected, DeletionInfo actual, int count)
    {
        for (int i = 0; i < count; i++)
        {
            Clustering<?> covered = Clustering.make(integer(i * 8 + 1));
            Clustering<?> uncovered = Clustering.make(integer(i * 8 + 5));
            DeletionInfoTransitionSupport.assertRangeEquivalent(expected.rangeCovering(covered), actual.rangeCovering(covered));
            DeletionInfoTransitionSupport.assertRangeEquivalent(expected.rangeCovering(uncovered), actual.rangeCovering(uncovered));
        }
    }

    private static Slice slice(int start, boolean startInclusive, int end, boolean endInclusive)
    {
        return Slice.make(startInclusive ? BufferClusteringBound.inclusiveStartOf(integer(start))
                                         : BufferClusteringBound.exclusiveStartOf(integer(start)),
                          endInclusive ? BufferClusteringBound.inclusiveEndOf(integer(end))
                                       : BufferClusteringBound.exclusiveEndOf(integer(end)));
    }

    private static ByteBuffer integer(int value)
    {
        return Int32Type.instance.decompose(value);
    }

    private static class ForwardingDeletionInfo implements DeletionInfo
    {
        private final DeletionInfo delegate;
        private boolean rangeIteratorCalled;

        private ForwardingDeletionInfo(DeletionInfo delegate)
        {
            this.delegate = delegate;
        }

        @Override
        public boolean isLive()
        {
            return delegate.isLive();
        }

        @Override
        public DeletionTime getPartitionDeletion()
        {
            return delegate.getPartitionDeletion();
        }

        @Override
        public Iterator<RangeTombstone> rangeIterator(boolean reversed)
        {
            rangeIteratorCalled = true;
            return delegate.rangeIterator(reversed);
        }

        @Override
        public Iterator<RangeTombstone> rangeIterator(Slice slice, boolean reversed)
        {
            rangeIteratorCalled = true;
            return delegate.rangeIterator(slice, reversed);
        }

        @Override
        public RangeTombstone rangeCovering(org.apache.cassandra.db.Clustering<?> name)
        {
            return delegate.rangeCovering(name);
        }

        @Override
        public void collectStats(EncodingStats.Collector collector)
        {
            delegate.collectStats(collector);
        }

        @Override
        public int dataSize()
        {
            return delegate.dataSize();
        }

        @Override
        public boolean hasRanges()
        {
            return delegate.hasRanges();
        }

        @Override
        public int rangeCount()
        {
            return delegate.rangeCount();
        }

        @Override
        public long maxTimestamp()
        {
            return delegate.maxTimestamp();
        }

        @Override
        public boolean mayModify(DeletionInfo deletionInfo)
        {
            return delegate.mayModify(deletionInfo);
        }

        @Override
        public MutableDeletionInfo mutableCopy()
        {
            return delegate.mutableCopy();
        }

        @Override
        public DeletionInfo clone(ByteBufferCloner cloner)
        {
            return delegate.clone(cloner);
        }

        @Override
        public long unsharedHeapSize()
        {
            return delegate.unsharedHeapSize();
        }
    }
}
