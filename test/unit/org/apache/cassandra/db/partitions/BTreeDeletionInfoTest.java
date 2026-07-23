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

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
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
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.utils.memory.ByteBufferCloner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.fail;

public class BTreeDeletionInfoTest
{
    private static final ClusteringComparator COMPARATOR = new ClusteringComparator(Int32Type.instance);
    private static final ClusteringComparator REVERSED_COMPARATOR = new ClusteringComparator(ReversedType.getInstance(Int32Type.instance));

    @BeforeClass
    public static void beforeClass()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Test(expected = IllegalArgumentException.class)
    public void mergeRejectsAMutableInputWithADifferentClusteringComparator()
    {
        BTreeDeletionInfo.merge(deletionInfo(COMPARATOR, 2), MutableDeletionInfo.live(), REVERSED_COMPARATOR);
    }

    @Test
    public void mergeRejectsUnknownRangeBearingInputsBeforeIteration()
    {
        MutableDeletionInfo source = deletionInfo(REVERSED_COMPARATOR, 1);
        ForwardingDeletionInfo existing = new ForwardingDeletionInfo(source);
        try
        {
            BTreeDeletionInfo.merge(existing, MutableDeletionInfo.live(), COMPARATOR);
            fail("expected an unknown range-bearing existing input to be rejected");
        }
        catch (IllegalArgumentException expected)
        {
        }
        assertFalse(existing.rangeIteratorCalled);

        BTreeDeletionInfo base = BTreeDeletionInfo.merge(deletionInfo(COMPARATOR, 1), MutableDeletionInfo.live(), COMPARATOR);
        ForwardingDeletionInfo update = new ForwardingDeletionInfo(source);
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

    @Test
    public void mutableDeletionInfoRejectsDifferentClusteringComparatorsBeforeMutation()
    {
        MutableDeletionInfo receiver = deletionInfo(COMPARATOR, 2);
        int dataSize = receiver.dataSize();
        try
        {
            receiver.add(range(0, 3, 200), REVERSED_COMPARATOR);
            fail("expected a range with another clustering comparator to be rejected");
        }
        catch (IllegalArgumentException expected)
        {
        }
        assertEquals(dataSize, receiver.dataSize());

        BTreeDeletionInfo other = BTreeDeletionInfo.merge(deletionInfo(REVERSED_COMPARATOR, 2),
                                                           MutableDeletionInfo.live(),
                                                           REVERSED_COMPARATOR);
        try
        {
            receiver.add(other);
            fail("expected a BTree with another clustering comparator to be rejected");
        }
        catch (IllegalArgumentException expected)
        {
        }
        assertEquals(dataSize, receiver.dataSize());
    }

    @Test
    public void exactBoundMergePreservesExistingRangeAtAnEqualTimestamp()
    {
        MutableDeletionInfo expected = deletionInfo(COMPARATOR, 1);
        MutableDeletionInfo update = MutableDeletionInfo.live();
        update.add(new RangeTombstone(expected.rangeIterator(false).next().deletedSlice(), DeletionTime.build(100, 20)), COMPARATOR);
        expected.add(update);

        BTreeDeletionInfo actual = BTreeDeletionInfo.merge(deletionInfo(COMPARATOR, 1), MutableDeletionInfo.live(), COMPARATOR);
        actual = BTreeDeletionInfo.merge(actual, update, COMPARATOR);
        assertEquals(expected.rangeIterator(false).next().deletionTime(), actual.rangeIterator(false).next().deletionTime());
    }

    @Test
    public void materializingGenericDeletionInfoOwnsRangeBoundBuffers()
    {
        ByteBuffer start = integer(10);
        MutableDeletionInfo generic = MutableDeletionInfo.live();
        generic.add(new RangeTombstone(Slice.make(BufferClusteringBound.inclusiveStartOf(start),
                                                   BufferClusteringBound.inclusiveEndOf(integer(20))),
                                       DeletionTime.build(1, 1)), COMPARATOR);

        BTreeDeletionInfo materialized = BTreeDeletionInfo.merge(generic, MutableDeletionInfo.live(), COMPARATOR);
        ByteBuffer snapshotStart = (ByteBuffer) materialized.rangeIterator(false).next().deletedSlice().start().get(0);
        assertNotSame(start, snapshotStart);

        start.putInt(0, 11);
        assertEquals(10, snapshotStart.getInt(0));
    }

    @Test
    public void boundedIterationMatchesMutableClippingAndDirection()
    {
        MutableDeletionInfo expected = deletionInfo(COMPARATOR, 4);
        BTreeDeletionInfo actual = BTreeDeletionInfo.merge(expected, MutableDeletionInfo.live(), COMPARATOR);
        Slice[] slices = new Slice[]{
        slice(1, true, 18, false),
        slice(3, false, 19, true),
        slice(4, true, 27, false),
        slice(8, true, 26, true)
        };
        for (Slice slice : slices)
        {
            assertIteratorEquivalent(expected.rangeIterator(slice, false), actual.rangeIterator(slice, false));
            assertIteratorEquivalent(expected.rangeIterator(slice, true), actual.rangeIterator(slice, true));
        }
    }

    @Test
    public void materializedBTreeDoesNotTrackLaterMutableRanges()
    {
        MutableDeletionInfo generic = deletionInfo(COMPARATOR, 1);
        BTreeDeletionInfo materialized = BTreeDeletionInfo.merge(generic, MutableDeletionInfo.live(), COMPARATOR);

        generic.add(range(16, 19, 200), COMPARATOR);
        assertEquals(1, materialized.rangeCount());
    }

    @Test
    public void mutableMaterializationDoesNotMutatePersistentBTree()
    {
        BTreeDeletionInfo persistent = BTreeDeletionInfo.merge(deletionInfo(COMPARATOR, 1), MutableDeletionInfo.live(), COMPARATOR);
        MutableDeletionInfo materialized = MutableDeletionInfo.live();
        materialized.add(persistent);

        materialized.add(range(16, 19, 200), COMPARATOR);
        assertEquals(1, persistent.rangeCount());
        assertEquals(2, materialized.rangeCount());
    }

    @Test
    public void materializedBTreeMatchesMutableDataSize()
    {
        assertMaterializedDataSize(deletionInfo(COMPARATOR, 1));
        assertMaterializedDataSize(deletionInfo(COMPARATOR, 2));

        MutableDeletionInfo overlap = deletionInfo(COMPARATOR, 2);
        overlap.add(range(1, 10, 200), COMPARATOR);
        assertMaterializedDataSize(overlap);
    }

    private static void assertMaterializedDataSize(MutableDeletionInfo mutable)
    {
        BTreeDeletionInfo materialized = BTreeDeletionInfo.merge(mutable, MutableDeletionInfo.live(), COMPARATOR);
        assertEquals(mutable.dataSize(), materialized.dataSize());
    }

    private static void assertIteratorEquivalent(Iterator<RangeTombstone> expected, Iterator<RangeTombstone> actual)
    {
        while (expected.hasNext() && actual.hasNext())
            assertEquals(expected.next(), actual.next());
        assertEquals(expected.hasNext(), actual.hasNext());
    }

    private static Slice slice(int start, boolean startInclusive, int end, boolean endInclusive)
    {
        return Slice.make(startInclusive ? BufferClusteringBound.inclusiveStartOf(integer(start))
                                         : BufferClusteringBound.exclusiveStartOf(integer(start)),
                          endInclusive ? BufferClusteringBound.inclusiveEndOf(integer(end))
                                       : BufferClusteringBound.exclusiveEndOf(integer(end)));
    }

    private static MutableDeletionInfo deletionInfo(ClusteringComparator comparator, int count)
    {
        MutableDeletionInfo info = MutableDeletionInfo.live();
        for (int i = 0; i < count; i++)
            info.add(range(i * 8, i * 8 + 3, 100), comparator);
        return info;
    }

    private static RangeTombstone range(int start, int end, long timestamp)
    {
        return new RangeTombstone(Slice.make(BufferClusteringBound.inclusiveStartOf(integer(start)),
                                             BufferClusteringBound.inclusiveEndOf(integer(end))),
                                  DeletionTime.build(timestamp, 10));
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
        public RangeTombstone rangeCovering(Clustering<?> name)
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
