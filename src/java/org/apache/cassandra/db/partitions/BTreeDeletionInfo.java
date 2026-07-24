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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringBound;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.ClusteringPrefix;
import org.apache.cassandra.db.DeletionInfo;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.MutableDeletionInfo;
import org.apache.cassandra.db.RangeTombstone;
import org.apache.cassandra.db.Slice;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.utils.AbstractIterator;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.btree.BTree;
import org.apache.cassandra.utils.memory.ByteBufferCloner;
import org.apache.cassandra.utils.memory.HeapCloner;

/**
 * An immutable deletion-info snapshot whose canonical range tombstones are held in a persistent BTree.
 *
 * <p>Updates use {@link org.apache.cassandra.db.RangeTombstoneList} to reconcile only the ranges surrounding
 * each incoming tombstone, then splice that canonical result into the tree. The tree itself is the published
 * representation; the temporary list is never retained by a snapshot.</p>
 */
final class BTreeDeletionInfo implements DeletionInfo
{
    private static final long EMPTY_SIZE = ObjectSizes.measure(new BTreeDeletionInfo(DeletionTime.LIVE,
                                                                                      null,
                                                                                      BTree.empty(),
                                                                                      0));
    private static final long RANGE_TOMBSTONE_SIZE = ObjectSizes.measure(new RangeTombstone(null, null));
    private static final long SLICE_SIZE = ObjectSizes.measure(Slice.ALL);

    private final DeletionTime partitionDeletion;
    private final ClusteringComparator comparator;
    private final Comparator<Object> rangeComparator;
    private final Object[] ranges;
    private final long rangesHeapSize;

    private BTreeDeletionInfo(DeletionTime partitionDeletion,
                              ClusteringComparator comparator,
                              Object[] ranges,
                              long rangesHeapSize)
    {
        this.partitionDeletion = partitionDeletion;
        this.comparator = comparator;
        this.rangeComparator = comparator == null ? null
                                                   : (left, right) -> comparator.compare(startOf(left), startOf(right));
        this.ranges = ranges;
        this.rangesHeapSize = rangesHeapSize;
    }

    /**
     * Reconciles an update into an immutable BTree-backed snapshot.
     *
     * <p>The package-private seam owns any range bounds it retains: mutable existing and update inputs are copied
     * before they can become part of the published snapshot. Range ordering is validated against the supplied
     * comparator before a BTree is built.</p>
     */
    static DeletionInfo merge(DeletionInfo existing, DeletionInfo update, ClusteringComparator comparator)
    {
        if (!(existing instanceof BTreeDeletionInfo) && existing.hasRanges())
            existing = existing.clone(HeapCloner.instance);
        update = update.clone(HeapCloner.instance);
        DeletionTime partitionDeletion = update.getPartitionDeletion().supersedes(existing.getPartitionDeletion())
                                          ? update.getPartitionDeletion()
                                          : existing.getPartitionDeletion();

        if (!existing.hasRanges())
        {
            if (!update.hasRanges())
                return partitionDeletion.equals(existing.getPartitionDeletion()) ? existing : update;

            return fromRanges(partitionDeletion, comparator, update.rangeIterator(false));
        }

        BTreeDeletionInfo result = from(existing, partitionDeletion, comparator);
        if (!update.hasRanges())
            return result;

        Iterator<RangeTombstone> updates = update.rangeIterator(false);
        while (updates.hasNext())
            result = result.add(updates.next());
        return result;
    }

    private static BTreeDeletionInfo from(DeletionInfo deletionInfo,
                                          DeletionTime partitionDeletion,
                                          ClusteringComparator comparator)
    {
        if (deletionInfo instanceof BTreeDeletionInfo)
        {
            BTreeDeletionInfo existing = (BTreeDeletionInfo) deletionInfo;
            if (!existing.comparator.equals(comparator))
                throw new IllegalArgumentException("Cannot merge deletion snapshots with different clustering comparators");

            if (existing.partitionDeletion.equals(partitionDeletion))
                return existing;

            // A range snapshot's comparator remains the ordering authority for every structurally
            // shared version. The updater only combines updates for one partition.
            return new BTreeDeletionInfo(partitionDeletion,
                                         existing.comparator,
                                         existing.ranges,
                                         existing.rangesHeapSize);
        }

        return fromRanges(partitionDeletion, comparator, deletionInfo.rangeIterator(false));
    }

    private static BTreeDeletionInfo fromRanges(DeletionTime partitionDeletion,
                                                ClusteringComparator comparator,
                                                Iterator<RangeTombstone> ranges)
    {
        long rangesHeapSize = 0;
        RangeTombstone previous = null;
        try (BTree.FastBuilder<RangeTombstone> builder = BTree.fastBuilder())
        {
            while (ranges.hasNext())
            {
                RangeTombstone range = ranges.next();
                validateRange(comparator, previous, range);
                builder.add(range);
                rangesHeapSize += unsharedHeapSize(range);
                previous = range;
            }
            return new BTreeDeletionInfo(partitionDeletion, comparator, builder.build(), rangesHeapSize);
        }
    }

    private static void validateRange(ClusteringComparator comparator, RangeTombstone previous, RangeTombstone range)
    {
        Slice slice = range.deletedSlice();
        if (Slice.isEmpty(comparator, slice.start(), slice.end()))
            throw new IllegalArgumentException("Range is empty for the supplied clustering comparator");

        if (previous == null)
            return;

        Slice previousSlice = previous.deletedSlice();
        if (comparator.compare(previousSlice.start(), slice.start()) >= 0)
            throw new IllegalArgumentException("Ranges are not strictly ordered for the supplied clustering comparator");
        if (comparator.compare(previousSlice.end(), slice.start()) > 0)
            throw new IllegalArgumentException("Ranges overlap for the supplied clustering comparator");
    }

    private BTreeDeletionInfo add(RangeTombstone addition)
    {
        List<RangeTombstone> affected = affectedRanges(addition);
        MutableDeletionInfo canonical = MutableDeletionInfo.live();
        for (RangeTombstone range : affected)
            canonical.add(range, comparator);
        canonical.add(addition, comparator);

        List<RangeTombstone> replacement = collect(canonical.rangeIterator(false));
        Object[] remaining = affected.isEmpty() ? ranges
                                                 : BTree.subtract(ranges, build(affected), rangeComparator);
        Object[] updated = BTree.update(remaining, build(replacement), rangeComparator);

        long replacementHeapSize = 0;
        for (RangeTombstone range : replacement)
            replacementHeapSize += unsharedHeapSize(range);

        long affectedHeapSize = 0;
        for (RangeTombstone range : affected)
            affectedHeapSize += unsharedHeapSize(range);

        return new BTreeDeletionInfo(partitionDeletion,
                                     comparator,
                                     updated,
                                     rangesHeapSize - affectedHeapSize + replacementHeapSize);
    }

    /**
     * Returns the local canonical-reconciliation window for one incoming interval. At most one
     * non-overlapping predecessor and successor are included, while all intersecting ranges are
     * retained so boundary splitting remains in RangeTombstoneList.
     */
    private List<RangeTombstone> affectedRanges(RangeTombstone addition)
    {
        if (BTree.isEmpty(ranges))
            return Collections.emptyList();

        int start = BTree.floorIndex(ranges, rangeComparator, addition.deletedSlice().start());
        if (start < 0)
            start = 0;

        List<RangeTombstone> affected = new ArrayList<>();
        Iterator<RangeTombstone> iterator = BTree.iterator(ranges, start, BTree.size(ranges) - 1, BTree.Dir.ASC);
        while (iterator.hasNext())
        {
            RangeTombstone range = iterator.next();
            if (comparator.compare(range.deletedSlice().start(), addition.deletedSlice().end()) > 0)
            {
                if (!affected.isEmpty())
                    affected.add(range);
                break;
            }
            affected.add(range);
        }
        return affected;
    }

    private static List<RangeTombstone> collect(Iterator<RangeTombstone> ranges)
    {
        List<RangeTombstone> collected = new ArrayList<>();
        while (ranges.hasNext())
            collected.add(ranges.next());
        return collected;
    }

    private static Object[] build(List<RangeTombstone> ranges)
    {
        if (ranges.isEmpty())
            return BTree.empty();

        try (BTree.FastBuilder<RangeTombstone> builder = BTree.fastBuilder())
        {
            for (RangeTombstone range : ranges)
                builder.add(range);
            return builder.build();
        }
    }

    private static ClusteringPrefix<?> startOf(Object value)
    {
        return value instanceof RangeTombstone
               ? ((RangeTombstone) value).deletedSlice().start()
               : (ClusteringPrefix<?>) value;
    }

    private static long unsharedHeapSize(RangeTombstone range)
    {
        Slice slice = range.deletedSlice();
        return RANGE_TOMBSTONE_SIZE
               + SLICE_SIZE
               + slice.start().unsharedHeapSize()
               + slice.end().unsharedHeapSize()
               + range.deletionTime().unsharedHeapSize();
    }

    @Override
    public boolean isLive()
    {
        return partitionDeletion.isLive() && BTree.isEmpty(ranges);
    }

    @Override
    public DeletionTime getPartitionDeletion()
    {
        return partitionDeletion;
    }

    @Override
    public Iterator<RangeTombstone> rangeIterator(boolean reversed)
    {
        return BTree.isEmpty(ranges) ? Collections.emptyIterator()
                                     : BTree.iterator(ranges, BTree.Dir.desc(reversed));
    }

    @Override
    public Iterator<RangeTombstone> rangeIterator(Slice slice, boolean reversed)
    {
        if (BTree.isEmpty(ranges))
            return Collections.emptyIterator();

        int start = startIndex(slice, reversed);
        if (start < 0)
            return Collections.emptyIterator();

        return new SlicedRangeIterator(BTree.iterator(ranges,
                                                       reversed ? 0 : start,
                                                       reversed ? start : BTree.size(ranges) - 1,
                                                       BTree.Dir.desc(reversed)),
                                        slice,
                                        reversed);
    }

    private int startIndex(Slice slice, boolean reversed)
    {
        if (!reversed)
        {
            if (slice.start().isBottom())
                return 0;

            int index = BTree.floorIndex(ranges, rangeComparator, slice.start());
            return index < 0 ? 0 : index;
        }

        if (slice.end().isTop())
            return BTree.size(ranges) - 1;

        return BTree.floorIndex(ranges, rangeComparator, slice.end());
    }

    @Override
    public RangeTombstone rangeCovering(Clustering<?> name)
    {
        RangeTombstone range = (RangeTombstone) BTree.floor(ranges, rangeComparator, (Object) name);
        if (range == null)
            return null;

        return comparator.compare(name, range.deletedSlice().start()) >= 0
               && comparator.compare(name, range.deletedSlice().end()) < 0 ? range : null;
    }

    @Override
    public void collectStats(EncodingStats.Collector collector)
    {
        collector.update(partitionDeletion);
        Iterator<RangeTombstone> iterator = rangeIterator(false);
        while (iterator.hasNext())
            collector.update(iterator.next().deletionTime());
    }

    @Override
    public int dataSize()
    {
        int size = TypeSizes.sizeof(partitionDeletion.markedForDeleteAt());
        if (hasRanges())
            size += TypeSizes.sizeof(rangeCount());

        Iterator<RangeTombstone> iterator = rangeIterator(false);
        while (iterator.hasNext())
        {
            RangeTombstone range = iterator.next();
            size += range.deletedSlice().start().dataSize();
            size += range.deletedSlice().end().dataSize();
            size += range.deletionTime().dataSize();
        }
        return size;
    }

    @Override
    public boolean hasRanges()
    {
        return !BTree.isEmpty(ranges);
    }

    @Override
    public int rangeCount()
    {
        return BTree.size(ranges);
    }

    @Override
    public long maxTimestamp()
    {
        long maxTimestamp = partitionDeletion.markedForDeleteAt();
        Iterator<RangeTombstone> iterator = rangeIterator(false);
        while (iterator.hasNext())
            maxTimestamp = Math.max(maxTimestamp, iterator.next().deletionTime().markedForDeleteAt());
        return maxTimestamp;
    }

    @Override
    public boolean mayModify(DeletionInfo deletionInfo)
    {
        return partitionDeletion.compareTo(deletionInfo.getPartitionDeletion()) > 0 || hasRanges();
    }

    @Override
    public MutableDeletionInfo mutableCopy()
    {
        MutableDeletionInfo copy = new MutableDeletionInfo(partitionDeletion);
        Iterator<RangeTombstone> iterator = rangeIterator(false);
        while (iterator.hasNext())
            copy.add(iterator.next(), comparator);
        return copy;
    }

    @Override
    public DeletionInfo clone(ByteBufferCloner cloner)
    {
        List<RangeTombstone> copy = new ArrayList<>(rangeCount());
        Iterator<RangeTombstone> iterator = rangeIterator(false);
        while (iterator.hasNext())
        {
            RangeTombstone range = iterator.next();
            Slice slice = range.deletedSlice();
            copy.add(new RangeTombstone(Slice.make(slice.start().clone(cloner), slice.end().clone(cloner)),
                                        range.deletionTime()));
        }
        return fromRanges(partitionDeletion, comparator, copy.iterator());
    }

    @Override
    public long unsharedHeapSize()
    {
        if (isLive())
            return 0;

        return EMPTY_SIZE
               + partitionDeletion.unsharedHeapSize()
               + BTree.sizeOnHeapOf(ranges)
               + rangesHeapSize;
    }

    private final class SlicedRangeIterator extends AbstractIterator<RangeTombstone>
    {
        private final Iterator<RangeTombstone> iterator;
        private final Slice slice;
        private final boolean reversed;
        private boolean first = true;
        private boolean done;

        private SlicedRangeIterator(Iterator<RangeTombstone> iterator, Slice slice, boolean reversed)
        {
            this.iterator = iterator;
            this.slice = slice;
            this.reversed = reversed;
        }

        @Override
        protected RangeTombstone computeNext()
        {
            if (done)
                return endOfData();

            return reversed ? computeReverse() : computeForward();
        }

        private RangeTombstone computeForward()
        {
            while (iterator.hasNext())
            {
                RangeTombstone range = iterator.next();
                Slice rangeSlice = range.deletedSlice();
                ClusteringBound<?> start = rangeSlice.start();
                ClusteringBound<?> end = rangeSlice.end();

                // startIndex positions the iterator at the only range that can precede slice.start().
                // Once it is consumed, canonical non-overlapping ranges all begin at or after slice.start().
                boolean clippedStart = false;
                if (first)
                {
                    first = false;
                    if (comparator.compare(end, slice.start()) <= 0)
                        continue;
                    if (comparator.compare(start, slice.start()) < 0)
                    {
                        start = slice.start();
                        clippedStart = true;
                    }
                }

                if (comparator.compare(start, slice.end()) >= 0)
                    return endOfData();

                if (comparator.compare(slice.end(), end) < 0)
                {
                    done = true;
                    return clipped(range, start, slice.end());
                }

                return clippedStart ? clipped(range, start, end) : range;
            }
            return endOfData();
        }

        private RangeTombstone computeReverse()
        {
            while (iterator.hasNext())
            {
                RangeTombstone range = iterator.next();
                Slice rangeSlice = range.deletedSlice();
                ClusteringBound<?> start = rangeSlice.start();
                ClusteringBound<?> end = rangeSlice.end();

                // startIndex positions the iterator at the only range that can extend past slice.end().
                // Once it is consumed, canonical non-overlapping ranges all end at or before slice.end().
                boolean clippedEnd = false;
                if (first)
                {
                    first = false;
                    if (comparator.compare(start, slice.end()) >= 0)
                        continue;
                    if (comparator.compare(slice.end(), end) < 0)
                    {
                        end = slice.end();
                        clippedEnd = true;
                    }
                }

                if (comparator.compare(end, slice.start()) <= 0)
                    return endOfData();

                if (comparator.compare(start, slice.start()) < 0)
                {
                    done = true;
                    return clipped(range, slice.start(), end);
                }

                return clippedEnd ? clipped(range, start, end) : range;
            }
            return endOfData();
        }

        private RangeTombstone clipped(RangeTombstone range, ClusteringBound<?> start, ClusteringBound<?> end)
        {
            if (Slice.isEmpty(comparator, start, end))
                return endOfData();

            Slice rangeSlice = range.deletedSlice();
            if (start == rangeSlice.start() && end == rangeSlice.end())
                return range;
            return new RangeTombstone(Slice.make(start, end), range.deletionTime());
        }
    }
}
