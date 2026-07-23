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
package org.apache.cassandra.db;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import com.google.common.collect.Iterators;

import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.utils.AbstractIterator;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.btree.BTree;
import org.apache.cassandra.utils.btree.BTreeRemoval;
import org.apache.cassandra.utils.btree.UpdateFunction;
import org.apache.cassandra.utils.memory.ByteBufferCloner;
import org.apache.cassandra.utils.memory.HeapCloner;

/**
 * Immutable deletion information backed by a persistent BTree of canonical range tombstones.
 *
 * A version keeps the partition deletion and range tree together.  Range updates reconcile only
 * the existing ranges that overlap each incoming range, then replace that bounded portion of the
 * tree.  This preserves the canonical {@link RangeTombstoneList} reconciliation while allowing
 * unaffected branches to remain shared with the published version.
 */
public final class BTreeDeletionInfo implements DeletionInfo
{
    private static final long EMPTY_SIZE = ObjectSizes.measure(new BTreeDeletionInfo(DeletionTime.LIVE, null, null, null, BTree.empty(), null, 0, 0));
    private static final long RANGE_TOMBSTONE_SIZE = ObjectSizes.measure(new RangeTombstone(Slice.ALL, DeletionTime.LIVE));
    private static final long SLICE_SIZE = ObjectSizes.measure(Slice.ALL);
    private static final long RANGE_LOOKUP_SIZE = ObjectSizes.measure(new RangeLookup(0));
    private static final UpdateFunction<RangeTombstone, RangeTombstone> RANGE_UPDATE_FUNCTION = new UpdateFunction<RangeTombstone, RangeTombstone>()
    {
        @Override
        public RangeTombstone insert(RangeTombstone range)
        {
            return range;
        }

        @Override
        public RangeTombstone merge(RangeTombstone existing, RangeTombstone update)
        {
            return update;
        }

        @Override
        public void onAllocatedOnHeap(long heapSize)
        {
        }
    };

    private final DeletionTime partitionDeletion;
    private final ClusteringComparator comparator;
    private final Comparator<Object> rangeComparator;
    private final Comparator<Object> lookupComparator;
    private final Object[] ranges;
    private final RangeLookup rangeLookup;
    private final long rangeHeapSize;
    private final long treeHeapSize;
    private final long unsharedHeapSize;

    private BTreeDeletionInfo(DeletionTime partitionDeletion,
                              ClusteringComparator comparator,
                              Comparator<Object> rangeComparator,
                              Comparator<Object> lookupComparator,
                              Object[] ranges,
                              RangeLookup rangeLookup,
                              long rangeHeapSize,
                              long treeHeapSize)
    {
        this.partitionDeletion = partitionDeletion;
        this.comparator = comparator;
        this.rangeComparator = rangeComparator;
        this.lookupComparator = lookupComparator;
        this.ranges = ranges;
        this.rangeLookup = rangeLookup;
        this.rangeHeapSize = rangeHeapSize;
        this.treeHeapSize = treeHeapSize;
        this.unsharedHeapSize = EMPTY_SIZE + partitionDeletion.unsharedHeapSize() + rangeHeapSize + treeHeapSize;
    }

    /**
     * Merge {@code update} into {@code existing}, retaining the existing tree outside the affected
     * intervals. The caller has already cloned the update's buffers for long-term retention. A
     * mutable existing input is materialized with owned range-bound buffers. Range-bearing inputs
     * other than the known mutable and BTree implementations are rejected because {@code DeletionInfo}
     * does not expose their ordering comparator. When either input is BTree-backed, or a mutable input
     * has ordered ranges, {@code comparator} must compare the same clustering columns as the comparator
     * that ordered those ranges.
     */
    public static BTreeDeletionInfo merge(DeletionInfo existing, DeletionInfo update, ClusteringComparator comparator)
    {
        requireMatchingComparator(existing, comparator);
        requireMatchingComparator(update, comparator);

        BTreeDeletionInfo base = existing instanceof BTreeDeletionInfo
                                 ? (BTreeDeletionInfo) existing
                                 : copyOf(existing, comparator);

        DeletionTime partitionDeletion = update.getPartitionDeletion().supersedes(base.partitionDeletion)
                                         ? update.getPartitionDeletion()
                                         : base.partitionDeletion;
        RangeTree merged = new RangeTree(base.ranges, base.rangeHeapSize);
        RangeLookup rangeLookup = base.rangeLookup;
        Iterator<RangeTombstone> updates = update.rangeIterator(false);
        while (updates.hasNext())
        {
            MergeResult result = mergeRange(merged,
                                            updates.next(),
                                            base.comparator,
                                            base.rangeComparator,
                                            base.lookupComparator,
                                            rangeLookup);
            merged = result.ranges;
            rangeLookup = result.rangeLookup;
        }

        // Deep trees materialized without a lookup build one once. Later versions retain that
        // projection and add bounded patches for their changed intervals until compaction is needed.
        if (rangeLookup == null && !BTree.isEmpty(merged.ranges))
            rangeLookup = RangeLookup.build(merged.ranges);
        long treeHeapSize = merged.ranges == base.ranges ? base.treeHeapSize : treeHeapSize(merged.ranges, rangeLookup);
        return new BTreeDeletionInfo(partitionDeletion,
                                     base.comparator,
                                     base.rangeComparator,
                                     base.lookupComparator,
                                     merged.ranges,
                                     rangeLookup,
                                     merged.rangeHeapSize,
                                     treeHeapSize);
    }

    private static void requireMatchingComparator(DeletionInfo deletionInfo, ClusteringComparator comparator)
    {
        ClusteringComparator sourceComparator;
        if (deletionInfo instanceof BTreeDeletionInfo)
            sourceComparator = ((BTreeDeletionInfo) deletionInfo).comparator;
        else if (deletionInfo instanceof MutableDeletionInfo)
            sourceComparator = ((MutableDeletionInfo) deletionInfo).rangeComparator();
        else if (deletionInfo.hasRanges())
            throw new IllegalArgumentException("Cannot merge range deletion information without its clustering comparator");
        else
            sourceComparator = null;

        if (sourceComparator != null && !sourceComparator.equals(comparator))
        {
            throw new IllegalArgumentException("Cannot merge BTree deletion information with a different clustering"
                                               + " comparator");
        }
    }

    private static BTreeDeletionInfo copyOf(DeletionInfo deletionInfo, ClusteringComparator comparator)
    {
        Comparator<Object> rangeComparator = new RangeComparator(comparator);
        Comparator<Object> lookupComparator = new LookupComparator(comparator);
        RangeTree ranges = buildRanges(deletionInfo.rangeIterator(false), deletionInfo.rangeCount(), rangeComparator, HeapCloner.instance);
        RangeLookup rangeLookup = RangeLookup.build(ranges.ranges);
        return new BTreeDeletionInfo(deletionInfo.getPartitionDeletion(),
                                     comparator,
                                     rangeComparator,
                                     lookupComparator,
                                     ranges.ranges,
                                     rangeLookup,
                                     ranges.rangeHeapSize,
                                     treeHeapSize(ranges.ranges, rangeLookup));
    }

    private static MergeResult mergeRange(RangeTree ranges,
                                          RangeTombstone update,
                                          ClusteringComparator comparator,
                                          Comparator<Object> rangeComparator,
                                          Comparator<Object> lookupComparator,
                                          RangeLookup rangeLookup)
    {
        List<RangeTombstone> affected = affectedRanges(ranges.ranges, update, comparator, rangeComparator, lookupComparator);
        if (affected.isEmpty())
        {
            Object[] replacement = BTree.singleton(update);
            RangeTree merged = new RangeTree(BTree.update(ranges.ranges, replacement, rangeComparator, RANGE_UPDATE_FUNCTION),
                                              ranges.rangeHeapSize + rangeHeapSize(update));
            return new MergeResult(merged,
                                   RangeLookup.append(rangeLookup,
                                                      affected,
                                                      replacement,
                                                      update,
                                                      comparator,
                                                      BTree.depth(ranges.ranges)));
        }

        MutableDeletionInfo reconciled = MutableDeletionInfo.live();
        long affectedHeapSize = 0;
        for (RangeTombstone range : affected)
        {
            reconciled.add(range, comparator);
            affectedHeapSize += rangeHeapSize(range);
        }
        reconciled.add(update, comparator);

        RangeTree replacement = buildRanges(reconciled.rangeIterator(false), reconciled.rangeCount(), rangeComparator);
        Object[] tree = ranges.ranges;
        for (RangeTombstone range : affected)
            tree = BTreeRemoval.remove(tree, rangeComparator, range);
        RangeTree merged = new RangeTree(BTree.update(tree, replacement.ranges, rangeComparator, RANGE_UPDATE_FUNCTION),
                                         ranges.rangeHeapSize - affectedHeapSize + replacement.rangeHeapSize);
        return new MergeResult(merged,
                               RangeLookup.append(rangeLookup,
                                                  affected,
                                                  replacement.ranges,
                                                  update,
                                                  comparator,
                                                  BTree.depth(ranges.ranges)));
    }

    private static List<RangeTombstone> affectedRanges(Object[] ranges,
                                                         RangeTombstone update,
                                                         ClusteringComparator comparator,
                                                         Comparator<Object> rangeComparator,
                                                         Comparator<Object> lookupComparator)
    {
        if (BTree.isEmpty(ranges))
            return Collections.emptyList();

        int size = BTree.size(ranges);
        int first = Math.max(0, BTree.<Object>floorIndex(ranges, lookupComparator, update.deletedSlice().start()));
        int last = Math.min(size - 1, BTree.<Object>ceilIndex(ranges, lookupComparator, update.deletedSlice().end()));
        if (first > last)
            return Collections.emptyList();

        List<RangeTombstone> affected = new ArrayList<>();
        Iterator<RangeTombstone> candidates = BTree.slice(ranges, rangeComparator, first, last, BTree.Dir.ASC);
        while (candidates.hasNext())
        {
            RangeTombstone candidate = candidates.next();
            if (candidate.deletedSlice().intersects(comparator, update.deletedSlice()))
                affected.add(candidate);
        }
        return affected;
    }

    private static RangeTree buildRanges(Iterator<RangeTombstone> ranges, int count, Comparator<Object> rangeComparator)
    {
        return buildRanges(ranges, count, rangeComparator, null);
    }

    private static RangeTree buildRanges(Iterator<RangeTombstone> ranges,
                                         int count,
                                         Comparator<Object> rangeComparator,
                                         ByteBufferCloner cloner)
    {
        if (!ranges.hasNext())
            return new RangeTree(BTree.empty(), 0);

        long rangeHeapSize = 0;
        BTree.Builder<RangeTombstone> builder = BTree.builder(rangeComparator, count);
        do
        {
            RangeTombstone range = ranges.next();
            if (cloner != null)
                range = cloneRange(range, cloner);
            builder.add(range);
            rangeHeapSize += rangeHeapSize(range);
        }
        while (ranges.hasNext());
        return new RangeTree(builder.build(), rangeHeapSize);
    }

    private static RangeTombstone cloneRange(RangeTombstone range, ByteBufferCloner cloner)
    {
        Slice slice = range.deletedSlice();
        return new RangeTombstone(Slice.make(slice.start().clone(cloner), slice.end().clone(cloner)), range.deletionTime());
    }

    private static long treeHeapSize(Object[] ranges, RangeLookup rangeLookup)
    {
        return BTree.sizeOnHeapOf(ranges) + (rangeLookup == null ? 0 : rangeLookup.unsharedHeapSize());
    }

    private static long rangeHeapSize(RangeTombstone range)
    {
        return RANGE_TOMBSTONE_SIZE
               + SLICE_SIZE
               + range.deletionTime().unsharedHeapSize()
               + range.deletedSlice().start().unsharedHeapSize()
               + range.deletedSlice().end().unsharedHeapSize();
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
        return BTree.isEmpty(ranges) ? Collections.emptyIterator() : BTree.iterator(ranges, BTree.Dir.desc(reversed));
    }

    @Override
    public Iterator<RangeTombstone> rangeIterator(Slice slice, boolean reversed)
    {
        if (BTree.isEmpty(ranges) || slice.isEmpty(comparator))
            return Collections.emptyIterator();

        int[] index = new int[1];
        RangeTombstone covering = (RangeTombstone) BTree.<Object>floor(ranges,
                                                                         lookupComparator,
                                                                         reversed ? slice.end() : slice.start(),
                                                                         index);
        RangeTombstone clippedCovering = covering == null ? null : clippedCovering(covering, slice);
        if (clippedCovering != null)
            return Iterators.singletonIterator(clippedCovering);

        int size = BTree.size(ranges);
        if (reversed)
        {
            if (covering == null)
                return Collections.emptyIterator();
            return clippedIterator(BTree.slice(ranges, rangeComparator, 0, index[0], BTree.Dir.DESC), slice, true);
        }

        int first = covering == null ? 0 : index[0];
        return clippedIterator(BTree.slice(ranges, rangeComparator, first, size - 1, BTree.Dir.ASC), slice, false);
    }

    private RangeTombstone clippedCovering(RangeTombstone range, Slice slice)
    {
        Slice deletedSlice = range.deletedSlice();
        int startComparison = comparator.compare(deletedSlice.start(), slice.start());
        int endComparison = comparator.compare(slice.end(), deletedSlice.end());
        if (startComparison > 0 || endComparison > 0)
            return null;

        if (startComparison == 0 && endComparison == 0)
            return range;
        return new RangeTombstone(Slice.make(startComparison < 0 ? slice.start() : deletedSlice.start(),
                                             endComparison < 0 ? slice.end() : deletedSlice.end()),
                                  range.deletionTime());
    }

    private Iterator<RangeTombstone> clippedIterator(Iterator<RangeTombstone> candidates, Slice slice, boolean reversed)
    {
        return new AbstractIterator<RangeTombstone>()
        {
            @Override
            protected RangeTombstone computeNext()
            {
                while (candidates.hasNext())
                {
                    RangeTombstone range = candidates.next();
                    Slice deletedSlice = range.deletedSlice();
                    if (!reversed && comparator.compare(deletedSlice.start(), slice.end()) > 0)
                        return endOfData();
                    if (reversed && comparator.compare(deletedSlice.end(), slice.start()) < 0)
                        return endOfData();

                    RangeTombstone clipped = clip(range, slice, comparator);
                    if (clipped != null)
                        return clipped;
                }
                return endOfData();
            }
        };
    }

    private static RangeTombstone clip(RangeTombstone range, Slice slice, ClusteringComparator comparator)
    {
        Slice deletedSlice = range.deletedSlice();
        if (!deletedSlice.intersects(comparator, slice))
            return null;

        ClusteringBound<?> start = comparator.compare(deletedSlice.start(), slice.start()) < 0 ? slice.start() : deletedSlice.start();
        ClusteringBound<?> end = comparator.compare(slice.end(), deletedSlice.end()) < 0 ? slice.end() : deletedSlice.end();
        if (Slice.isEmpty(comparator, start, end))
            return null;

        return start == deletedSlice.start() && end == deletedSlice.end()
               ? range
               : new RangeTombstone(Slice.make(start, end), range.deletionTime());
    }

    @Override
    public RangeTombstone rangeCovering(Clustering<?> name)
    {
        if (rangeLookup != null)
            return rangeLookup.search(name, comparator);

        RangeTombstone range = (RangeTombstone) BTree.<Object>floor(ranges, lookupComparator, name);
        return range != null && range.deletedSlice().includes(comparator, name) ? range : null;
    }

    @Override
    public void collectStats(EncodingStats.Collector collector)
    {
        collector.update(partitionDeletion);
        for (Iterator<RangeTombstone> iterator = rangeIterator(false); iterator.hasNext(); )
        {
            DeletionTime deletion = iterator.next().deletionTime();
            collector.updateTimestamp(deletion.markedForDeleteAt());
            collector.updateLocalDeletionTime(deletion.localDeletionTime());
        }
    }

    @Override
    public int dataSize()
    {
        int size = TypeSizes.sizeof(partitionDeletion.markedForDeleteAt());
        for (Iterator<RangeTombstone> iterator = rangeIterator(false); iterator.hasNext(); )
        {
            RangeTombstone range = iterator.next();
            size += range.deletedSlice().start().dataSize() + range.deletedSlice().end().dataSize();
            size += TypeSizes.sizeof(range.deletionTime().markedForDeleteAt());
            size += TypeSizes.sizeof(range.deletionTime().localDeletionTimeUnsignedInteger());
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
        for (Iterator<RangeTombstone> iterator = rangeIterator(false); iterator.hasNext(); )
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
        if (BTree.isEmpty(ranges))
            return new MutableDeletionInfo(partitionDeletion);

        RangeTombstoneList copy = new RangeTombstoneList(comparator, BTree.size(ranges));
        for (Iterator<RangeTombstone> iterator = rangeIterator(false); iterator.hasNext(); )
            copy.add(iterator.next());
        return new MutableDeletionInfo(partitionDeletion, copy);
    }

    @Override
    public BTreeDeletionInfo clone(ByteBufferCloner cloner)
    {
        BTree.Builder<RangeTombstone> builder = BTree.builder(rangeComparator, BTree.size(ranges));
        long rangeHeapSize = 0;
        for (Iterator<RangeTombstone> iterator = rangeIterator(false); iterator.hasNext(); )
        {
            RangeTombstone clone = cloneRange(iterator.next(), cloner);
            builder.add(clone);
            rangeHeapSize += rangeHeapSize(clone);
        }
        Object[] ranges = builder.build();
        RangeLookup rangeLookup = RangeLookup.build(ranges);
        return new BTreeDeletionInfo(partitionDeletion,
                                     comparator,
                                     rangeComparator,
                                     lookupComparator,
                                     ranges,
                                     rangeLookup,
                                     rangeHeapSize,
                                     treeHeapSize(ranges, rangeLookup));
    }

    @Override
    public long unsharedHeapSize()
    {
        return unsharedHeapSize;
    }

    @Override
    public boolean equals(Object other)
    {
        if (this == other)
            return true;
        if (!(other instanceof DeletionInfo))
            return false;

        DeletionInfo that = (DeletionInfo) other;
        if (!partitionDeletion.equals(that.getPartitionDeletion()))
            return false;

        Iterator<RangeTombstone> left = rangeIterator(false);
        Iterator<RangeTombstone> right = that.rangeIterator(false);
        while (left.hasNext() && right.hasNext())
        {
            if (!left.next().equals(right.next()))
                return false;
        }
        return !left.hasNext() && !right.hasNext();
    }

    @Override
    public int hashCode()
    {
        int hash = partitionDeletion.hashCode();
        for (Iterator<RangeTombstone> iterator = rangeIterator(false); iterator.hasNext(); )
            hash = 31 * hash + iterator.next().hashCode();
        return hash;
    }

    @Override
    public String toString()
    {
        if (BTree.isEmpty(ranges))
            return String.format("{%s}", partitionDeletion);

        StringBuilder builder = new StringBuilder();
        builder.append('{').append(partitionDeletion).append(", ranges=");
        for (Iterator<RangeTombstone> iterator = rangeIterator(false); iterator.hasNext(); )
        {
            RangeTombstone range = iterator.next();
            builder.append(range.deletedSlice().toString(comparator));
            builder.append('@');
            builder.append(range.deletionTime());
        }
        return builder.append('}').toString();
    }

    private static class RangeLookup
    {
        private final RangeLookup previous;
        private final ClusteringBound<?> patchStart;
        private final ClusteringBound<?> patchEnd;
        private final ClusteringBound<?>[] starts;
        private final ClusteringBound<?>[] ends;
        private final RangeTombstone[] ranges;
        private final int patchCount;
        private int next;

        private RangeLookup(int size)
        {
            this(null, null, null, size);
        }

        private RangeLookup(RangeLookup previous, ClusteringBound<?> patchStart, ClusteringBound<?> patchEnd, int size)
        {
            this.previous = previous;
            this.patchStart = patchStart;
            this.patchEnd = patchEnd;
            this.starts = new ClusteringBound<?>[size];
            this.ends = new ClusteringBound<?>[size];
            this.ranges = new RangeTombstone[size];
            this.patchCount = previous == null ? 0 : previous.patchCount + 1;
        }

        private static RangeLookup build(Object[] ranges)
        {
            // A root-and-leaves tree needs at most two node searches; deeper trees retain this projection for point lookups.
            if (BTree.depth(ranges) <= 2)
                return null;

            RangeLookup lookup = new RangeLookup(BTree.size(ranges));
            BTree.<RangeTombstone, RangeLookup>apply(ranges, RangeLookup::add, lookup);
            assert lookup.next == lookup.starts.length;
            return lookup;
        }

        private static RangeLookup append(RangeLookup previous,
                                          List<RangeTombstone> affected,
                                          Object[] replacement,
                                          RangeTombstone update,
                                          ClusteringComparator comparator,
                                          int maxPatches)
        {
            // Keep point lookup's patch walk no deeper than the persistent tree navigation it replaces.
            if (previous == null || previous.patchCount >= maxPatches)
                return null;

            ClusteringBound<?> start = update.deletedSlice().start();
            ClusteringBound<?> end = update.deletedSlice().end();
            for (RangeTombstone range : affected)
            {
                Slice slice = range.deletedSlice();
                if (comparator.compare(slice.start(), start) < 0)
                    start = slice.start();
                if (comparator.compare(end, slice.end()) < 0)
                    end = slice.end();
            }

            RangeLookup patch = new RangeLookup(previous, start, end, BTree.size(replacement));
            BTree.<RangeTombstone, RangeLookup>apply(replacement, RangeLookup::add, patch);
            assert patch.next == patch.starts.length;
            return patch;
        }

        private static void add(RangeLookup lookup, RangeTombstone range)
        {
            int index = lookup.next++;
            Slice slice = range.deletedSlice();
            lookup.starts[index] = slice.start();
            lookup.ends[index] = slice.end();
            lookup.ranges[index] = range;
        }

        private RangeTombstone search(Clustering<?> name, ClusteringComparator comparator)
        {
            RangeLookup lookup = this;
            while (lookup.previous != null)
            {
                if (comparator.compare(lookup.patchStart, name) <= 0 && comparator.compare(name, lookup.patchEnd) <= 0)
                    return lookup.searchEntries(name, comparator);
                lookup = lookup.previous;
            }
            return lookup.searchEntries(name, comparator);
        }

        private RangeTombstone searchEntries(Clustering<?> name, ClusteringComparator comparator)
        {
            int index = Arrays.binarySearch(starts, name, comparator);
            if (index >= 0)
                return null;

            index = -2 - index;
            return index < 0 || comparator.compare(name, ends[index]) >= 0 ? null : ranges[index];
        }

        private long unsharedHeapSize()
        {
            return RANGE_LOOKUP_SIZE
                   + ObjectSizes.sizeOfArray(starts)
                   + ObjectSizes.sizeOfArray(ends)
                   + ObjectSizes.sizeOfArray(ranges)
                   + (previous == null ? 0 : previous.unsharedHeapSize());
        }
    }

    private static class MergeResult
    {
        private final RangeTree ranges;
        private final RangeLookup rangeLookup;

        private MergeResult(RangeTree ranges, RangeLookup rangeLookup)
        {
            this.ranges = ranges;
            this.rangeLookup = rangeLookup;
        }
    }

    private static class RangeTree
    {
        private final Object[] ranges;
        private final long rangeHeapSize;

        private RangeTree(Object[] ranges, long rangeHeapSize)
        {
            this.ranges = ranges;
            this.rangeHeapSize = rangeHeapSize;
        }
    }

    private static class RangeComparator implements Comparator<Object>
    {
        private final ClusteringComparator comparator;

        private RangeComparator(ClusteringComparator comparator)
        {
            this.comparator = comparator;
        }

        @Override
        public int compare(Object left, Object right)
        {
            return comparator.compare(((RangeTombstone) left).deletedSlice().start(),
                                      ((RangeTombstone) right).deletedSlice().start());
        }
    }

    private static class LookupComparator implements Comparator<Object>
    {
        private final ClusteringComparator comparator;

        private LookupComparator(ClusteringComparator comparator)
        {
            this.comparator = comparator;
        }

        @Override
        public int compare(Object left, Object right)
        {
            return comparator.compare(((RangeTombstone) left).deletedSlice().start(), (ClusteringPrefix<?>) right);
        }
    }
}
