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
package org.apache.cassandra.db;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import com.google.common.collect.Iterators;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.RangeTombstoneMarker;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.memory.ByteBufferCloner;

/**
 * A mutable implementation of {@code DeletionInfo}.
 */
public class MutableDeletionInfo implements DeletionInfo
{
    private static final long EMPTY_SIZE = ObjectSizes.measure(new MutableDeletionInfo(0, 0));

    /**
     * This represents a deletion of the entire partition. We can't represent this within the RangeTombstoneList, so it's
     * kept separately. This also slightly optimizes the common case of a full partition deletion.
     */
    private DeletionTime partitionDeletion;

    /**
     * The locally owned range tombstones. When {@link #previous} is non-null these are a disjoint, ordered suffix;
     * otherwise this is the complete list.
     */
    private RangeTombstoneList ranges;

    /**
     * The immutable range prefix retained by a memtable merge copy. Keeping the prefix as a separate deletion-info
     * node avoids copying its backing arrays until a non-append update needs a materialized list.
     */
    private MutableDeletionInfo previous;

    private volatile long retainedHeapSize;
    private volatile boolean retainedHeapSizeValid;

    /**
     * Creates a DeletionInfo with only a top-level (row) tombstone.
     * @param markedForDeleteAt the time after which the entire row should be considered deleted
     * @param localDeletionTime what time the deletion write was applied locally (for purposes of
     *                          purging the tombstone after gc_grace_seconds).
     */
    public MutableDeletionInfo(long markedForDeleteAt, long localDeletionTime)
    {
        // Pre-1.1 node may return MIN_VALUE for non-deleted container, but the new default is MAX_VALUE
        // (see CASSANDRA-3872)
        this(DeletionTime.build(markedForDeleteAt, localDeletionTime == Integer.MIN_VALUE ? Long.MAX_VALUE : localDeletionTime));
    }

    public MutableDeletionInfo(DeletionTime partitionDeletion)
    {
        this(partitionDeletion, null);
    }

    public MutableDeletionInfo(DeletionTime partitionDeletion, RangeTombstoneList ranges)
    {
        this(partitionDeletion, ranges, null);
    }

    private MutableDeletionInfo(DeletionTime partitionDeletion, RangeTombstoneList ranges, MutableDeletionInfo previous)
    {
        this.partitionDeletion = partitionDeletion;
        this.ranges = ranges;
        this.previous = previous;
    }

    /**
     * Returns a new DeletionInfo that has no top-level tombstone or any range tombstones.
     */
    public static MutableDeletionInfo live()
    {
        return new MutableDeletionInfo(DeletionTime.LIVE);
    }

    public MutableDeletionInfo mutableCopy()
    {
        RangeTombstoneList materialized = materializedRanges();
        return new MutableDeletionInfo(partitionDeletion, materialized == null ? null : materialized.copy());
    }

    /**
     * Creates the copy used by the memtable merge path. The returned object initially retains this object's range
     * list as an immutable prefix; {@link #add(DeletionInfo)} adopts an ordered suffix without writing that prefix.
     */
    public MutableDeletionInfo mutableCopyForMemtable()
    {
        return new MutableDeletionInfo(copyDeletionTime(partitionDeletion), null, this);
    }

    private static DeletionTime copyDeletionTime(DeletionTime deletionTime)
    {
        return deletionTime.isLive()
             ? DeletionTime.LIVE
             : DeletionTime.build(deletionTime.markedForDeleteAt(), deletionTime.localDeletionTime());
    }

    @Override
    public MutableDeletionInfo clone(ByteBufferCloner cloner)
    {
        RangeTombstoneList materialized = materializedRanges();
        return new MutableDeletionInfo(partitionDeletion, materialized == null ? null : materialized.clone(cloner));
    }

    /**
     * Returns whether this DeletionInfo is live, that is deletes no columns.
     */
    public boolean isLive()
    {
        return partitionDeletion.isLive() && !hasRanges();
    }

    /**
     * Potentially replaces the top-level tombstone with another, keeping whichever has the higher markedForDeleteAt
     * timestamp.
     * @param newInfo the deletion time to add to this deletion info.
     */
    public void add(DeletionTime newInfo)
    {
        if (newInfo.supersedes(partitionDeletion))
        {
            partitionDeletion = newInfo;
            invalidateRetainedHeapSize();
        }
    }

    public void add(RangeTombstone tombstone, ClusteringComparator comparator)
    {
        ensureMaterializedRanges();
        if (ranges == null) // Introduce getInitialRangeTombstoneAllocationSize
            ranges = new RangeTombstoneList(comparator, DatabaseDescriptor.getInitialRangeTombstoneListAllocationSize());

        ranges.add(tombstone);
        invalidateRetainedHeapSize();
    }

    /**
     * Combines another DeletionInfo with this one and returns the result. Whichever top-level tombstone
     * has the higher markedForDeleteAt timestamp will be kept, along with its localDeletionTime. The
     * range tombstones will be combined.
     *
     * @return this object.
     */
    public DeletionInfo add(DeletionInfo newInfo)
    {
        add(newInfo.getPartitionDeletion());

        // We know MutableDeletionInfo is the only implementation and are not mutating newInfo; the cast lets an
        // ordered memtable copy retain a cloned one-range update as its own suffix.
        assert newInfo instanceof MutableDeletionInfo;
        MutableDeletionInfo source = (MutableDeletionInfo) newInfo;

        if (!source.hasRanges())
            return this;

        if (previous != null && ranges == null && source.previous == null && canAppend(source.ranges))
        {
            ranges = source.ranges;
        }
        else
        {
            // Reserve the final merge size before materializing a prefix. A prefix insert can then shift the copied
            // prefix in place rather than first allocating an exact copy and immediately growing it again.
            ensureMaterializedRanges(rangeCount() + source.rangeCount());
            RangeTombstoneList newRanges = source.materializedRanges();
            if (ranges == null)
                ranges = newRanges == null ? null : newRanges.copy();
            else if (newRanges != null)
                ranges.addAll(newRanges);
        }

        invalidateRetainedHeapSize();
        return this;
    }

    private boolean canAppend(RangeTombstoneList suffix)
    {
        RangeTombstone previousLast = previous.lastRange();
        if (previousLast == null)
            return true;

        RangeTombstone suffixFirst = suffix.iterator().next();
        return suffix.comparator().compare(previousLast.deletedSlice().end(), suffixFirst.deletedSlice().start()) <= 0;
    }

    private RangeTombstone lastRange()
    {
        for (MutableDeletionInfo current = this; current != null; current = current.previous)
        {
            if (current.ranges != null && !current.ranges.isEmpty())
                return current.ranges.iterator(true).next();
        }
        return null;
    }

    public DeletionTime getPartitionDeletion()
    {
        return partitionDeletion;
    }

    // Use sparingly, not the most efficient thing
    public Iterator<RangeTombstone> rangeIterator(boolean reversed)
    {
        if (previous == null)
            return ranges == null ? Collections.emptyIterator() : ranges.iterator(reversed);

        List<RangeTombstoneList> lists = rangeLists(reversed);
        List<Iterator<RangeTombstone>> iterators = new ArrayList<>(lists.size());
        for (RangeTombstoneList list : lists)
            iterators.add(list.iterator(reversed));
        return Iterators.concat(iterators.iterator());
    }

    public Iterator<RangeTombstone> rangeIterator(Slice slice, boolean reversed)
    {
        if (previous == null)
            return ranges == null ? Collections.emptyIterator() : ranges.iterator(slice, reversed);

        List<RangeTombstoneList> lists = rangeLists(reversed);
        List<Iterator<RangeTombstone>> iterators = new ArrayList<>(lists.size());
        for (RangeTombstoneList list : lists)
            iterators.add(list.iterator(slice, reversed));
        return Iterators.concat(iterators.iterator());
    }

    private List<RangeTombstoneList> rangeLists(boolean reversed)
    {
        List<RangeTombstoneList> lists = new ArrayList<>();
        for (MutableDeletionInfo current = this; current != null; current = current.previous)
        {
            if (current.ranges != null && !current.ranges.isEmpty())
                lists.add(current.ranges);
        }
        if (!reversed)
            Collections.reverse(lists);
        return lists;
    }

    public RangeTombstone rangeCovering(Clustering<?> name)
    {
        for (MutableDeletionInfo current = this; current != null; current = current.previous)
        {
            if (current.ranges != null)
            {
                RangeTombstone covering = current.ranges.search(name);
                if (covering != null)
                    return covering;
            }
        }
        return null;
    }

    public int dataSize()
    {
        if (previous == null)
        {
            int size = TypeSizes.sizeof(partitionDeletion.markedForDeleteAt());
            return size + (ranges == null ? 0 : ranges.dataSize());
        }

        int dataSize = TypeSizes.sizeof(partitionDeletion.markedForDeleteAt()) + TypeSizes.sizeof(rangeCount());
        Iterator<RangeTombstone> iterator = rangeIterator(false);
        while (iterator.hasNext())
        {
            RangeTombstone tombstone = iterator.next();
            dataSize += tombstone.deletedSlice().start().dataSize() + tombstone.deletedSlice().end().dataSize();
            dataSize += TypeSizes.sizeof(tombstone.deletionTime().markedForDeleteAt());
            dataSize += TypeSizes.sizeof(tombstone.deletionTime().localDeletionTimeUnsignedInteger());
        }
        return dataSize;
    }

    public boolean hasRanges()
    {
        for (MutableDeletionInfo current = this; current != null; current = current.previous)
        {
            if (current.ranges != null && !current.ranges.isEmpty())
                return true;
        }
        return false;
    }

    public int rangeCount()
    {
        int count = 0;
        for (MutableDeletionInfo current = this; current != null; current = current.previous)
            count += current.ranges == null ? 0 : current.ranges.size();
        return count;
    }

    public long maxTimestamp()
    {
        return Math.max(partitionDeletion.markedForDeleteAt(), maxRangeTimestamp());
    }

    private long maxRangeTimestamp()
    {
        long max = Long.MIN_VALUE;
        for (MutableDeletionInfo current = this; current != null; current = current.previous)
        {
            if (current.ranges != null)
                max = Math.max(max, current.ranges.maxMarkedAt());
        }
        return max;
    }

    /**
     * Whether this deletion info may modify the provided one if added to it.
     */
    public boolean mayModify(DeletionInfo delInfo)
    {
        return partitionDeletion.compareTo(delInfo.getPartitionDeletion()) > 0 || hasRanges();
    }

    @Override
    public String toString()
    {
        return hasRanges()
             ? String.format("{%s, ranges=%s}", partitionDeletion, rangesAsString())
             : String.format("{%s}", partitionDeletion);
    }

    private String rangesAsString()
    {
        StringBuilder sb = new StringBuilder();
        ClusteringComparator comparator = rangeComparator();
        Iterator<RangeTombstone> iter = rangeIterator(false);
        while (iter.hasNext())
        {
            RangeTombstone i = iter.next();
            sb.append(i.deletedSlice().toString(comparator));
            sb.append('@');
            sb.append(i.deletionTime());
        }
        return sb.toString();
    }

    // Updates all the timestamp of the deletion contained in this DeletionInfo to be {@code timestamp}.
    public DeletionInfo updateAllTimestamp(long timestamp)
    {
        ensureMaterializedRanges();
        if (partitionDeletion.markedForDeleteAt() != Long.MIN_VALUE)
            partitionDeletion = DeletionTime.build(timestamp, partitionDeletion.localDeletionTime());

        if (ranges != null)
            ranges.updateAllTimestamp(timestamp);
        invalidateRetainedHeapSize();
        return this;
    }

    public DeletionInfo updateAllTimestampAndLocalDeletionTime(long timestamp, long localDeletionTime)
    {
        ensureMaterializedRanges();
        if (partitionDeletion.markedForDeleteAt() != Long.MIN_VALUE)
            partitionDeletion = DeletionTime.build(timestamp, localDeletionTime);

        if (ranges != null)
            ranges.updateAllTimestampAndLocalDeletionTime(timestamp, localDeletionTime);
        invalidateRetainedHeapSize();
        return this;
    }

    @Override
    public boolean equals(Object o)
    {
        if (!(o instanceof MutableDeletionInfo))
            return false;

        MutableDeletionInfo that = (MutableDeletionInfo) o;
        if (!partitionDeletion.equals(that.partitionDeletion) || rangeCount() != that.rangeCount())
            return false;

        Iterator<RangeTombstone> left = rangeIterator(false);
        Iterator<RangeTombstone> right = that.rangeIterator(false);
        while (left.hasNext())
        {
            if (!left.next().equals(right.next()))
                return false;
        }
        return true;
    }

    @Override
    public final int hashCode()
    {
        int result = partitionDeletion.hashCode();
        Iterator<RangeTombstone> iterator = rangeIterator(false);
        while (iterator.hasNext())
            result = 31 * result + iterator.next().hashCode();
        return result;
    }

    @Override
    public long unsharedHeapSize()
    {
        if (this == LIVE)
            return 0;

        // previous owns its arrays and bounds; this node charges only its locally retained suffix.
        return EMPTY_SIZE + partitionDeletion.unsharedHeapSize() + (ranges == null ? 0 : ranges.unsharedHeapSize());
    }

    /**
     * Returns the retained size of this deletion-info graph without double-counting the immutable prefixes shared by
     * memtable merge copies. The updater uses this value to account only the storage added by a successful merge.
     */
    public long retainedHeapSize()
    {
        if (retainedHeapSizeValid)
            return retainedHeapSize;

        List<MutableDeletionInfo> uncached = new ArrayList<>();
        MutableDeletionInfo current = this;
        while (current != null && !current.retainedHeapSizeValid)
        {
            uncached.add(current);
            current = current.previous;
        }

        long size = current == null ? 0 : current.retainedHeapSize;
        for (int i = uncached.size() - 1; i >= 0; i--)
        {
            MutableDeletionInfo info = uncached.get(i);
            size += info.unsharedHeapSize();
            info.retainedHeapSize = size;
            info.retainedHeapSizeValid = true;
        }
        return retainedHeapSize;
    }

    public void collectStats(EncodingStats.Collector collector)
    {
        collector.update(partitionDeletion);
        collectRangeStats(collector);
    }

    private void collectRangeStats(EncodingStats.Collector collector)
    {
        for (RangeTombstoneList list : rangeLists(false))
            list.collectStats(collector);
    }

    private ClusteringComparator rangeComparator()
    {
        for (MutableDeletionInfo current = this; current != null; current = current.previous)
        {
            if (current.ranges != null)
                return current.ranges.comparator();
        }
        throw new IllegalStateException("No range tombstone comparator");
    }

    private RangeTombstoneList materializedRanges()
    {
        if (previous == null)
            return ranges;
        if (!hasRanges())
            return null;

        return materializedRanges(rangeCount());
    }

    private RangeTombstoneList materializedRanges(int capacity)
    {
        RangeTombstoneList materialized = new RangeTombstoneList(rangeComparator(), capacity);
        appendRangesTo(materialized);
        return materialized;
    }

    private void appendRangesTo(RangeTombstoneList target)
    {
        for (RangeTombstoneList list : rangeLists(false))
            target.appendOrdered(list);
    }

    private void ensureMaterializedRanges()
    {
        ensureMaterializedRanges(rangeCount());
    }

    private void ensureMaterializedRanges(int capacity)
    {
        if (previous == null)
            return;

        ranges = hasRanges() ? materializedRanges(capacity) : null;
        previous = null;
        invalidateRetainedHeapSize();
    }

    private void invalidateRetainedHeapSize()
    {
        retainedHeapSizeValid = false;
    }

    public static Builder builder(DeletionTime partitionLevelDeletion, ClusteringComparator comparator, boolean reversed)
    {
        return new Builder(partitionLevelDeletion, comparator, reversed);
    }

    /**
     * Builds DeletionInfo object from (in order) range tombstone markers.
     */
    public static class Builder
    {
        private final MutableDeletionInfo deletion;
        private final ClusteringComparator comparator;

        private final boolean reversed;

        private RangeTombstoneMarker openMarker;

        private Builder(DeletionTime partitionLevelDeletion, ClusteringComparator comparator, boolean reversed)
        {
            this.deletion = new MutableDeletionInfo(partitionLevelDeletion);
            this.comparator = comparator;
            this.reversed = reversed;
        }

        public void add(RangeTombstoneMarker marker)
        {
            // We need to start by the close case in case that's a boundary

            if (marker.isClose(reversed))
            {
                DeletionTime openDeletion = openMarker.openDeletionTime(reversed);
                assert marker.closeDeletionTime(reversed).equals(openDeletion);

                ClusteringBound<?> open = openMarker.openBound(reversed);
                ClusteringBound<?> close = marker.closeBound(reversed);

                Slice slice = reversed ? Slice.make(close, open) : Slice.make(open, close);
                deletion.add(new RangeTombstone(slice, openDeletion), comparator);
            }

            if (marker.isOpen(reversed))
            {
                openMarker = marker;
            }
        }

        public MutableDeletionInfo build()
        {
            return deletion;
        }
    }
}
