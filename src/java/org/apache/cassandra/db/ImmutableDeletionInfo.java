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

import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;

import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.utils.AbstractIterator;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.btree.BTree;
import org.apache.cassandra.utils.btree.UpdateFunction;
import org.apache.cassandra.utils.memory.ByteBufferCloner;
import org.apache.cassandra.utils.memory.HeapCloner;

/**
 * An immutable deletion-info representation used by atomic memtable partitions.
 *
 * The mutable representation remains the canonical owner of range reconciliation.  This class only
 * appends already-canonical, disjoint ranges to an immutable BTree; all other updates materialize a
 * {@link MutableDeletionInfo} and use its existing reconciliation logic.
 */
public final class ImmutableDeletionInfo implements DeletionInfo
{
    private static final long EMPTY_SIZE = ObjectSizes.measure(new ImmutableDeletionInfo(DeletionTime.LIVE, null, new RangeComparator(null), BTree.empty(), 0, 0));
    private static final long RANGE_COMPARATOR_SIZE = ObjectSizes.measure(new RangeComparator(null));
    private static final long RANGE_TOMBSTONE_SIZE = ObjectSizes.measure(new RangeTombstone(Slice.ALL, DeletionTime.LIVE));
    private static final long SLICE_SIZE = ObjectSizes.measure(Slice.ALL);

    private final DeletionTime partitionDeletion;
    private final ClusteringComparator clusteringComparator;
    private final RangeComparator rangeComparator;
    private final Object[] ranges;
    private final long rangeHeapSize;
    private final long rangeBoundaryHeapSize;

    private ImmutableDeletionInfo(DeletionTime partitionDeletion,
                                  ClusteringComparator clusteringComparator,
                                  RangeComparator rangeComparator,
                                  Object[] ranges,
                                  long rangeHeapSize,
                                  long rangeBoundaryHeapSize)
    {
        this.partitionDeletion = partitionDeletion;
        this.clusteringComparator = clusteringComparator;
        this.rangeComparator = rangeComparator;
        this.ranges = ranges;
        this.rangeHeapSize = rangeHeapSize;
        this.rangeBoundaryHeapSize = rangeBoundaryHeapSize;
    }

    /**
     * Builds an immutable tree from a canonical mutable update. The source partition deletion and the
     * previously published partition deletion are reconciled here, so callers cannot supply a second,
     * competing partition-deletion authority. All retained bounds are cloned before publication.
     */
    public static ImmutableDeletionInfo copyOf(MutableDeletionInfo source,
                                                DeletionTime priorPartitionDeletion,
                                                UpdateFunction<?, ?> allocationListener)
    {
        ClusteringComparator clusteringComparator = source.clusteringComparator();
        if (clusteringComparator == null)
            throw new IllegalArgumentException("Cannot build immutable deletion info without range tombstones");

        DeletionTime sourcePartitionDeletion = source.getPartitionDeletion();
        DeletionTime partitionDeletion = sourcePartitionDeletion.supersedes(priorPartitionDeletion)
                                        ? sourcePartitionDeletion
                                        : priorPartitionDeletion;
        DeletionTime ownedPartitionDeletion = copyDeletionTime(partitionDeletion);
        RangeComparator rangeComparator = new RangeComparator(clusteringComparator);
        RangeTreeUpdater updater = new RangeTreeUpdater(allocationListener);
        Object[] ranges = appendCopiedRanges(BTree.empty(), source, rangeComparator, updater);

        allocationListener.onAllocatedOnHeap(EMPTY_SIZE + RANGE_COMPARATOR_SIZE + ownedPartitionDeletion.unsharedHeapSize());
        return new ImmutableDeletionInfo(ownedPartitionDeletion, clusteringComparator, rangeComparator, ranges, updater.rangeHeapSize, updater.rangeBoundaryHeapSize);
    }

    /**
     * Appends a canonical, disjoint update using BTree's persistent update path.  Range bounds are
     * cloned before publication. A null result tells the caller to materialize through MutableDeletionInfo instead.
     */
    public ImmutableDeletionInfo tryAppend(MutableDeletionInfo update, UpdateFunction<?, ?> allocationListener)
    {
        if (update.hasRanges() && !clusteringComparator.equals(update.clusteringComparator()))
            throw new IllegalArgumentException("Cannot append ranges with a different clustering comparator");

        DeletionTime newPartitionDeletion = update.getPartitionDeletion().supersedes(partitionDeletion)
                                            ? copyDeletionTime(update.getPartitionDeletion())
                                            : partitionDeletion;

        if (!update.hasRanges())
        {
            if (newPartitionDeletion == partitionDeletion)
                return this;

            allocationListener.onAllocatedOnHeap(EMPTY_SIZE + newPartitionDeletion.unsharedHeapSize());
            return new ImmutableDeletionInfo(newPartitionDeletion, clusteringComparator, rangeComparator, ranges, rangeHeapSize, rangeBoundaryHeapSize);
        }

        if (!canAppend(update))
            return null;

        RangeTreeUpdater updater = new RangeTreeUpdater(allocationListener);
        Object[] newRanges = appendCopiedRanges(ranges, update, rangeComparator, updater);

        allocationListener.onAllocatedOnHeap(EMPTY_SIZE);
        if (newPartitionDeletion != partitionDeletion)
            allocationListener.onAllocatedOnHeap(newPartitionDeletion.unsharedHeapSize());

        return new ImmutableDeletionInfo(newPartitionDeletion, clusteringComparator, rangeComparator, newRanges, rangeHeapSize + updater.rangeHeapSize, rangeBoundaryHeapSize + updater.rangeBoundaryHeapSize);
    }

    private boolean canAppend(MutableDeletionInfo update)
    {
        RangeTombstone previous = BTree.isEmpty(ranges) ? null : BTree.findByIndex(ranges, BTree.size(ranges) - 1);
        Iterator<RangeTombstone> iterator = update.rangeIterator(false);
        while (iterator.hasNext())
        {
            RangeTombstone next = iterator.next();
            if (previous != null && clusteringComparator.compare(previous.deletedSlice().end(), next.deletedSlice().start()) > 0)
                return false;
            previous = next;
        }
        return true;
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
        return BTree.iterator(ranges, reversed ? BTree.Dir.DESC : BTree.Dir.ASC);
    }

    @Override
    public Iterator<RangeTombstone> rangeIterator(Slice slice, boolean reversed)
    {
        if (BTree.isEmpty(ranges))
            return Collections.emptyIterator();

        int first = firstIntersectingRange(slice);
        int last = lastIntersectingRange(slice);
        if (first > last)
            return Collections.emptyIterator();

        Iterator<RangeTombstone> iterator = BTree.iterator(ranges, first, last, reversed ? BTree.Dir.DESC : BTree.Dir.ASC);
        return new AbstractIterator<RangeTombstone>()
        {
            @Override
            protected RangeTombstone computeNext()
            {
                while (iterator.hasNext())
                {
                    RangeTombstone range = iterator.next();
                    RangeTombstone clipped = clip(range, slice);
                    if (clipped != null)
                        return clipped;
                }
                return endOfData();
            }
        };
    }

    @Override
    public RangeTombstone rangeCovering(Clustering<?> name)
    {
        RangeTombstone range = (RangeTombstone) BTree.<Object>floor(ranges, rangeComparator, name);
        return range != null && range.deletedSlice().includes(clusteringComparator, name) ? range : null;
    }

    @Override
    public void collectStats(EncodingStats.Collector collector)
    {
        collector.update(partitionDeletion);
        for (RangeTombstone range : BTree.<RangeTombstone>iterable(ranges))
        {
            DeletionTime deletion = range.deletionTime();
            collector.updateTimestamp(deletion.markedForDeleteAt());
            collector.updateLocalDeletionTime(deletion.localDeletionTime());
        }
    }

    @Override
    public int dataSize()
    {
        int size = TypeSizes.sizeof(partitionDeletion.markedForDeleteAt()) + TypeSizes.sizeof(BTree.size(ranges));
        for (RangeTombstone range : BTree.<RangeTombstone>iterable(ranges))
        {
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
        long max = partitionDeletion.markedForDeleteAt();
        for (RangeTombstone range : BTree.<RangeTombstone>iterable(ranges))
            max = Math.max(max, range.deletionTime().markedForDeleteAt());
        return max;
    }

    @Override
    public boolean mayModify(DeletionInfo deletionInfo)
    {
        return partitionDeletion.compareTo(deletionInfo.getPartitionDeletion()) > 0 || hasRanges();
    }

    @Override
    public MutableDeletionInfo mutableCopy()
    {
        // Bounds and deletion times were copied when this immutable snapshot was published and are never mutated.
        // RangeTombstoneList owns fresh, exact-capacity arrays and remains the canonical mutable merge owner;
        // clone(ByteBufferCloner) remains the deep-copy operation for external buffers.
        RangeTombstone[] copy = new RangeTombstone[rangeCount()];
        BTree.toArray(ranges, copy, 0);
        return new MutableDeletionInfo(partitionDeletion, RangeTombstoneList.fromCanonicalRanges(clusteringComparator, copy, rangeBoundaryHeapSize));
    }

    @Override
    public DeletionInfo clone(ByteBufferCloner cloner)
    {
        return materialize(cloner);
    }

    private MutableDeletionInfo materialize(ByteBufferCloner cloner)
    {
        MutableDeletionInfo copy = new MutableDeletionInfo(partitionDeletion);
        Iterator<RangeTombstone> iterator = rangeIterator(false);
        while (iterator.hasNext())
        {
            copy.add(clone(iterator.next(), cloner), clusteringComparator);
        }
        return copy;
    }

    @Override
    public long unsharedHeapSize()
    {
        return EMPTY_SIZE
               + RANGE_COMPARATOR_SIZE
               + partitionDeletion.unsharedHeapSize()
               + rangeHeapSize
               + BTree.sizeOnHeapOf(ranges);
    }

    @Override
    public boolean equals(Object other)
    {
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
        int rangesHash = rangeCount();
        Iterator<RangeTombstone> iterator = rangeIterator(false);
        while (iterator.hasNext())
        {
            RangeTombstone range = iterator.next();
            DeletionTime deletion = range.deletionTime();
            rangesHash += range.deletedSlice().start().hashCode() + range.deletedSlice().end().hashCode();
            rangesHash += (int) (deletion.markedForDeleteAt() ^ (deletion.markedForDeleteAt() >>> 32));
            rangesHash += deletion.localDeletionTimeUnsignedInteger();
        }
        return 31 * (31 + partitionDeletion.hashCode()) + rangesHash;
    }

    private int firstIntersectingRange(Slice slice)
    {
        if (slice.start().isBottom())
            return 0;

        int index = BTree.floorIndex(ranges, rangeComparator, slice.start());
        if (index < 0)
            return 0;

        RangeTombstone range = BTree.findByIndex(ranges, index);
        return clusteringComparator.compare(range.deletedSlice().end(), slice.start()) <= 0 ? index + 1 : index;
    }

    private int lastIntersectingRange(Slice slice)
    {
        if (slice.end().isTop())
            return BTree.size(ranges) - 1;

        return BTree.floorIndex(ranges, rangeComparator, slice.end().invert());
    }

    private RangeTombstone clip(RangeTombstone range, Slice slice)
    {
        ClusteringBound<?> start = range.deletedSlice().start();
        ClusteringBound<?> end = range.deletedSlice().end();
        if (clusteringComparator.compare(start, slice.start()) < 0)
            start = slice.start();
        if (clusteringComparator.compare(slice.end(), end) < 0)
            end = slice.end();

        if (Slice.isEmpty(clusteringComparator, start, end))
            return null;
        if (start == range.deletedSlice().start() && end == range.deletedSlice().end())
            return range;
        return new RangeTombstone(Slice.make(start, end), range.deletionTime());
    }

    ClusteringComparator clusteringComparator()
    {
        return clusteringComparator;
    }

    private static Object[] appendCopiedRanges(Object[] existing,
                                                MutableDeletionInfo source,
                                                RangeComparator rangeComparator,
                                                RangeTreeUpdater updater)
    {
        BTree.Builder<RangeTombstone> builder = BTree.builder(rangeComparator, source.rangeCount());
        Iterator<RangeTombstone> iterator = source.rangeIterator(false);
        while (iterator.hasNext())
            builder.add(clone(iterator.next(), HeapCloner.instance));
        return BTree.update(existing, builder.build(), rangeComparator, updater);
    }

    private static RangeTombstone clone(RangeTombstone range, ByteBufferCloner cloner)
    {
        Slice slice = range.deletedSlice();
        return new RangeTombstone(Slice.make(slice.start().clone(cloner), slice.end().clone(cloner)),
                                  copyDeletionTime(range.deletionTime()));
    }

    private static DeletionTime copyDeletionTime(DeletionTime deletionTime)
    {
        if (deletionTime == DeletionTime.LIVE)
            return deletionTime;
        return DeletionTime.buildUnsafeWithUnsignedInteger(deletionTime.markedForDeleteAt(),
                                                            deletionTime.localDeletionTimeUnsignedInteger());
    }

    private static long unsharedHeapSize(RangeTombstone range)
    {
        return RANGE_TOMBSTONE_SIZE
               + SLICE_SIZE
               + range.deletionTime().unsharedHeapSize()
               + range.deletedSlice().start().unsharedHeapSize()
               + range.deletedSlice().end().unsharedHeapSize();
    }

    private static class RangeComparator implements Comparator<Object>
    {
        private final ClusteringComparator clusteringComparator;

        private RangeComparator(ClusteringComparator clusteringComparator)
        {
            this.clusteringComparator = clusteringComparator;
        }

        @Override
        public int compare(Object left, Object right)
        {
            return clusteringComparator.compare(startOf(left), startOf(right));
        }

        private static ClusteringPrefix<?> startOf(Object value)
        {
            return value instanceof RangeTombstone
                   ? ((RangeTombstone) value).deletedSlice().start()
                   : (ClusteringPrefix<?>) value;
        }
    }

    private static class RangeTreeUpdater implements UpdateFunction<RangeTombstone, RangeTombstone>
    {
        private final UpdateFunction<?, ?> allocationListener;
        private long rangeHeapSize;
        private long rangeBoundaryHeapSize;

        private RangeTreeUpdater(UpdateFunction<?, ?> allocationListener)
        {
            this.allocationListener = allocationListener;
        }

        @Override
        public RangeTombstone insert(RangeTombstone range)
        {
            long heapSize = unsharedHeapSize(range);
            rangeHeapSize += heapSize;
            rangeBoundaryHeapSize += range.deletedSlice().start().unsharedHeapSize() + range.deletedSlice().end().unsharedHeapSize();
            allocationListener.onAllocatedOnHeap(heapSize);
            return range;
        }

        @Override
        public RangeTombstone merge(RangeTombstone existing, RangeTombstone update)
        {
            throw new AssertionError("append path received duplicate range starts");
        }

        @Override
        public void onAllocatedOnHeap(long heapSize)
        {
            allocationListener.onAllocatedOnHeap(heapSize);
        }
    }
}
