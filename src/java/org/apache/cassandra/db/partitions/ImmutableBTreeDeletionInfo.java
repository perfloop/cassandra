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

import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;

import com.google.common.collect.Iterators;

import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringBound;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.ClusteringPrefix;
import org.apache.cassandra.db.DeletionInfo;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.MutableDeletionInfo;
import org.apache.cassandra.db.RangeTombstone;
import org.apache.cassandra.db.RangeTombstoneList;
import org.apache.cassandra.db.Slice;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.utils.AbstractIterator;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.btree.BTree;
import org.apache.cassandra.utils.btree.UpdateFunction;
import org.apache.cassandra.utils.memory.ByteBufferCloner;
import org.apache.cassandra.utils.memory.HeapCloner;

/**
 * Persistent deletion state for AtomicBTreePartition's ordered single-range append path.
 *
 * Overlapping and multi-range updates continue through MutableDeletionInfo's canonical reconciliation.
 */
final class ImmutableBTreeDeletionInfo implements DeletionInfo
{
    private static final long EMPTY_SIZE = ObjectSizes.measure(new ImmutableBTreeDeletionInfo(null,
                                                                                                    DeletionTime.LIVE,
                                                                                                    BTree.empty(),
                                                                                                    new RangeComparator(null)));
    private static final long RANGE_COMPARATOR_SIZE = ObjectSizes.measure(new RangeComparator(null));
    private static final long RANGE_TOMBSTONE_SIZE = ObjectSizes.measure(new RangeTombstone(null, null));
    private static final long SLICE_SIZE = ObjectSizes.measure(Slice.ALL);

    private final ClusteringComparator comparator;
    private final DeletionTime partitionDeletion;
    private final Object[] ranges;
    private final RangeComparator rangeComparator;

    private ImmutableBTreeDeletionInfo(ClusteringComparator comparator,
                                       DeletionTime partitionDeletion,
                                       Object[] ranges,
                                       RangeComparator rangeComparator)
    {
        this.comparator = comparator;
        this.partitionDeletion = partitionDeletion;
        this.ranges = ranges;
        this.rangeComparator = rangeComparator;
    }

    static DeletionInfo tryMerge(DeletionInfo existing,
                                 DeletionInfo update,
                                 ClusteringComparator comparator,
                                 BTreePartitionUpdater updater)
    {
        if (update.hasRanges() && update.getPartitionDeletion().isLive() && update.rangeCount() == 1)
        {
            RangeTombstone range = update.rangeIterator(false).next();
            if (existing == DeletionInfo.LIVE)
                return create(comparator, range, updater);

            if (existing instanceof ImmutableBTreeDeletionInfo)
                return ((ImmutableBTreeDeletionInfo) existing).append(range, updater);
        }

        if (existing instanceof ImmutableBTreeDeletionInfo && !update.hasRanges() && !update.getPartitionDeletion().isLive())
            return ((ImmutableBTreeDeletionInfo) existing).withPartitionDeletion(update.getPartitionDeletion(), updater);

        return null;
    }

    private static ImmutableBTreeDeletionInfo create(ClusteringComparator comparator,
                                                     RangeTombstone range,
                                                     BTreePartitionUpdater updater)
    {
        RangeComparator rangeComparator = new RangeComparator(comparator);
        Object[] updated = BTree.update(BTree.empty(), BTree.singleton(range), rangeComparator, new RangeCloningUpdateFunction(updater));
        updater.onAllocatedOnHeap(EMPTY_SIZE + RANGE_COMPARATOR_SIZE);
        return new ImmutableBTreeDeletionInfo(comparator, DeletionTime.LIVE, updated, rangeComparator);
    }

    private ImmutableBTreeDeletionInfo append(RangeTombstone range, BTreePartitionUpdater updater)
    {
        if (!canAppend(range))
            return null;

        Object[] updated = BTree.update(ranges, BTree.singleton(range), rangeComparator, new RangeCloningUpdateFunction(updater));
        updater.onAllocatedOnHeap(EMPTY_SIZE);
        return new ImmutableBTreeDeletionInfo(comparator, partitionDeletion, updated, rangeComparator);
    }

    private ImmutableBTreeDeletionInfo withPartitionDeletion(DeletionTime deletion, BTreePartitionUpdater updater)
    {
        if (!deletion.supersedes(partitionDeletion))
            return this;

        updater.onAllocatedOnHeap(EMPTY_SIZE + deletion.unsharedHeapSize());
        return new ImmutableBTreeDeletionInfo(comparator, deletion, ranges, rangeComparator);
    }

    private boolean canAppend(RangeTombstone range)
    {
        if (BTree.isEmpty(ranges))
            return true;

        RangeTombstone last = BTree.findByIndex(ranges, BTree.size(ranges) - 1);
        return comparator.compare(last.deletedSlice().end(), range.deletedSlice().start()) <= 0;
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

        int size = rangeCount();
        if (!reversed)
        {
            int startIndex = slice.start().isBottom() ? 0 : search(slice.start());
            int start = startIndex < 0 ? -startIndex - 1 : startIndex;
            if (start >= size)
                return Collections.emptyIterator();

            int finishIndex = slice.end().isTop() ? size - 1 : search(slice.end());
            int finish = finishIndex < 0 ? -finishIndex - 2 : finishIndex;
            if (start > finish)
                return Collections.emptyIterator();

            return slicedRangeIterator(slice, start, finish, false);
        }

        int startIndex = slice.end().isTop() ? size - 1 : search(slice.end());
        int start = startIndex < 0 ? -startIndex - 2 : startIndex;
        if (start < 0)
            return Collections.emptyIterator();

        int finishIndex = slice.start().isBottom() ? 0 : search(slice.start());
        int finish = finishIndex < 0 ? -finishIndex - 1 : finishIndex;
        if (start < finish)
            return Collections.emptyIterator();

        return slicedRangeIterator(slice, start, finish, true);
    }

    @Override
    public RangeTombstone rangeCovering(Clustering<?> name)
    {
        int index = search(name);
        return index < 0 ? null : BTree.findByIndex(ranges, index);
    }

    private int search(ClusteringPrefix<?> name)
    {
        int position = BTree.findIndex(ranges, rangeComparator, (Object) name);
        if (position >= 0)
            return -position - 1;

        int index = -position - 2;
        if (index < 0)
            return -1;

        RangeTombstone range = BTree.findByIndex(ranges, index);
        return comparator.compare(name, range.deletedSlice().end()) < 0 ? index : -index - 2;
    }

    private Iterator<RangeTombstone> slicedRangeIterator(Slice slice, int start, int finish, boolean reversed)
    {
        if (start == finish)
        {
            RangeTombstone range = BTree.findByIndex(ranges, start);
            ClusteringBound<?> rangeStart = comparator.compare(range.deletedSlice().start(), slice.start()) < 0
                                             ? slice.start()
                                             : range.deletedSlice().start();
            ClusteringBound<?> rangeEnd = comparator.compare(slice.end(), range.deletedSlice().end()) < 0
                                           ? slice.end()
                                           : range.deletedSlice().end();
            if (Slice.isEmpty(comparator, rangeStart, rangeEnd))
                return Collections.emptyIterator();

            return Iterators.singletonIterator(new RangeTombstone(Slice.make(rangeStart, rangeEnd), range.deletionTime()));
        }

        return new SlicedRangeIterator(slice, start, finish, reversed);
    }

    @Override
    public void collectStats(EncodingStats.Collector collector)
    {
        collector.update(partitionDeletion);
        rangeIterator(false).forEachRemaining(range -> {
            collector.updateTimestamp(range.deletionTime().markedForDeleteAt());
            collector.updateLocalDeletionTime(range.deletionTime().localDeletionTime());
        });
    }

    @Override
    public int dataSize()
    {
        int size = TypeSizes.sizeof(partitionDeletion.markedForDeleteAt());
        if (!hasRanges())
            return size;

        size += TypeSizes.sizeof(rangeCount());
        Iterator<RangeTombstone> iterator = rangeIterator(false);
        while (iterator.hasNext())
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
        long max = partitionDeletion.markedForDeleteAt();
        Iterator<RangeTombstone> iterator = rangeIterator(false);
        while (iterator.hasNext())
            max = Math.max(max, iterator.next().deletionTime().markedForDeleteAt());
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
        RangeTombstoneList copy = new RangeTombstoneList(comparator, rangeCount());
        rangeIterator(false).forEachRemaining(copy::add);
        return new MutableDeletionInfo(partitionDeletion, copy);
    }

    @Override
    public DeletionInfo clone(ByteBufferCloner cloner)
    {
        return mutableCopy().clone(cloner);
    }

    @Override
    public long unsharedHeapSize()
    {
        long size = EMPTY_SIZE + RANGE_COMPARATOR_SIZE + partitionDeletion.unsharedHeapSize() + BTree.sizeOnHeapOf(ranges);
        Iterator<RangeTombstone> iterator = rangeIterator(false);
        while (iterator.hasNext())
            size += rangeHeapSize(iterator.next());
        return size;
    }

    @Override
    public boolean equals(Object other)
    {
        if (!(other instanceof DeletionInfo))
            return false;

        DeletionInfo that = (DeletionInfo) other;
        return partitionDeletion.equals(that.getPartitionDeletion())
               && rangeCount() == that.rangeCount()
               && Iterators.elementsEqual(rangeIterator(false), that.rangeIterator(false));
    }

    @Override
    public int hashCode()
    {
        int rangesHash = rangeCount();
        Iterator<RangeTombstone> iterator = rangeIterator(false);
        while (iterator.hasNext())
        {
            RangeTombstone range = iterator.next();
            rangesHash += range.deletedSlice().start().hashCode() + range.deletedSlice().end().hashCode();
            long markedAt = range.deletionTime().markedForDeleteAt();
            rangesHash += (int) (markedAt ^ (markedAt >>> 32));
            rangesHash += range.deletionTime().localDeletionTimeUnsignedInteger();
        }

        int hash = 1;
        hash = 31 * hash + partitionDeletion.hashCode();
        return 31 * hash + rangesHash;
    }

    private final class SlicedRangeIterator extends AbstractIterator<RangeTombstone>
    {
        private final Slice slice;
        private final int start;
        private final int finish;
        private final boolean reversed;
        private int index;

        private SlicedRangeIterator(Slice slice, int start, int finish, boolean reversed)
        {
            this.slice = slice;
            this.start = start;
            this.finish = finish;
            this.reversed = reversed;
            this.index = start;
        }

        @Override
        protected RangeTombstone computeNext()
        {
            if ((!reversed && index > finish) || (reversed && index < finish))
                return endOfData();

            RangeTombstone range = BTree.findByIndex(ranges, index);
            if (!reversed)
            {
                if (index == start && comparator.compare(range.deletedSlice().start(), slice.start()) < 0)
                {
                    index++;
                    return new RangeTombstone(Slice.make(slice.start(), range.deletedSlice().end()), range.deletionTime());
                }
                if (index == finish && comparator.compare(slice.end(), range.deletedSlice().end()) < 0)
                {
                    index++;
                    return new RangeTombstone(Slice.make(range.deletedSlice().start(), slice.end()), range.deletionTime());
                }
                index++;
                return range;
            }

            if (index == start && comparator.compare(slice.end(), range.deletedSlice().end()) < 0)
            {
                index--;
                return new RangeTombstone(Slice.make(range.deletedSlice().start(), slice.end()), range.deletionTime());
            }
            if (index == finish && comparator.compare(range.deletedSlice().start(), slice.start()) < 0)
            {
                index--;
                return new RangeTombstone(Slice.make(slice.start(), range.deletedSlice().end()), range.deletionTime());
            }
            index--;
            return range;
        }
    }

    private static ClusteringPrefix<?> start(Object object)
    {
        return object instanceof RangeTombstone
               ? ((RangeTombstone) object).deletedSlice().start()
               : (ClusteringPrefix<?>) object;
    }

    private static long rangeHeapSize(RangeTombstone range)
    {
        return RANGE_TOMBSTONE_SIZE
               + SLICE_SIZE
               + range.deletedSlice().start().unsharedHeapSize()
               + range.deletedSlice().end().unsharedHeapSize()
               + range.deletionTime().unsharedHeapSize();
    }

    private static final class RangeComparator implements Comparator<Object>
    {
        private final ClusteringComparator comparator;

        private RangeComparator(ClusteringComparator comparator)
        {
            this.comparator = comparator;
        }

        @Override
        public int compare(Object left, Object right)
        {
            return comparator.compare(start(left), start(right));
        }
    }

    private static final class RangeCloningUpdateFunction implements UpdateFunction<RangeTombstone, RangeTombstone>
    {
        private final BTreePartitionUpdater updater;

        private RangeCloningUpdateFunction(BTreePartitionUpdater updater)
        {
            this.updater = updater;
        }

        @Override
        public RangeTombstone insert(RangeTombstone range)
        {
            ClusteringBound<?> start = range.deletedSlice().start().clone(HeapCloner.instance);
            ClusteringBound<?> end = range.deletedSlice().end().clone(HeapCloner.instance);
            DeletionTime deletion = DeletionTime.build(range.deletionTime().markedForDeleteAt(),
                                                       range.deletionTime().localDeletionTime());
            RangeTombstone copy = new RangeTombstone(Slice.make(start, end), deletion);
            updater.onAllocatedOnHeap(rangeHeapSize(copy));
            return copy;
        }

        @Override
        public RangeTombstone merge(RangeTombstone replacing, RangeTombstone update)
        {
            throw new AssertionError("ordered range appends must not replace an existing tombstone");
        }

        @Override
        public void onAllocatedOnHeap(long heapSize)
        {
            updater.onAllocatedOnHeap(heapSize);
        }
    }
}
