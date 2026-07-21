/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;

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
 * Immutable range-deletion state published only by {@link AtomicBTreePartition}.
 *
 * <p>Ordered single-range appends path-copy this BTree, so a CAS loser and readers of an
 * earlier partition snapshot retain their original tombstones.  Any update that requires
 * canonical range reconciliation is materialized through {@link MutableDeletionInfo} instead.
 */
final class ImmutableBTreeDeletionInfo implements DeletionInfo
{
    private static final long EMPTY_SIZE = ObjectSizes.measure(new ImmutableBTreeDeletionInfo(null,
                                                                                               DeletionTime.LIVE,
                                                                                               BTree.empty(),
                                                                                               TypeSizes.sizeof(0),
                                                                                               Long.MIN_VALUE));
    private static final long RANGE_TOMBSTONE_SIZE = ObjectSizes.measure(new RangeTombstone(Slice.ALL, DeletionTime.LIVE));
    private static final long SLICE_SIZE = ObjectSizes.measure(Slice.make(ClusteringBound.BOTTOM, ClusteringBound.MIN_END));

    private final ClusteringComparator comparator;
    private final DeletionTime partitionDeletion;
    private final Object[] ranges;
    private final int dataSize;
    private final long maxTimestamp;

    private ImmutableBTreeDeletionInfo(ClusteringComparator comparator,
                                       DeletionTime partitionDeletion,
                                       Object[] ranges,
                                       int dataSize,
                                       long maxTimestamp)
    {
        this.comparator = comparator;
        this.partitionDeletion = partitionDeletion;
        this.ranges = ranges;
        this.dataSize = dataSize;
        this.maxTimestamp = maxTimestamp;
    }

    static boolean canAppend(DeletionInfo existing, DeletionInfo update, ClusteringComparator comparator)
    {
        if (update.rangeCount() != 1 || !(existing.isLive() || existing instanceof ImmutableBTreeDeletionInfo))
            return false;

        if (!(existing instanceof ImmutableBTreeDeletionInfo))
            return true;

        ImmutableBTreeDeletionInfo immutable = (ImmutableBTreeDeletionInfo) existing;
        if (BTree.isEmpty(immutable.ranges))
            return true;

        RangeTombstone next = update.rangeIterator(false).next();
        RangeTombstone previous = BTree.findByIndex(immutable.ranges, BTree.size(immutable.ranges) - 1);
        return comparator.compare(previous.deletedSlice().start(), next.deletedSlice().start()) < 0
               && comparator.compare(previous.deletedSlice().end(), next.deletedSlice().start()) <= 0;
    }

    static ImmutableBTreeDeletionInfo append(DeletionInfo existing,
                                             DeletionInfo update,
                                             ClusteringComparator comparator,
                                             BTreePartitionUpdater allocator)
    {
        ImmutableBTreeDeletionInfo previous = existing instanceof ImmutableBTreeDeletionInfo
                                              ? (ImmutableBTreeDeletionInfo) existing
                                              : null;
        RangeTombstone range = update.rangeIterator(false).next();
        Object[] previousRanges = previous == null ? BTree.empty() : previous.ranges;
        RangeAppendFunction appendFunction = new RangeAppendFunction();
        Object[] appendedRanges = BTree.update(previousRanges,
                                               BTree.singleton(range),
                                               rangeComparator(comparator),
                                               appendFunction);

        DeletionTime partitionDeletion = maxDeletion(existing.getPartitionDeletion(), update.getPartitionDeletion());
        int dataSize = previous == null ? existing.dataSize() : previous.dataSize;
        if (previous == null || BTree.isEmpty(previousRanges))
            dataSize += TypeSizes.sizeof(0);
        dataSize += dataSize(range);
        long maxTimestamp = Math.max(previous == null ? existing.maxTimestamp() : previous.maxTimestamp,
                                     range.deletionTime().markedForDeleteAt());
        maxTimestamp = Math.max(maxTimestamp, partitionDeletion.markedForDeleteAt());

        long metadataDelta = previous == null
                             ? EMPTY_SIZE + partitionDeletion.unsharedHeapSize() - existing.unsharedHeapSize()
                             : partitionDeletion.unsharedHeapSize() - previous.partitionDeletion.unsharedHeapSize();
        long treeDelta = BTree.sizeOnHeapDeltaForAppend(previousRanges, appendedRanges);
        allocator.onAllocatedOnHeap(metadataDelta + appendFunction.rangeBytes + treeDelta);
        return new ImmutableBTreeDeletionInfo(comparator, partitionDeletion, appendedRanges, dataSize, maxTimestamp);
    }

    ImmutableBTreeDeletionInfo withPartitionDeletion(DeletionTime deletion, BTreePartitionUpdater allocator)
    {
        DeletionTime partitionDeletion = maxDeletion(this.partitionDeletion, deletion);
        allocator.onAllocatedOnHeap(partitionDeletion.unsharedHeapSize() - this.partitionDeletion.unsharedHeapSize());
        return new ImmutableBTreeDeletionInfo(comparator, partitionDeletion, ranges, dataSize,
                                              Math.max(maxTimestamp, partitionDeletion.markedForDeleteAt()));
    }

    private static DeletionTime maxDeletion(DeletionTime left, DeletionTime right)
    {
        return right.supersedes(left) ? right : left;
    }

    private static Comparator<Object> rangeComparator(ClusteringComparator comparator)
    {
        return (left, right) -> comparator.compare(startOf(left), startOf(right));
    }

    private static ClusteringPrefix<?> startOf(Object value)
    {
        return value instanceof RangeTombstone
               ? ((RangeTombstone) value).deletedSlice().start()
               : (ClusteringPrefix<?>) value;
    }

    private static int dataSize(RangeTombstone range)
    {
        return range.deletedSlice().start().dataSize()
               + range.deletedSlice().end().dataSize()
               + TypeSizes.sizeof(range.deletionTime().markedForDeleteAt())
               + TypeSizes.sizeof(range.deletionTime().localDeletionTimeUnsignedInteger());
    }

    private static RangeTombstone cloneRange(RangeTombstone range)
    {
        Slice slice = range.deletedSlice();
        return new RangeTombstone(Slice.make(slice.start().clone(HeapCloner.instance), slice.end().clone(HeapCloner.instance)),
                                  range.deletionTime());
    }

    private static long unsharedHeapSize(RangeTombstone range)
    {
        Slice slice = range.deletedSlice();
        return RANGE_TOMBSTONE_SIZE
               + SLICE_SIZE
               + range.deletionTime().unsharedHeapSize()
               + slice.start().unsharedHeapSize()
               + slice.end().unsharedHeapSize();
    }

    @Override
    public boolean isLive()
    {
        return partitionDeletion.isLive() && !hasRanges();
    }

    @Override
    public DeletionTime getPartitionDeletion()
    {
        return partitionDeletion;
    }

    @Override
    public Iterator<RangeTombstone> rangeIterator(boolean reversed)
    {
        return BTree.iterator(ranges, BTree.Dir.desc(reversed));
    }

    @Override
    public Iterator<RangeTombstone> rangeIterator(Slice slice, boolean reversed)
    {
        if (BTree.isEmpty(ranges))
            return Collections.emptyIterator();

        Comparator<Object> rangeComparator = rangeComparator(comparator);
        int first = Math.max(0, BTree.floorIndex(ranges, rangeComparator, (Object) slice.start()));
        int last = BTree.floorIndex(ranges, rangeComparator, (Object) slice.end());
        if (first > last)
            return Collections.emptyIterator();

        Iterator<RangeTombstone> iterator = BTree.iterator(ranges, first, last, BTree.Dir.desc(reversed));
        return new AbstractIterator<RangeTombstone>()
        {
            @Override
            protected RangeTombstone computeNext()
            {
                while (iterator.hasNext())
                {
                    RangeTombstone range = iterator.next();
                    ClusteringBound<?> start = comparator.compare(range.deletedSlice().start(), slice.start()) < 0
                                                ? slice.start()
                                                : range.deletedSlice().start();
                    ClusteringBound<?> end = comparator.compare(slice.end(), range.deletedSlice().end()) < 0
                                              ? slice.end()
                                              : range.deletedSlice().end();
                    if (!Slice.isEmpty(comparator, start, end))
                        return start == range.deletedSlice().start() && end == range.deletedSlice().end()
                               ? range
                               : new RangeTombstone(Slice.make(start, end), range.deletionTime());
                }
                return endOfData();
            }
        };
    }

    @Override
    public RangeTombstone rangeCovering(Clustering<?> name)
    {
        Object found = BTree.floor(ranges, rangeComparator(comparator), (Object) name);
        RangeTombstone range = (RangeTombstone) found;
        return range != null && comparator.compare(name, range.deletedSlice().end()) < 0 ? range : null;
    }

    @Override
    public int dataSize()
    {
        return dataSize;
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
        RangeTombstoneList copy = new RangeTombstoneList(comparator, rangeCount());
        Iterator<RangeTombstone> iterator = rangeIterator(false);
        while (iterator.hasNext())
            copy.add(iterator.next());
        return new MutableDeletionInfo(partitionDeletion, copy);
    }

    @Override
    public DeletionInfo clone(ByteBufferCloner cloner)
    {
        return mutableCopy().clone(cloner);
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
    public long unsharedHeapSize()
    {
        long size = EMPTY_SIZE + partitionDeletion.unsharedHeapSize() + BTree.sizeOnHeapOf(ranges);
        Iterator<RangeTombstone> iterator = rangeIterator(false);
        while (iterator.hasNext())
            size += unsharedHeapSize(iterator.next());
        return size;
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
            rangesHash += range.deletedSlice().start().hashCode() + range.deletedSlice().end().hashCode();
            rangesHash += (int) (range.deletionTime().markedForDeleteAt() ^ (range.deletionTime().markedForDeleteAt() >>> 32));
            rangesHash += range.deletionTime().localDeletionTimeUnsignedInteger();
        }

        int result = 1;
        result = 31 * result + partitionDeletion.hashCode();
        return 31 * result + rangesHash;
    }

    private static final class RangeAppendFunction implements UpdateFunction<RangeTombstone, RangeTombstone>
    {
        private long rangeBytes;

        @Override
        public RangeTombstone insert(RangeTombstone update)
        {
            RangeTombstone copy = cloneRange(update);
            rangeBytes += unsharedHeapSize(copy);
            return copy;
        }

        @Override
        public RangeTombstone merge(RangeTombstone existing, RangeTombstone update)
        {
            throw new AssertionError("ordered range append unexpectedly replaced an existing tombstone");
        }

        @Override
        public void onAllocatedOnHeap(long heapSize)
        {
            // The immutable tree may retain most nodes from the prior snapshot.  Its net retained
            // structure delta is reconciled after update by sizeOnHeapDeltaForAppend.
        }
    }
}
