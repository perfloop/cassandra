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

import java.util.Comparator;
import java.util.Iterator;

import com.google.common.base.Objects;

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
import org.apache.cassandra.utils.btree.BTreeSearchIterator;
import org.apache.cassandra.utils.btree.UpdateFunction;
import org.apache.cassandra.utils.memory.ByteBufferCloner;
import org.apache.cassandra.utils.memory.HeapCloner;

/**
 * Immutable deletion information used only by {@link AtomicBTreePartition} while ordered single-range
 * updates are appended. The BTree versions share their unchanged nodes; callers that need to mutate a
 * deletion info materialize the normal compact {@link MutableDeletionInfo} representation.
 */
final class ImmutableBTreeDeletionInfo implements DeletionInfo
{
    private static final long EMPTY_SIZE = ObjectSizes.measure(new ImmutableBTreeDeletionInfo(null, DeletionTime.LIVE, BTree.empty()));
    private static final long RANGE_TOMBSTONE_SIZE = ObjectSizes.measure(new RangeTombstone(Slice.ALL, DeletionTime.LIVE));
    private static final long SLICE_SIZE = ObjectSizes.measure(Slice.ALL);

    private final ClusteringComparator comparator;
    private final DeletionTime partitionDeletion;
    private final Object[] ranges;

    private ImmutableBTreeDeletionInfo(ClusteringComparator comparator, DeletionTime partitionDeletion, Object[] ranges)
    {
        this.comparator = comparator;
        this.partitionDeletion = partitionDeletion;
        this.ranges = ranges;
    }

    static ImmutableBTreeDeletionInfo create(ClusteringComparator comparator,
                                             DeletionTime partitionDeletion,
                                             RangeTombstone range,
                                             UpdateFunction<RangeTombstone, RangeTombstone> updateFunction)
    {
        return new ImmutableBTreeDeletionInfo(comparator, partitionDeletion, BTree.empty()).append(range, partitionDeletion, updateFunction);
    }

    boolean canAppend(RangeTombstone range)
    {
        if (BTree.isEmpty(ranges))
            return true;

        RangeTombstone last = BTree.findByIndex(ranges, BTree.size(ranges) - 1);
        return comparator.compare(last.deletedSlice().end(), range.deletedSlice().start()) <= 0;
    }

    ImmutableBTreeDeletionInfo append(RangeTombstone range,
                                      DeletionTime newPartitionDeletion,
                                      UpdateFunction<RangeTombstone, RangeTombstone> updateFunction)
    {
        RangeTombstone copy = copyForHeap(range);
        Object[] updated = BTree.update(ranges, BTree.singleton(copy), rangeComparator(), updateFunction);
        return new ImmutableBTreeDeletionInfo(comparator, newPartitionDeletion, updated);
    }

    ImmutableBTreeDeletionInfo withPartitionDeletion(DeletionTime deletion)
    {
        if (!deletion.supersedes(partitionDeletion))
            return this;

        return new ImmutableBTreeDeletionInfo(comparator, deletion, ranges);
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
        if (slice == Slice.ALL)
            return rangeIterator(reversed);

        Iterator<RangeTombstone> iterator = rangeIterator(reversed);
        return new AbstractIterator<RangeTombstone>()
        {
            @Override
            protected RangeTombstone computeNext()
            {
                while (iterator.hasNext())
                {
                    RangeTombstone range = iterator.next();
                    Slice rangeSlice = range.deletedSlice();
                    if (!rangeSlice.intersects(comparator, slice))
                        continue;

                    ClusteringBound<?> start = max(rangeSlice.start(), slice.start());
                    ClusteringBound<?> end = min(rangeSlice.end(), slice.end());
                    if (Slice.isEmpty(comparator, start, end))
                        continue;

                    if (start == rangeSlice.start() && end == rangeSlice.end())
                        return range;

                    return new RangeTombstone(Slice.make(start, end), range.deletionTime());
                }
                return endOfData();
            }
        };
    }

    @Override
    public RangeTombstone rangeCovering(Clustering<?> name)
    {
        BTreeSearchIterator<Clustering<?>, RangeTombstone> iterator = BTree.slice(ranges, rangeComparator(), BTree.Dir.DESC);
        RangeTombstone range = iterator.next(name);
        if (range == null && iterator.hasNext())
            range = iterator.next();

        return range != null && range.deletedSlice().includes(comparator, name) ? range : null;
    }

    @Override
    public void collectStats(EncodingStats.Collector collector)
    {
        collector.update(partitionDeletion);
        for (RangeTombstone range : BTree.<RangeTombstone>iterable(ranges))
            collector.update(range.deletionTime());
    }

    @Override
    public int dataSize()
    {
        int size = TypeSizes.sizeof(partitionDeletion.markedForDeleteAt());
        if (BTree.isEmpty(ranges))
            return size;

        size += TypeSizes.sizeof(BTree.size(ranges));
        for (RangeTombstone range : BTree.<RangeTombstone>iterable(ranges))
        {
            Slice slice = range.deletedSlice();
            size += slice.start().dataSize() + slice.end().dataSize();
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
        if (BTree.isEmpty(ranges))
            return new MutableDeletionInfo(partitionDeletion);

        RangeTombstoneList rangesCopy = new RangeTombstoneList(comparator, BTree.size(ranges));
        Iterator<RangeTombstone> iterator = rangeIterator(false);
        while (iterator.hasNext())
            rangesCopy.add(iterator.next());
        return new MutableDeletionInfo(partitionDeletion, rangesCopy);
    }

    @Override
    public DeletionInfo clone(ByteBufferCloner cloner)
    {
        return mutableCopy().clone(cloner);
    }

    @Override
    public long unsharedHeapSize()
    {
        // BTree nodes and range payloads are accounted at their allocation site through the updater callback.
        // A version only owns this wrapper and its partition deletion; the BTree itself is shared by versions.
        return EMPTY_SIZE + partitionDeletion.unsharedHeapSize();
    }

    @Override
    public boolean equals(Object other)
    {
        if (!(other instanceof DeletionInfo))
            return false;

        DeletionInfo that = (DeletionInfo) other;
        if (!partitionDeletion.equals(that.getPartitionDeletion()) || rangeCount() != that.rangeCount())
            return false;

        Iterator<RangeTombstone> left = rangeIterator(false);
        Iterator<RangeTombstone> right = that.rangeIterator(false);
        while (left.hasNext())
        {
            if (!left.next().equals(right.next()))
                return false;
        }
        return !right.hasNext();
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(partitionDeletion, rangeHash());
    }

    static long rangeTombstoneHeapSize(RangeTombstone range)
    {
        Slice slice = range.deletedSlice();
        return RANGE_TOMBSTONE_SIZE
               + (slice == Slice.ALL ? 0 : SLICE_SIZE)
               + slice.start().unsharedHeapSize()
               + slice.end().unsharedHeapSize()
               + range.deletionTime().unsharedHeapSize();
    }

    private int rangeHash()
    {
        int result = rangeCount();
        for (RangeTombstone range : BTree.<RangeTombstone>iterable(ranges))
        {
            Slice slice = range.deletedSlice();
            long markedAt = range.deletionTime().markedForDeleteAt();
            result += slice.start().hashCode() + slice.end().hashCode();
            result += (int) (markedAt ^ (markedAt >>> 32));
            result += range.deletionTime().localDeletionTimeUnsignedInteger();
        }
        return result;
    }

    private ClusteringBound<?> max(ClusteringBound<?> left, ClusteringBound<?> right)
    {
        return comparator.compare(left, right) < 0 ? right : left;
    }

    private ClusteringBound<?> min(ClusteringBound<?> left, ClusteringBound<?> right)
    {
        return comparator.compare(left, right) < 0 ? left : right;
    }

    private Comparator<Object> rangeComparator()
    {
        return new RangeComparator(comparator);
    }

    private static RangeTombstone copyForHeap(RangeTombstone range)
    {
        Slice slice = range.deletedSlice();
        DeletionTime deletion = range.deletionTime();
        return new RangeTombstone(Slice.make(slice.start().clone(HeapCloner.instance), slice.end().clone(HeapCloner.instance)),
                                  DeletionTime.build(deletion.markedForDeleteAt(), deletion.localDeletionTime()));
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
            RangeTombstone range = (RangeTombstone) left;
            ClusteringPrefix<?> rightStart = right instanceof RangeTombstone
                                             ? ((RangeTombstone) right).deletedSlice().start()
                                             : (ClusteringPrefix<?>) right;
            return comparator.compare(range.deletedSlice().start(), rightStart);
        }
    }
}
