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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

import com.google.common.collect.Iterators;

import accord.utils.AsymmetricComparator;

import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.utils.AbstractIterator;
import org.apache.cassandra.utils.CassandraUInt;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.btree.BTree;
import org.apache.cassandra.utils.btree.BTreeRemoval;
import org.apache.cassandra.utils.btree.UpdateFunction;
import org.apache.cassandra.utils.memory.ByteBufferCloner;
import org.apache.cassandra.utils.memory.HeapCloner;

/**
 * An immutable deletion-info version backed by a persistent BTree of ranges.
 *
 * <p>The tree holds canonical, non-overlapping ranges ordered by their start bound. A range update reconciles only
 * the affected tree interval through {@link RangeTombstoneList}, then publishes a new tree that reuses every
 * unaffected node from the previous version. A reference-only index over the same immutable ranges retains the
 * contiguous point-lookup access pattern without retaining {@code RangeTombstoneList} storage.</p>
 */
public final class BTreeDeletionInfo implements DeletionInfo
{
    private static final long EMPTY_SIZE = ObjectSizes.measure(new BTreeDeletionInfo(null,
                                                                                      null,
                                                                                      null,
                                                                                      DeletionTime.LIVE,
                                                                                      BTree.empty(),
                                                                                      new Range[0],
                                                                                      0,
                                                                                      0,
                                                                                      0,
                                                                                      Long.MIN_VALUE,
                                                                                      0));
    private static final long RANGE_COMPARATOR_SIZE = ObjectSizes.measure(new RangeComparator(null));
    private static final long RANGE_SEARCH_COMPARATOR_SIZE = ObjectSizes.measure(new RangeSearchComparator(null));
    private static final long READ_ONLY_BOUND_SIZE = ObjectSizes.measure(new ReadOnlyClusteringBound(ClusteringBound.BOTTOM));
    private static final long RANGE_SIZE = ObjectSizes.measure(new Range(ClusteringBound.BOTTOM,
                                                                           ClusteringBound.TOP,
                                                                           0,
                                                                           0));

    private final ClusteringComparator clusteringComparator;
    private final Comparator<Range> rangeComparator;
    private final AsymmetricComparator<ClusteringPrefix<?>, Range> rangeSearchComparator;
    private final DeletionTime partitionDeletion;
    private final Object[] ranges;
    // References the same immutable Range values as ranges, avoiding a BTree walk for point lookups.
    private final Range[] rangeIndex;
    private final long treeHeapSize;
    private final long rangeHeapSize;
    private final int rangeDataSize;
    private final long maxRangeTimestamp;
    private final long unsharedHeapSize;

    private BTreeDeletionInfo(ClusteringComparator clusteringComparator,
                              Comparator<Range> rangeComparator,
                              AsymmetricComparator<ClusteringPrefix<?>, Range> rangeSearchComparator,
                              DeletionTime partitionDeletion,
                              Object[] ranges,
                              Range[] rangeIndex,
                              long treeHeapSize,
                              long rangeHeapSize,
                              int rangeDataSize,
                              long maxRangeTimestamp,
                              long unsharedHeapSize)
    {
        this.clusteringComparator = clusteringComparator;
        this.rangeComparator = rangeComparator;
        this.rangeSearchComparator = rangeSearchComparator;
        this.partitionDeletion = partitionDeletion;
        this.ranges = ranges;
        this.rangeIndex = rangeIndex;
        this.treeHeapSize = treeHeapSize;
        this.rangeHeapSize = rangeHeapSize;
        this.rangeDataSize = rangeDataSize;
        this.maxRangeTimestamp = maxRangeTimestamp;
        this.unsharedHeapSize = unsharedHeapSize;
    }

    /**
     * Merges two deletion infos into an immutable BTree version. The comparator must equal the ordering authority of
     * every input that retains ranges. Inputs whose ordering authority cannot be determined are rejected. Existing
     * mutable ranges are converted only once; subsequent updates reuse the persistent tree.
     */
    public static DeletionInfo merge(DeletionInfo existing, DeletionInfo update, ClusteringComparator comparator)
    {
        Objects.requireNonNull(comparator, "comparator");
        verifyComparator(existing, comparator);
        verifyComparator(update, comparator);

        if (existing instanceof BTreeDeletionInfo)
            return ((BTreeDeletionInfo) existing).merge(update);

        return from(existing, comparator).merge(update);
    }

    private static void verifyComparator(DeletionInfo deletionInfo, ClusteringComparator comparator)
    {
        ClusteringComparator owner = null;
        if (deletionInfo instanceof BTreeDeletionInfo)
            owner = ((BTreeDeletionInfo) deletionInfo).clusteringComparator;
        else if (deletionInfo instanceof MutableDeletionInfo)
            owner = ((MutableDeletionInfo) deletionInfo).rangeComparator();
        if (owner == null)
        {
            if (deletionInfo.hasRanges())
                throw new IllegalArgumentException("Cannot merge range tombstones without a clustering comparator");
            return;
        }

        if (!owner.equals(comparator))
            throw new IllegalArgumentException("Cannot merge deletion infos with different clustering comparators");
    }

    private static BTreeDeletionInfo from(DeletionInfo deletionInfo, ClusteringComparator comparator)
    {
        Comparator<Range> rangeComparator = new RangeComparator(comparator);
        AsymmetricComparator<ClusteringPrefix<?>, Range> rangeSearchComparator = new RangeSearchComparator(comparator);
        List<Range> ranges = new ArrayList<>(deletionInfo.rangeCount());
        Iterator<RangeTombstone> iterator = deletionInfo.rangeIterator(false);
        long rangeHeapSize = 0;
        int rangeDataSize = 0;
        long maxRangeTimestamp = Long.MIN_VALUE;
        while (iterator.hasNext())
        {
            Range range = Range.from(clone(iterator.next(), HeapCloner.instance));
            ranges.add(range);
            rangeHeapSize += range.unsharedHeapSize();
            rangeDataSize += range.dataSize();
            maxRangeTimestamp = Math.max(maxRangeTimestamp, range.markedAt);
        }

        Object[] tree = build(ranges, rangeComparator);
        Range[] rangeIndex = ranges.toArray(new Range[0]);
        if (!ranges.isEmpty())
            rangeDataSize += TypeSizes.sizeof(ranges.size());

        return create(comparator,
                      rangeComparator,
                      rangeSearchComparator,
                      deletionInfo.getPartitionDeletion(),
                      tree,
                      rangeIndex,
                      BTree.sizeOnHeapOf(tree),
                      rangeHeapSize,
                      rangeDataSize,
                      maxRangeTimestamp);
    }

    private static BTreeDeletionInfo create(ClusteringComparator comparator,
                                            Comparator<Range> rangeComparator,
                                            AsymmetricComparator<ClusteringPrefix<?>, Range> rangeSearchComparator,
                                            DeletionTime partitionDeletion,
                                            Object[] ranges,
                                            Range[] rangeIndex,
                                            long treeHeapSize,
                                            long rangeHeapSize,
                                            int rangeDataSize,
                                            long maxRangeTimestamp)
    {
        long unsharedHeapSize = EMPTY_SIZE
                                + RANGE_COMPARATOR_SIZE
                                + RANGE_SEARCH_COMPARATOR_SIZE
                                + partitionDeletion.unsharedHeapSize()
                                + treeHeapSize
                                + rangeHeapSize
                                + ObjectSizes.sizeOfReferenceArray(rangeIndex.length);
        return new BTreeDeletionInfo(comparator,
                                     rangeComparator,
                                     rangeSearchComparator,
                                     partitionDeletion,
                                     ranges,
                                     rangeIndex,
                                     treeHeapSize,
                                     rangeHeapSize,
                                     rangeDataSize,
                                     maxRangeTimestamp,
                                     unsharedHeapSize);
    }

    private BTreeDeletionInfo merge(DeletionInfo update)
    {
        DeletionTime newPartitionDeletion = update.getPartitionDeletion().supersedes(partitionDeletion)
                                            ? update.getPartitionDeletion()
                                            : partitionDeletion;

        if (!update.hasRanges())
        {
            if (newPartitionDeletion == partitionDeletion)
                return this;

            return create(clusteringComparator,
                          rangeComparator,
                          rangeSearchComparator,
                          newPartitionDeletion,
                          ranges,
                          rangeIndex,
                          treeHeapSize,
                          rangeHeapSize,
                          rangeDataSize,
                          maxRangeTimestamp);
        }

        return mergeRanges(update, newPartitionDeletion);
    }

    private BTreeDeletionInfo mergeRanges(DeletionInfo update, DeletionTime newPartitionDeletion)
    {
        List<RangeTombstone> updates = new ArrayList<>(update.rangeCount());
        ClusteringBound<?> firstStart = null;
        ClusteringBound<?> lastEnd = null;
        Iterator<RangeTombstone> updateIterator = update.rangeIterator(false);
        while (updateIterator.hasNext())
        {
            RangeTombstone updateRange = clone(updateIterator.next(), HeapCloner.instance);
            updates.add(updateRange);

            ClusteringBound<?> start = updateRange.deletedSlice().start();
            ClusteringBound<?> end = updateRange.deletedSlice().end();
            if (firstStart == null || clusteringComparator.compare(start, firstStart) < 0)
                firstStart = start;
            if (lastEnd == null || clusteringComparator.compare(lastEnd, end) < 0)
                lastEnd = end;
        }

        AffectedRanges affected = affectedRanges(firstStart, lastEnd);
        MutableDeletionInfo reconciliation = new MutableDeletionInfo(DeletionTime.LIVE);
        for (Range range : affected.ranges)
            reconciliation.add(range.toTombstone(), clusteringComparator);
        for (RangeTombstone updateRange : updates)
            reconciliation.add(updateRange, clusteringComparator);

        List<Range> replacements = new ArrayList<>(reconciliation.rangeCount());
        Iterator<RangeTombstone> reconciled = reconciliation.rangeIterator(false);
        long replacementHeapSize = 0;
        int replacementDataSize = 0;
        long replacementMaxTimestamp = Long.MIN_VALUE;
        while (reconciled.hasNext())
        {
            Range replacement = Range.from(reconciled.next());
            replacements.add(replacement);
            replacementHeapSize += replacement.unsharedHeapSize();
            replacementDataSize += replacement.dataSize();
            replacementMaxTimestamp = Math.max(replacementMaxTimestamp, replacement.markedAt);
        }

        Object[] newRanges = ranges;
        long affectedHeapSize = 0;
        int affectedDataSize = 0;
        for (Range range : affected.ranges)
        {
            newRanges = BTreeRemoval.remove(newRanges, rangeComparator, range);
            affectedHeapSize += range.unsharedHeapSize();
            affectedDataSize += range.dataSize();
        }

        if (!replacements.isEmpty())
        {
            Object[] replacementTree = build(replacements, rangeComparator);
            UpdateFunction.Simple<Range> replace = UpdateFunction.Simple.of((existing, replacement) -> replacement);
            newRanges = BTree.update(newRanges, replacementTree, rangeComparator, replace);
        }

        Range[] newRangeIndex = replaceRangeIndex(affected, replacements);
        int oldRangeCount = rangeCount();
        int newRangeCount = BTree.size(newRanges);
        assert newRangeCount == newRangeIndex.length;
        int newRangeDataSize = rangeDataSize - affectedDataSize + replacementDataSize;
        if (oldRangeCount == 0 && newRangeCount > 0)
            newRangeDataSize += TypeSizes.sizeof(newRangeCount);
        else if (oldRangeCount > 0 && newRangeCount == 0)
            newRangeDataSize -= TypeSizes.sizeof(oldRangeCount);

        return create(clusteringComparator,
                      rangeComparator,
                      rangeSearchComparator,
                      newPartitionDeletion,
                      newRanges,
                      newRangeIndex,
                      BTree.sizeOnHeapOf(newRanges),
                      rangeHeapSize - affectedHeapSize + replacementHeapSize,
                      newRangeDataSize,
                      Math.max(maxRangeTimestamp, replacementMaxTimestamp));
    }

    private Range[] replaceRangeIndex(AffectedRanges affected, List<Range> replacements)
    {
        int affectedCount = affected.end - affected.start + 1;
        Range[] updated = new Range[rangeIndex.length - affectedCount + replacements.size()];
        System.arraycopy(rangeIndex, 0, updated, 0, affected.start);
        for (int i = 0; i < replacements.size(); i++)
            updated[affected.start + i] = replacements.get(i);
        System.arraycopy(rangeIndex,
                         affected.end + 1,
                         updated,
                         affected.start + replacements.size(),
                         rangeIndex.length - affected.end - 1);
        return updated;
    }

    private AffectedRanges affectedRanges(ClusteringBound<?> firstStart, ClusteringBound<?> lastEnd)
    {
        if (BTree.isEmpty(ranges))
            return new AffectedRanges(0, -1, Collections.emptyList());

        int start = BTree.floorIndex(ranges, rangeComparator, Range.search(firstStart));
        if (start < 0)
            start = 0;

        int end = BTree.ceilIndex(ranges, rangeComparator, Range.search(lastEnd));
        int size = BTree.size(ranges);
        if (end >= size)
            end = size - 1;
        if (start > end)
            return new AffectedRanges(start, end, Collections.emptyList());

        List<Range> affected = new ArrayList<>(end - start + 1);
        Iterator<Range> iterator = BTree.slice(ranges, rangeComparator, start, end, BTree.Dir.ASC);
        iterator.forEachRemaining(affected::add);
        return new AffectedRanges(start, end, affected);
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
        if (BTree.isEmpty(ranges))
            return Collections.emptyIterator();

        return rangeIterator(BTree.iterator(ranges, reversed ? BTree.Dir.DESC : BTree.Dir.ASC), null, reversed);
    }

    @Override
    public Iterator<RangeTombstone> rangeIterator(Slice slice, boolean reversed)
    {
        if (BTree.isEmpty(ranges))
            return Collections.emptyIterator();

        return reversed ? reverseRangeIterator(slice) : forwardRangeIterator(slice);
    }

    private Iterator<RangeTombstone> forwardRangeIterator(Slice slice)
    {
        int index = BTree.floorIndex(ranges, rangeSearchComparator, slice.start());
        if (index < 0)
            index = 0;

        Range range = rangeIndex[index];
        if (clusteringComparator.compare(slice.end(), range.startBound()) < 0)
            return Collections.emptyIterator();

        RangeTombstone clipped = clipped(range, slice);
        if (clusteringComparator.compare(slice.end(), range.end) < 0 || index == rangeCount() - 1)
            return rangeIterator(clipped);

        Range next = rangeIndex[index + 1];
        if (clusteringComparator.compare(slice.end(), next.startBound()) < 0)
            return rangeIterator(clipped);

        return rangeIterator(BTree.slice(ranges, rangeComparator, index, rangeCount() - 1, BTree.Dir.ASC), slice, false);
    }

    private Iterator<RangeTombstone> reverseRangeIterator(Slice slice)
    {
        int index = BTree.floorIndex(ranges, rangeSearchComparator, slice.end());
        if (index < 0)
            return Collections.emptyIterator();

        Range range = rangeIndex[index];
        if (clusteringComparator.compare(range.end, slice.start()) < 0)
            return Collections.emptyIterator();

        RangeTombstone clipped = clipped(range, slice);
        if (clusteringComparator.compare(range.startBound(), slice.start()) < 0 || index == 0)
            return rangeIterator(clipped);

        Range previous = rangeIndex[index - 1];
        if (clusteringComparator.compare(previous.end, slice.start()) < 0)
            return rangeIterator(clipped);

        return rangeIterator(BTree.slice(ranges, rangeComparator, 0, index, BTree.Dir.DESC), slice, true);
    }

    private Iterator<RangeTombstone> rangeIterator(Iterator<Range> iterator, Slice slice, boolean reversed)
    {
        return new AbstractIterator<RangeTombstone>()
        {
            @Override
            protected RangeTombstone computeNext()
            {
                while (iterator.hasNext())
                {
                    Range range = iterator.next();
                    if (slice == null)
                        return range.toReadTombstone();

                    if (reversed ? clusteringComparator.compare(range.end, slice.start()) < 0
                                 : clusteringComparator.compare(slice.end(), range.startBound()) < 0)
                        return endOfData();

                    RangeTombstone clipped = clipped(range, slice);
                    if (clipped != null)
                        return clipped;
                }
                return endOfData();
            }
        };
    }

    private Iterator<RangeTombstone> rangeIterator(RangeTombstone range)
    {
        return range == null ? Collections.emptyIterator() : Iterators.singletonIterator(range);
    }

    private RangeTombstone clipped(Range range, Slice slice)
    {
        ClusteringBound<?> start = clusteringComparator.compare(range.startBound(), slice.start()) < 0
                                    ? slice.start()
                                    : range.readStartBound();
        ClusteringBound<?> end = clusteringComparator.compare(slice.end(), range.end) < 0
                                  ? slice.end()
                                  : range.readEndBound();
        return Slice.isEmpty(clusteringComparator, start, end)
               ? null
               : new RangeTombstone(Slice.make(start, end),
                                    DeletionTime.buildUnsafeWithUnsignedInteger(range.markedAt, range.localDeletionTime));
    }

    @Override
    public RangeTombstone rangeCovering(Clustering<?> name)
    {
        int index = floorRangeIndex(name);
        if (index < 0)
            return null;

        Range range = rangeIndex[index];
        return clusteringComparator.compare(name, range.end) < 0 ? range.toReadTombstone() : null;
    }

    private int floorRangeIndex(Clustering<?> name)
    {
        int low = 0;
        int high = rangeIndex.length - 1;
        while (low <= high)
        {
            int mid = (low + high) >>> 1;
            int comparison = clusteringComparator.compare(name, rangeIndex[mid].start);
            if (comparison > 0)
                low = mid + 1;
            else if (comparison < 0)
                high = mid - 1;
            else
                return mid;
        }
        return high;
    }

    @Override
    public void collectStats(EncodingStats.Collector collector)
    {
        collector.update(partitionDeletion);
        Iterator<Range> iterator = BTree.iterator(ranges);
        while (iterator.hasNext())
        {
            Range range = iterator.next();
            collector.updateTimestamp(range.markedAt);
            collector.updateLocalDeletionTime(CassandraUInt.toLong(range.localDeletionTime));
        }
    }

    @Override
    public int dataSize()
    {
        return TypeSizes.sizeof(partitionDeletion.markedForDeleteAt()) + rangeDataSize;
    }

    @Override
    public boolean hasRanges()
    {
        return !BTree.isEmpty(ranges);
    }

    @Override
    public int rangeCount()
    {
        return rangeIndex.length;
    }

    @Override
    public long maxTimestamp()
    {
        return Math.max(partitionDeletion.markedForDeleteAt(), maxRangeTimestamp);
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
        Iterator<Range> iterator = BTree.iterator(ranges);
        while (iterator.hasNext())
            copy.add(clone(iterator.next().toTombstone(), HeapCloner.instance), clusteringComparator);
        return copy;
    }

    @Override
    public DeletionInfo clone(ByteBufferCloner cloner)
    {
        if (BTree.isEmpty(ranges))
            return create(clusteringComparator,
                          rangeComparator,
                          rangeSearchComparator,
                          partitionDeletion,
                          BTree.empty(),
                          new Range[0],
                          0,
                          0,
                          0,
                          Long.MIN_VALUE);

        List<Range> cloned = new ArrayList<>(rangeCount());
        Iterator<Range> iterator = BTree.iterator(ranges);
        long clonedHeapSize = 0;
        int clonedDataSize = TypeSizes.sizeof(rangeCount());
        while (iterator.hasNext())
        {
            Range range = iterator.next().clone(cloner);
            cloned.add(range);
            clonedHeapSize += range.unsharedHeapSize();
            clonedDataSize += range.dataSize();
        }

        Object[] clonedTree = build(cloned, rangeComparator);
        Range[] clonedIndex = cloned.toArray(new Range[0]);
        return create(clusteringComparator,
                      rangeComparator,
                      rangeSearchComparator,
                      partitionDeletion,
                      clonedTree,
                      clonedIndex,
                      BTree.sizeOnHeapOf(clonedTree),
                      clonedHeapSize,
                      clonedDataSize,
                      maxRangeTimestamp);
    }

    @Override
    public long unsharedHeapSize()
    {
        return unsharedHeapSize;
    }

    private static Object[] build(List<Range> ranges, Comparator<Range> comparator)
    {
        if (ranges.isEmpty())
            return BTree.empty();

        BTree.Builder<Range> builder = BTree.builder(comparator, ranges.size()).auto(false);
        for (Range range : ranges)
            builder.add(range);
        return builder.build();
    }

    private static RangeTombstone clone(RangeTombstone tombstone, ByteBufferCloner cloner)
    {
        Slice slice = tombstone.deletedSlice();
        return new RangeTombstone(Slice.make(slice.start().clone(cloner), slice.end().clone(cloner)), tombstone.deletionTime());
    }

    private static final class AffectedRanges
    {
        private final int start;
        private final int end;
        private final List<Range> ranges;

        private AffectedRanges(int start, int end, List<Range> ranges)
        {
            this.start = start;
            this.end = end;
            this.ranges = ranges;
        }
    }

    private static final class RangeComparator implements Comparator<Range>
    {
        private final ClusteringComparator clusteringComparator;

        private RangeComparator(ClusteringComparator clusteringComparator)
        {
            this.clusteringComparator = clusteringComparator;
        }

        @Override
        public int compare(Range left, Range right)
        {
            return clusteringComparator.compare(left.start, right.start);
        }
    }

    private static final class RangeSearchComparator implements AsymmetricComparator<ClusteringPrefix<?>, Range>
    {
        private final ClusteringComparator clusteringComparator;

        private RangeSearchComparator(ClusteringComparator clusteringComparator)
        {
            this.clusteringComparator = clusteringComparator;
        }

        @Override
        public int compare(ClusteringPrefix<?> name, Range range)
        {
            return clusteringComparator.compare(name, range.start);
        }
    }

    private static final class Range
    {
        private final ClusteringBound<?> start;
        private final ClusteringBound<?> end;
        private final ReadOnlyClusteringBound readStart;
        private final ReadOnlyClusteringBound readEnd;
        private final long markedAt;
        private final int localDeletionTime;

        private Range(ClusteringBound<?> start, ClusteringBound<?> end, long markedAt, int localDeletionTime)
        {
            this(start, end, markedAt, localDeletionTime, true);
        }

        private Range(ClusteringBound<?> start,
                      ClusteringBound<?> end,
                      long markedAt,
                      int localDeletionTime,
                      boolean retain)
        {
            this.start = start;
            this.end = end;
            this.readStart = retain ? readOnly(start) : null;
            this.readEnd = retain ? readOnly(end) : null;
            this.markedAt = markedAt;
            this.localDeletionTime = localDeletionTime;
        }

        private static Range from(RangeTombstone tombstone)
        {
            Slice slice = tombstone.deletedSlice();
            DeletionTime deletionTime = tombstone.deletionTime();
            return new Range(slice.start(),
                             slice.end(),
                             deletionTime.markedForDeleteAt(),
                             deletionTime.localDeletionTimeUnsignedInteger());
        }

        private static Range search(ClusteringBound<?> start)
        {
            return new Range(start, null, 0, 0, false);
        }

        private ClusteringBound<?> startBound()
        {
            return start;
        }

        private ClusteringBound<?> readStartBound()
        {
            return readStart;
        }

        private ClusteringBound<?> readEndBound()
        {
            return readEnd;
        }

        private RangeTombstone toTombstone()
        {
            return new RangeTombstone(Slice.make(startBound(), end),
                                      DeletionTime.buildUnsafeWithUnsignedInteger(markedAt, localDeletionTime));
        }

        private RangeTombstone toReadTombstone()
        {
            return new RangeTombstone(Slice.make(readStartBound(), readEndBound()),
                                      DeletionTime.buildUnsafeWithUnsignedInteger(markedAt, localDeletionTime));
        }

        private Range clone(ByteBufferCloner cloner)
        {
            return new Range(startBound().clone(cloner), end.clone(cloner), markedAt, localDeletionTime);
        }

        private int dataSize()
        {
            return startBound().dataSize()
                   + end.dataSize()
                   + TypeSizes.sizeof(markedAt)
                   + TypeSizes.sizeof(localDeletionTime);
        }

        private long unsharedHeapSize()
        {
            return RANGE_SIZE
                   + startBound().unsharedHeapSize()
                   + end.unsharedHeapSize()
                   + readStart.unsharedHeapSize()
                   + readEnd.unsharedHeapSize();
        }
    }

    /**
     * Keeps public range bounds detached from the values retained by the BTree. Each value view is created only when
     * a caller reads it, so changing a returned buffer's contents, position, or limit cannot alter stored ranges.
     */
    private static final class ReadOnlyClusteringBound extends AbstractBufferClusteringPrefix implements ClusteringBound<ByteBuffer>
    {
        private final ClusteringBound<?> bound;

        private ReadOnlyClusteringBound(ClusteringBound<?> bound)
        {
            super(bound.kind(), EMPTY_VALUES_ARRAY);
            this.bound = bound;
        }

        @Override
        public int size()
        {
            return bound.size();
        }

        @Override
        public ByteBuffer get(int i)
        {
            Object value = bound.get(i);
            return value == null ? null : ((ByteBuffer) value).asReadOnlyBuffer();
        }

        @Override
        public ByteBuffer[] getRawValues()
        {
            ByteBuffer[] values = new ByteBuffer[size()];
            for (int i = 0; i < values.length; i++)
                values[i] = get(i);
            return values;
        }

        @Override
        public ClusteringPrefix<ByteBuffer> retainable()
        {
            return this;
        }

        @Override
        public ClusteringBound<ByteBuffer> invert()
        {
            return new ReadOnlyClusteringBound(bound.invert());
        }

        @Override
        public ClusteringBound<ByteBuffer> clone(ByteBufferCloner cloner)
        {
            return bound.clone(cloner);
        }

        @Override
        public long unsharedHeapSize()
        {
            return READ_ONLY_BOUND_SIZE;
        }
    }

    private static ReadOnlyClusteringBound readOnly(ClusteringBound<?> bound)
    {
        return bound instanceof ReadOnlyClusteringBound
               ? (ReadOnlyClusteringBound) bound
               : new ReadOnlyClusteringBound(bound);
    }
}
