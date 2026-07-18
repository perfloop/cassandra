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
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;

import com.google.common.collect.Iterators;

import org.apache.cassandra.cache.IMeasurableMemory;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.utils.AbstractIterator;
import org.apache.cassandra.utils.CassandraUInt;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.memory.ByteBufferCloner;

/**
 * Data structure holding the range tombstones of a ColumnFamily.
 * <p>
 * This is essentially a sorted list of non-overlapping (tombstone) ranges.
 * <p>
 * A range tombstone has 4 elements: the start and end of the range covered,
 * and the deletion infos (markedAt timestamp and local deletion time). The
 * markedAt timestamp is what define the priority of 2 overlapping tombstones.
 * That is, given 2 tombstones {@code [0, 10]@t1 and [5, 15]@t2, then if t2 > t1} (and
 * are the tombstones markedAt values), the 2nd tombstone take precedence over
 * the first one on [5, 10]. If such tombstones are added to a RangeTombstoneList,
 * the range tombstone list will store them as [[0, 5]@t1, [5, 15]@t2].
 * <p>
 * The only use of the local deletion time is to know when a given tombstone can
 * be purged, which will be done by the purge() method.
 */
public class RangeTombstoneList implements Iterable<RangeTombstone>, IMeasurableMemory
{
    private static long EMPTY_SIZE = ObjectSizes.measure(new RangeTombstoneList(null, 0));
    private static final int PAGE_SHIFT = 6;
    private static final int PAGE_SIZE = 1 << PAGE_SHIFT;
    private static final int PAGE_MASK = PAGE_SIZE - 1;
    private static final long PAGE_SIZE_ON_HEAP = ObjectSizes.measure(new Page());

    private final ClusteringComparator comparator;

    // Note: we don't want to use a List for the markedAts and delTimes to avoid boxing. We could
    // use a List for starts and ends, but having arrays everywhere is almost simpler.
    private ClusteringBound<?>[] starts;
    private ClusteringBound<?>[] ends;
    private long[] markedAts;
    private int[] delTimesUnsignedIntegers;

    private long boundaryHeapSize;
    private int size;

    // Ordered memtable successors share immutable pages. Flat arrays remain the representation for general mutation.
    private Page[] pages;
    private ClusteringBound<?>[] pageStarts;
    private FlatView flatView;
    private long pageAllocationOnHeap;

    private RangeTombstoneList(ClusteringComparator comparator,
                               ClusteringBound<?>[] starts,
                               ClusteringBound<?>[] ends,
                               long[] markedAts,
                               int[] delTimesUnsignedIntegers,
                               long boundaryHeapSize,
                               int size)
    {
        assert starts.length == ends.length && starts.length == markedAts.length && starts.length == delTimesUnsignedIntegers.length;
        this.comparator = comparator;
        this.starts = starts;
        this.ends = ends;
        this.markedAts = markedAts;
        this.delTimesUnsignedIntegers = delTimesUnsignedIntegers;
        this.size = size;
        this.boundaryHeapSize = boundaryHeapSize;
    }

    private RangeTombstoneList(ClusteringComparator comparator,
                               Page[] pages,
                               ClusteringBound<?>[] pageStarts,
                               FlatView flatView,
                               long boundaryHeapSize,
                               int size,
                               long pageAllocationOnHeap)
    {
        this.comparator = comparator;
        this.starts = null;
        this.ends = null;
        this.markedAts = null;
        this.delTimesUnsignedIntegers = null;
        this.pages = pages;
        this.pageStarts = pageStarts;
        this.flatView = flatView;
        this.boundaryHeapSize = boundaryHeapSize;
        this.size = size;
        this.pageAllocationOnHeap = pageAllocationOnHeap;
    }

    public RangeTombstoneList(ClusteringComparator comparator, int capacity)
    {
        this(comparator, new ClusteringBound<?>[capacity], new ClusteringBound<?>[capacity], new long[capacity], new int[capacity], 0, 0);
    }

    public boolean isEmpty()
    {
        return size == 0;
    }

    public int size()
    {
        return size;
    }

    public ClusteringComparator comparator()
    {
        return comparator;
    }

    public RangeTombstoneList copy()
    {
        if (pages == null)
        {
            return new RangeTombstoneList(comparator,
                                          Arrays.copyOf(starts, size),
                                          Arrays.copyOf(ends, size),
                                          Arrays.copyOf(markedAts, size),
                                          Arrays.copyOf(delTimesUnsignedIntegers, size),
                                          boundaryHeapSize, size);
        }

        if (hasFlatView())
        {
            return new RangeTombstoneList(comparator,
                                          Arrays.copyOf(flatView.starts, size),
                                          Arrays.copyOf(flatView.ends, size),
                                          Arrays.copyOf(flatView.markedAts, size),
                                          Arrays.copyOf(flatView.delTimesUnsignedIntegers, size),
                                          boundaryHeapSize, size);
        }

        RangeTombstoneList copy = new RangeTombstoneList(comparator, size);
        copyArrays(this, copy);
        return copy;
    }

    /**
     * Creates an immutable paged snapshot for an ordered memtable update. Only this path shares storage; every
     * non-append mutation uses a flat copy so the public mutable-list contract remains unchanged.
     */
    RangeTombstoneList copyForMemtable()
    {
        if (pages != null)
            return new RangeTombstoneList(comparator, pages, pageStarts, flatView, boundaryHeapSize, size, 0);

        Page[] copiedPages = pagesFromFlatArrays();
        ClusteringBound<?>[] copiedPageStarts = pageStartsFromFlatArrays(copiedPages.length);
        FlatView copiedFlatView = createsFlatView(size)
                                  ? FlatView.copyOf(starts, ends, markedAts, delTimesUnsignedIntegers, size)
                                  : null;
        return new RangeTombstoneList(comparator,
                                      copiedPages,
                                      copiedPageStarts,
                                      copiedFlatView,
                                      boundaryHeapSize,
                                      size,
                                      pageStorageSize(copiedPages));
    }

    RangeTombstoneList copyForMemtable(RangeTombstoneList update)
    {
        if (update == null || update.isEmpty())
            return pages == null ? copy() : copyForMemtable();
        if (!canAppend(update))
            return flatCopyForMemtable();

        return copyForMemtable();
    }

    boolean canAppend(RangeTombstoneList update)
    {
        return !isEmpty()
            && !update.isEmpty()
            && comparator.compare(endAt(size - 1), update.startAt(0)) < 0;
    }

    private RangeTombstoneList flatCopyForMemtable()
    {
        RangeTombstoneList copy = copy();
        if (pages != null)
            copy.pageAllocationOnHeap = -pageStorageSize(pages);
        return copy;
    }

    public RangeTombstoneList clone(ByteBufferCloner cloner)
    {
        RangeTombstoneList copy =  new RangeTombstoneList(comparator,
                                                          new ClusteringBound<?>[size],
                                                          new ClusteringBound<?>[size],
                                                          new long[size],
                                                          new int[size],
                                                          boundaryHeapSize, size);

        for (int i = 0; i < size; i++)
        {
            copy.starts[i] = clone(startAt(i), cloner);
            copy.ends[i] = clone(endAt(i), cloner);
            copy.markedAts[i] = markedAtAt(i);
            copy.delTimesUnsignedIntegers[i] = deletionTimeAt(i);
        }

        return copy;
    }

    private boolean hasFlatView()
    {
        return flatView != null && flatView.size == size;
    }

    private ClusteringBound<?> startAt(int index)
    {
        if (pages == null)
            return starts[index];
        if (hasFlatView())
            return flatView.starts[index];
        return pages[index >>> PAGE_SHIFT].starts[index & PAGE_MASK];
    }

    private ClusteringBound<?> endAt(int index)
    {
        if (pages == null)
            return ends[index];
        if (hasFlatView())
            return flatView.ends[index];
        return pages[index >>> PAGE_SHIFT].ends[index & PAGE_MASK];
    }

    private long markedAtAt(int index)
    {
        if (pages == null)
            return markedAts[index];
        if (hasFlatView())
            return flatView.markedAts[index];
        return pages[index >>> PAGE_SHIFT].markedAts[index & PAGE_MASK];
    }

    private int deletionTimeAt(int index)
    {
        if (pages == null)
            return delTimesUnsignedIntegers[index];
        if (hasFlatView())
            return flatView.delTimesUnsignedIntegers[index];
        return pages[index >>> PAGE_SHIFT].delTimesUnsignedIntegers[index & PAGE_MASK];
    }

    private static <T> ClusteringBound<ByteBuffer> clone(ClusteringBound<T> bound, ByteBufferCloner cloner)
    {
        ByteBuffer[] values = new ByteBuffer[bound.size()];
        for (int i = 0; i < values.length; i++)
            values[i] = cloner.clone(bound.get(i), bound.accessor());
        return new BufferClusteringBound(bound.kind(), values);
    }

    public void add(RangeTombstone tombstone)
    {
        add(tombstone.deletedSlice().start(),
            tombstone.deletedSlice().end(),
            tombstone.deletionTime().markedForDeleteAt(),
            tombstone.deletionTime().localDeletionTimeUnsignedInteger());
    }

    /**
     * Adds a new range tombstone.
     *
     * This method will be faster if the new tombstone sort after all the currently existing ones (this is a common use case),
     * but it doesn't assume it.
     */
    private void add(ClusteringBound<?> start, ClusteringBound<?> end, long markedAt, int delTimeUnsignedInteger)
    {
        if (pages != null)
        {
            if (!isEmpty() && comparator.compare(endAt(size - 1), start) <= 0)
            {
                appendPaged(start, end, markedAt, delTimeUnsignedInteger);
                return;
            }
            materializeForMutation();
        }

        if (isEmpty())
        {
            addInternal(0, start, end, markedAt, delTimeUnsignedInteger);
            return;
        }

        int c = comparator.compare(ends[size-1], start);

        // Fast path if we add in sorted order
        if (c <= 0)
        {
            addInternal(size, start, end, markedAt, delTimeUnsignedInteger);
        }
        else
        {
            // Note: insertFrom expect i to be the insertion point in term of interval ends
            int pos = Arrays.binarySearch(ends, 0, size, start, comparator);
            insertFrom((pos >= 0 ? pos+1 : -pos-1), start, end, markedAt, delTimeUnsignedInteger);
        }
        boundaryHeapSize += start.unsharedHeapSize() + end.unsharedHeapSize();
    }

    /**
     * Adds all the range tombstones of {@code tombstones} to this RangeTombstoneList.
     */
    public void addAll(RangeTombstoneList tombstones)
    {
        if (tombstones.isEmpty())
            return;

        if (pages != null)
        {
            if (canAppend(tombstones))
            {
                appendAllPaged(tombstones);
                return;
            }
            materializeForMutation();
        }

        if (isEmpty())
        {
            copyArrays(tombstones, this);
            return;
        }

        /*
         * We basically have 2 techniques we can use here: either we repeatedly call add() on tombstones values,
         * or we do a merge of both (sorted) lists. If this lists is bigger enough than the one we add, then
         * calling add() will be faster, otherwise it's merging that will be faster.
         *
         * Let's note that during memtables updates, it might not be uncommon that a new update has only a few range
         * tombstones, while the CF we're adding it to (the one in the memtable) has many. In that case, using add() is
         * likely going to be faster.
         *
         * In other cases however, like when diffing responses from multiple nodes, the tombstone lists we "merge" will
         * be likely sized, so using add() might be a bit inefficient.
         *
         * Roughly speaking (this ignore the fact that updating an element is not exactly constant but that's not a big
         * deal), if n is the size of this list and m is tombstones size, merging is O(n+m) while using add() is O(m*log(n)).
         *
         * But let's not crank up a logarithm computation for that. Long story short, merging will be a bad choice only
         * if this list size is lot bigger that the other one, so let's keep it simple.
         */
        if (size > 10 * tombstones.size)
        {
            for (int i = 0; i < tombstones.size; i++)
                add(tombstones.startAt(i), tombstones.endAt(i), tombstones.markedAtAt(i), tombstones.deletionTimeAt(i));
        }
        else
        {
            int i = 0;
            int j = 0;
            while (i < size && j < tombstones.size)
            {
                if (comparator.compare(tombstones.startAt(j), ends[i]) < 0)
                {
                    insertFrom(i, tombstones.startAt(j), tombstones.endAt(j), tombstones.markedAtAt(j), tombstones.deletionTimeAt(j));
                    j++;
                }
                else
                {
                    i++;
                }
            }
            // Addds the remaining ones from tombstones if any (note that addInternal will increment size if relevant).
            for (; j < tombstones.size; j++)
                addInternal(size, tombstones.startAt(j), tombstones.endAt(j), tombstones.markedAtAt(j), tombstones.deletionTimeAt(j));
        }
    }

    /**
     * Returns whether the given name/timestamp pair is deleted by one of the tombstone
     * of this RangeTombstoneList.
     */
    public boolean isDeleted(Clustering<?> clustering, Cell<?> cell)
    {
        int idx = searchInternal(clustering, 0, size);
        // No matter what the counter cell's timestamp is, a tombstone always takes precedence. See CASSANDRA-7346.
        return idx >= 0 && (cell.isCounterCell() || markedAtAt(idx) >= cell.timestamp());
    }

    /**
     * Returns the DeletionTime for the tombstone overlapping {@code name} (there can't be more than one),
     * or null if {@code name} is not covered by any tombstone.
     */
    public DeletionTime searchDeletionTime(Clustering<?> name)
    {
        int idx = searchInternal(name, 0, size);
        if (idx < 0)
            return null;

        if (pages == null)
            return DeletionTime.buildUnsafeWithUnsignedInteger(markedAts[idx], delTimesUnsignedIntegers[idx]);
        if (hasFlatView())
            return DeletionTime.buildUnsafeWithUnsignedInteger(flatView.markedAts[idx], flatView.delTimesUnsignedIntegers[idx]);

        Page page = pages[idx >>> PAGE_SHIFT];
        int offset = idx & PAGE_MASK;
        return DeletionTime.buildUnsafeWithUnsignedInteger(page.markedAts[offset], page.delTimesUnsignedIntegers[offset]);
    }

    public RangeTombstone search(Clustering<?> name)
    {
        int idx = searchInternal(name, 0, size);
        return idx < 0 ? null : rangeTombstone(idx);
    }

    /*
     * Return is the index of the range covering name if name is covered. If the return idx is negative,
     * no range cover name and -idx-1 is the index of the first range whose start is greater than name.
     *
     * Note that bounds are not in the range if they fall on its boundary.
     */
    private int searchInternal(ClusteringPrefix<?> name, int startIdx, int endIdx)
    {
        if (isEmpty())
            return -1;

        if (pages == null)
            return searchFlat(name, starts, ends, startIdx, endIdx);
        if (hasFlatView())
            return searchFlat(name, flatView.starts, flatView.ends, startIdx, endIdx);
        return searchPaged(name, startIdx, endIdx);
    }

    private int searchFlat(ClusteringPrefix<?> name,
                           ClusteringBound<?>[] directStarts,
                           ClusteringBound<?>[] directEnds,
                           int startIdx,
                           int endIdx)
    {
        int pos = Arrays.binarySearch(directStarts, startIdx, endIdx, name, comparator);
        if (pos >= 0)
        {
            // Equality only happens for bounds (as used by forward/reverseIterator), and bounds are equal only if they
            // are the same or complementary, in either case the bound itself is not part of the range.
            return -pos - 1;
        }

        // We potentially intersect the range before our "insertion point"
        int idx = -pos - 2;
        if (idx < 0)
            return -1;

        return comparator.compare(name, directEnds[idx]) < 0 ? idx : -idx - 2;
    }

    private int searchPaged(ClusteringPrefix<?> name, int startIdx, int endIdx)
    {
        if (startIdx >= endIdx)
            return -startIdx - 1;

        int firstPage = startIdx >>> PAGE_SHIFT;
        int lastPage = (endIdx - 1) >>> PAGE_SHIFT;
        int pos = Arrays.binarySearch(pageStarts, firstPage, lastPage + 1, name, comparator);
        int pageIndex;
        if (pos >= 0)
        {
            if (pos != firstPage || (startIdx & PAGE_MASK) == 0)
                return -(pos << PAGE_SHIFT) - 1;
            pageIndex = pos;
        }
        else
        {
            pageIndex = -pos - 2;
            if (pageIndex < firstPage)
                return -startIdx - 1;
        }

        Page page = pages[pageIndex];
        int pageFirst = pageIndex << PAGE_SHIFT;
        int first = pageIndex == firstPage ? startIdx & PAGE_MASK : 0;
        int last = pageIndex == lastPage ? ((endIdx - 1) & PAGE_MASK) + 1 : PAGE_SIZE;
        int pagePos = Arrays.binarySearch(page.starts, first, last, name, comparator);
        if (pagePos >= 0)
            return -(pageFirst + pagePos) - 1;

        int idx = pageFirst + -pagePos - 2;
        if (idx < startIdx)
            return -startIdx - 1;

        return comparator.compare(name, page.ends[idx & PAGE_MASK]) < 0 ? idx : -idx - 2;
    }

    public int dataSize()
    {
        int dataSize = TypeSizes.sizeof(size);
        for (int i = 0; i < size; i++)
        {
            dataSize += startAt(i).dataSize() + endAt(i).dataSize();
            dataSize += TypeSizes.sizeof(markedAtAt(i));
            dataSize += TypeSizes.sizeof(deletionTimeAt(i));
        }
        return dataSize;
    }

    public long maxMarkedAt()
    {
        long max = Long.MIN_VALUE;
        for (int i = 0; i < size; i++)
            max = Math.max(max, markedAtAt(i));
        return max;
    }

    public void collectStats(EncodingStats.Collector collector)
    {
        for (int i = 0; i < size; i++)
        {
            collector.updateTimestamp(markedAtAt(i));
            collector.updateLocalDeletionTime(CassandraUInt.toLong(deletionTimeAt(i)));
        }
    }

    public void updateAllTimestamp(long timestamp)
    {
        materializeForMutation();
        for (int i = 0; i < size; i++)
            markedAts[i] = timestamp;
    }

    public void updateAllTimestampAndLocalDeletionTime(long timestamp, long localDeletionTime)
    {
        materializeForMutation();
        int unsignedLocalDeletionTime = Cell.deletionTimeLongToUnsignedInteger(localDeletionTime);
        for (int i = 0; i < size; i++)
        {
            markedAts[i] = timestamp;
            delTimesUnsignedIntegers[i] = unsignedLocalDeletionTime;
        }
    }

    private RangeTombstone rangeTombstone(int idx)
    {
        if (pages == null)
            return new RangeTombstone(Slice.make(starts[idx], ends[idx]),
                                      DeletionTime.buildUnsafeWithUnsignedInteger(markedAts[idx], delTimesUnsignedIntegers[idx]));
        if (hasFlatView())
            return new RangeTombstone(Slice.make(flatView.starts[idx], flatView.ends[idx]),
                                      DeletionTime.buildUnsafeWithUnsignedInteger(flatView.markedAts[idx], flatView.delTimesUnsignedIntegers[idx]));

        return rangeTombstone(pages[idx >>> PAGE_SHIFT], idx & PAGE_MASK);
    }

    private RangeTombstone rangeTombstone(Page page, int offset)
    {
        return new RangeTombstone(Slice.make(page.starts[offset], page.ends[offset]),
                                  DeletionTime.buildUnsafeWithUnsignedInteger(page.markedAts[offset], page.delTimesUnsignedIntegers[offset]));
    }

    private RangeTombstone rangeTombstoneWithNewStart(int idx, ClusteringBound<?> newStart)
    {
        if (pages == null)
            return new RangeTombstone(Slice.make(newStart, ends[idx]),
                                      DeletionTime.buildUnsafeWithUnsignedInteger(markedAts[idx], delTimesUnsignedIntegers[idx]));
        if (hasFlatView())
            return new RangeTombstone(Slice.make(newStart, flatView.ends[idx]),
                                      DeletionTime.buildUnsafeWithUnsignedInteger(flatView.markedAts[idx], flatView.delTimesUnsignedIntegers[idx]));

        Page page = pages[idx >>> PAGE_SHIFT];
        int offset = idx & PAGE_MASK;
        return new RangeTombstone(Slice.make(newStart, page.ends[offset]),
                                  DeletionTime.buildUnsafeWithUnsignedInteger(page.markedAts[offset], page.delTimesUnsignedIntegers[offset]));
    }

    private RangeTombstone rangeTombstoneWithNewEnd(int idx, ClusteringBound<?> newEnd)
    {
        if (pages == null)
            return new RangeTombstone(Slice.make(starts[idx], newEnd),
                                      DeletionTime.buildUnsafeWithUnsignedInteger(markedAts[idx], delTimesUnsignedIntegers[idx]));
        if (hasFlatView())
            return new RangeTombstone(Slice.make(flatView.starts[idx], newEnd),
                                      DeletionTime.buildUnsafeWithUnsignedInteger(flatView.markedAts[idx], flatView.delTimesUnsignedIntegers[idx]));

        Page page = pages[idx >>> PAGE_SHIFT];
        int offset = idx & PAGE_MASK;
        return new RangeTombstone(Slice.make(page.starts[offset], newEnd),
                                  DeletionTime.buildUnsafeWithUnsignedInteger(page.markedAts[offset], page.delTimesUnsignedIntegers[offset]));
    }

    private RangeTombstone rangeTombstoneWithNewBounds(int idx, ClusteringBound<?> newStart, ClusteringBound<?> newEnd)
    {
        if (pages == null)
            return new RangeTombstone(Slice.make(newStart, newEnd),
                                      DeletionTime.buildUnsafeWithUnsignedInteger(markedAts[idx], delTimesUnsignedIntegers[idx]));
        if (hasFlatView())
            return new RangeTombstone(Slice.make(newStart, newEnd),
                                      DeletionTime.buildUnsafeWithUnsignedInteger(flatView.markedAts[idx], flatView.delTimesUnsignedIntegers[idx]));

        Page page = pages[idx >>> PAGE_SHIFT];
        int offset = idx & PAGE_MASK;
        return new RangeTombstone(Slice.make(newStart, newEnd),
                                  DeletionTime.buildUnsafeWithUnsignedInteger(page.markedAts[offset], page.delTimesUnsignedIntegers[offset]));
    }

    public Iterator<RangeTombstone> iterator()
    {
        return iterator(false);
    }

    public Iterator<RangeTombstone> iterator(boolean reversed)
    {
        return reversed
             ? new AbstractIterator<RangeTombstone>()
             {
                 private int idx = size - 1;

                 protected RangeTombstone computeNext()
                 {
                     if (idx < 0)
                         return endOfData();

                     return rangeTombstone(idx--);
                 }
             }
             : new AbstractIterator<RangeTombstone>()
             {
                 private int idx;

                 protected RangeTombstone computeNext()
                 {
                     if (idx >= size)
                         return endOfData();

                     return rangeTombstone(idx++);
                 }
             };
    }

    public Iterator<RangeTombstone> iterator(final Slice slice, boolean reversed)
    {
        return reversed ? reverseIterator(slice) : forwardIterator(slice);
    }

    private Iterator<RangeTombstone> forwardIterator(final Slice slice)
    {
        int startIdx = slice.start().isBottom() ? 0 : searchInternal(slice.start(), 0, size);
        final int start = startIdx < 0 ? -startIdx-1 : startIdx;

        if (start >= size)
            return Collections.emptyIterator();

        int finishIdx = slice.end().isTop() ? size - 1 : searchInternal(slice.end(), start, size);
        // if stopIdx is the first range after 'slice.end()' we care only until the previous range
        final int finish = finishIdx < 0 ? -finishIdx-2 : finishIdx;

        if (start > finish)
            return Collections.emptyIterator();

        if (start == finish)
        {
            // We want to make sure the range are stricly included within the queried slice as this
            // make it easier to combine things when iterating over successive slices.
            ClusteringBound<?> s = comparator.compare(startAt(start), slice.start()) < 0 ? slice.start() : startAt(start);
            ClusteringBound<?> e = comparator.compare(slice.end(), endAt(start)) < 0 ? slice.end() : endAt(start);
            if (Slice.isEmpty(comparator, s, e))
                return Collections.emptyIterator();
            return Iterators.<RangeTombstone>singletonIterator(rangeTombstoneWithNewBounds(start, s, e));
        }

        if (pages == null || hasFlatView())
        {
            final ClusteringBound<?>[] directStarts = pages == null ? starts : flatView.starts;
            final ClusteringBound<?>[] directEnds = pages == null ? ends : flatView.ends;
            return new AbstractIterator<RangeTombstone>()
            {
                private int idx = start;

                protected RangeTombstone computeNext()
                {
                    if (idx >= size || idx > finish)
                        return endOfData();

                    // We want to make sure the range are stricly included within the queried slice as this
                    // make it easier to combine things when iterating over successive slices. This means that
                    // for the first and last range we might have to "cut" the range returned.
                    if (idx == start && comparator.compare(directStarts[idx], slice.start()) < 0)
                        return rangeTombstoneWithNewStart(idx++, slice.start());
                    if (idx == finish && comparator.compare(slice.end(), directEnds[idx]) < 0)
                        return rangeTombstoneWithNewEnd(idx++, slice.end());
                    return rangeTombstone(idx++);
                }
            };
        }

        return new AbstractIterator<RangeTombstone>()
        {
            private int idx = start;

            protected RangeTombstone computeNext()
            {
                if (idx > finish)
                    return endOfData();

                // We want to make sure the range are stricly included within the queried slice as this
                // make it easier to combine things when iterating over successive slices. This means that
                // for the first and last range we might have to "cut" the range returned.
                Page page = pages[idx >>> PAGE_SHIFT];
                int offset = idx & PAGE_MASK;
                RangeTombstone range;
                if (idx == start && comparator.compare(startAt(idx), slice.start()) < 0)
                    range = rangeTombstoneWithNewStart(idx, slice.start());
                else if (idx == finish && comparator.compare(slice.end(), page.ends[offset]) < 0)
                    range = rangeTombstoneWithNewEnd(idx, slice.end());
                else
                    range = rangeTombstone(page, offset);

                idx++;
                return range;
            }
        };
    }

    private Iterator<RangeTombstone> reverseIterator(final Slice slice)
    {
        int startIdx = slice.end().isTop() ? size - 1 : searchInternal(slice.end(), 0, size);
        // if startIdx is the first range after 'slice.end()' we care only until the previous range
        final int start = startIdx < 0 ? -startIdx-2 : startIdx;

        if (start < 0)
            return Collections.emptyIterator();

        int finishIdx = slice.start().isBottom() ? 0 : searchInternal(slice.start(), 0, start + 1);  // include same as finish
        // if stopIdx is the first range after 'slice.end()' we care only until the previous range
        final int finish = finishIdx < 0 ? -finishIdx-1 : finishIdx;

        if (start < finish)
            return Collections.emptyIterator();

        if (start == finish)
        {
            // We want to make sure the range are stricly included within the queried slice as this
            // make it easier to combine things when iterator over successive slices.
            ClusteringBound<?> s = comparator.compare(startAt(start), slice.start()) < 0 ? slice.start() : startAt(start);
            ClusteringBound<?> e = comparator.compare(slice.end(), endAt(start)) < 0 ? slice.end() : endAt(start);
            if (Slice.isEmpty(comparator, s, e))
                return Collections.emptyIterator();
            return Iterators.<RangeTombstone>singletonIterator(rangeTombstoneWithNewBounds(start, s, e));
        }

        return new AbstractIterator<RangeTombstone>()
        {
            private int idx = start;

            protected RangeTombstone computeNext()
            {
                if (idx < 0 || idx < finish)
                    return endOfData();
                // We want to make sure the range are stricly included within the queried slice as this
                // make it easier to combine things when iterator over successive slices. This means that
                // for the first and last range we might have to "cut" the range returned.
                if (idx == start && comparator.compare(slice.end(), endAt(idx)) < 0)
                    return rangeTombstoneWithNewEnd(idx--, slice.end());
                if (idx == finish && comparator.compare(startAt(idx), slice.start()) < 0)
                    return rangeTombstoneWithNewStart(idx--, slice.start());
                return rangeTombstone(idx--);
            }
        };
    }

    @Override
    public boolean equals(Object o)
    {
        if(!(o instanceof RangeTombstoneList))
            return false;
        RangeTombstoneList that = (RangeTombstoneList)o;
        if (size != that.size)
            return false;

        for (int i = 0; i < size; i++)
        {
            if (!startAt(i).equals(that.startAt(i)))
                return false;
            if (!endAt(i).equals(that.endAt(i)))
                return false;
            if (markedAtAt(i) != that.markedAtAt(i))
                return false;
            if (deletionTimeAt(i) != that.deletionTimeAt(i))
                return false;
        }
        return true;
    }

    @Override
    public final int hashCode()
    {
        int result = size;
        for (int i = 0; i < size; i++)
        {
            result += startAt(i).hashCode() + endAt(i).hashCode();
            long markedAt = markedAtAt(i);
            result += (int)(markedAt ^ (markedAt >>> 32));
            result += deletionTimeAt(i);
        }
        return result;
    }

    private static void copyArrays(RangeTombstoneList src, RangeTombstoneList dst)
    {
        dst.grow(src.size);
        if (src.pages == null)
        {
            System.arraycopy(src.starts, 0, dst.starts, 0, src.size);
            System.arraycopy(src.ends, 0, dst.ends, 0, src.size);
            System.arraycopy(src.markedAts, 0, dst.markedAts, 0, src.size);
            System.arraycopy(src.delTimesUnsignedIntegers, 0, dst.delTimesUnsignedIntegers, 0, src.size);
        }
        else if (src.hasFlatView())
        {
            System.arraycopy(src.flatView.starts, 0, dst.starts, 0, src.size);
            System.arraycopy(src.flatView.ends, 0, dst.ends, 0, src.size);
            System.arraycopy(src.flatView.markedAts, 0, dst.markedAts, 0, src.size);
            System.arraycopy(src.flatView.delTimesUnsignedIntegers, 0, dst.delTimesUnsignedIntegers, 0, src.size);
        }
        else
        {
            for (int i = 0; i < src.size; i++)
            {
                dst.starts[i] = src.startAt(i);
                dst.ends[i] = src.endAt(i);
                dst.markedAts[i] = src.markedAtAt(i);
                dst.delTimesUnsignedIntegers[i] = src.deletionTimeAt(i);
            }
        }
        dst.size = src.size;
        dst.boundaryHeapSize = src.boundaryHeapSize;
    }

    private Page[] pagesFromFlatArrays()
    {
        Page[] copiedPages = new Page[(size + PAGE_MASK) >>> PAGE_SHIFT];
        for (int pageIndex = 0; pageIndex < copiedPages.length; pageIndex++)
        {
            Page page = new Page(null);
            copiedPages[pageIndex] = page;
            int first = pageIndex << PAGE_SHIFT;
            int count = Math.min(PAGE_SIZE, size - first);
            System.arraycopy(starts, first, page.starts, 0, count);
            System.arraycopy(ends, first, page.ends, 0, count);
            System.arraycopy(markedAts, first, page.markedAts, 0, count);
            System.arraycopy(delTimesUnsignedIntegers, first, page.delTimesUnsignedIntegers, 0, count);
        }
        return copiedPages;
    }

    private ClusteringBound<?>[] pageStartsFromFlatArrays(int pageCount)
    {
        ClusteringBound<?>[] copiedPageStarts = new ClusteringBound<?>[pageCount];
        for (int pageIndex = 0; pageIndex < pageCount; pageIndex++)
            copiedPageStarts[pageIndex] = starts[pageIndex << PAGE_SHIFT];
        return copiedPageStarts;
    }

    private void appendPaged(ClusteringBound<?> start, ClusteringBound<?> end, long markedAt, int delTimeUnsignedInteger)
    {
        Page[] oldPages = pages;
        int pageIndex = size >>> PAGE_SHIFT;
        int offset = size & PAGE_MASK;
        Page[] newPages = Arrays.copyOf(oldPages, offset == 0 ? oldPages.length + 1 : oldPages.length);
        Page page = new Page(offset == 0 ? null : oldPages[pageIndex]);
        newPages[pageIndex] = page;
        page.starts[offset] = start;
        page.ends[offset] = end;
        page.markedAts[offset] = markedAt;
        page.delTimesUnsignedIntegers[offset] = delTimeUnsignedInteger;

        ClusteringBound<?>[] newPageStarts = pageStarts;
        if (offset == 0)
        {
            newPageStarts = Arrays.copyOf(pageStarts, pageIndex + 1);
            newPageStarts[pageIndex] = start;
        }

        pageAllocationOnHeap += pageStorageSize(newPages) - pageStorageSize(oldPages);
        pages = newPages;
        pageStarts = newPageStarts;
        size++;
        flatView = createsFlatView(size) ? FlatView.copyOf(pages, size) : null;
        boundaryHeapSize += start.unsharedHeapSize() + end.unsharedHeapSize();
    }

    private void appendAllPaged(RangeTombstoneList tombstones)
    {
        Page[] oldPages = pages;
        int oldSize = size;
        int newSize = oldSize + tombstones.size;
        int newPageCount = (newSize + PAGE_MASK) >>> PAGE_SHIFT;
        Page[] newPages = Arrays.copyOf(oldPages, newPageCount);
        for (int i = 0; i < tombstones.size; i++)
        {
            int index = oldSize + i;
            int pageIndex = index >>> PAGE_SHIFT;
            int offset = index & PAGE_MASK;
            Page page;
            if (offset == 0)
            {
                page = new Page(null);
                newPages[pageIndex] = page;
            }
            else
            {
                page = newPages[pageIndex];
                if (pageIndex < oldPages.length && page == oldPages[pageIndex])
                {
                    page = new Page(page);
                    newPages[pageIndex] = page;
                }
            }

            page.starts[offset] = tombstones.startAt(i);
            page.ends[offset] = tombstones.endAt(i);
            page.markedAts[offset] = tombstones.markedAtAt(i);
            page.delTimesUnsignedIntegers[offset] = tombstones.deletionTimeAt(i);
            boundaryHeapSize += page.starts[offset].unsharedHeapSize() + page.ends[offset].unsharedHeapSize();
        }

        ClusteringBound<?>[] newPageStarts = pageStarts;
        if (newPageCount > oldPages.length)
        {
            newPageStarts = Arrays.copyOf(pageStarts, newPageCount);
            for (int pageIndex = oldPages.length; pageIndex < newPageCount; pageIndex++)
                newPageStarts[pageIndex] = newPages[pageIndex].starts[0];
        }

        pageAllocationOnHeap += pageStorageSize(newPages) - pageStorageSize(oldPages);
        pages = newPages;
        pageStarts = newPageStarts;
        size = newSize;
        flatView = createsFlatView(size) ? FlatView.copyOf(pages, size) : null;
    }

    // Direct views are retained at geometric checkpoints: rebuilding one for every append would reintroduce
    // quadratic copying, while checkpoint views cost O(size) over an ordered append chain.
    private static boolean createsFlatView(int size)
    {
        return size >= PAGE_SIZE && (size & (size - 1)) == 0;
    }

    private void materializeForMutation()
    {
        if (pages == null)
            return;

        Page[] oldPages = pages;
        if (hasFlatView())
        {
            starts = Arrays.copyOf(flatView.starts, size);
            ends = Arrays.copyOf(flatView.ends, size);
            markedAts = Arrays.copyOf(flatView.markedAts, size);
            delTimesUnsignedIntegers = Arrays.copyOf(flatView.delTimesUnsignedIntegers, size);
        }
        else
        {
            starts = new ClusteringBound<?>[size];
            ends = new ClusteringBound<?>[size];
            markedAts = new long[size];
            delTimesUnsignedIntegers = new int[size];
            for (int i = 0; i < size; i++)
            {
                starts[i] = startAt(i);
                ends[i] = endAt(i);
                markedAts[i] = markedAtAt(i);
                delTimesUnsignedIntegers[i] = deletionTimeAt(i);
            }
        }
        pages = null;
        pageStarts = null;
        flatView = null;
        pageAllocationOnHeap -= pageStorageSize(oldPages);
    }

    private static long pageStorageSize(Page[] pages)
    {
        long size = ObjectSizes.sizeOfArray(pages);
        for (Page page : pages)
            size += page.unsharedHeapSize();
        return size;
    }

    long memtableUnsharedHeapSize()
    {
        if (pages == null)
            return unsharedHeapSize();

        return EMPTY_SIZE
             + boundaryHeapSize
             + ObjectSizes.sizeOfArray(pageStarts)
             + (flatView == null ? 0 : flatView.unsharedHeapSize());
    }

    long pageAllocationOnHeap()
    {
        return pageAllocationOnHeap;
    }

    private static final class Page
    {
        private final ClusteringBound<?>[] starts;
        private final ClusteringBound<?>[] ends;
        private final long[] markedAts;
        private final int[] delTimesUnsignedIntegers;

        private Page()
        {
            starts = null;
            ends = null;
            markedAts = null;
            delTimesUnsignedIntegers = null;
        }

        private Page(Page source)
        {
            starts = new ClusteringBound<?>[PAGE_SIZE];
            ends = new ClusteringBound<?>[PAGE_SIZE];
            markedAts = new long[PAGE_SIZE];
            delTimesUnsignedIntegers = new int[PAGE_SIZE];
            if (source != null)
            {
                System.arraycopy(source.starts, 0, starts, 0, PAGE_SIZE);
                System.arraycopy(source.ends, 0, ends, 0, PAGE_SIZE);
                System.arraycopy(source.markedAts, 0, markedAts, 0, PAGE_SIZE);
                System.arraycopy(source.delTimesUnsignedIntegers, 0, delTimesUnsignedIntegers, 0, PAGE_SIZE);
            }
        }

        private long unsharedHeapSize()
        {
            return PAGE_SIZE_ON_HEAP
                 + ObjectSizes.sizeOfArray(starts)
                 + ObjectSizes.sizeOfArray(ends)
                 + ObjectSizes.sizeOfArray(markedAts)
                 + ObjectSizes.sizeOfArray(delTimesUnsignedIntegers);
        }
    }

    private static final class FlatView
    {
        private static final long EMPTY_SIZE = ObjectSizes.measure(new FlatView());

        private final ClusteringBound<?>[] starts;
        private final ClusteringBound<?>[] ends;
        private final long[] markedAts;
        private final int[] delTimesUnsignedIntegers;
        private final int size;

        private FlatView()
        {
            starts = null;
            ends = null;
            markedAts = null;
            delTimesUnsignedIntegers = null;
            size = 0;
        }

        private FlatView(ClusteringBound<?>[] starts,
                         ClusteringBound<?>[] ends,
                         long[] markedAts,
                         int[] delTimesUnsignedIntegers,
                         int size)
        {
            this.starts = starts;
            this.ends = ends;
            this.markedAts = markedAts;
            this.delTimesUnsignedIntegers = delTimesUnsignedIntegers;
            this.size = size;
        }

        private static FlatView copyOf(ClusteringBound<?>[] starts,
                                       ClusteringBound<?>[] ends,
                                       long[] markedAts,
                                       int[] delTimesUnsignedIntegers,
                                       int size)
        {
            return new FlatView(Arrays.copyOf(starts, size),
                                Arrays.copyOf(ends, size),
                                Arrays.copyOf(markedAts, size),
                                Arrays.copyOf(delTimesUnsignedIntegers, size),
                                size);
        }

        private static FlatView copyOf(Page[] pages, int size)
        {
            ClusteringBound<?>[] starts = new ClusteringBound<?>[size];
            ClusteringBound<?>[] ends = new ClusteringBound<?>[size];
            long[] markedAts = new long[size];
            int[] delTimesUnsignedIntegers = new int[size];
            for (int pageIndex = 0; pageIndex < pages.length; pageIndex++)
            {
                Page page = pages[pageIndex];
                int first = pageIndex << PAGE_SHIFT;
                int count = Math.min(PAGE_SIZE, size - first);
                System.arraycopy(page.starts, 0, starts, first, count);
                System.arraycopy(page.ends, 0, ends, first, count);
                System.arraycopy(page.markedAts, 0, markedAts, first, count);
                System.arraycopy(page.delTimesUnsignedIntegers, 0, delTimesUnsignedIntegers, first, count);
            }
            return new FlatView(starts, ends, markedAts, delTimesUnsignedIntegers, size);
        }

        private long unsharedHeapSize()
        {
            return EMPTY_SIZE
                 + ObjectSizes.sizeOfArray(starts)
                 + ObjectSizes.sizeOfArray(ends)
                 + ObjectSizes.sizeOfArray(markedAts)
                 + ObjectSizes.sizeOfArray(delTimesUnsignedIntegers);
        }
    }

    /*
     * Inserts a new element starting at index i. This method assumes that:
     *    ends[i-1] <= start < ends[i]
     * (note that start can be equal to ends[i-1] in the case where we have a boundary, i.e. for instance
     * ends[i-1] is the exclusive end of X and start is the inclusive start of X).
     *
     * A RangeTombstoneList is a list of range [s_0, e_0]...[s_n, e_n] such that:
     *   - s_i is a start bound and e_i is a end bound
     *   - s_i < e_i
     *   - e_i <= s_i+1
     * Basically, range are non overlapping and in order.
     */
    private void insertFrom(int i, ClusteringBound<?> start, ClusteringBound<?> end, long markedAt, int delTimeUnsignedInternal)
    {
        while (i < size)
        {
            assert start.isStart() && end.isEnd();
            assert i == 0 || comparator.compare(ends[i-1], start) <= 0;
            assert comparator.compare(start, ends[i]) < 0;

            if (Slice.isEmpty(comparator, start, end))
                return;

            // Do we overwrite the current element?
            if (markedAt > markedAts[i])
            {
                // We do overwrite.

                // First deal with what might come before the newly added one.
                if (comparator.compare(starts[i], start) < 0)
                {
                    ClusteringBound<?> newEnd = start.invert();
                    if (!Slice.isEmpty(comparator, starts[i], newEnd))
                    {
                        addInternal(i, starts[i], newEnd, markedAts[i], delTimesUnsignedIntegers[i]);
                        i++;
                        setInternal(i, start, ends[i], markedAts[i], delTimesUnsignedIntegers[i]);
                    }
                }

                // now, start <= starts[i]

                // Does the new element stops before the current one,
                int endCmp = comparator.compare(end, starts[i]);
                if (endCmp < 0)
                {
                    // Here start <= starts[i] and end < starts[i]
                    // This means the current element is before the current one.
                    addInternal(i, start, end, markedAt, delTimeUnsignedInternal);
                    return;
                }

                // Do we overwrite the current element fully?
                int cmp = comparator.compare(ends[i], end);
                if (cmp <= 0)
                {
                    // We do overwrite fully:
                    // update the current element until it's end and continue on with the next element (with the new inserted start == current end).

                    // If we're on the last element, or if we stop before the next start, we set the current element and are done
                    // Note that the comparison below is inclusive: if a end equals a start, this means they form a boundary, or
                    // in other words that they are for the same element but one is inclusive while the other exclusive. In which case we know
                    // we're good with the next element
                    if (i == size-1 || comparator.compare(end, starts[i+1]) <= 0)
                    {
                        setInternal(i, start, end, markedAt, delTimeUnsignedInternal);
                        return;
                    }

                    setInternal(i, start, starts[i+1].invert(), markedAt, delTimeUnsignedInternal);
                    start = starts[i+1];
                    i++;
                }
                else
                {
                    // We don't overwrite fully. Insert the new interval, and then update the now next
                    // one to reflect the not overwritten parts. We're then done.
                    addInternal(i, start, end, markedAt, delTimeUnsignedInternal);
                    i++;
                    ClusteringBound<?> newStart = end.invert();
                    if (!Slice.isEmpty(comparator, newStart, ends[i]))
                    {
                        setInternal(i, newStart, ends[i], markedAts[i], delTimesUnsignedIntegers[i]);
                    }
                    return;
                }
            }
            else
            {
                // we don't overwrite the current element

                // If the new interval starts before the current one, insert that new interval
                if (comparator.compare(start, starts[i]) < 0)
                {
                    // If we stop before the start of the current element, just insert the new interval and we're done;
                    // otherwise insert until the beginning of the current element
                    if (comparator.compare(end, starts[i]) <= 0)
                    {
                        addInternal(i, start, end, markedAt, delTimeUnsignedInternal);
                        return;
                    }
                    ClusteringBound<?> newEnd = starts[i].invert();
                    if (!Slice.isEmpty(comparator, start, newEnd))
                    {
                        addInternal(i, start, newEnd, markedAt, delTimeUnsignedInternal);
                        i++;
                    }
                }

                // After that, we're overwritten on the current element but might have
                // some residual parts after ...

                // ... unless we don't extend beyond it.
                if (comparator.compare(end, ends[i]) <= 0)
                    return;

                start = ends[i].invert();
                i++;
            }
        }

        // If we got there, then just insert the remainder at the end
        addInternal(i, start, end, markedAt, delTimeUnsignedInternal);
    }

    private int capacity()
    {
        return starts.length;
    }

    /*
     * Adds the new tombstone at index i, growing and/or moving elements to make room for it.
     */
    private void addInternal(int i, ClusteringBound<?> start, ClusteringBound<?> end, long markedAt, int delTimeUnsignedInteger)
    {
        assert i >= 0;

        if (size == capacity())
            growToFree(i);
        else if (i < size)
            moveElements(i);

        setInternal(i, start, end, markedAt, delTimeUnsignedInteger);
        size++;
    }

    /*
     * Grow the arrays, leaving index i "free" in the process.
     */
    private void growToFree(int i)
    {
        // Introduce getRangeTombstoneResizeFactor
        int newLength = (int) Math.ceil(capacity() * DatabaseDescriptor.getRangeTombstoneListGrowthFactor());
        // Fallback to the original calculation if the newLength calculated from the resize factor is not valid.
        if (newLength <= capacity())
            newLength = ((capacity() * 3) / 2) + 1;
        
        grow(i, newLength);
    }

    /*
     * Grow the arrays to match newLength capacity.
     */
    private void grow(int newLength)
    {
        if (capacity() < newLength)
            grow(-1, newLength);
    }

    private void grow(int i, int newLength)
    {
        starts = grow(starts, size, newLength, i);
        ends = grow(ends, size, newLength, i);
        markedAts = grow(markedAts, size, newLength, i);
        delTimesUnsignedIntegers = grow(delTimesUnsignedIntegers, size, newLength, i);
    }

    private static ClusteringBound<?>[] grow(ClusteringBound<?>[] a, int size, int newLength, int i)
    {
        if (i < 0 || i >= size)
            return Arrays.copyOf(a, newLength);

        ClusteringBound<?>[] newA = new ClusteringBound<?>[newLength];
        System.arraycopy(a, 0, newA, 0, i);
        System.arraycopy(a, i, newA, i+1, size - i);
        return newA;
    }

    private static long[] grow(long[] a, int size, int newLength, int i)
    {
        if (i < 0 || i >= size)
            return Arrays.copyOf(a, newLength);

        long[] newA = new long[newLength];
        System.arraycopy(a, 0, newA, 0, i);
        System.arraycopy(a, i, newA, i+1, size - i);
        return newA;
    }

    private static int[] grow(int[] a, int size, int newLength, int i)
    {
        if (i < 0 || i >= size)
            return Arrays.copyOf(a, newLength);

        int[] newA = new int[newLength];
        System.arraycopy(a, 0, newA, 0, i);
        System.arraycopy(a, i, newA, i+1, size - i);
        return newA;
    }

    /*
     * Move elements so that index i is "free", assuming the arrays have at least one free slot at the end.
     */
    private void moveElements(int i)
    {
        if (i >= size)
            return;

        System.arraycopy(starts, i, starts, i+1, size - i);
        System.arraycopy(ends, i, ends, i+1, size - i);
        System.arraycopy(markedAts, i, markedAts, i+1, size - i);
        System.arraycopy(delTimesUnsignedIntegers, i, delTimesUnsignedIntegers, i+1, size - i);
        // we set starts[i] to null to indicate the position is now empty, so that we update boundaryHeapSize
        // when we set it
        starts[i] = null;
    }

    private void setInternal(int i, ClusteringBound<?> start, ClusteringBound<?> end, long markedAt, int delTimeUnsignedInteger)
    {
        if (starts[i] != null)
            boundaryHeapSize -= starts[i].unsharedHeapSize() + ends[i].unsharedHeapSize();
        starts[i] = start;
        ends[i] = end;
        markedAts[i] = markedAt;
        delTimesUnsignedIntegers[i] = delTimeUnsignedInteger;
        boundaryHeapSize += start.unsharedHeapSize() + end.unsharedHeapSize();
    }

    @Override
    public long unsharedHeapSize()
    {
        if (pages != null)
            return EMPTY_SIZE
                 + boundaryHeapSize
                 + ObjectSizes.sizeOfArray(pageStarts)
                 + (flatView == null ? 0 : flatView.unsharedHeapSize())
                 + pageStorageSize(pages);

        return EMPTY_SIZE
             + boundaryHeapSize
             + ObjectSizes.sizeOfArray(starts)
             + ObjectSizes.sizeOfArray(ends)
             + ObjectSizes.sizeOfArray(markedAts)
             + ObjectSizes.sizeOfArray(delTimesUnsignedIntegers);
    }
}
