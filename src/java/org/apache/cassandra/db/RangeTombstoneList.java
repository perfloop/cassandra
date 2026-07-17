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
import java.util.Comparator;
import java.util.Iterator;

import com.google.common.collect.Iterators;

import org.apache.cassandra.cache.IMeasurableMemory;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.utils.AbstractIterator;
import org.apache.cassandra.utils.CassandraUInt;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.btree.BTree;
import org.apache.cassandra.utils.btree.UpdateFunction;
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
    private static final long EMPTY_SIZE = ObjectSizes.measure(new RangeTombstoneList(null, 0));
    private static final long TREE_COMPARATOR_SIZE = ObjectSizes.measure(treeComparator(null));
    private static final long TREE_ENTRY_SIZE = ObjectSizes.measure(new TreeEntry(null, null, 0, 0));
    private static final long READ_CACHE_SIZE = ObjectSizes.measure(new ReadCache(null, null, null, null));

    private final ClusteringComparator comparator;
    private final Comparator<Object> treeComparator;

    // Note: we don't want to use a List for the markedAts and delTimes to avoid boxing. We could
    // use a List for starts and ends, but having arrays everywhere is almost simpler.
    private ClusteringBound<?>[] starts;
    private ClusteringBound<?>[] ends;
    private long[] markedAts;
    private int[] delTimesUnsignedIntegers;

    /**
     * An immutable snapshot used only by the atomic memtable append path. Its entries contain values rather than
     * references to the arrays above, so later mutations of a flat list cannot alter a copied snapshot.
     */
    private Object[] tree;

    /**
     * Lazily materialized arrays for read-heavy published snapshots. The cache is local to this list and is never
     * inherited by a memtable copy, so it cannot make a writer alias mutable arrays with another snapshot.
     */
    private volatile ReadCache readCache;

    private long boundaryHeapSize;
    private int size;

    /**
     * BTree node bytes allocated while constructing this snapshot. Node graphs are persistent, so the memtable
     * allocator charges this incremental amount rather than recursively charging shared nodes on every successor.
     */
    private long treeAllocatedOnHeap;

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
        this.treeComparator = treeComparator(comparator);
        this.starts = starts;
        this.ends = ends;
        this.markedAts = markedAts;
        this.delTimesUnsignedIntegers = delTimesUnsignedIntegers;
        this.size = size;
        this.boundaryHeapSize = boundaryHeapSize;
    }

    private RangeTombstoneList(ClusteringComparator comparator, Object[] tree, long boundaryHeapSize, int size, long treeAllocatedOnHeap)
    {
        this.comparator = comparator;
        this.treeComparator = treeComparator(comparator);
        this.tree = tree;
        this.size = size;
        this.boundaryHeapSize = boundaryHeapSize;
        this.treeAllocatedOnHeap = treeAllocatedOnHeap;
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
        return tree == null
             ? new RangeTombstoneList(comparator,
                                      Arrays.copyOf(starts, size),
                                      Arrays.copyOf(ends, size),
                                      Arrays.copyOf(markedAts, size),
                                      Arrays.copyOf(delTimesUnsignedIntegers, size),
                                      boundaryHeapSize, size)
             : materializedCopy();
    }

    /**
     * Creates an immutable, array-independent snapshot for an ordered memtable merge. This is deliberately
     * package-private: ordinary mutable copies retain the array representation, while the atomic merge path alone
     * can extend the snapshot with BTree's persistent update operation.
     */
    RangeTombstoneList copyForMemtable()
    {
        if (isEmpty())
            return new RangeTombstoneList(comparator, 0);

        if (tree != null)
            return new RangeTombstoneList(comparator, tree, boundaryHeapSize, size, 0);

        Object[] snapshot = treeFromArrays();
        return new RangeTombstoneList(comparator, snapshot, boundaryHeapSize, size, BTree.sizeOnHeapOf(snapshot));
    }

    /**
     * Whether the update is a strictly ordered suffix suitable for the persistent tree representation.
     */
    boolean canAppend(RangeTombstoneList tombstones)
    {
        return !isEmpty()
            && !tombstones.isEmpty()
            && comparator.compare(endAt(size - 1), tombstones.startAt(0)) < 0;
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
            copy.markedAts[i] = markedAt(i);
            copy.delTimesUnsignedIntegers[i] = delTimeAt(i);
        }

        return copy;
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
        if (tree != null)
        {
            if (!isEmpty() && comparator.compare(endAt(size - 1), start) < 0)
            {
                updateTree(BTree.singleton(new TreeEntry(start, end, markedAt, delTimeUnsignedInteger)));
                clearReadCache();
                size++;
                // Keep the accounting behavior of the flat add path, whose set and add phases both account for
                // a non-empty ordered append.
                boundaryHeapSize += 2L * boundaryHeapSize(start, end);
                return;
            }

            materialize(size);
        }

        if (isEmpty())
        {
            addInternal(0, start, end, markedAt, delTimeUnsignedInteger);
            return;
        }

        int c = comparator.compare(ends[size - 1], start);

        // Fast path if we add in sorted order
        if (c <= 0)
        {
            addInternal(size, start, end, markedAt, delTimeUnsignedInteger);
        }
        else
        {
            // Note: insertFrom expect i to be the insertion point in term of interval ends
            int pos = Arrays.binarySearch(ends, 0, size, start, comparator);
            insertFrom((pos >= 0 ? pos + 1 : -pos - 1), start, end, markedAt, delTimeUnsignedInteger);
        }
        boundaryHeapSize += boundaryHeapSize(start, end);
    }

    /**
     * Adds all the range tombstones of {@code tombstones} to this RangeTombstoneList.
     */
    public void addAll(RangeTombstoneList tombstones)
    {
        if (tombstones.isEmpty())
            return;

        if (tree != null)
        {
            if (canAppend(tombstones))
            {
                boolean useRepeatedAdds = size > 10 * tombstones.size;
                // Preserve the flat merge path's linear behavior for an update containing several ranges instead of
                // issuing one persistent tree update per range.
                updateTree(treeForUpdate(tombstones));
                clearReadCache();
                size += tombstones.size;
                long addedBoundaryHeapSize = tombstones.actualBoundaryHeapSize();
                // Match the flat implementation: its small-update path calls add(), while its merge path reaches
                // addInternal() for an ordered suffix.
                boundaryHeapSize += useRepeatedAdds ? 2L * addedBoundaryHeapSize : addedBoundaryHeapSize;
                return;
            }

            materialize(size);
        }

        if (tombstones.tree != null)
            tombstones = tombstones.materializedCopy();

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
                add(tombstones.starts[i], tombstones.ends[i], tombstones.markedAts[i], tombstones.delTimesUnsignedIntegers[i]);
        }
        else
        {
            int i = 0;
            int j = 0;
            while (i < size && j < tombstones.size)
            {
                if (comparator.compare(tombstones.starts[j], ends[i]) < 0)
                {
                    insertFrom(i, tombstones.starts[j], tombstones.ends[j], tombstones.markedAts[j], tombstones.delTimesUnsignedIntegers[j]);
                    j++;
                }
                else
                {
                    i++;
                }
            }
            // Adds the remaining ones from tombstones if any (note that addInternal will increment size if relevant).
            for (; j < tombstones.size; j++)
                addInternal(size, tombstones.starts[j], tombstones.ends[j], tombstones.markedAts[j], tombstones.delTimesUnsignedIntegers[j]);
        }
    }

    /**
     * Returns whether the given name/timestamp pair is deleted by one of the tombstone
     * of this RangeTombstoneList.
     */
    public boolean isDeleted(Clustering<?> clustering, Cell<?> cell)
    {
        int idx = searchInternal(clustering, 0, size);
        if (idx < 0)
            return false;

        // No matter what the counter cell's timestamp is, a tombstone always takes precedence. See CASSANDRA-7346.
        if (cell.isCounterCell())
            return true;
        return markedAts[idx] >= cell.timestamp();
    }

    /**
     * Returns the DeletionTime for the tombstone overlapping {@code name} (there can't be more than one),
     * or null if {@code name} is not covered by any tombstone.
     */
    public DeletionTime searchDeletionTime(Clustering<?> name)
    {
        int idx = searchInternal(name, 0, size);
        return idx < 0 ? null : DeletionTime.buildUnsafeWithUnsignedInteger(markedAts[idx], delTimesUnsignedIntegers[idx]);
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

        ensureReadArrays();
        int pos = Arrays.binarySearch(starts, startIdx, endIdx, name, comparator);
        if (pos >= 0)
        {
            // Equality only happens for bounds (as used by forward/reverseIterator), and bounds are equal only if they
            // are the same or complementary, in either case the bound itself is not part of the range.
            return -pos - 1;
        }
        else
        {
            // We potentially intersect the range before our "insertion point"
            int idx = -pos - 2;
            if (idx < 0)
                return -1;

            return comparator.compare(name, ends[idx]) < 0 ? idx : -idx - 2;
        }
    }

    public int dataSize()
    {
        int dataSize = TypeSizes.sizeof(size);
        for (int i = 0; i < size; i++)
        {
            dataSize += startAt(i).dataSize() + endAt(i).dataSize();
            dataSize += TypeSizes.sizeof(markedAt(i));
            dataSize += TypeSizes.sizeof(delTimeAt(i));
        }
        return dataSize;
    }

    public long maxMarkedAt()
    {
        long max = Long.MIN_VALUE;
        for (int i = 0; i < size; i++)
            max = Math.max(max, markedAt(i));
        return max;
    }

    public void collectStats(EncodingStats.Collector collector)
    {
        for (int i = 0; i < size; i++)
        {
            collector.updateTimestamp(markedAt(i));
            collector.updateLocalDeletionTime(CassandraUInt.toLong(delTimeAt(i)));
        }
    }

    public void updateAllTimestamp(long timestamp)
    {
        materialize(size);
        for (int i = 0; i < size; i++)
            markedAts[i] = timestamp;
    }

    public void updateAllTimestampAndLocalDeletionTime(long timestamp, long localDeletionTime)
    {
        materialize(size);
        int unsignedLocalDeletionTime = Cell.deletionTimeLongToUnsignedInteger(localDeletionTime);
        for (int i = 0; i < size; i++)
        {
            markedAts[i] = timestamp;
            delTimesUnsignedIntegers[i] = unsignedLocalDeletionTime;
        }
    }

    private RangeTombstone rangeTombstone(int idx)
    {
        return new RangeTombstone(Slice.make(starts[idx], ends[idx]), DeletionTime.buildUnsafeWithUnsignedInteger(markedAts[idx], delTimesUnsignedIntegers[idx]));
    }

    private RangeTombstone rangeTombstoneWithNewStart(int idx, ClusteringBound<?> newStart)
    {
        return new RangeTombstone(Slice.make(newStart, ends[idx]), DeletionTime.buildUnsafeWithUnsignedInteger(markedAts[idx], delTimesUnsignedIntegers[idx]));
    }

    private RangeTombstone rangeTombstoneWithNewEnd(int idx, ClusteringBound<?> newEnd)
    {
        return new RangeTombstone(Slice.make(starts[idx], newEnd), DeletionTime.buildUnsafeWithUnsignedInteger(markedAts[idx], delTimesUnsignedIntegers[idx]));
    }

    private RangeTombstone rangeTombstoneWithNewBounds(int idx, ClusteringBound<?> newStart, ClusteringBound<?> newEnd)
    {
        return new RangeTombstone(Slice.make(newStart, newEnd), DeletionTime.buildUnsafeWithUnsignedInteger(markedAts[idx], delTimesUnsignedIntegers[idx]));
    }

    public Iterator<RangeTombstone> iterator()
    {
        return iterator(false);
    }

    public Iterator<RangeTombstone> iterator(boolean reversed)
    {
        ensureReadArrays();
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
        ensureReadArrays();
        int startIdx = slice.start().isBottom() ? 0 : searchInternal(slice.start(), 0, size);
        final int start = startIdx < 0 ? -startIdx - 1 : startIdx;

        if (start >= size)
            return Collections.emptyIterator();

        int finishIdx = slice.end().isTop() ? size - 1 : searchInternal(slice.end(), start, size);
        // if stopIdx is the first range after 'slice.end()' we care only until the previous range
        final int finish = finishIdx < 0 ? -finishIdx - 2 : finishIdx;

        if (start > finish)
            return Collections.emptyIterator();

        if (start == finish)
        {
            // We want to make sure the range are stricly included within the queried slice as this
            // make it easier to combine things when iterating over successive slices.
            ClusteringBound<?> s = comparator.compare(starts[start], slice.start()) < 0 ? slice.start() : starts[start];
            ClusteringBound<?> e = comparator.compare(slice.end(), ends[start]) < 0 ? slice.end() : ends[start];
            if (Slice.isEmpty(comparator, s, e))
                return Collections.emptyIterator();
            return Iterators.<RangeTombstone>singletonIterator(rangeTombstoneWithNewBounds(start, s, e));
        }

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
                if (idx == start && comparator.compare(starts[idx], slice.start()) < 0)
                    return rangeTombstoneWithNewStart(idx++, slice.start());
                if (idx == finish && comparator.compare(slice.end(), ends[idx]) < 0)
                    return rangeTombstoneWithNewEnd(idx++, slice.end());
                return rangeTombstone(idx++);
            }
        };
    }

    private Iterator<RangeTombstone> reverseIterator(final Slice slice)
    {
        ensureReadArrays();
        int startIdx = slice.end().isTop() ? size - 1 : searchInternal(slice.end(), 0, size);
        // if startIdx is the first range after 'slice.end()' we care only until the previous range
        final int start = startIdx < 0 ? -startIdx - 2 : startIdx;

        if (start < 0)
            return Collections.emptyIterator();

        int finishIdx = slice.start().isBottom() ? 0 : searchInternal(slice.start(), 0, start + 1);  // include same as finish
        // if stopIdx is the first range after 'slice.end()' we care only until the previous range
        final int finish = finishIdx < 0 ? -finishIdx - 1 : finishIdx;

        if (start < finish)
            return Collections.emptyIterator();

        if (start == finish)
        {
            // We want to make sure the range are stricly included within the queried slice as this
            // make it easier to combine things when iterator over successive slices.
            ClusteringBound<?> s = comparator.compare(starts[start], slice.start()) < 0 ? slice.start() : starts[start];
            ClusteringBound<?> e = comparator.compare(slice.end(), ends[start]) < 0 ? slice.end() : ends[start];
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
                if (idx == start && comparator.compare(slice.end(), ends[idx]) < 0)
                    return rangeTombstoneWithNewEnd(idx--, slice.end());
                if (idx == finish && comparator.compare(starts[idx], slice.start()) < 0)
                    return rangeTombstoneWithNewStart(idx--, slice.start());
                return rangeTombstone(idx--);
            }
        };
    }

    @Override
    public boolean equals(Object o)
    {
        if (!(o instanceof RangeTombstoneList))
            return false;
        RangeTombstoneList that = (RangeTombstoneList) o;
        if (size != that.size)
            return false;

        for (int i = 0; i < size; i++)
        {
            if (!startAt(i).equals(that.startAt(i)))
                return false;
            if (!endAt(i).equals(that.endAt(i)))
                return false;
            if (markedAt(i) != that.markedAt(i))
                return false;
            if (delTimeAt(i) != that.delTimeAt(i))
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
            long markedAt = markedAt(i);
            result += (int) (markedAt ^ (markedAt >>> 32));
            result += delTimeAt(i);
        }
        return result;
    }

    private ClusteringBound<?> startAt(int index)
    {
        if (tree == null)
            return starts[index];

        ReadCache cache = readCache;
        return cache == null ? entryAt(index).start : cache.starts[index];
    }

    private ClusteringBound<?> endAt(int index)
    {
        if (tree == null)
            return ends[index];

        ReadCache cache = readCache;
        return cache == null ? entryAt(index).end : cache.ends[index];
    }

    private long markedAt(int index)
    {
        if (tree == null)
            return markedAts[index];

        ReadCache cache = readCache;
        return cache == null ? entryAt(index).markedAt : cache.markedAts[index];
    }

    private int delTimeAt(int index)
    {
        if (tree == null)
            return delTimesUnsignedIntegers[index];

        ReadCache cache = readCache;
        return cache == null ? entryAt(index).delTimeUnsignedInteger : cache.delTimesUnsignedIntegers[index];
    }

    private void ensureReadArrays()
    {
        if (tree != null && readCache == null)
            readCacheView();
    }

    private void clearReadCache()
    {
        readCache = null;
        ends = null;
        markedAts = null;
        delTimesUnsignedIntegers = null;
        starts = null;
    }

    private ReadCache readCacheView()
    {
        ReadCache cache = readCache;
        if (cache != null)
            return cache;

        synchronized (this)
        {
            cache = readCache;
            if (cache == null)
            {
                cache = newReadCache();
                ends = cache.ends;
                markedAts = cache.markedAts;
                delTimesUnsignedIntegers = cache.delTimesUnsignedIntegers;
                starts = cache.starts;
                readCache = cache;
            }
            return cache;
        }
    }

    private ReadCache newReadCache()
    {
        ClusteringBound<?>[] cachedStarts = new ClusteringBound<?>[size];
        ClusteringBound<?>[] cachedEnds = new ClusteringBound<?>[size];
        long[] cachedMarkedAts = new long[size];
        int[] cachedDelTimesUnsignedIntegers = new int[size];
        Iterator<TreeEntry> entries = BTree.iterator(tree);
        for (int i = 0; i < size; i++)
        {
            TreeEntry entry = entries.next();
            cachedStarts[i] = entry.start;
            cachedEnds[i] = entry.end;
            cachedMarkedAts[i] = entry.markedAt;
            cachedDelTimesUnsignedIntegers[i] = entry.delTimeUnsignedInteger;
        }
        return new ReadCache(cachedStarts, cachedEnds, cachedMarkedAts, cachedDelTimesUnsignedIntegers);
    }

    private TreeEntry entryAt(int index)
    {
        return BTree.findByIndex(tree, index);
    }

    private Object[] treeFromArrays()
    {
        BTree.Builder<TreeEntry> builder = BTree.builder(treeComparator, size);
        for (int i = 0; i < size; i++)
            builder.add(new TreeEntry(starts[i], ends[i], markedAts[i], delTimesUnsignedIntegers[i]));
        return builder.build();
    }

    private Object[] treeForUpdate(RangeTombstoneList tombstones)
    {
        if (tombstones.tree != null)
            return tombstones.tree;

        if (tombstones.size == 1)
        {
            return BTree.singleton(new TreeEntry(tombstones.starts[0],
                                                  tombstones.ends[0],
                                                  tombstones.markedAts[0],
                                                  tombstones.delTimesUnsignedIntegers[0]));
        }
        return tombstones.treeFromArrays();
    }

    private void updateTree(Object[] updates)
    {
        TreeUpdateFunction updateFunction = new TreeUpdateFunction();
        tree = BTree.update(tree, updates, treeComparator, updateFunction);
        treeAllocatedOnHeap += updateFunction.allocatedOnHeap;
    }

    /**
     * The allocator's deletion-info delta deliberately excludes persistent BTree nodes: a successor shares most of
     * them with its predecessor, so their incremental allocation is reported separately by {@link #treeAllocatedOnHeap}.
     * The ordinary retained-size method below still describes the complete object graph for non-memtable consumers.
     */
    long memtableUnsharedHeapSize()
    {
        if (tree != null)
            return EMPTY_SIZE + TREE_COMPARATOR_SIZE + boundaryHeapSize + (long) size * TREE_ENTRY_SIZE;

        return EMPTY_SIZE
             + TREE_COMPARATOR_SIZE
             + boundaryHeapSize
             + ObjectSizes.sizeOfArray(starts)
             + ObjectSizes.sizeOfArray(ends)
             + ObjectSizes.sizeOfArray(markedAts)
             + ObjectSizes.sizeOfArray(delTimesUnsignedIntegers);
    }

    long treeAllocatedOnHeap()
    {
        return treeAllocatedOnHeap;
    }

    private RangeTombstoneList materializedCopy()
    {
        RangeTombstoneList copy = new RangeTombstoneList(comparator, size);
        copy.size = size;
        copy.boundaryHeapSize = boundaryHeapSize;
        Iterator<TreeEntry> entries = BTree.iterator(tree);
        for (int i = 0; i < size; i++)
        {
            TreeEntry entry = entries.next();
            copy.starts[i] = entry.start;
            copy.ends[i] = entry.end;
            copy.markedAts[i] = entry.markedAt;
            copy.delTimesUnsignedIntegers[i] = entry.delTimeUnsignedInteger;
        }
        return copy;
    }

    private void materialize(int capacity)
    {
        if (tree == null)
            return;

        int newCapacity = Math.max(size, capacity);
        ReadCache cache = readCache;
        if (cache != null)
        {
            ends = newCapacity == size ? cache.ends : Arrays.copyOf(cache.ends, newCapacity);
            markedAts = newCapacity == size ? cache.markedAts : Arrays.copyOf(cache.markedAts, newCapacity);
            delTimesUnsignedIntegers = newCapacity == size ? cache.delTimesUnsignedIntegers : Arrays.copyOf(cache.delTimesUnsignedIntegers, newCapacity);
            starts = newCapacity == size ? cache.starts : Arrays.copyOf(cache.starts, newCapacity);
        }
        else
        {
            ClusteringBound<?>[] newStarts = new ClusteringBound<?>[newCapacity];
            ClusteringBound<?>[] newEnds = new ClusteringBound<?>[newCapacity];
            long[] newMarkedAts = new long[newCapacity];
            int[] newDelTimesUnsignedIntegers = new int[newCapacity];
            for (int i = 0; i < size; i++)
            {
                TreeEntry entry = entryAt(i);
                newStarts[i] = entry.start;
                newEnds[i] = entry.end;
                newMarkedAts[i] = entry.markedAt;
                newDelTimesUnsignedIntegers[i] = entry.delTimeUnsignedInteger;
            }
            ends = newEnds;
            markedAts = newMarkedAts;
            delTimesUnsignedIntegers = newDelTimesUnsignedIntegers;
            starts = newStarts;
        }
        tree = null;
        treeAllocatedOnHeap = 0;
        readCache = null;
    }

    private long actualBoundaryHeapSize()
    {
        long result = 0;
        for (int i = 0; i < size; i++)
            result += boundaryHeapSize(startAt(i), endAt(i));
        return result;
    }

    private static long boundaryHeapSize(ClusteringBound<?> start, ClusteringBound<?> end)
    {
        return start.unsharedHeapSize() + end.unsharedHeapSize();
    }

    private static Comparator<Object> treeComparator(ClusteringComparator comparator)
    {
        return (left, right) ->
        {
            int comparison = comparator.compare(treeStart(left), treeStart(right));
            if (comparison != 0 || !(left instanceof TreeEntry) || !(right instanceof TreeEntry))
                return comparison;

            return comparator.compare(((TreeEntry) left).end, ((TreeEntry) right).end);
        };
    }

    private static ClusteringPrefix<?> treeStart(Object value)
    {
        return value instanceof TreeEntry ? ((TreeEntry) value).start : (ClusteringPrefix<?>) value;
    }

    private static final class ReadCache
    {
        final ClusteringBound<?>[] starts;
        final ClusteringBound<?>[] ends;
        final long[] markedAts;
        final int[] delTimesUnsignedIntegers;

        private ReadCache(ClusteringBound<?>[] starts,
                          ClusteringBound<?>[] ends,
                          long[] markedAts,
                          int[] delTimesUnsignedIntegers)
        {
            this.starts = starts;
            this.ends = ends;
            this.markedAts = markedAts;
            this.delTimesUnsignedIntegers = delTimesUnsignedIntegers;
        }
    }

    private static final class TreeUpdateFunction implements UpdateFunction<TreeEntry, TreeEntry>
    {
        private long allocatedOnHeap;

        @Override
        public TreeEntry insert(TreeEntry update)
        {
            return update;
        }

        @Override
        public TreeEntry merge(TreeEntry existing, TreeEntry update)
        {
            return existing;
        }

        @Override
        public void onAllocatedOnHeap(long heapSize)
        {
            allocatedOnHeap += heapSize;
        }
    }

    private static final class TreeEntry
    {
        final ClusteringBound<?> start;
        final ClusteringBound<?> end;
        final long markedAt;
        final int delTimeUnsignedInteger;

        private TreeEntry(ClusteringBound<?> start, ClusteringBound<?> end, long markedAt, int delTimeUnsignedInteger)
        {
            this.start = start;
            this.end = end;
            this.markedAt = markedAt;
            this.delTimeUnsignedInteger = delTimeUnsignedInteger;
        }
    }

    private static void copyArrays(RangeTombstoneList src, RangeTombstoneList dst)
    {
        dst.grow(src.size);
        System.arraycopy(src.starts, 0, dst.starts, 0, src.size);
        System.arraycopy(src.ends, 0, dst.ends, 0, src.size);
        System.arraycopy(src.markedAts, 0, dst.markedAts, 0, src.size);
        System.arraycopy(src.delTimesUnsignedIntegers, 0, dst.delTimesUnsignedIntegers, 0, src.size);
        dst.size = src.size;
        dst.boundaryHeapSize = src.boundaryHeapSize;
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
        if (tree != null)
        {
            long treeSize = EMPTY_SIZE
                          + TREE_COMPARATOR_SIZE
                          + boundaryHeapSize
                          + BTree.sizeOnHeapOf(tree)
                          + (long) size * TREE_ENTRY_SIZE;
            ReadCache cache = readCache;
            if (cache != null)
            {
                treeSize += READ_CACHE_SIZE
                         + ObjectSizes.sizeOfArray(cache.starts)
                         + ObjectSizes.sizeOfArray(cache.ends)
                         + ObjectSizes.sizeOfArray(cache.markedAts)
                         + ObjectSizes.sizeOfArray(cache.delTimesUnsignedIntegers);
            }
            return treeSize;
        }

        return EMPTY_SIZE
             + TREE_COMPARATOR_SIZE
             + boundaryHeapSize
             + ObjectSizes.sizeOfArray(starts)
             + ObjectSizes.sizeOfArray(ends)
             + ObjectSizes.sizeOfArray(markedAts)
             + ObjectSizes.sizeOfArray(delTimesUnsignedIntegers);
    }
}
