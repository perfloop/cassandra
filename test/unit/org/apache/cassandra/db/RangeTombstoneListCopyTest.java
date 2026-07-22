/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.db;

import org.junit.Test;

import org.apache.cassandra.db.marshal.Int32Type;

import static org.junit.Assert.assertEquals;

public class RangeTombstoneListCopyTest
{
    private static final ClusteringComparator COMPARATOR = new ClusteringComparator(Int32Type.instance);

    @Test
    public void mutableCopyIsIndependentAndCapacityExact()
    {
        RangeTombstone tombstone = tombstone(1, 2, 1);
        RangeTombstoneList oversized = new RangeTombstoneList(COMPARATOR, 64);
        oversized.add(tombstone);
        MutableDeletionInfo source = new MutableDeletionInfo(DeletionTime.LIVE, oversized);

        MutableDeletionInfo copy = source.mutableCopy();
        source.updateAllTimestamp(2);

        assertEquals(1L, copy.rangeIterator(false).next().deletionTime().markedForDeleteAt());
        assertEquals(2L, source.rangeIterator(false).next().deletionTime().markedForDeleteAt());

        RangeTombstoneList compact = new RangeTombstoneList(COMPARATOR, 1);
        compact.add(tombstone);
        assertEquals(new MutableDeletionInfo(DeletionTime.LIVE, compact).unsharedHeapSize(), copy.unsharedHeapSize());

        copy.updateAllTimestampAndLocalDeletionTime(3, 4);
        assertEquals(2L, source.rangeIterator(false).next().deletionTime().markedForDeleteAt());
        assertEquals(3L, copy.rangeIterator(false).next().deletionTime().markedForDeleteAt());
    }

    private static RangeTombstone tombstone(int start, int end, long timestamp)
    {
        return new RangeTombstone(Slice.make(COMPARATOR.make(start), COMPARATOR.make(end)), DeletionTime.build(timestamp, 1));
    }
}
