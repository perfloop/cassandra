/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.cassandra.db;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.utils.ObjectSizes;

import static org.junit.Assert.assertEquals;

public class RangeTombstoneListCopyTest
{
    private static final ClusteringComparator comparator = new ClusteringComparator(Int32Type.instance);

    @BeforeClass
    public static void beforeClass()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Test
    public void copyAndEmptyTargetAddAllRemainIndependentAndCompact()
    {
        RangeTombstoneList source = listWithTwoRanges(64);
        RangeTombstoneList copy = source.copy();
        RangeTombstoneList added = new RangeTombstoneList(comparator, 0);
        added.addAll(source);

        assertEquals(2, source.size());
        assertEquals(2, copy.size());
        assertEquals(2, added.size());
        assertEquals(copy.unsharedHeapSize(), added.unsharedHeapSize());
        assertEquals(backingArraysSize(64) - backingArraysSize(2), source.unsharedHeapSize() - copy.unsharedHeapSize());

        copy.add(tombstone(4, 5, 3));
        added.add(tombstone(6, 7, 4));

        assertEquals(2, source.size());
        assertEquals(3, copy.size());
        assertEquals(3, added.size());
    }

    @Test
    public void mutableCopyRemainsDeepAndExactSized()
    {
        MutableDeletionInfo source = new MutableDeletionInfo(DeletionTime.LIVE, listWithTwoRanges(64));
        MutableDeletionInfo copy = source.mutableCopy();

        assertEquals(2, source.rangeCount());
        assertEquals(2, copy.rangeCount());
        assertEquals(backingArraysSize(64) - backingArraysSize(2), source.unsharedHeapSize() - copy.unsharedHeapSize());

        copy.add(tombstone(4, 5, 3), comparator);

        assertEquals(2, source.rangeCount());
        assertEquals(3, copy.rangeCount());
    }

    private static RangeTombstoneList listWithTwoRanges(int capacity)
    {
        RangeTombstoneList list = new RangeTombstoneList(comparator, capacity);
        list.add(tombstone(0, 1, 1));
        list.add(tombstone(2, 3, 2));
        return list;
    }

    private static RangeTombstone tombstone(int start, int end, long timestamp)
    {
        return new RangeTombstone(Slice.make(BufferClusteringBound.inclusiveStartOf(Int32Type.instance.decompose(start)),
                                             BufferClusteringBound.inclusiveEndOf(Int32Type.instance.decompose(end))),
                                  DeletionTime.build(timestamp, 1));
    }

    private static long backingArraysSize(int capacity)
    {
        return 2 * ObjectSizes.sizeOfReferenceArray(capacity)
               + ObjectSizes.sizeOfArray(new long[capacity])
               + ObjectSizes.sizeOfArray(new int[capacity]);
    }
}
