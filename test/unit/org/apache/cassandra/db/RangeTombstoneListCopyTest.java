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

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.marshal.Int32Type;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;

public class RangeTombstoneListCopyTest
{
    private static final ClusteringComparator comparator = new ClusteringComparator(Int32Type.instance);

    @BeforeClass
    public static void setup()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Test
    public void copyIsDeepAndCompactsSpareCapacity()
    {
        RangeTombstoneList source = listWithSpareCapacity();
        RangeTombstoneList copy = source.copy();

        assertNotSame(source, copy);
        assertEquals(2, copy.size());
        assertTrue(source.unsharedHeapSize() > copy.unsharedHeapSize());

        copy.updateAllTimestamp(100);
        copy.updateAllTimestampAndLocalDeletionTime(101, 7);

        assertEquals(1, timestamp(source, 1));
        assertEquals(2, timestamp(source, 5));
        assertEquals(101, timestamp(copy, 1));
        assertEquals(101, timestamp(copy, 5));
    }

    @Test
    public void mutableCopyIsDeepAndCompactsSpareCapacity()
    {
        RangeTombstoneList sourceRanges = listWithSpareCapacity();
        MutableDeletionInfo source = new MutableDeletionInfo(DeletionTime.build(3, 1), sourceRanges);
        MutableDeletionInfo copy = source.mutableCopy();

        assertNotSame(source, copy);
        assertEquals(source, copy);
        assertTrue(source.unsharedHeapSize() > copy.unsharedHeapSize());

        copy.updateAllTimestampAndLocalDeletionTime(200, 9);

        assertEquals(3, source.getPartitionDeletion().markedForDeleteAt());
        assertEquals(1, timestamp(source, 1));
        assertEquals(2, timestamp(source, 5));
        assertEquals(200, copy.getPartitionDeletion().markedForDeleteAt());
        assertEquals(200, timestamp(copy, 1));
        assertEquals(200, timestamp(copy, 5));
    }

    @Test
    public void emptyTargetAddAllCopiesCompactArraysWithoutAliasing()
    {
        RangeTombstoneList source = listWithSpareCapacity();
        RangeTombstoneList compact = source.copy();
        RangeTombstoneList target = new RangeTombstoneList(comparator, 0);

        target.addAll(source);

        assertEquals(compact.unsharedHeapSize(), target.unsharedHeapSize());
        target.updateAllTimestamp(300);

        assertEquals(1, timestamp(source, 1));
        assertEquals(2, timestamp(source, 5));
        assertEquals(300, timestamp(target, 1));
        assertEquals(300, timestamp(target, 5));
    }

    private static RangeTombstoneList listWithSpareCapacity()
    {
        RangeTombstoneList list = new RangeTombstoneList(comparator, 128);
        list.add(tombstone(0, 2, 1));
        list.add(tombstone(4, 6, 2));
        return list;
    }

    private static RangeTombstone tombstone(int start, int end, long timestamp)
    {
        return new RangeTombstone(Slice.make(ClusteringBound.create(comparator, true, true, start),
                                             ClusteringBound.create(comparator, false, true, end)),
                                  DeletionTime.build(timestamp, 1));
    }

    private static long timestamp(DeletionInfo info, int key)
    {
        return timestamp(info.rangeCovering(Clustering.make(Int32Type.instance.decompose(key))));
    }

    private static long timestamp(RangeTombstoneList list, int key)
    {
        return timestamp(list.search(Clustering.make(Int32Type.instance.decompose(key))));
    }

    private static long timestamp(RangeTombstone tombstone)
    {
        return tombstone.deletionTime().markedForDeleteAt();
    }
}
