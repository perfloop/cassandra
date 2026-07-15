/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.cassandra.db;

import java.util.Iterator;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.junit.Assert.assertEquals;

public class RangeTombstoneListCopyTest
{
    private static final ClusteringComparator comparator = new ClusteringComparator(Int32Type.instance);

    @BeforeClass
    public static void initializeDatabaseDescriptor()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Test
    public void copiesRemainIndependentAfterSeparateAppends()
    {
        RangeTombstoneList original = list(2);
        RangeTombstoneList firstCopy = original.copy();
        RangeTombstoneList secondCopy = original.copy();

        firstCopy.add(tombstone(10, 11, 3));
        secondCopy.add(tombstone(12, 13, 4));
        original.add(tombstone(14, 15, 5));

        assertTimestamps(original, 1, 2, 5);
        assertTimestamps(firstCopy, 1, 2, 3);
        assertTimestamps(secondCopy, 1, 2, 4);
    }

    @Test
    public void copyRemainsIndependentAfterPrefixMutation()
    {
        RangeTombstoneList original = list(2);
        RangeTombstoneList copy = original.copy();

        copy.add(tombstone(0, 1, 3));

        assertTimestamps(original, 1, 2);
        assertTimestamps(copy, 3, 1, 2);
    }

    private static RangeTombstoneList list(int count)
    {
        RangeTombstoneList list = new RangeTombstoneList(comparator, 8);
        for (int i = 0; i < count; i++)
            list.add(tombstone(2 * i + 2, 2 * i + 3, i + 1));
        return list;
    }

    private static RangeTombstone tombstone(int start, int end, long timestamp)
    {
        return new RangeTombstone(Slice.make(BufferClusteringBound.inclusiveStartOf(ByteBufferUtil.bytes(start)),
                                             BufferClusteringBound.inclusiveEndOf(ByteBufferUtil.bytes(end))),
                                  DeletionTime.build(timestamp, 1));
    }

    private static void assertTimestamps(RangeTombstoneList list, long... timestamps)
    {
        assertEquals(timestamps.length, list.size());

        Iterator<RangeTombstone> tombstones = list.iterator();
        for (long timestamp : timestamps)
            assertEquals(timestamp, tombstones.next().deletionTime().markedForDeleteAt());
    }
}
