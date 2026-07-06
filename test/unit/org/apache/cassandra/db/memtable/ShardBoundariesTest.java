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

package org.apache.cassandra.db.memtable;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.tcm.Epoch;

import static org.junit.Assert.assertEquals;

@RunWith(JUnit4.class)
public class ShardBoundariesTest
{
    @Test
    public void testGetShardForToken()
    {
        // Setup boundaries: -100, 0, 100
        Token[] boundaries = new Token[] {
            new Murmur3Partitioner.LongToken(-100),
            new Murmur3Partitioner.LongToken(0),
            new Murmur3Partitioner.LongToken(100)
        };
        ShardBoundaries sb = new ShardBoundaries(boundaries, Epoch.EMPTY);

        // Under-bounds
        assertEquals(0, sb.getShardForToken(new Murmur3Partitioner.LongToken(-200)));
        // Exact boundary match
        assertEquals(0, sb.getShardForToken(new Murmur3Partitioner.LongToken(-100)));
        // Just above boundary
        assertEquals(1, sb.getShardForToken(new Murmur3Partitioner.LongToken(-99)));
        // Exactly on next boundary
        assertEquals(1, sb.getShardForToken(new Murmur3Partitioner.LongToken(0)));
        // Just above boundary
        assertEquals(2, sb.getShardForToken(new Murmur3Partitioner.LongToken(1)));
        // Exactly on last boundary
        assertEquals(2, sb.getShardForToken(new Murmur3Partitioner.LongToken(100)));
        // Over last boundary
        assertEquals(3, sb.getShardForToken(new Murmur3Partitioner.LongToken(101)));
    }

    @Test
    public void testEmptyBoundaries()
    {
        ShardBoundaries sb = ShardBoundaries.NONE;
        assertEquals(0, sb.getShardForToken(new Murmur3Partitioner.LongToken(0)));
        assertEquals(0, sb.getShardForToken(new Murmur3Partitioner.LongToken(100)));
    }
}
