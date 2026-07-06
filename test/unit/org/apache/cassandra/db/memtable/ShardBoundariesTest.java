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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.tcm.Epoch;

import static org.junit.Assert.assertEquals;

public class ShardBoundariesTest
{
    private static IPartitioner partitioner;
    private static Token minimumToken;

    @BeforeClass
    public static void setup()
    {
        partitioner = Murmur3Partitioner.instance;
        minimumToken = partitioner.getMinimumToken();
    }

    private static int linearScan(Token[] boundaries, Token tk)
    {
        for (int i = 0; i < boundaries.length; i++)
        {
            if (tk.compareTo(boundaries[i]) <= 0)
                return i;
        }
        return boundaries.length;
    }

    private void runDifferentialTest(Token[] boundaries, Token[] lookupTokens)
    {
        ShardBoundaries sb = new ShardBoundaries(boundaries, Epoch.EMPTY);
        for (Token tk : lookupTokens)
        {
            int expected = linearScan(boundaries, tk);
            int actual = sb.getShardForToken(tk);
            assertEquals("Divergence found for token: " + tk + " with boundaries: " + Arrays.toString(boundaries),
                         expected, actual);
        }
    }

    @Test
    public void testEmptyBoundaries()
    {
        Token[] boundaries = new Token[0];
        Token[] lookupTokens = new Token[] {
            partitioner.split(minimumToken, minimumToken, 0.1),
            partitioner.split(minimumToken, minimumToken, 0.5),
            partitioner.split(minimumToken, minimumToken, 0.9)
        };
        runDifferentialTest(boundaries, lookupTokens);
    }

    @Test
    public void testSingleBoundary()
    {
        Token[] boundaries = new Token[] {
            partitioner.split(minimumToken, minimumToken, 0.5)
        };
        Token[] lookupTokens = new Token[] {
            partitioner.split(minimumToken, minimumToken, 0.1),
            partitioner.split(minimumToken, minimumToken, 0.5),
            partitioner.split(minimumToken, minimumToken, 0.9)
        };
        runDifferentialTest(boundaries, lookupTokens);
    }

    @Test
    public void testDistinctBoundaries()
    {
        int[] shardCounts = {4, 16, 32, 64, 128, 256};
        Random random = new Random(101);

        for (int count : shardCounts)
        {
            Token[] boundaries = new Token[count - 1];
            for (int i = 0; i < count - 1; i++)
            {
                double pos = (double) (i + 1) / count;
                boundaries[i] = partitioner.split(minimumToken, minimumToken, pos);
            }

            Token[] lookupTokens = new Token[1000];
            for (int j = 0; j < 1000; j++)
            {
                lookupTokens[j] = partitioner.split(minimumToken, minimumToken, random.nextDouble());
            }

            runDifferentialTest(boundaries, lookupTokens);
        }
    }

    @Test
    public void testBoundariesWithDuplicates()
    {
        // Generate boundaries containing duplicates
        Token t1 = partitioner.split(minimumToken, minimumToken, 0.2);
        Token t2 = partitioner.split(minimumToken, minimumToken, 0.5);
        Token t3 = partitioner.split(minimumToken, minimumToken, 0.8);

        List<Token[]> boundaryScenarios = new ArrayList<>();
        boundaryScenarios.add(new Token[] { t1, t1, t2, t3 });
        boundaryScenarios.add(new Token[] { t1, t2, t2, t3 });
        boundaryScenarios.add(new Token[] { t1, t2, t3, t3 });
        boundaryScenarios.add(new Token[] { t1, t1, t1, t1 });
        boundaryScenarios.add(new Token[] { t1, t1, t2, t2, t2, t3, t3 });

        Token[] lookupTokens = new Token[] {
            partitioner.split(minimumToken, minimumToken, 0.1),
            t1,
            partitioner.split(minimumToken, minimumToken, 0.3),
            t2,
            partitioner.split(minimumToken, minimumToken, 0.6),
            t3,
            partitioner.split(minimumToken, minimumToken, 0.9)
        };

        for (Token[] boundaries : boundaryScenarios)
        {
            runDifferentialTest(boundaries, lookupTokens);
        }
    }
}
