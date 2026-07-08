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

package org.apache.cassandra.test.microbench;

import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.memtable.ShardBoundaries;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.tcm.Epoch;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(value = 1)
@State(Scope.Benchmark)
public class ShardBoundariesBench
{
    private static final int LOOKUP_SIZE = 4096;
    private final Token[] lookupTokens = new Token[LOOKUP_SIZE];
    private int lookupIndex = 0;

    private ShardBoundaries boundaries4;
    private ShardBoundaries boundaries16;
    private ShardBoundaries boundaries32;
    private ShardBoundaries boundaries64;
    private ShardBoundaries boundaries128;

    @Setup(Level.Trial)
    public void setup()
    {
        DatabaseDescriptor.clientInitialization(false);
        if (DatabaseDescriptor.getPartitioner() == null)
            DatabaseDescriptor.setPartitionerUnsafe(Murmur3Partitioner.instance);

        IPartitioner partitioner = Murmur3Partitioner.instance;
        Token minimumToken = partitioner.getMinimumToken();

        // Populate lookup tokens randomly
        Random random = new Random(42);
        for (int i = 0; i < LOOKUP_SIZE; i++)
        {
            lookupTokens[i] = partitioner.split(minimumToken, minimumToken, random.nextDouble());
        }

        boundaries4 = createBoundaries(partitioner, minimumToken, 4);
        boundaries16 = createBoundaries(partitioner, minimumToken, 16);
        boundaries32 = createBoundaries(partitioner, minimumToken, 32);
        boundaries64 = createBoundaries(partitioner, minimumToken, 64);
        boundaries128 = createBoundaries(partitioner, minimumToken, 128);
    }

    private ShardBoundaries createBoundaries(IPartitioner partitioner, Token minimumToken, int numShards)
    {
        if (numShards <= 1)
            return ShardBoundaries.NONE;
        
        Token[] bounds = new Token[numShards - 1];
        for (int i = 0; i < numShards - 1; i++)
        {
            double pos = (double) (i + 1) / numShards;
            bounds[i] = partitioner.split(minimumToken, minimumToken, pos);
        }
        return new ShardBoundaries(bounds, Epoch.EMPTY);
    }

    private Token nextToken()
    {
        Token tk = lookupTokens[lookupIndex];
        lookupIndex = (lookupIndex + 1) & (LOOKUP_SIZE - 1);
        return tk;
    }

    @Benchmark
    public int getShardForToken_4()
    {
        return boundaries4.getShardForToken(nextToken());
    }

    @Benchmark
    public int getShardForToken_16()
    {
        return boundaries16.getShardForToken(nextToken());
    }

    @Benchmark
    public int getShardForToken_32()
    {
        return boundaries32.getShardForToken(nextToken());
    }

    @Benchmark
    public int getShardForToken_64()
    {
        return boundaries64.getShardForToken(nextToken());
    }

    @Benchmark
    public int getShardForToken_128()
    {
        return boundaries128.getShardForToken(nextToken());
    }
}
