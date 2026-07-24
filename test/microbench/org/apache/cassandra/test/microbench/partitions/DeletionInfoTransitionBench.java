/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.cassandra.test.microbench.partitions;

import java.util.Iterator;
import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DeletionInfo;
import org.apache.cassandra.db.RangeTombstone;
import org.apache.cassandra.db.Slice;
import org.apache.cassandra.db.partitions.DeletionInfoTransitionFixture;
import org.apache.cassandra.db.partitions.PartitionUpdate;

/**
 * Times one mutation-to-memtable deletion transition after an immutable range prefix has been
 * installed outside the measured operation. The invocation setup restores the same published
 * prefix, so every timed operation observes the intended cardinality without timing prefix build.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 4, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(1)
@Threads(1)
public class DeletionInfoTransitionBench
{
    static
    {
        DeletionInfoTransitionFixture.initialize();
    }

    @State(Scope.Thread)
    public static class WriteState
    {
        @Param({ "128", "4096" })
        public int prefixRangeCount;

        @Param({ "ADJACENT", "OPPOSITE" })
        public String exactBoundSeparation;

        private DeletionInfoTransitionFixture.State fixture;
        private int variant;

        @Setup(Level.Trial)
        public void setup()
        {
            fixture = new DeletionInfoTransitionFixture.State(prefixRangeCount);
        }

        @Setup(Level.Invocation)
        public void reset()
        {
            fixture.reset();
        }

        private int nextVariant()
        {
            return variant++;
        }
    }

    @State(Scope.Thread)
    public static class ReadState
    {
        @Param({ "4096" })
        public int prefixRangeCount;

        private DeletionInfoTransitionFixture.State fixture;
        private int rotation;

        @Setup(Level.Trial)
        public void setup()
        {
            fixture = new DeletionInfoTransitionFixture.State(prefixRangeCount);
        }
    }

    @Benchmark
    public void twoExactBoundRanges(WriteState state, Blackhole blackhole)
    {
        PartitionUpdate update = state.fixture.exactBoundUpdate(DeletionInfoTransitionFixture.ExactBoundSeparation.valueOf(state.exactBoundSeparation),
                                                                 state.nextVariant());
        state.fixture.apply(update);
        blackhole.consume(state.fixture.partition.deletionInfo());
    }

    @Benchmark
    public void overlap(WriteState state, Blackhole blackhole)
    {
        state.fixture.apply(state.fixture.overlapUpdate(state.nextVariant()));
        blackhole.consume(state.fixture.partition.deletionInfo());
    }

    @Benchmark
    public void outOfOrder(WriteState state, Blackhole blackhole)
    {
        state.fixture.apply(state.fixture.outOfOrderUpdate(state.nextVariant()));
        blackhole.consume(state.fixture.partition.deletionInfo());
    }

    @Benchmark
    public void partitionDelete(WriteState state, Blackhole blackhole)
    {
        state.fixture.apply(state.fixture.partitionDeleteUpdate(state.nextVariant()));
        blackhole.consume(state.fixture.partition.deletionInfo());
    }

    @Benchmark
    public long allSlicesAndDirections(ReadState state)
    {
        DeletionInfo deletionInfo = state.fixture.partition.deletionInfo();
        Slice[] slices = state.fixture.slices();
        long fingerprint = 0;
        int offset = state.rotation++ & (slices.length - 1);

        for (int i = 0; i < slices.length; i++)
        {
            Slice slice = slices[(i + offset) & (slices.length - 1)];
            fingerprint = consumeRanges(deletionInfo.rangeIterator(slice, false), fingerprint);
            fingerprint = consumeRanges(deletionInfo.rangeIterator(slice, true), fingerprint);
        }
        return fingerprint;
    }

    @Benchmark
    public long coveredAndUncovered(ReadState state)
    {
        DeletionInfo deletionInfo = state.fixture.partition.deletionInfo();
        Clustering<?>[] probes = state.fixture.probes();
        long fingerprint = 0;
        int offset = state.rotation++ % probes.length;

        for (int i = 0; i < probes.length; i++)
        {
            RangeTombstone range = deletionInfo.rangeCovering(probes[(i + offset) % probes.length]);
            fingerprint = 31 * fingerprint + (range == null ? 1 : DeletionInfoTransitionFixture.fingerprint(range));
        }
        return fingerprint;
    }

    private static long consumeRanges(Iterator<RangeTombstone> ranges, long fingerprint)
    {
        while (ranges.hasNext())
            fingerprint = 31 * fingerprint + DeletionInfoTransitionFixture.fingerprint(ranges.next());
        return fingerprint;
    }
}
