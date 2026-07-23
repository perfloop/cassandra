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

package org.apache.cassandra.test.microbench.partitions;

import java.util.Iterator;
import java.util.function.Consumer;

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

import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DeletionInfo;
import org.apache.cassandra.db.RangeTombstone;
import org.apache.cassandra.db.Slice;
import org.apache.cassandra.db.partitions.BTreePartitionData;

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.apache.cassandra.test.microbench.partitions.DeletionInfoTransitionSupport.Operation.OUT_OF_ORDER;
import static org.apache.cassandra.test.microbench.partitions.DeletionInfoTransitionSupport.Operation.OVERLAP;
import static org.apache.cassandra.test.microbench.partitions.DeletionInfoTransitionSupport.Operation.PARTITION_DELETE;
import static org.apache.cassandra.test.microbench.partitions.DeletionInfoTransitionSupport.Operation.TWO_EXACT_BOUND;

/**
 * Measures one mutation-to-memtable deletion transition from immutable prefixes prepared outside timing.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(NANOSECONDS)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 1, time = 1)
@Fork(1)
@Threads(1)
public class DeletionInfoTransitionBench
{
    @Benchmark
    public BTreePartitionData partitionDelete(PartitionDeleteState state)
    {
        return state.apply();
    }

    @Benchmark
    public BTreePartitionData overlap(OverlapState state)
    {
        return state.apply();
    }

    @Benchmark
    public BTreePartitionData outOfOrder(OutOfOrderState state)
    {
        return state.apply();
    }

    @Benchmark
    public BTreePartitionData twoExactBoundRanges(TwoExactBoundRangesState state)
    {
        return state.apply();
    }

    @Benchmark
    public long allSlicesAndDirections(AllSlicesAndDirectionsState state)
    {
        long checksum = 0;
        for (Slice slice : state.slices)
        {
            checksum = consume(state.deletionInfo.rangeIterator(slice, false), checksum);
            checksum = consume(state.deletionInfo.rangeIterator(slice, true), checksum);
        }
        return checksum;
    }

    @Benchmark
    public long coveredAndUncovered(CoveredAndUncoveredState state)
    {
        long checksum = 0;
        checksum = cover(state.deletionInfo, state.covered, checksum);
        return cover(state.deletionInfo, state.uncovered, checksum);
    }

    private static long consume(Iterator<RangeTombstone> iterator, long checksum)
    {
        while (iterator.hasNext())
        {
            RangeTombstone range = iterator.next();
            checksum = 31 * checksum + range.deletionTime().markedForDeleteAt();
            checksum = 31 * checksum + range.deletedSlice().start().hashCode();
            checksum = 31 * checksum + range.deletedSlice().end().hashCode();
        }
        return checksum;
    }

    private static long cover(DeletionInfo deletionInfo, Clustering<?>[] keys, long checksum)
    {
        for (Clustering<?> key : keys)
        {
            RangeTombstone range = deletionInfo.rangeCovering(key);
            checksum = 31 * checksum + (range == null ? 0 : range.deletionTime().markedForDeleteAt());
        }
        return checksum;
    }

    @State(Scope.Benchmark)
    public static class PartitionDeleteState
    {
        @Param({"128", "4096"})
        public int prefixRanges;

        private DeletionInfoTransitionSupport.Fixture fixture;
        private DeletionInfoTransitionSupport.Transition[] transitions;
        private DeletionInfoTransitionSupport.Transition current;
        private int next;

        @Setup(Level.Trial)
        public void setup()
        {
            setup(PARTITION_DELETE);
        }

        @Setup(Level.Invocation)
        public void reset()
        {
            current = transitions[next++ & 1];
            fixture.prepare(current);
        }

        BTreePartitionData apply()
        {
            return fixture.applyPrepared(current);
        }

        private void setup(DeletionInfoTransitionSupport.Operation operation)
        {
            fixture = DeletionInfoTransitionSupport.fixture(prefixRanges, "OPPOSITE");
            transitions = fixture.transitions(operation);
            validate(fixture, transitions);
        }
    }

    @State(Scope.Benchmark)
    public static class OverlapState
    {
        @Param({"4096"})
        public int prefixRanges;

        private DeletionInfoTransitionSupport.Fixture fixture;
        private DeletionInfoTransitionSupport.Transition[] transitions;
        private DeletionInfoTransitionSupport.Transition current;
        private int next;

        @Setup(Level.Trial)
        public void setup()
        {
            fixture = DeletionInfoTransitionSupport.fixture(prefixRanges, "OPPOSITE");
            transitions = fixture.transitions(OVERLAP);
            validate(fixture, transitions);
        }

        @Setup(Level.Invocation)
        public void reset()
        {
            current = transitions[next++ & 1];
            fixture.prepare(current);
        }

        BTreePartitionData apply()
        {
            return fixture.applyPrepared(current);
        }
    }

    @State(Scope.Benchmark)
    public static class OutOfOrderState
    {
        @Param({"4096"})
        public int prefixRanges;

        private DeletionInfoTransitionSupport.Fixture fixture;
        private DeletionInfoTransitionSupport.Transition[] transitions;
        private DeletionInfoTransitionSupport.Transition current;
        private int next;

        @Setup(Level.Trial)
        public void setup()
        {
            fixture = DeletionInfoTransitionSupport.fixture(prefixRanges, "OPPOSITE");
            transitions = fixture.transitions(OUT_OF_ORDER);
            validate(fixture, transitions);
        }

        @Setup(Level.Invocation)
        public void reset()
        {
            current = transitions[next++ & 1];
            fixture.prepare(current);
        }

        BTreePartitionData apply()
        {
            return fixture.applyPrepared(current);
        }
    }

    @State(Scope.Benchmark)
    public static class TwoExactBoundRangesState
    {
        @Param({"128", "4096"})
        public int prefixRanges;

        @Param({"ADJACENT", "OPPOSITE"})
        public String separation;

        private DeletionInfoTransitionSupport.Fixture fixture;
        private DeletionInfoTransitionSupport.Transition[] transitions;
        private DeletionInfoTransitionSupport.Transition current;
        private int next;

        @Setup(Level.Trial)
        public void setup()
        {
            fixture = DeletionInfoTransitionSupport.fixture(prefixRanges, separation);
            transitions = fixture.transitions(TWO_EXACT_BOUND);
            validate(fixture, transitions);
        }

        @Setup(Level.Invocation)
        public void reset()
        {
            current = transitions[next++ & 1];
            fixture.prepare(current);
        }

        BTreePartitionData apply()
        {
            return fixture.applyPrepared(current);
        }
    }

    @State(Scope.Benchmark)
    public static class AllSlicesAndDirectionsState
    {
        @Param({"4096"})
        public int prefixRanges;

        private DeletionInfoTransitionSupport.Fixture fixture;
        private DeletionInfoTransitionSupport.Transition[] transitions;
        private int next;
        private DeletionInfo deletionInfo;
        private Slice[] slices;

        @Setup(Level.Trial)
        public void setup()
        {
            fixture = DeletionInfoTransitionSupport.fixture(prefixRanges, "OPPOSITE");
            transitions = fixture.transitions(TWO_EXACT_BOUND);
            for (DeletionInfoTransitionSupport.Transition transition : transitions)
            {
                DeletionInfoTransitionSupport.assertEquivalent(transition.expectedPrefix(), transition.published());
                for (String position : new String[]{ "EARLY", "MIDDLE", "LATE", "MISS" })
                {
                    Slice slice = DeletionInfoTransitionSupport.sliceFor(transition.offset(), prefixRanges, position);
                    DeletionInfoTransitionSupport.assertIteratorEquivalent(transition.expectedPrefix(), transition.published(), slice, false);
                    DeletionInfoTransitionSupport.assertIteratorEquivalent(transition.expectedPrefix(), transition.published(), slice, true);
                }
            }
        }

        @Setup(Level.Invocation)
        public void reset()
        {
            DeletionInfoTransitionSupport.Transition transition = transitions[next++ & 1];
            deletionInfo = transition.published();
            slices = new Slice[]{
            DeletionInfoTransitionSupport.sliceFor(transition.offset(), prefixRanges, "EARLY"),
            DeletionInfoTransitionSupport.sliceFor(transition.offset(), prefixRanges, "MIDDLE"),
            DeletionInfoTransitionSupport.sliceFor(transition.offset(), prefixRanges, "LATE"),
            DeletionInfoTransitionSupport.sliceFor(transition.offset(), prefixRanges, "MISS")
            };
        }
    }

    @State(Scope.Benchmark)
    public static class CoveredAndUncoveredState
    {
        @Param({"4096"})
        public int prefixRanges;

        private DeletionInfoTransitionSupport.Fixture fixture;
        private DeletionInfoTransitionSupport.Transition[] transitions;
        private int next;
        private DeletionInfo deletionInfo;
        private Clustering<?>[] covered;
        private Clustering<?>[] uncovered;

        @Setup(Level.Trial)
        public void setup()
        {
            fixture = DeletionInfoTransitionSupport.fixture(prefixRanges, "OPPOSITE");
            transitions = fixture.transitions(TWO_EXACT_BOUND);
            for (DeletionInfoTransitionSupport.Transition transition : transitions)
            {
                DeletionInfoTransitionSupport.assertEquivalent(transition.expectedPrefix(), transition.published());
                for (Clustering<?> key : DeletionInfoTransitionSupport.clusteringProbes(transition.offset(), prefixRanges, "COVERED"))
                    DeletionInfoTransitionSupport.assertRangeEquivalent(transition.expectedPrefix().rangeCovering(key), transition.published().rangeCovering(key));
                for (Clustering<?> key : DeletionInfoTransitionSupport.clusteringProbes(transition.offset(), prefixRanges, "UNCOVERED"))
                    DeletionInfoTransitionSupport.assertRangeEquivalent(transition.expectedPrefix().rangeCovering(key), transition.published().rangeCovering(key));
            }
        }

        @Setup(Level.Invocation)
        public void reset()
        {
            DeletionInfoTransitionSupport.Transition transition = transitions[next++ & 1];
            deletionInfo = transition.published();
            covered = DeletionInfoTransitionSupport.clusteringProbes(transition.offset(), prefixRanges, "COVERED");
            uncovered = DeletionInfoTransitionSupport.clusteringProbes(transition.offset(), prefixRanges, "UNCOVERED");
        }
    }

    private static void validate(DeletionInfoTransitionSupport.Fixture fixture, DeletionInfoTransitionSupport.Transition[] transitions)
    {
        for (DeletionInfoTransitionSupport.Transition transition : transitions)
        {
            fixture.prepare(transition);
            fixture.applyPrepared(transition);
            DeletionInfoTransitionSupport.assertEquivalent(transition.expected(), fixture.deletionInfo());
            DeletionInfoTransitionSupport.assertEquivalent(transition.expectedPrefix(), transition.published());
        }
    }
}
