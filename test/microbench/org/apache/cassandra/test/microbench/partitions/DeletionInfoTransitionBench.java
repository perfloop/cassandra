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
package org.apache.cassandra.test.microbench.partitions;

import java.nio.ByteBuffer;
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

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.BufferDecoratedKey;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.MutableDeletionInfo;
import org.apache.cassandra.db.RangeTombstone;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.Slice;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.partitions.AtomicBTreePartition;
import org.apache.cassandra.db.partitions.BTreePartitionData;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.ByteOrderedPartitioner;
import org.apache.cassandra.index.transactions.UpdateTransaction;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.utils.btree.BTree;
import org.apache.cassandra.utils.concurrent.ImmediateFuture;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.memory.Cloner;
import org.apache.cassandra.utils.memory.HeapPool;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 1, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(1)
@Threads(1)
public class DeletionInfoTransitionBench
{
    private static final long PREFIX_TIMESTAMP = 100L;
    private static final long UPDATE_TIMESTAMP = 200L;
    private static final long LOCAL_DELETION_TIME = 1L;
    private static final HeapPool POOL = new HeapPool(Long.MAX_VALUE,
                                                       1.0f,
                                                       () -> ImmediateFuture.success(Boolean.TRUE));
    private static final OpOrder ORDER = new OpOrder();

    static
    {
        DatabaseDescriptor.daemonInitialization();
    }

    public enum ExactBoundSeparation
    {
        ADJACENT,
        OPPOSITE
    }

    @State(Scope.Thread)
    public static class TransitionState
    {
        @Param({ "128", "4096" })
        public int prefixSize;

        @Param({ "ADJACENT", "OPPOSITE" })
        public ExactBoundSeparation exactBoundSeparation;

        TableMetadata metadata;
        AtomicBTreePartition partition;
        BTreePartitionData prefix;
        Cloner cloner;
        OpOrder.Group writeOp;
        PartitionUpdate partitionDelete;
        PartitionUpdate exactBound;
        PartitionUpdate overlap;
        PartitionUpdate outOfOrder;
        Slice[] slices;
        Clustering<?>[] lookups;
        ColumnFilter selection;

        @Setup(Level.Trial)
        public void setup()
        {
            metadata = TableMetadata.builder("deletion_info_bench", "transitions")
                                    .addPartitionKeyColumn("pk", Int32Type.instance)
                                    .addClusteringColumn("ck", Int32Type.instance)
                                    .partitioner(ByteOrderedPartitioner.instance)
                                    .build();

            ByteBuffer key = Int32Type.instance.decompose(0);
            DecoratedKey partitionKey = new BufferDecoratedKey(ByteOrderedPartitioner.instance.getToken(key), key);
            HeapPool.Allocator allocator = new HeapPool.Allocator(POOL);
            partition = new AtomicBTreePartition(TableMetadataRef.forOfflineTools(metadata), partitionKey, allocator);
            writeOp = ORDER.getCurrent();
            cloner = allocator.cloner(writeOp);

            MutableDeletionInfo prefixInfo = prefix(metadata, prefixSize);
            partition.addAll(update(metadata, partitionKey, prefixInfo), cloner, writeOp, UpdateTransaction.NO_OP);
            prefix = partition.unsafeGetHolder();

            partitionDelete = update(metadata, partitionKey, new MutableDeletionInfo(deletionTime(UPDATE_TIMESTAMP)));
            exactBound = update(metadata, partitionKey, exactBoundUpdate(metadata, prefixSize, exactBoundSeparation));
            overlap = update(metadata, partitionKey, overlappingUpdate(metadata, prefixSize));
            outOfOrder = update(metadata, partitionKey, outOfOrderUpdate(metadata, prefixSize));
            slices = slices(metadata, prefixSize);
            lookups = lookups(metadata, prefixSize);
            selection = ColumnFilter.all(partition.columns());
        }

        @Setup(Level.Invocation)
        public void resetToPrefix()
        {
            partition.unsafeSetHolder(prefix);
        }
    }

    /** Applies a partition deletion after an immutable range-tombstone prefix. */
    @Benchmark
    public BTreePartitionData partitionDelete(TransitionState state)
    {
        return transition(state, state.partitionDelete);
    }

    /** Applies two exact-bound, higher-timestamp replacements to the prepared prefix. */
    @Benchmark
    public BTreePartitionData twoExactBoundRanges(TransitionState state)
    {
        return transition(state, state.exactBound);
    }

    /** Applies a higher-timestamp range overlapping three retained ranges. */
    @Benchmark
    public BTreePartitionData overlap(TransitionState state)
    {
        return transition(state, state.overlap);
    }

    /** Applies two ranges that were supplied to the canonical reconciler out of order. */
    @Benchmark
    public BTreePartitionData outOfOrder(TransitionState state)
    {
        return transition(state, state.outOfOrder);
    }

    /** Fully consumes early, middle, late, and miss slices in both iterator directions. */
    @Benchmark
    public int allSlicesAndDirections(TransitionState state, Blackhole blackhole)
    {
        int count = 0;
        for (Slice slice : state.slices)
        {
            count += consumeSlice(state.partition, state.selection, slice, false, blackhole);
            count += consumeSlice(state.partition, state.selection, slice, true, blackhole);
        }
        return count;
    }

    /** Repeats covered and uncovered clustering lookups through the partition read API. */
    @Benchmark
    public int coveredAndUncovered(TransitionState state, Blackhole blackhole)
    {
        int count = 0;
        for (Clustering<?> clustering : state.lookups)
        {
            Row row = state.partition.getRow(clustering);
            blackhole.consume(row);
            count += row == null ? 0 : 1;
        }
        return count;
    }

    private static BTreePartitionData transition(TransitionState state, PartitionUpdate update)
    {
        state.partition.addAll(update, state.cloner, state.writeOp, UpdateTransaction.NO_OP);
        return state.partition.unsafeGetHolder();
    }

    private static int consumeSlice(AtomicBTreePartition partition,
                                    ColumnFilter selection,
                                    Slice slice,
                                    boolean reversed,
                                    Blackhole blackhole)
    {
        int count = 0;
        try (UnfilteredRowIterator iterator = partition.unfilteredIterator(selection, slice, reversed))
        {
            while (iterator.hasNext())
            {
                Unfiltered unfiltered = iterator.next();
                blackhole.consume(unfiltered);
                count++;
            }
        }
        return count;
    }

    private static PartitionUpdate update(TableMetadata metadata,
                                           DecoratedKey partitionKey,
                                           MutableDeletionInfo deletionInfo)
    {
        BTreePartitionData holder = BTreePartitionData.unsafeConstruct(RegularAndStaticColumns.NONE,
                                                                         BTree.empty(),
                                                                         deletionInfo,
                                                                         Rows.EMPTY_STATIC_ROW,
                                                                         EncodingStats.NO_STATS);
        return PartitionUpdate.unsafeConstruct(metadata, partitionKey, holder, deletionInfo, false);
    }

    private static MutableDeletionInfo prefix(TableMetadata metadata, int count)
    {
        MutableDeletionInfo deletionInfo = new MutableDeletionInfo(DeletionTime.LIVE);
        for (int i = 0; i < count; i++)
            deletionInfo.add(range(metadata, i, PREFIX_TIMESTAMP), metadata.comparator);
        return deletionInfo;
    }

    private static MutableDeletionInfo exactBoundUpdate(TableMetadata metadata,
                                                         int count,
                                                         ExactBoundSeparation separation)
    {
        MutableDeletionInfo deletionInfo = new MutableDeletionInfo(DeletionTime.LIVE);
        int first = separation == ExactBoundSeparation.OPPOSITE ? 0 : count / 2;
        int second = separation == ExactBoundSeparation.OPPOSITE ? count - 1 : first + 1;
        deletionInfo.add(range(metadata, first, UPDATE_TIMESTAMP), metadata.comparator);
        deletionInfo.add(range(metadata, second, UPDATE_TIMESTAMP), metadata.comparator);
        return deletionInfo;
    }

    private static MutableDeletionInfo overlappingUpdate(TableMetadata metadata, int count)
    {
        int middle = count / 2;
        MutableDeletionInfo deletionInfo = new MutableDeletionInfo(DeletionTime.LIVE);
        deletionInfo.add(range(metadata,
                               rangeStart(middle) - 1,
                               rangeEnd(middle + 2) + 1,
                               UPDATE_TIMESTAMP), metadata.comparator);
        return deletionInfo;
    }

    private static MutableDeletionInfo outOfOrderUpdate(TableMetadata metadata, int count)
    {
        MutableDeletionInfo deletionInfo = new MutableDeletionInfo(DeletionTime.LIVE);
        deletionInfo.add(range(metadata, count * 3 / 4, UPDATE_TIMESTAMP), metadata.comparator);
        deletionInfo.add(range(metadata, count / 4, UPDATE_TIMESTAMP + 1), metadata.comparator);
        return deletionInfo;
    }

    private static Slice[] slices(TableMetadata metadata, int count)
    {
        return new Slice[]{
            slice(metadata, rangeStart(0), rangeEnd(1)),
            slice(metadata, rangeStart(count / 2), rangeEnd(count / 2 + 1)),
            slice(metadata, rangeStart(count - 2), rangeEnd(count - 1)),
            slice(metadata, rangeEnd(count - 1) + 2, rangeEnd(count - 1) + 8)
        };
    }

    private static Clustering<?>[] lookups(TableMetadata metadata, int count)
    {
        return new Clustering<?>[]{
            clustering(metadata, rangeStart(0)),
            clustering(metadata, rangeEnd(0) + 2),
            clustering(metadata, rangeStart(count / 2)),
            clustering(metadata, rangeEnd(count / 2) + 2),
            clustering(metadata, rangeStart(count - 1)),
            clustering(metadata, rangeEnd(count - 1) + 2)
        };
    }

    private static RangeTombstone range(TableMetadata metadata, int index, long timestamp)
    {
        return range(metadata, rangeStart(index), rangeEnd(index), timestamp);
    }

    private static RangeTombstone range(TableMetadata metadata, int start, int end, long timestamp)
    {
        return new RangeTombstone(slice(metadata, start, end), deletionTime(timestamp));
    }

    private static DeletionTime deletionTime(long timestamp)
    {
        return DeletionTime.build(timestamp, LOCAL_DELETION_TIME);
    }

    private static Slice slice(TableMetadata metadata, int start, int end)
    {
        return Slice.make(clustering(metadata, start), clustering(metadata, end));
    }

    private static Clustering<?> clustering(TableMetadata metadata, int value)
    {
        return metadata.comparator.make(value);
    }

    private static int rangeStart(int index)
    {
        return index * 4;
    }

    private static int rangeEnd(int index)
    {
        return rangeStart(index) + 1;
    }
}
