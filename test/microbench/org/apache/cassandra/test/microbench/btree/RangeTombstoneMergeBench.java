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
package org.apache.cassandra.test.microbench.btree;

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
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.partitions.AtomicBTreePartition;
import org.apache.cassandra.db.partitions.BTreePartitionData;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.index.transactions.UpdateTransaction;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.concurrent.ImmediateFuture;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.memory.HeapCloner;
import org.apache.cassandra.utils.memory.HeapPool;
import org.apache.cassandra.utils.memory.MemtableAllocator;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 1, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(1)
@Threads(1)
@State(Scope.Benchmark)
public class RangeTombstoneMergeBench
{
    private static final int RANGE_COUNT = 4_096;
    private static final HeapPool POOL = new HeapPool(Long.MAX_VALUE, 1.0f, () -> ImmediateFuture.success(Boolean.TRUE));

    private TableMetadata metadata;
    private DecoratedKey key;
    private PartitionUpdate[] rangeUpdates;
    private PartitionUpdate supersedingPartitionDelete;

    @Setup(Level.Trial)
    public void setup()
    {
        DatabaseDescriptor.daemonInitialization();
        metadata = TableMetadata.builder("range_tombstone_bench", "ranges")
                                .addPartitionKeyColumn("pk", org.apache.cassandra.db.marshal.Int32Type.instance)
                                .addClusteringColumn("ck", org.apache.cassandra.db.marshal.Int32Type.instance)
                                .build();
        key = metadata.partitioner.decorateKey(ByteBufferUtil.bytes(0));
        rangeUpdates = new PartitionUpdate[RANGE_COUNT];
        for (int i = 0; i < RANGE_COUNT; i++)
        {
            rangeUpdates[i] = new RowUpdateBuilder(metadata, 1, i + 1L, 0)
                              .addRangeTombstone(i * 3, i * 3 + 1)
                              .buildUpdate();
        }
        supersedingPartitionDelete = PartitionUpdate.fullPartitionDelete(metadata, key, RANGE_COUNT + 1L, 1);
    }

    @Benchmark
    public void merge4096SingleRangeUpdates(Blackhole blackhole)
    {
        AtomicBTreePartition partition = newPartition();
        OpOrder writeOrder = new OpOrder();
        for (PartitionUpdate update : rangeUpdates)
            merge(partition, update, writeOrder);

        int rangeCount = partition.deletionInfo().rangeCount();
        if (rangeCount != RANGE_COUNT)
            throw new AssertionError("Expected " + RANGE_COUNT + " ranges but found " + rangeCount);
        blackhole.consume(rangeCount);
    }

    @Benchmark
    public void mergeSupersedingPartitionDelete(SupersedingPartitionDeleteState state, Blackhole blackhole)
    {
        state.partition.unsafeSetHolder(state.initial);
        merge(state.partition, supersedingPartitionDelete, state.writeOrder);

        long timestamp = state.partition.deletionInfo().getPartitionDeletion().markedForDeleteAt();
        if (timestamp != RANGE_COUNT + 1L)
            throw new AssertionError("Expected partition deletion timestamp " + (RANGE_COUNT + 1L) + " but found " + timestamp);
        blackhole.consume(timestamp);
    }

    private AtomicBTreePartition newPartition()
    {
        MemtableAllocator allocator = POOL.newAllocator("range-tombstone-merge");
        return new AtomicBTreePartition(TableMetadataRef.forOfflineTools(metadata), key, allocator);
    }

    private static void merge(AtomicBTreePartition partition, PartitionUpdate update, OpOrder writeOrder)
    {
        try (OpOrder.Group group = writeOrder.start())
        {
            partition.addAll(update, HeapCloner.instance, group, UpdateTransaction.NO_OP);
        }
    }

    @State(Scope.Thread)
    public static class SupersedingPartitionDeleteState
    {
        private AtomicBTreePartition partition;
        private BTreePartitionData initial;
        private OpOrder writeOrder;

        @Setup(Level.Trial)
        public void setup(RangeTombstoneMergeBench benchmark)
        {
            partition = benchmark.newPartition();
            writeOrder = new OpOrder();
            for (PartitionUpdate update : benchmark.rangeUpdates)
                merge(partition, update, writeOrder);
            initial = partition.unsafeGetHolder();
        }
    }
}
