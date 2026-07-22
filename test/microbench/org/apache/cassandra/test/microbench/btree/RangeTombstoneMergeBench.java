/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.BufferDecoratedKey;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.partitions.AtomicBTreePartition;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.dht.ByteOrderedPartitioner;
import org.apache.cassandra.index.transactions.UpdateTransaction;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.utils.concurrent.ImmediateFuture;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.memory.HeapPool;
import org.apache.cassandra.utils.memory.MemtableAllocator;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 1, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(1)
@Threads(1)
public class RangeTombstoneMergeBench
{
    private static final int RANGE_COUNT = 4096;
    private static final OpOrder NO_ORDER = new OpOrder();
    private static final HeapPool POOL = new HeapPool(Long.MAX_VALUE, 1.0f, () -> ImmediateFuture.success(Boolean.TRUE));
    private static final DecoratedKey PARTITION_KEY;
    private static final TableMetadata METADATA;
    private static final TableMetadataRef METADATA_REF;

    static
    {
        DatabaseDescriptor.daemonInitialization();
        DatabaseDescriptor.setPartitionerUnsafe(ByteOrderedPartitioner.instance);
        METADATA = TableMetadata.builder("perfloop", "range_tombstone_merge")
                                .addPartitionKeyColumn("pk", Int32Type.instance)
                                .addClusteringColumn("ck", Int32Type.instance)
                                .build();
        PARTITION_KEY = new BufferDecoratedKey(ByteOrderedPartitioner.instance.getToken(Int32Type.instance.decompose(0)),
                                               Int32Type.instance.decompose(0));
        METADATA_REF = TableMetadataRef.forOfflineTools(METADATA);
    }

    @Benchmark
    public int merge4096SingleRangeUpdates(MergeState state)
    {
        PartitionUpdate[] updates = state.updates[state.invocation++ & 1];
        for (PartitionUpdate update : updates)
            apply(state.partition, state.allocator, update, UpdateTransaction.NO_OP);

        int rangeCount = state.partition.deletionInfo().rangeCount();
        if (rangeCount != RANGE_COUNT)
            throw new IllegalStateException("Expected " + RANGE_COUNT + " ranges but found " + rangeCount);
        return rangeCount;
    }

    @Benchmark
    public int mergeSupersedingPartitionDelete(PartitionDeleteState state)
    {
        apply(state.partition, state.allocator, state.partitionDelete, UpdateTransaction.NO_OP);

        if (state.partition.deletionInfo().getPartitionDeletion().markedForDeleteAt() != RANGE_COUNT + 1L)
            throw new IllegalStateException("Partition deletion was not retained");
        return state.partition.deletionInfo().rangeCount();
    }

    @State(Scope.Thread)
    public static class MergeState
    {
        private PartitionUpdate[][] updates;
        private AtomicBTreePartition partition;
        private MemtableAllocator allocator;
        private int invocation;

        @Setup(Level.Trial)
        public void setupTrial()
        {
            updates = new PartitionUpdate[][] { updates(0), updates(RANGE_COUNT * 10) };
        }

        @Setup(Level.Invocation)
        public void setupInvocation()
        {
            allocator = new HeapPool.Allocator(POOL);
            partition = new AtomicBTreePartition(METADATA_REF, PARTITION_KEY, allocator);
        }
    }

    @State(Scope.Thread)
    public static class PartitionDeleteState
    {
        private AtomicBTreePartition prefix;
        private AtomicBTreePartition partition;
        private MemtableAllocator prefixAllocator;
        private MemtableAllocator allocator;
        private PartitionUpdate partitionDelete;

        @Setup(Level.Trial)
        public void setupTrial()
        {
            prefixAllocator = new HeapPool.Allocator(POOL);
            prefix = new AtomicBTreePartition(METADATA_REF, PARTITION_KEY, prefixAllocator);
            for (PartitionUpdate update : updates(0))
                apply(prefix, prefixAllocator, update, UpdateTransaction.NO_OP);
            partitionDelete = partitionDelete();
        }

        @Setup(Level.Invocation)
        public void setupInvocation()
        {
            allocator = new HeapPool.Allocator(POOL);
            partition = new AtomicBTreePartition(METADATA_REF, PARTITION_KEY, allocator);
            partition.unsafeSetHolder(prefix.unsafeGetHolder());
        }
    }

    private static PartitionUpdate[] updates(int timestampOffset)
    {
        PartitionUpdate[] updates = new PartitionUpdate[RANGE_COUNT];
        for (int i = 0; i < RANGE_COUNT; i++)
            updates[i] = rangeUpdate(i, timestampOffset + i + 1L);
        return updates;
    }

    private static PartitionUpdate rangeUpdate(int range, long timestamp)
    {
        int start = range * 4;
        PartitionUpdate.SimpleBuilder builder = PartitionUpdate.simpleBuilder(METADATA, 0)
                                                              .timestamp(timestamp)
                                                              .nowInSec(1);
        builder.addRangeTombstone().start(start).end(start + 2);
        return builder.build();
    }

    private static PartitionUpdate partitionDelete()
    {
        return PartitionUpdate.simpleBuilder(METADATA, 0)
                              .timestamp(RANGE_COUNT + 1L)
                              .nowInSec(1)
                              .delete()
                              .build();
    }

    private static void apply(AtomicBTreePartition partition,
                              MemtableAllocator allocator,
                              PartitionUpdate update,
                              UpdateTransaction indexer)
    {
        OpOrder.Group writeOp = NO_ORDER.getCurrent();
        partition.addAll(update, allocator.cloner(writeOp), writeOp, indexer);
    }
}
