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

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.BufferDecoratedKey;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionInfo;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.RangeTombstone;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.Slice;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.partitions.AtomicBTreePartition;
import org.apache.cassandra.db.partitions.BTreePartitionData;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.dht.ByteOrderedPartitioner;
import org.apache.cassandra.index.transactions.UpdateTransaction;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.utils.concurrent.ImmediateFuture;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.memory.Cloner;
import org.apache.cassandra.utils.memory.HeapPool;
import org.apache.cassandra.utils.memory.MemtableAllocator;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Benchmark)
public class RangeTombstoneMergeBench
{
    private static final int UPDATE_COUNT = 4096;
    private static final int RANGE_WIDTH = 4;
    private static final OpOrder NO_ORDER = new OpOrder();
    private static final HeapPool POOL = new HeapPool(Long.MAX_VALUE, 1.0f, () -> ImmediateFuture.success(Boolean.TRUE));

    static
    {
        DatabaseDescriptor.setPartitionerUnsafe(ByteOrderedPartitioner.instance);
    }

    private TableMetadata metadata;
    private DecoratedKey key;
    private PartitionUpdate[] appendUpdates;
    private PartitionUpdate supersedingPartitionDelete;
    private PartitionUpdate overlappingRange;

    @Setup(Level.Trial)
    public void setup()
    {
        metadata = TableMetadata.builder("range_tombstone_bench", "updates")
                                .addPartitionKeyColumn("pk", Int32Type.instance)
                                .addClusteringColumn("ck", Int32Type.instance)
                                .partitioner(ByteOrderedPartitioner.instance)
                                .build();
        ByteBuffer keyBytes = Int32Type.instance.decompose(0);
        key = new BufferDecoratedKey(ByteOrderedPartitioner.instance.getToken(keyBytes), keyBytes);
        appendUpdates = new PartitionUpdate[UPDATE_COUNT];
        for (int i = 0; i < UPDATE_COUNT; i++)
            appendUpdates[i] = rangeUpdate(i, i + 1L);

        supersedingPartitionDelete = PartitionUpdate.fullPartitionDelete(metadata, key, UPDATE_COUNT + 1L, UPDATE_COUNT + 1L);
        overlappingRange = rangeUpdate(UPDATE_COUNT / 2, UPDATE_COUNT + 2L);
    }

    @Benchmark
    public long merge4096SingleRangeUpdates()
    {
        MemtableAllocator allocator = new HeapPool.Allocator(POOL);
        AtomicBTreePartition partition = newPartition(allocator);
        applyPrefix(partition, allocator.cloner(NO_ORDER.getCurrent()));
        return result(partition.deletionInfo());
    }

    @Benchmark
    public long mergeSupersedingPartitionDelete(PrefixState state)
    {
        state.partition.addAll(supersedingPartitionDelete, state.cloner, NO_ORDER.getCurrent(), UpdateTransaction.NO_OP);
        return result(state.partition.deletionInfo());
    }

    @Benchmark
    public long mergeOverlappingRangeAfter4096Prefix(PrefixState state)
    {
        state.partition.addAll(overlappingRange, state.cloner, NO_ORDER.getCurrent(), UpdateTransaction.NO_OP);
        return result(state.partition.deletionInfo());
    }

    private AtomicBTreePartition newPartition(MemtableAllocator allocator)
    {
        return new AtomicBTreePartition(TableMetadataRef.forOfflineTools(metadata), key, allocator);
    }

    private void applyPrefix(AtomicBTreePartition partition, Cloner cloner)
    {
        for (PartitionUpdate update : appendUpdates)
            partition.addAll(update, cloner, NO_ORDER.getCurrent(), UpdateTransaction.NO_OP);
    }

    private PartitionUpdate rangeUpdate(int index, long timestamp)
    {
        PartitionUpdate.Builder builder = new PartitionUpdate.Builder(metadata, key, RegularAndStaticColumns.NONE, 0, false);
        builder.add(new RangeTombstone(Slice.make(clustering(index * RANGE_WIDTH), clustering(index * RANGE_WIDTH + 1)),
                                       DeletionTime.build(timestamp, timestamp)));
        return builder.build();
    }

    private static Clustering<ByteBuffer> clustering(int value)
    {
        return Clustering.make(Int32Type.instance.decompose(value));
    }

    private static long result(DeletionInfo info)
    {
        return ((long) info.rangeCount() << 32) ^ info.dataSize() ^ info.maxTimestamp();
    }

    @State(Scope.Thread)
    public static class PrefixState
    {
        private BTreePartitionData prefix;
        private AtomicBTreePartition partition;
        private Cloner cloner;

        @Setup(Level.Trial)
        public void setupPrefix(RangeTombstoneMergeBench bench)
        {
            MemtableAllocator allocator = new HeapPool.Allocator(POOL);
            AtomicBTreePartition source = bench.newPartition(allocator);
            bench.applyPrefix(source, allocator.cloner(NO_ORDER.getCurrent()));
            prefix = source.unsafeGetHolder();
        }

        @Setup(Level.Invocation)
        public void setupInvocation(RangeTombstoneMergeBench bench)
        {
            MemtableAllocator allocator = new HeapPool.Allocator(POOL);
            partition = bench.newPartition(allocator);
            partition.unsafeSetHolder(prefix);
            cloner = allocator.cloner(NO_ORDER.getCurrent());
        }
    }
}
