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

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.DecoratedKey;
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
import org.apache.cassandra.utils.memory.HeapPool;
import org.apache.cassandra.utils.memory.MemtableAllocator;

/**
 * Measures sequential single-range updates through the AtomicBTreePartition write path.
 *
 * Each invocation creates an empty partition and applies 4,096 ordered, non-overlapping
 * range tombstone updates. The input updates are built once per trial so their construction
 * is outside the measured merge work.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 1, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(1)
@Threads(1)
@State(Scope.Benchmark)
public class RangeTombstoneMergeBench
{
    private static final int RANGE_COUNT = 4096;
    private static final long LOCAL_DELETION_TIME = 1;

    private final OpOrder order = new OpOrder();
    private final HeapPool pool = new HeapPool(Long.MAX_VALUE, 1.0f, () -> ImmediateFuture.success(Boolean.TRUE));

    private TableMetadata metadata;
    private TableMetadataRef metadataRef;
    private DecoratedKey key;
    private PartitionUpdate[] rangeUpdates;
    private PartitionUpdate partitionDelete;
    private BTreePartitionData deletePrefix;

    @Setup(Level.Trial)
    public void setup()
    {
        DatabaseDescriptor.daemonInitialization();
        metadata = TableMetadata.builder("range_tombstone_bench", "merge")
                                .addPartitionKeyColumn("pk", Int32Type.instance)
                                .addClusteringColumn("ck", Int32Type.instance)
                                .partitioner(ByteOrderedPartitioner.instance)
                                .build();
        metadataRef = TableMetadataRef.forOfflineTools(metadata);
        key = metadata.partitioner.decorateKey(Int32Type.instance.decompose(0));

        rangeUpdates = new PartitionUpdate[RANGE_COUNT];
        for (int i = 0; i < RANGE_COUNT; i++)
            rangeUpdates[i] = rangeUpdate(i, i + 1L);

        partitionDelete = partitionDelete(RANGE_COUNT + 1L);

        MemtableAllocator prefixAllocator = pool.newAllocator("prefix");
        AtomicBTreePartition prefix = newPartition(prefixAllocator);
        for (PartitionUpdate update : rangeUpdates)
            apply(prefix, prefixAllocator, update);
        deletePrefix = prefix.unsafeGetHolder();
    }

    @Benchmark
    public int merge4096SingleRangeUpdates()
    {
        MemtableAllocator allocator = pool.newAllocator("merge");
        AtomicBTreePartition partition = newPartition(allocator);
        for (PartitionUpdate update : rangeUpdates)
            apply(partition, allocator, update);

        int count = partition.deletionInfo().rangeCount();
        if (count != RANGE_COUNT)
            throw new AssertionError("expected " + RANGE_COUNT + " ranges but found " + count);
        return count;
    }

    @Benchmark
    public int mergeSupersedingPartitionDelete()
    {
        MemtableAllocator allocator = pool.newAllocator("partition-delete");
        AtomicBTreePartition partition = newPartition(allocator);
        partition.unsafeSetHolder(deletePrefix);
        apply(partition, allocator, partitionDelete);

        int count = partition.deletionInfo().rangeCount();
        if (count != RANGE_COUNT || partition.deletionInfo().getPartitionDeletion().isLive())
            throw new AssertionError("partition deletion did not preserve the range tombstones");
        return count;
    }

    private AtomicBTreePartition newPartition(MemtableAllocator allocator)
    {
        return new AtomicBTreePartition(metadataRef, key, allocator);
    }

    private void apply(AtomicBTreePartition partition, MemtableAllocator allocator, PartitionUpdate update)
    {
        OpOrder.Group writeOp = order.getCurrent();
        partition.addAll(update, allocator.cloner(writeOp), writeOp, UpdateTransaction.NO_OP);
    }

    private PartitionUpdate rangeUpdate(int index, long timestamp)
    {
        PartitionUpdate.SimpleBuilder builder = PartitionUpdate.simpleBuilder(metadata, 0);
        builder.timestamp(timestamp).nowInSec(LOCAL_DELETION_TIME);
        builder.addRangeTombstone().start(index * 4).end(index * 4 + 2);
        return builder.build();
    }

    private PartitionUpdate partitionDelete(long timestamp)
    {
        PartitionUpdate.SimpleBuilder builder = PartitionUpdate.simpleBuilder(metadata, 0);
        builder.timestamp(timestamp).nowInSec(LOCAL_DELETION_TIME).delete();
        return builder.build();
    }
}
