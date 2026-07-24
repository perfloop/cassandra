/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.MutableDeletionInfo;
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

/** Measures one overlapping update after a 4,096-range ordered prefix. */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Fork(1)
@Threads(1)
@State(Scope.Benchmark)
public class RangeTombstoneFallbackBench
{
    private static final int RANGE_COUNT = 4096;
    private static final long LOCAL_DELETION_TIME = 1;

    private final OpOrder order = new OpOrder();
    private final HeapPool pool = new HeapPool(Long.MAX_VALUE, 1.0f, () -> ImmediateFuture.success(Boolean.TRUE));
    private TableMetadata metadata;
    private TableMetadataRef metadataRef;
    private DecoratedKey key;
    private BTreePartitionData prefix;
    private PartitionUpdate overlap;
    private int expectedRangeCount;

    @Setup(Level.Trial)
    public void setup()
    {
        DatabaseDescriptor.daemonInitialization();
        metadata = TableMetadata.builder("range_tombstone_fallback_bench", "merge")
                                .addPartitionKeyColumn("pk", Int32Type.instance)
                                .addClusteringColumn("ck", Int32Type.instance)
                                .partitioner(ByteOrderedPartitioner.instance)
                                .build();
        metadataRef = TableMetadataRef.forOfflineTools(metadata);
        key = metadata.partitioner.decorateKey(Int32Type.instance.decompose(0));

        MemtableAllocator allocator = pool.newAllocator("prefix");
        AtomicBTreePartition partition = newPartition(allocator);
        MutableDeletionInfo expected = MutableDeletionInfo.live();
        for (int i = 0; i < RANGE_COUNT; i++)
        {
            PartitionUpdate update = rangeUpdate(i * 4, i * 4 + 2, i + 1L);
            expected.add(update.deletionInfo());
            apply(partition, allocator, update);
        }
        overlap = rangeUpdate(1, 5, RANGE_COUNT + 100L);
        expected.add(overlap.deletionInfo());
        expectedRangeCount = expected.rangeCount();
        prefix = partition.unsafeGetHolder();
    }

    @Benchmark
    public int reconcileFallback()
    {
        MemtableAllocator allocator = pool.newAllocator("fallback");
        AtomicBTreePartition partition = newPartition(allocator);
        partition.unsafeSetHolder(prefix);
        apply(partition, allocator, overlap);
        int count = partition.deletionInfo().rangeCount();
        if (count != expectedRangeCount)
            throw new AssertionError("expected " + expectedRangeCount + " ranges but found " + count);
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

    private PartitionUpdate rangeUpdate(int start, int end, long timestamp)
    {
        PartitionUpdate.SimpleBuilder builder = PartitionUpdate.simpleBuilder(metadata, 0);
        builder.timestamp(timestamp).nowInSec(LOCAL_DELETION_TIME);
        builder.addRangeTombstone().start(start).end(end);
        return builder.build();
    }
}
