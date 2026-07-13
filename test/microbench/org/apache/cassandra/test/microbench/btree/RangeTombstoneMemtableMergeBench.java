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
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.RowUpdateBuilder;
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

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(1)
@Threads(1)
@State(Scope.Benchmark)
public class RangeTombstoneMemtableMergeBench
{
    private static final HeapPool pool = new HeapPool(Long.MAX_VALUE, 1.0f, () -> ImmediateFuture.success(Boolean.TRUE));
    private static final OpOrder order = new OpOrder();

    static
    {
        DatabaseDescriptor.daemonInitialization();
        DatabaseDescriptor.setPartitionerUnsafe(ByteOrderedPartitioner.instance);
    }

    @Param({ "128", "1024", "4096" })
    int tombstoneCount;

    @State(Scope.Thread)
    public static class Workload
    {
        private TableMetadata metadata;
        private PartitionUpdate[] updates;
        private AtomicBTreePartition partition;
        private MemtableAllocator allocator;

        @Setup(Level.Trial)
        public void setup(RangeTombstoneMemtableMergeBench bench)
        {
            metadata = TableMetadata.builder("bench", "range_tombstone_memtable")
                                    .addPartitionKeyColumn("pk", Int32Type.instance)
                                    .addClusteringColumn("ck", Int32Type.instance)
                                    .partitioner(ByteOrderedPartitioner.instance)
                                    .build();
            allocator = pool.newAllocator("range_tombstone_memtable");
            org.apache.cassandra.db.DecoratedKey key = metadata.partitioner.decorateKey(Int32Type.instance.decompose(0));
            partition = new AtomicBTreePartition(TableMetadataRef.forOfflineTools(metadata), key, allocator);
            updates = new PartitionUpdate[bench.tombstoneCount];
            for (int i = 0; i < updates.length; i++)
            {
                updates[i] = new RowUpdateBuilder(metadata, 1, i + 1L, key)
                             .addRangeTombstone(2 * i, 2 * i + 1)
                             .buildUpdate();
            }
        }

        @TearDown(Level.Invocation)
        public void reset()
        {
            partition.unsafeSetHolder(BTreePartitionData.unsafeGetEmpty());
        }

        @TearDown(Level.Trial)
        public void tearDown()
        {
            allocator.setDiscarding();
            allocator.setDiscarded();
        }
    }

    @Benchmark
    public int mergeSequentialNonOverlappingRangeDeletes(Workload workload)
    {
        for (PartitionUpdate update : workload.updates)
        {
            OpOrder.Group writeOp = order.getCurrent();
            workload.partition.addAll(update, workload.allocator.cloner(writeOp), writeOp, UpdateTransaction.NO_OP);
        }

        int rangeCount = workload.partition.deletionInfo().rangeCount();
        if (rangeCount != tombstoneCount)
            throw new AssertionError("Expected " + tombstoneCount + " ranges but found " + rangeCount);
        return rangeCount;
    }
}
