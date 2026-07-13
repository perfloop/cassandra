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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.RowUpdateBuilder;
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

/**
 * Exercises the same AtomicBTreePartition range-delete merge as the long sequential benchmark at shapes where
 * a fixed copy-on-write cost or a competing optimistic update could otherwise be obscured by the long build.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(1)
public class RangeTombstoneMemtableMergeShapeBench
{
    private static final int CONCURRENT_MAX_WRITERS = 8;
    private static final int CONCURRENT_UPDATES_PER_WRITER = 256;
    private static final HeapPool pool = new HeapPool(Long.MAX_VALUE, 1.0f, () -> ImmediateFuture.success(Boolean.TRUE));

    static
    {
        DatabaseDescriptor.daemonInitialization();
        DatabaseDescriptor.setPartitionerUnsafe(ByteOrderedPartitioner.instance);
    }

    @State(Scope.Benchmark)
    public static class SingleTailWorkload
    {
        @Param({ "1", "2", "61", "62" })
        public int existingRangeCount;

        private TableMetadata metadata;
        private DecoratedKey key;
        private PartitionUpdate[] existingUpdates;
        private PartitionUpdate tailUpdate;
        private MemtableAllocator allocator;
        private OpOrder order;
        private AtomicBTreePartition partition;

        @Setup(Level.Trial)
        public void setupTrial()
        {
            metadata = metadata("single_tail");
            key = metadata.partitioner.decorateKey(Int32Type.instance.decompose(0));
            existingUpdates = new PartitionUpdate[existingRangeCount];
            for (int i = 0; i < existingUpdates.length; i++)
                existingUpdates[i] = rangeDelete(metadata, key, i);
            tailUpdate = rangeDelete(metadata, key, existingRangeCount);
            allocator = pool.newAllocator("single_tail");
            order = new OpOrder();
        }

        @Setup(Level.Invocation)
        public void setupInvocation()
        {
            partition = new AtomicBTreePartition(TableMetadataRef.forOfflineTools(metadata), key, allocator);
            for (PartitionUpdate update : existingUpdates)
                apply(partition, allocator, order, update);
        }

        @TearDown(Level.Invocation)
        public void verifyInvocation()
        {
            int actual = partition.deletionInfo().rangeCount();
            int expected = existingRangeCount + 1;
            if (actual != expected)
                throw new AssertionError("Expected " + expected + " ranges but found " + actual);
        }

        @TearDown(Level.Trial)
        public void tearDownTrial()
        {
            allocator.setDiscarding();
            allocator.setDiscarded();
        }
    }

    @State(Scope.Thread)
    public static class ConcurrentTailWorkload
    {
        @Param({ "62" })
        public int existingRangeCount;

        private TableMetadata metadata;
        private DecoratedKey key;
        private PartitionUpdate[] existingUpdates;
        private PartitionUpdate[] concurrentUpdates;
        private ExecutorService writers;
        private MemtableAllocator allocator;
        private OpOrder order;
        private AtomicBTreePartition partition;
        private int expectedRangeCount;

        @Setup(Level.Trial)
        public void setupTrial()
        {
            metadata = metadata("concurrent_tail");
            key = metadata.partitioner.decorateKey(Int32Type.instance.decompose(0));
            existingUpdates = new PartitionUpdate[existingRangeCount];
            for (int i = 0; i < existingUpdates.length; i++)
                existingUpdates[i] = rangeDelete(metadata, key, i);

            concurrentUpdates = new PartitionUpdate[CONCURRENT_MAX_WRITERS * CONCURRENT_UPDATES_PER_WRITER];
            for (int i = 0; i < concurrentUpdates.length; i++)
                concurrentUpdates[i] = rangeDelete(metadata, key, existingRangeCount + i);
            writers = Executors.newFixedThreadPool(CONCURRENT_MAX_WRITERS);
        }

        @Setup(Level.Invocation)
        public void setupInvocation()
        {
            allocator = pool.newAllocator("concurrent_tail");
            order = new OpOrder();
            partition = new AtomicBTreePartition(TableMetadataRef.forOfflineTools(metadata), key, allocator);
            for (PartitionUpdate update : existingUpdates)
                apply(partition, allocator, order, update);
            expectedRangeCount = existingRangeCount;
        }

        @TearDown(Level.Invocation)
        public void verifyInvocation()
        {
            int actual = partition.deletionInfo().rangeCount();
            if (actual != expectedRangeCount)
                throw new AssertionError("Expected " + expectedRangeCount + " ranges but found " + actual);
            allocator.setDiscarding();
            allocator.setDiscarded();
        }

        @TearDown(Level.Trial)
        public void tearDownTrial()
        {
            writers.shutdownNow();
        }

        private int mergeConcurrentTailRangeDeletes(int writerCount) throws InterruptedException, ExecutionException
        {
            CountDownLatch ready = new CountDownLatch(writerCount);
            CountDownLatch start = new CountDownLatch(1);
            List<Future<Integer>> futures = new ArrayList<>(writerCount);
            for (int writer = 0; writer < writerCount; writer++)
            {
                int firstUpdate = writer * CONCURRENT_UPDATES_PER_WRITER;
                futures.add(writers.submit(() -> {
                    ready.countDown();
                    start.await();
                    for (int i = 0; i < CONCURRENT_UPDATES_PER_WRITER; i++)
                        apply(partition, allocator, order, concurrentUpdates[firstUpdate + i]);
                    return CONCURRENT_UPDATES_PER_WRITER;
                }));
            }

            ready.await();
            start.countDown();

            int completed = 0;
            for (Future<Integer> future : futures)
                completed += future.get();
            expectedRangeCount = existingRangeCount + completed;
            return completed;
        }
    }

    @Benchmark
    @Threads(1)
    public int mergeTailIntoPrepopulatedPartition(SingleTailWorkload workload)
    {
        apply(workload.partition, workload.allocator, workload.order, workload.tailUpdate);
        return workload.partition.deletionInfo().rangeCount();
    }

    @Benchmark
    @Threads(1)
    @OperationsPerInvocation(2 * CONCURRENT_UPDATES_PER_WRITER)
    public int mergeConcurrentTailRangeDeletesTwoWriters(ConcurrentTailWorkload workload) throws ExecutionException, InterruptedException
    {
        return workload.mergeConcurrentTailRangeDeletes(2);
    }

    @Benchmark
    @Threads(1)
    @OperationsPerInvocation(CONCURRENT_MAX_WRITERS * CONCURRENT_UPDATES_PER_WRITER)
    public int mergeConcurrentTailRangeDeletesEightWriters(ConcurrentTailWorkload workload) throws ExecutionException, InterruptedException
    {
        return workload.mergeConcurrentTailRangeDeletes(CONCURRENT_MAX_WRITERS);
    }

    private static TableMetadata metadata(String table)
    {
        return TableMetadata.builder("bench", table)
                            .addPartitionKeyColumn("pk", Int32Type.instance)
                            .addClusteringColumn("ck", Int32Type.instance)
                            .partitioner(ByteOrderedPartitioner.instance)
                            .build();
    }

    private static PartitionUpdate rangeDelete(TableMetadata metadata, DecoratedKey key, int ordinal)
    {
        return new RowUpdateBuilder(metadata, 1, ordinal + 1L, key)
               .addRangeTombstone(2 * ordinal, 2 * ordinal + 1)
               .buildUpdate();
    }

    private static void apply(AtomicBTreePartition partition, MemtableAllocator allocator, OpOrder order, PartitionUpdate update)
    {
        OpOrder.Group writeOp = order.getCurrent();
        partition.addAll(update, allocator.cloner(writeOp), writeOp, UpdateTransaction.NO_OP);
    }
}
