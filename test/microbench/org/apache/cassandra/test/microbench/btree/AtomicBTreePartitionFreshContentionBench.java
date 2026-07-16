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

package org.apache.cassandra.test.microbench.btree;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

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
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.partitions.AtomicBTreePartition;
import org.apache.cassandra.db.partitions.BTreePartitionData;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.RangeTombstone;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.dht.ByteOrderedPartitioner;
import org.apache.cassandra.index.transactions.UpdateTransaction;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.concurrent.ImmediateFuture;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.memory.ByteBufferCloner;
import org.apache.cassandra.utils.memory.Cloner;
import org.apache.cassandra.utils.memory.HeapPool;
import org.apache.cassandra.utils.memory.MemtableAllocator;

/**
 * Measures post-activation writes to a single wide partition. Every timed invocation receives
 * a new PartitionUpdate with a newer timestamp and distinct values; construction occurs in the
 * per-invocation setup so the timed body isolates AtomicBTreePartition.addAll.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 1, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(1)
@Threads(4)
@State(Scope.Benchmark)
public class AtomicBTreePartitionFreshContentionBench
{
    static
    {
        DatabaseDescriptor.daemonInitialization();
        DatabaseDescriptor.setPartitionerUnsafe(ByteOrderedPartitioner.instance);
    }

    private static final AtomicLong NEXT_VERSION = new AtomicLong(1L << 20);

    private Fixture fixture;

    @Setup(Level.Trial)
    public void setup()
    {
        NEXT_VERSION.set(1L << 20);
        fixture = new Fixture(false);
        fixture.activatePessimisticLocking();
    }

    @Benchmark
    public BTreePartitionData freshHotPartitionUpdate(FreshUpdate update)
    {
        return fixture.add(update.update);
    }

    @State(Scope.Thread)
    public static class FreshUpdate
    {
        private PartitionUpdate update;

        @Setup(Level.Invocation)
        public void setup()
        {
            long version = NEXT_VERSION.getAndIncrement();
            update = Fixture.update((int) (version % Fixture.ROW_COUNT), version);
        }
    }

    public static final class Fixture
    {
        static
        {
            DatabaseDescriptor.daemonInitialization();
            DatabaseDescriptor.setPartitionerUnsafe(ByteOrderedPartitioner.instance);
        }

        static final int THREADS = 4;
        static final int ROW_COUNT = 64;
        private static final int COLUMN_COUNT = 64;
        private static final int VALUE_SIZE = 64;
        private static final int MAX_ACTIVATION_ROUNDS = 1024;
        private static final HeapPool POOL = new HeapPool(Long.MAX_VALUE, 1.0f, () -> ImmediateFuture.success(Boolean.TRUE));
        private static final OpOrder ORDER = new OpOrder();
        private static final TableMetadata METADATA = metadata();
        private static final DecoratedKey PARTITION_KEY = ByteOrderedPartitioner.instance.decorateKey(ByteBufferUtil.bytes(0));

        private final AtomicLong allocationAttempts;
        private final MemtableAllocator allocator;
        private final ThreadLocal<Cloner> cloner;
        private final AtomicBTreePartition partition;
        private final Clustering<?>[] clusterings;

        public Fixture(boolean countAllocations)
        {
            allocationAttempts = countAllocations ? new AtomicLong() : null;
            allocator = new HeapPool.Allocator(POOL)
            {
                @Override
                public Cloner cloner(OpOrder.Group opGroup)
                {
                    return new ByteBufferCloner()
                    {
                        @Override
                        public boolean isContextAwareCloningSupported()
                        {
                            return false;
                        }

                        @Override
                        public ByteBuffer allocate(int size)
                        {
                            if (allocationAttempts != null)
                                allocationAttempts.incrementAndGet();
                            return ByteBuffer.allocate(size);
                        }
                    };
                }
            };
            cloner = ThreadLocal.withInitial(() -> allocator.cloner(ORDER.getCurrent()));
            partition = new AtomicBTreePartition(TableMetadataRef.forOfflineTools(METADATA), PARTITION_KEY, allocator);
            clusterings = new Clustering<?>[ROW_COUNT];
            for (int row = 0 ; row < ROW_COUNT ; ++row)
                clusterings[row] = Clustering.make(Int32Type.instance.decompose(row));
        }

        public int activatePessimisticLocking()
        {
            AtomicBoolean locked = new AtomicBoolean();
            AtomicInteger rounds = new AtomicInteger();
            CyclicBarrier start = new CyclicBarrier(THREADS);
            CyclicBarrier callbacks = new CyclicBarrier(THREADS);
            CyclicBarrier finish = new CyclicBarrier(THREADS, () -> {
                int completedRounds = rounds.incrementAndGet();
                locked.set(partition.useLock());
                if (completedRounds % (ROW_COUNT / THREADS) == 0)
                    partition.unsafeSetHolder(BTreePartitionData.unsafeGetEmpty());
            });

            runWorkers(thread -> {
                for (int round = 0 ; round < MAX_ACTIVATION_ROUNDS && !locked.get() ; ++round)
                {
                    await(start);
                    long version = ((long) round + 1) * THREADS + thread;
                    partition.addAll(update((round * THREADS + thread) % ROW_COUNT, version),
                                     cloner.get(),
                                     ORDER.getCurrent(),
                                     new BarrierTransaction(callbacks));
                    await(finish);
                }
            });

            if (!partition.useLock())
                throw new IllegalStateException("Deterministic CAS conflicts did not activate pessimistic locking");

            return rounds.get();
        }

        public long runConcurrentFreshUpdates(int updatesPerWorker)
        {
            if (allocationAttempts == null)
                throw new IllegalStateException("Allocation counting is disabled");

            allocationAttempts.set(0);
            AtomicLong version = new AtomicLong(1L << 20);
            CyclicBarrier start = new CyclicBarrier(THREADS);
            CyclicBarrier finish = new CyclicBarrier(THREADS);
            runWorkers(thread -> {
                for (int update = 0 ; update < updatesPerWorker ; ++update)
                {
                    await(start);
                    long timestamp = version.getAndIncrement();
                    partition.addAll(Fixture.update((thread + update * THREADS) % ROW_COUNT, timestamp),
                                     cloner.get(),
                                     ORDER.getCurrent(),
                                     UpdateTransaction.NO_OP);
                    await(finish);
                }
            });
            return allocationAttempts.get();
        }

        public boolean hasEveryRow()
        {
            for (Clustering<?> clustering : clusterings)
            {
                if (partition.getRow(clustering) == null)
                    return false;
            }
            return true;
        }

        public long timestampAt(int row)
        {
            Row current = partition.getRow(clusterings[row]);
            return current.getCell(METADATA.getColumn(new ColumnIdentifier("v0", false))).timestamp();
        }

        public boolean usesPessimisticLocking()
        {
            return partition.useLock();
        }

        public boolean currentThreadHoldsPartitionMonitor()
        {
            return Thread.holdsLock(partition);
        }

        public void add(int row, long timestamp, UpdateTransaction indexer)
        {
            partition.addAll(update(row, timestamp), cloner.get(), ORDER.getCurrent(), indexer);
        }

        BTreePartitionData add(PartitionUpdate update)
        {
            partition.addAll(update, cloner.get(), ORDER.getCurrent(), UpdateTransaction.NO_OP);
            return partition.unsafeGetHolder();
        }

        static PartitionUpdate update(int row, long timestamp)
        {
            Row.Builder builder = BTreeRow.unsortedBuilder();
            builder.newRow(Clustering.make(Int32Type.instance.decompose(row)));
            for (int column = 0 ; column < COLUMN_COUNT ; ++column)
            {
                ColumnMetadata metadata = METADATA.getColumn(new ColumnIdentifier("v" + column, false));
                builder.addCell(new BufferCell(metadata,
                                               timestamp,
                                               Cell.NO_TTL,
                                               Cell.NO_DELETION_TIME,
                                               value(row, column, timestamp),
                                               null));
            }
            return PartitionUpdate.singleRowUpdate(METADATA, PARTITION_KEY, builder.build(), null);
        }

        private static TableMetadata metadata()
        {
            TableMetadata.Builder builder = TableMetadata.builder("bench", "atomic_btree_partition_fresh_contention")
                                                         .addPartitionKeyColumn("pk", Int32Type.instance)
                                                         .addClusteringColumn("ck", Int32Type.instance);
            for (int column = 0 ; column < COLUMN_COUNT ; ++column)
                builder.addRegularColumn("v" + column, BytesType.instance);
            return builder.partitioner(ByteOrderedPartitioner.instance).build();
        }

        private static ByteBuffer value(int row, int column, long timestamp)
        {
            byte[] bytes = new byte[VALUE_SIZE];
            for (int index = 0 ; index < bytes.length ; ++index)
                bytes[index] = (byte) (timestamp + row * 31L + column * 17L + index);
            return ByteBuffer.wrap(bytes);
        }

        private static void await(CyclicBarrier barrier)
        {
            try
            {
                barrier.await();
            }
            catch (InterruptedException e)
            {
                Thread.currentThread().interrupt();
                throw new AssertionError(e);
            }
            catch (BrokenBarrierException e)
            {
                throw new AssertionError(e);
            }
        }

        private static void runWorkers(Worker worker)
        {
            ExecutorService executor = Executors.newFixedThreadPool(THREADS);
            List<Future<?>> futures = new ArrayList<>(THREADS);
            try
            {
                for (int thread = 0 ; thread < THREADS ; ++thread)
                {
                    final int threadNumber = thread;
                    futures.add(executor.submit(() -> {
                        worker.run(threadNumber);
                        return null;
                    }));
                }
                for (Future<?> future : futures)
                    future.get(2, TimeUnit.MINUTES);
            }
            catch (TimeoutException e)
            {
                throw new AssertionError("Contended update workers did not finish", e);
            }
            catch (Exception e)
            {
                throw new AssertionError("Contended update worker failed", e);
            }
            finally
            {
                executor.shutdownNow();
                try
                {
                    executor.awaitTermination(1, TimeUnit.MINUTES);
                }
                catch (InterruptedException e)
                {
                    Thread.currentThread().interrupt();
                    throw new AssertionError(e);
                }
            }
        }

        @FunctionalInterface
        private interface Worker
        {
            void run(int thread) throws Exception;
        }

        private static final class BarrierTransaction implements UpdateTransaction
        {
            private final CyclicBarrier callbacks;
            private boolean awaited;

            private BarrierTransaction(CyclicBarrier callbacks)
            {
                this.callbacks = callbacks;
            }

            public void start()
            {
            }

            public void onPartitionDeletion(DeletionTime deletionTime)
            {
            }

            public void onRangeTombstone(RangeTombstone rangeTombstone)
            {
            }

            public void onInserted(Row row)
            {
                awaitOnce();
            }

            public void onUpdated(Row existing, Row updated)
            {
                awaitOnce();
            }

            public void commit()
            {
            }

            private void awaitOnce()
            {
                if (!awaited)
                {
                    awaited = true;
                    await(callbacks);
                }
            }
        }
    }
}
