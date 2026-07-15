/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
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
import java.util.concurrent.atomic.AtomicLong;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.BufferClusteringBound;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.MutableDeletionInfo;
import org.apache.cassandra.db.RangeTombstone;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.Slice;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.partitions.AtomicBTreePartition;
import org.apache.cassandra.db.partitions.BTreePartitionData;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.index.transactions.UpdateTransaction;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.btree.BTree;
import org.apache.cassandra.utils.concurrent.ImmediateFuture;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.memory.Cloner;
import org.apache.cassandra.utils.memory.HeapPool;
import org.apache.cassandra.utils.memory.MemtableAllocator;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class ConcurrentRangeTombstoneMergeBench
{
    private static final int BATCH_COUNT = 16;
    private static final int INDEX_MASK = (1 << 20) - 1;
    private static final long COMPLETED_INCREMENT = 1L << 20;
    private static final OpOrder NO_ORDER = new OpOrder();
    private static final HeapPool POOL = new HeapPool(Long.MAX_VALUE, 1.0f, () -> ImmediateFuture.success(Boolean.TRUE));

    @Benchmark
    @Threads(4)
    public int mergeRangeTombstone(ConcurrentState state)
    {
        return state.mergeOne();
    }

    @State(Scope.Benchmark)
    public static class ConcurrentState
    {
        @Param({ "256" })
        int rangesPerPartition;

        private final AtomicLong batchState = new AtomicLong();
        private Batch[] batches;

        @Setup(Level.Trial)
        public void setupTrial()
        {
            DatabaseDescriptor.daemonInitialization();

            TableMetadata metadata = TableMetadata.builder("bench", "concurrent_range_tombstone_merge")
                                                  .addPartitionKeyColumn("pk", Int32Type.instance)
                                                  .addClusteringColumn("ck", Int32Type.instance)
                                                  .build();
            DecoratedKey key = metadata.partitioner.decorateKey(ByteBufferUtil.bytes(0));
            PartitionUpdate[] updates = new PartitionUpdate[rangesPerPartition];
            for (int i = 0; i < rangesPerPartition; i++)
                updates[i] = update(metadata, key, i);

            TableMetadataRef metadataRef = TableMetadataRef.forOfflineTools(metadata);
            batches = new Batch[BATCH_COUNT];
            for (int i = 0; i < BATCH_COUNT; i++)
                batches[i] = new Batch(metadataRef, key, updates);
            batchState.set(0);
        }

        int mergeOne()
        {
            while (true)
            {
                long current = batchState.get();
                int generation = (int) (current >>> 32);
                int index = (int) current;
                int merged = batches[index].mergeOne(generation);
                if (merged >= 0)
                    return merged;

                int nextIndex = index + 1;
                int nextGeneration = generation;
                if (nextIndex == batches.length)
                {
                    nextIndex = 0;
                    nextGeneration++;
                }
                long next = ((long) nextGeneration << 32) | nextIndex;
                batchState.compareAndSet(current, next);
            }
        }
    }

    private static class Batch
    {
        private final AtomicBTreePartition partition;
        private final PartitionUpdate[] updates;
        private final Cloner cloner;
        private final AtomicLong state = new AtomicLong();

        private Batch(TableMetadataRef metadata, DecoratedKey key, PartitionUpdate[] updates)
        {
            MemtableAllocator allocator = new HeapPool.Allocator(POOL);
            partition = new AtomicBTreePartition(metadata, key, allocator);
            cloner = allocator.cloner(NO_ORDER.getCurrent());
            this.updates = updates;
        }

        private int mergeOne(int expectedGeneration)
        {
            int updateIndex;
            while (true)
            {
                long current = state.get();
                int currentGeneration = (int) (current >>> 40);
                if (currentGeneration != expectedGeneration)
                {
                    if (expectedGeneration < currentGeneration)
                        return -1;
                    Thread.yield();
                    continue;
                }

                updateIndex = (int) current & INDEX_MASK;
                if (updateIndex == updates.length)
                    return -1;
                if (state.compareAndSet(current, current + 1))
                    break;
            }

            try
            {
                partition.addAll(updates[updateIndex], cloner, NO_ORDER.getCurrent(), UpdateTransaction.NO_OP);
                return updateIndex;
            }
            finally
            {
                long completed = state.addAndGet(COMPLETED_INCREMENT);
                long expectedCompleted = ((long) expectedGeneration << 40)
                                         | ((long) updates.length << 20)
                                         | updates.length;
                if (completed == expectedCompleted)
                {
                    int mergedRangeCount = partition.deletionInfo().rangeCount();
                    if (mergedRangeCount != updates.length)
                        throw new AssertionError("Expected " + updates.length + " ranges but found " + mergedRangeCount);
                    partition.unsafeSetHolder(BTreePartitionData.unsafeGetEmpty());
                    state.set(((long) (expectedGeneration + 1)) << 40);
                }
            }
        }
    }

    private static PartitionUpdate update(TableMetadata metadata, DecoratedKey key, int index)
    {
        MutableDeletionInfo deletionInfo = MutableDeletionInfo.live();
        deletionInfo.add(new RangeTombstone(Slice.make(BufferClusteringBound.inclusiveStartOf(ByteBufferUtil.bytes(index * 2)),
                                                        BufferClusteringBound.inclusiveEndOf(ByteBufferUtil.bytes(index * 2 + 1))),
                                            DeletionTime.build(index + 1, 1)),
                         metadata.comparator);

        BTreePartitionData holder = BTreePartitionData.unsafeConstruct(RegularAndStaticColumns.NONE,
                                                                        BTree.empty(),
                                                                        deletionInfo,
                                                                        Rows.EMPTY_STATIC_ROW,
                                                                        EncodingStats.NO_STATS);
        return PartitionUpdate.unsafeConstruct(metadata, key, holder, deletionInfo, false);
    }
}
