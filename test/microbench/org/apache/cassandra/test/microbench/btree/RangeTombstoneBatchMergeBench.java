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

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

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
@State(Scope.Thread)
public class RangeTombstoneBatchMergeBench
{
    private static final int EXISTING_RANGE_COUNT = 4096;
    private static final OpOrder NO_ORDER = new OpOrder();
    private static final HeapPool POOL = new HeapPool(Long.MAX_VALUE, 1.0f, () -> ImmediateFuture.success(Boolean.TRUE));

    @Param({ "64", "4096" })
    int batchRangeCount;

    private AtomicBTreePartition partition;
    private BTreePartitionData existingHolder;
    private PartitionUpdate batch;
    private Cloner cloner;
    private OpOrder.Group writeOp;

    @Setup(Level.Trial)
    public void setupTrial()
    {
        DatabaseDescriptor.daemonInitialization();

        TableMetadata metadata = TableMetadata.builder("bench", "range_tombstone_batch_merge")
                                              .addPartitionKeyColumn("pk", Int32Type.instance)
                                              .addClusteringColumn("ck", Int32Type.instance)
                                              .build();
        DecoratedKey key = metadata.partitioner.decorateKey(ByteBufferUtil.bytes(0));
        MemtableAllocator allocator = new HeapPool.Allocator(POOL);
        writeOp = NO_ORDER.getCurrent();
        cloner = allocator.cloner(writeOp);
        partition = new AtomicBTreePartition(TableMetadataRef.forOfflineTools(metadata), key, allocator);

        for (int i = 0; i < EXISTING_RANGE_COUNT; i++)
            partition.addAll(update(metadata, key, i, 1), cloner, writeOp, UpdateTransaction.NO_OP);
        existingHolder = partition.unsafeGetHolder();
        batch = update(metadata, key, EXISTING_RANGE_COUNT, batchRangeCount);
    }

    @Setup(Level.Invocation)
    public void resetPartition()
    {
        partition.unsafeSetHolder(existingHolder);
    }

    @Benchmark
    public int mergeOrderedRangeTombstoneBatch()
    {
        partition.addAll(batch, cloner, writeOp, UpdateTransaction.NO_OP);

        int expectedRangeCount = EXISTING_RANGE_COUNT + batchRangeCount;
        int mergedRangeCount = partition.deletionInfo().rangeCount();
        if (mergedRangeCount != expectedRangeCount)
            throw new AssertionError("Expected " + expectedRangeCount + " ranges but found " + mergedRangeCount);
        return mergedRangeCount;
    }

    private static PartitionUpdate update(TableMetadata metadata, DecoratedKey key, int firstIndex, int count)
    {
        MutableDeletionInfo deletionInfo = MutableDeletionInfo.live();
        for (int i = firstIndex; i < firstIndex + count; i++)
        {
            deletionInfo.add(new RangeTombstone(Slice.make(BufferClusteringBound.inclusiveStartOf(ByteBufferUtil.bytes(i * 2)),
                                                            BufferClusteringBound.inclusiveEndOf(ByteBufferUtil.bytes(i * 2 + 1))),
                                                DeletionTime.build(i + 1, 1)),
                             metadata.comparator);
        }

        BTreePartitionData holder = BTreePartitionData.unsafeConstruct(RegularAndStaticColumns.NONE,
                                                                        BTree.empty(),
                                                                        deletionInfo,
                                                                        Rows.EMPTY_STATIC_ROW,
                                                                        EncodingStats.NO_STATS);
        return PartitionUpdate.unsafeConstruct(metadata, key, holder, deletionInfo, false);
    }
}
