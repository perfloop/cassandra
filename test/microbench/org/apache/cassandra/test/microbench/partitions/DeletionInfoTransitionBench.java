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

package org.apache.cassandra.test.microbench.partitions;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.infra.Blackhole;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.BufferClusteringBound;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DeletionInfo;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.MutableDeletionInfo;
import org.apache.cassandra.db.RangeTombstone;
import org.apache.cassandra.db.Slice;
import org.apache.cassandra.db.partitions.AtomicBTreePartition;
import org.apache.cassandra.db.partitions.BTreePartitionData;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.dht.ByteOrderedPartitioner;
import org.apache.cassandra.index.transactions.UpdateTransaction;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.concurrent.ImmediateFuture;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.memory.Cloner;
import org.apache.cassandra.utils.memory.HeapPool;
import org.apache.cassandra.utils.memory.MemtableAllocator;

/**
 * Measures immutable deletion-info transitions after a large ordered tombstone prefix.
 *
 * Prefix construction and partition reset happen in JMH setup so each measured invocation
 * starts from the same published version.  The result is consumed through JMH's blackhole.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(1)
@Threads(1)
public class DeletionInfoTransitionBench
{
    private static final int RANGE_STRIDE = 10;
    private static final int LOOKUPS_PER_INVOCATION = 64;
    private static final HeapPool POOL = new HeapPool(Long.MAX_VALUE, 1.0f, () -> ImmediateFuture.success(Boolean.TRUE));
    private static final TableMetadata METADATA;
    private static final org.apache.cassandra.db.DecoratedKey KEY;

    static
    {
        DatabaseDescriptor.clientInitialization(false);
        DatabaseDescriptor.setPartitionerUnsafe(ByteOrderedPartitioner.instance);
        METADATA = TableMetadata.builder("microbench", "deletion_info")
                                .addPartitionKeyColumn("pk", org.apache.cassandra.db.marshal.Int32Type.instance)
                                .addClusteringColumn("ck", org.apache.cassandra.db.marshal.Int32Type.instance)
                                .partitioner(ByteOrderedPartitioner.instance)
                                .build();
        KEY = METADATA.partitioner.decorateKey(ByteBufferUtil.bytes(0));
    }

    @State(Scope.Thread)
    public static class WriteState
    {
        @Param({"128", "4096"})
        public int prefixRanges;

        @Param({"PARTITION_DELETE", "OVERLAP", "OUT_OF_ORDER", "TWO_RANGES"})
        public String transition;

        private AtomicBTreePartition partition;
        private BTreePartitionData publishedPrefix;
        private PartitionUpdate update;
        private OpOrder.Group writeOp;
        private Cloner cloner;

        @Setup(Level.Trial)
        public void setup()
        {
            MemtableAllocator allocator = POOL.newAllocator("DeletionInfoTransitionBench");
            OpOrder order = new OpOrder();
            writeOp = order.start();
            cloner = allocator.cloner(writeOp);
            partition = new AtomicBTreePartition(TableMetadataRef.forOfflineTools(METADATA), KEY, allocator);
            partition.addAll(update(prefix(prefixRanges)), cloner, writeOp, UpdateTransaction.NO_OP);
            publishedPrefix = partition.unsafeGetHolder();
            update = transition(prefixRanges, transition);
        }

        @Setup(Level.Invocation)
        public void reset()
        {
            partition.unsafeSetHolder(publishedPrefix);
        }

        @TearDown(Level.Trial)
        public void tearDown()
        {
            writeOp.close();
        }
    }

    @State(Scope.Thread)
    public static class SliceState
    {
        @Param({"128", "4096"})
        public int prefixRanges;

        @Param({"EARLY", "MIDDLE", "LATE", "MISS"})
        public String position;

        @Param({"false", "true"})
        public boolean reversed;

        private DeletionInfo deletionInfo;
        private Slice slice;

        @Setup(Level.Trial)
        public void setup()
        {
            AtomicBTreePartition partition = partitionWithPrefix(prefixRanges);
            deletionInfo = partition.deletionInfo();
            slice = slice(prefixRanges, position);
        }
    }

    @State(Scope.Thread)
    public static class LookupState
    {
        @Param({"128", "4096"})
        public int prefixRanges;

        @Param({"COVERED", "UNCOVERED"})
        public String lookup;

        private DeletionInfo deletionInfo;
        private Clustering<?>[] clusterings;

        @Setup(Level.Trial)
        public void setup()
        {
            deletionInfo = partitionWithPrefix(prefixRanges).deletionInfo();
            clusterings = new Clustering<?>[LOOKUPS_PER_INVOCATION];
            int start = prefixRanges / 4;
            for (int i = 0; i < clusterings.length; i++)
            {
                int range = (start + i) % prefixRanges;
                int value = range * RANGE_STRIDE + ("COVERED".equals(lookup) ? 2 : 6);
                clusterings[i] = Clustering.make(ByteBufferUtil.bytes(value));
            }
        }
    }

    @Benchmark
    public void writeTransition(WriteState state, Blackhole blackhole)
    {
        state.partition.addAll(state.update, state.cloner, state.writeOp, UpdateTransaction.NO_OP);
        DeletionInfo result = state.partition.deletionInfo();
        blackhole.consume(result.getPartitionDeletion());
        blackhole.consume(result.rangeCount());
    }

    @Benchmark
    public void rangeSlice(SliceState state, Blackhole blackhole)
    {
        Iterator<RangeTombstone> iterator = state.deletionInfo.rangeIterator(state.slice, state.reversed);
        long timestampSum = 0;
        int count = 0;
        while (iterator.hasNext())
        {
            RangeTombstone tombstone = iterator.next();
            timestampSum += tombstone.deletionTime().markedForDeleteAt();
            count++;
        }
        blackhole.consume(timestampSum);
        blackhole.consume(count);
    }

    @Benchmark
    @OperationsPerInvocation(LOOKUPS_PER_INVOCATION)
    public void rangeCovering(LookupState state, Blackhole blackhole)
    {
        long timestampSum = 0;
        for (Clustering<?> clustering : state.clusterings)
        {
            RangeTombstone tombstone = state.deletionInfo.rangeCovering(clustering);
            if (tombstone != null)
                timestampSum += tombstone.deletionTime().markedForDeleteAt();
        }
        blackhole.consume(timestampSum);
    }

    private static AtomicBTreePartition partitionWithPrefix(int rangeCount)
    {
        MemtableAllocator allocator = POOL.newAllocator("DeletionInfoTransitionBench");
        OpOrder order = new OpOrder();
        AtomicBTreePartition partition = new AtomicBTreePartition(TableMetadataRef.forOfflineTools(METADATA), KEY, allocator);
        try (OpOrder.Group writeOp = order.start())
        {
            partition.addAll(update(prefix(rangeCount)), allocator.cloner(writeOp), writeOp, UpdateTransaction.NO_OP);
        }
        return partition;
    }

    private static MutableDeletionInfo prefix(int rangeCount)
    {
        MutableDeletionInfo deletionInfo = new MutableDeletionInfo(DeletionTime.LIVE);
        for (int i = 0; i < rangeCount; i++)
            deletionInfo.add(range(i * RANGE_STRIDE, i * RANGE_STRIDE + 4, 1), METADATA.comparator);
        return deletionInfo;
    }

    private static PartitionUpdate transition(int prefixRanges, String transition)
    {
        MutableDeletionInfo deletionInfo = new MutableDeletionInfo(DeletionTime.LIVE);
        int middle = prefixRanges / 2;
        switch (transition)
        {
            case "PARTITION_DELETE":
                deletionInfo.add(DeletionTime.build(2, 2));
                break;
            case "OVERLAP":
                deletionInfo.add(range(middle * RANGE_STRIDE + 2, middle * RANGE_STRIDE + 18, 2), METADATA.comparator);
                break;
            case "OUT_OF_ORDER":
                deletionInfo.add(range(RANGE_STRIDE / 2, RANGE_STRIDE + 2, 2), METADATA.comparator);
                break;
            case "TWO_RANGES":
                deletionInfo.add(range(middle * RANGE_STRIDE + 2, middle * RANGE_STRIDE + 8, 2), METADATA.comparator);
                deletionInfo.add(range((middle + 2) * RANGE_STRIDE + 2, (middle + 2) * RANGE_STRIDE + 8, 2), METADATA.comparator);
                break;
            default:
                throw new IllegalArgumentException("Unknown transition: " + transition);
        }
        return update(deletionInfo);
    }

    private static PartitionUpdate update(MutableDeletionInfo deletionInfo)
    {
        return PartitionUpdate.unsafeConstruct(METADATA,
                                               KEY,
                                               BTreePartitionData.unsafeGetEmpty(),
                                               deletionInfo,
                                               false);
    }

    private static RangeTombstone range(int start, int end, long timestamp)
    {
        return new RangeTombstone(slice(start, true, end, true), DeletionTime.build(timestamp, 1));
    }

    private static Slice slice(int rangeCount, String position)
    {
        int range;
        switch (position)
        {
            case "EARLY":
                range = 0;
                break;
            case "MIDDLE":
                range = rangeCount / 2;
                break;
            case "LATE":
                range = rangeCount - 1;
                break;
            case "MISS":
                range = rangeCount / 2;
                return slice(range * RANGE_STRIDE + 5, true, (range + 1) * RANGE_STRIDE - 1, true);
            default:
                throw new IllegalArgumentException("Unknown slice position: " + position);
        }
        return slice(range * RANGE_STRIDE + 1, true, range * RANGE_STRIDE + 3, true);
    }

    private static Slice slice(int start, boolean startInclusive, int end, boolean endInclusive)
    {
        ByteBuffer startBuffer = ByteBufferUtil.bytes(start);
        ByteBuffer endBuffer = ByteBufferUtil.bytes(end);
        return Slice.make(startInclusive ? BufferClusteringBound.inclusiveStartOf(startBuffer)
                                         : BufferClusteringBound.exclusiveStartOf(startBuffer),
                          endInclusive ? BufferClusteringBound.inclusiveEndOf(endBuffer)
                                       : BufferClusteringBound.exclusiveEndOf(endBuffer));
    }
}
