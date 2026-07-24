/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.cassandra.db.partitions;

import java.nio.ByteBuffer;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.BufferClusteringBound;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DeletionInfo;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.MutableDeletionInfo;
import org.apache.cassandra.db.RangeTombstone;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.Slice;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.dht.ByteOrderedPartitioner;
import org.apache.cassandra.index.transactions.UpdateTransaction;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.utils.concurrent.ImmediateFuture;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.memory.HeapCloner;
import org.apache.cassandra.utils.memory.HeapPool;

/**
 * Shared, deterministic fixtures for deletion-info transition tests and JMH benchmarks.
 *
 * <p>The initial prefix is installed before a measured transition. Ranges deliberately
 * leave gaps so the fixture can exercise exact-bound replacement, an insertion in the
 * middle of the sorted order, overlap, and covered/uncovered reads.</p>
 */
public final class DeletionInfoTransitionFixture
{
    private static final int RANGE_STRIDE = 16;
    private static final int RANGE_WIDTH = 7;
    private static final long PREFIX_TIMESTAMP = 10;
    private static final long TRANSITION_TIMESTAMP = 100;
    private static final long LOCAL_DELETION_TIME = 1;
    private static final int UPDATE_VARIANTS = 16;

    private static final HeapPool POOL = new HeapPool(Long.MAX_VALUE, 1.0f, () -> ImmediateFuture.success(Boolean.TRUE));
    private static final OpOrder WRITE_ORDER = new OpOrder();

    private DeletionInfoTransitionFixture()
    {
    }

    public enum ExactBoundSeparation
    {
        ADJACENT,
        OPPOSITE
    }

    public static final class State
    {
        public final int prefixRangeCount;
        public final TableMetadata metadata;
        public final DecoratedKey key;
        public final AtomicBTreePartition partition;

        private final BTreePartitionData prefix;
        private final PartitionUpdate[] adjacentExactBounds;
        private final PartitionUpdate[] oppositeExactBounds;
        private final PartitionUpdate[] overlapping;
        private final PartitionUpdate[] outOfOrder;
        private final PartitionUpdate[] partitionDeletes;
        private final Slice[] slices;
        private final Clustering<?>[] probes;

        public State(int prefixRangeCount)
        {
            if (prefixRangeCount < 4)
                throw new IllegalArgumentException("prefix needs at least four ranges");

            this.prefixRangeCount = prefixRangeCount;
            this.metadata = metadata();
            this.key = key();
            this.partition = new AtomicBTreePartition(TableMetadataRef.forOfflineTools(metadata), key, POOL.newAllocator("deletion-info-transition"));

            apply(prefixUpdate());
            this.prefix = partition.unsafeGetHolder();
            this.adjacentExactBounds = updates(Transition.ADJACENT_EXACT);
            this.oppositeExactBounds = updates(Transition.OPPOSITE_EXACT);
            this.overlapping = updates(Transition.OVERLAP);
            this.outOfOrder = updates(Transition.OUT_OF_ORDER);
            this.partitionDeletes = updates(Transition.PARTITION_DELETE);
            this.slices = buildSlices();
            this.probes = buildProbes();
        }

        public void reset()
        {
            partition.unsafeSetHolder(prefix);
        }

        public void apply(PartitionUpdate update)
        {
            partition.addAll(update, HeapCloner.instance, WRITE_ORDER.getCurrent(), UpdateTransaction.NO_OP);
        }

        public DeletionInfo prefixDeletionInfo()
        {
            return prefix.deletionInfo;
        }

        /** Builds an oracle through Cassandra's canonical mutable reconciler. */
        public MutableDeletionInfo canonicalPrefix()
        {
            MutableDeletionInfo result = MutableDeletionInfo.live();
            for (int i = 0; i < prefixRangeCount; i++)
                result.add(rangeForIndex(i, PREFIX_TIMESTAMP), metadata.comparator);
            return result;
        }

        public PartitionUpdate exactBoundUpdate(ExactBoundSeparation separation, int variant)
        {
            return (separation == ExactBoundSeparation.ADJACENT ? adjacentExactBounds : oppositeExactBounds)[variant & (UPDATE_VARIANTS - 1)];
        }

        public PartitionUpdate overlapUpdate(int variant)
        {
            return overlapping[variant & (UPDATE_VARIANTS - 1)];
        }

        public PartitionUpdate outOfOrderUpdate(int variant)
        {
            return outOfOrder[variant & (UPDATE_VARIANTS - 1)];
        }

        public PartitionUpdate partitionDeleteUpdate(int variant)
        {
            return partitionDeletes[variant & (UPDATE_VARIANTS - 1)];
        }

        public Slice[] slices()
        {
            return slices.clone();
        }

        public Clustering<?>[] probes()
        {
            return probes.clone();
        }

        private PartitionUpdate prefixUpdate()
        {
            PartitionUpdate.Builder builder = new PartitionUpdate.Builder(metadata, key, RegularAndStaticColumns.NONE, prefixRangeCount);
            for (int i = 0; i < prefixRangeCount; i++)
                builder.add(rangeForIndex(i, PREFIX_TIMESTAMP));
            return builder.build();
        }

        private PartitionUpdate[] updates(Transition transition)
        {
            PartitionUpdate[] updates = new PartitionUpdate[UPDATE_VARIANTS];
            for (int i = 0; i < updates.length; i++)
            {
                long timestamp = TRANSITION_TIMESTAMP + i;
                switch (transition)
                {
                    case ADJACENT_EXACT:
                        updates[i] = rangesUpdate(timestamp, rangeForIndex(middle(), timestamp), rangeForIndex(middle() + 1, timestamp));
                        break;
                    case OPPOSITE_EXACT:
                        updates[i] = rangesUpdate(timestamp, rangeForIndex(0, timestamp), rangeForIndex(prefixRangeCount - 1, timestamp));
                        break;
                    case OVERLAP:
                        updates[i] = rangesUpdate(timestamp, range(rangeStart(middle()), rangeEnd(middle() + 1), timestamp));
                        break;
                    case OUT_OF_ORDER:
                        updates[i] = rangesUpdate(timestamp, range(rangeEnd(middle()) + 1, rangeStart(middle() + 1) - 1, timestamp));
                        break;
                    case PARTITION_DELETE:
                        updates[i] = PartitionUpdate.fullPartitionDelete(metadata, key, timestamp, LOCAL_DELETION_TIME + i);
                        break;
                    default:
                        throw new AssertionError(transition);
                }
            }
            return updates;
        }

        private PartitionUpdate rangesUpdate(long timestamp, RangeTombstone... ranges)
        {
            PartitionUpdate.Builder builder = new PartitionUpdate.Builder(metadata, key, RegularAndStaticColumns.NONE, ranges.length);
            for (RangeTombstone range : ranges)
            {
                Slice slice = range.deletedSlice();
                builder.add(new RangeTombstone(slice, DeletionTime.build(timestamp, LOCAL_DELETION_TIME)));
            }
            return builder.build();
        }

        private Slice[] buildSlices()
        {
            return new Slice[]
            {
                slice(rangeStart(0), rangeEnd(2)),
                slice(rangeStart(middle()), rangeEnd(middle() + 2)),
                slice(rangeStart(prefixRangeCount - 3), rangeEnd(prefixRangeCount - 1)),
                slice(rangeEnd(middle()) + 1, rangeStart(middle() + 1) - 1)
            };
        }

        private Clustering<?>[] buildProbes()
        {
            return new Clustering[]
            {
                clustering(rangeStart(0)),
                clustering(rangeEnd(0) + 1),
                clustering(rangeStart(middle()) + 1),
                clustering(rangeEnd(middle()) + 1),
                clustering(rangeStart(prefixRangeCount - 1)),
                clustering(rangeEnd(prefixRangeCount - 1) + 1)
            };
        }

        private int middle()
        {
            return prefixRangeCount / 2 - 1;
        }

        private RangeTombstone rangeForIndex(int index, long timestamp)
        {
            return range(rangeStart(index), rangeEnd(index), timestamp);
        }

        private int rangeStart(int index)
        {
            return index * RANGE_STRIDE;
        }

        private int rangeEnd(int index)
        {
            return rangeStart(index) + RANGE_WIDTH;
        }
    }

    public static void initialize()
    {
        DatabaseDescriptor.daemonInitialization();
        DatabaseDescriptor.setPartitionerUnsafe(ByteOrderedPartitioner.instance);
    }

    public static long fingerprint(RangeTombstone range)
    {
        return 31L * range.deletionTime().markedForDeleteAt()
             + range.deletedSlice().start().hashCode()
             + range.deletedSlice().end().hashCode();
    }

    private static TableMetadata metadata()
    {
        return TableMetadata.builder("deletion_info_transition", "ranges")
                            .addPartitionKeyColumn("pk", Int32Type.instance)
                            .addClusteringColumn("ck", Int32Type.instance)
                            .partitioner(ByteOrderedPartitioner.instance)
                            .build();
    }

    private static DecoratedKey key()
    {
        ByteBuffer key = Int32Type.instance.decompose(0);
        return new org.apache.cassandra.db.BufferDecoratedKey(ByteOrderedPartitioner.instance.getToken(key), key);
    }

    private static RangeTombstone range(int start, int end, long timestamp)
    {
        return new RangeTombstone(slice(start, end), DeletionTime.build(timestamp, LOCAL_DELETION_TIME));
    }

    private static Slice slice(int start, int end)
    {
        return Slice.make(BufferClusteringBound.inclusiveStartOf(Int32Type.instance.decompose(start)),
                          BufferClusteringBound.inclusiveEndOf(Int32Type.instance.decompose(end)));
    }

    private static Clustering<?> clustering(int value)
    {
        return Clustering.make(Int32Type.instance.decompose(value));
    }

    private enum Transition
    {
        ADJACENT_EXACT,
        OPPOSITE_EXACT,
        OVERLAP,
        OUT_OF_ORDER,
        PARTITION_DELETE
    }
}
