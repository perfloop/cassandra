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
package org.apache.cassandra.db.partitions;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.dht.ByteOrderedPartitioner;
import org.apache.cassandra.index.transactions.UpdateTransaction;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.utils.concurrent.ImmediateFuture;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.memory.HeapPool;
import org.apache.cassandra.utils.memory.MemtableAllocator;

import static org.junit.Assert.assertEquals;

public class BTreePartitionUpdaterRangeTombstoneTest
{
    private static final HeapPool pool = new HeapPool(Long.MAX_VALUE, 1.0f, () -> ImmediateFuture.success(Boolean.TRUE));

    private static TableMetadata metadata;

    @BeforeClass
    public static void beforeClass()
    {
        DatabaseDescriptor.daemonInitialization();
        DatabaseDescriptor.setPartitionerUnsafe(ByteOrderedPartitioner.instance);
        metadata = TableMetadata.builder("ks", "range_tombstone_memtable")
                                .addPartitionKeyColumn("pk", Int32Type.instance)
                                .addClusteringColumn("ck", Int32Type.instance)
                                .partitioner(ByteOrderedPartitioner.instance)
                                .build();
    }

    @Test
    public void sequentialAppendKeepsThePriorHolderUnchanged()
    {
        try (Fixture fixture = new Fixture())
        {
            apply(fixture, 0, 1, 1);
            BTreePartitionData before = fixture.partition.unsafeGetHolder();

            apply(fixture, 2, 3, 2);

            assertEquals(1, before.deletionInfo.rangeCount());
            assertEquals(2, fixture.partition.unsafeGetHolder().deletionInfo.rangeCount());
        }
    }

    @Test
    public void overlappingMergeKeepsThePriorHolderUnchanged()
    {
        try (Fixture fixture = new Fixture())
        {
            apply(fixture, 0, 10, 1);
            BTreePartitionData before = fixture.partition.unsafeGetHolder();

            apply(fixture, 5, 15, 2);

            assertEquals(1, before.deletionInfo.rangeCount());
            assertEquals(1, before.deletionInfo.rangeCovering(clustering(7)).deletionTime().markedForDeleteAt());
            assertEquals(2, fixture.partition.unsafeGetHolder().deletionInfo.rangeCount());
            assertEquals(2, fixture.partition.unsafeGetHolder().deletionInfo.rangeCovering(clustering(7)).deletionTime().markedForDeleteAt());
        }
    }

    @Test
    public void rangeTombstoneAccountingTracksEachMerge()
    {
        try (Fixture fixture = new Fixture())
        {
            apply(fixture, 0, 1, 1);
            for (int i = 1; i < 64; i++)
            {
                BTreePartitionData before = fixture.partition.unsafeGetHolder();
                long beforeAccounting = fixture.allocator.onHeap().owns();
                long beforeDeletionInfo = before.deletionInfo.unsharedHeapSize();

                apply(fixture, 2 * i, 2 * i + 1, i + 1);

                BTreePartitionData after = fixture.partition.unsafeGetHolder();
                assertEquals(after.deletionInfo.unsharedHeapSize() - beforeDeletionInfo,
                             fixture.allocator.onHeap().owns() - beforeAccounting);
            }
        }
    }

    private static void apply(Fixture fixture, int start, int end, long timestamp)
    {
        PartitionUpdate update = new RowUpdateBuilder(metadata, 1, timestamp, fixture.key)
                                 .addRangeTombstone(start, end)
                                 .buildUpdate();
        try (OpOrder.Group writeOp = fixture.order.start())
        {
            fixture.partition.addAll(update, fixture.allocator.cloner(writeOp), writeOp, UpdateTransaction.NO_OP);
        }
    }

    private static Clustering<?> clustering(int value)
    {
        return Clustering.make(Int32Type.instance.decompose(value));
    }

    private static final class Fixture implements AutoCloseable
    {
        private final MemtableAllocator allocator = pool.newAllocator("range_tombstone_memtable");
        private final OpOrder order = new OpOrder();
        private final AtomicBTreePartition partition = new AtomicBTreePartition(TableMetadataRef.forOfflineTools(metadata),
                                                                                  metadata.partitioner.decorateKey(Int32Type.instance.decompose(0)),
                                                                                  allocator);
        private final org.apache.cassandra.db.DecoratedKey key = metadata.partitioner.decorateKey(Int32Type.instance.decompose(0));

        @Override
        public void close()
        {
            allocator.setDiscarding();
            allocator.setDiscarded();
        }
    }
}
