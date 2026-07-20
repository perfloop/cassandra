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
package org.apache.cassandra.db;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.partitions.AtomicBTreePartition;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.index.transactions.UpdateTransaction;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.concurrent.ImmediateFuture;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.memory.HeapCloner;
import org.apache.cassandra.utils.memory.HeapPool;
import org.apache.cassandra.utils.memory.MemtableAllocator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class RangeTombstoneListCopyIsolationTest
{
    private static final ClusteringComparator COMPARATOR = new ClusteringComparator(Int32Type.instance);

    @BeforeClass
    public static void initializeDatabaseDescriptor()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Test
    public void copiesStayIsolatedAcrossAddIncludingPrefixInsertion()
    {
        assertCopiesStayIsolated(list -> {
            list.add(tombstone(90, 99, 100, 1));
            list.add(tombstone(5, 15, 101, 2));
        });
    }

    @Test
    public void copiesStayIsolatedAcrossAddAll()
    {
        assertCopiesStayIsolated(list -> {
            RangeTombstoneList additions = new RangeTombstoneList(COMPARATOR, 2);
            additions.add(tombstone(5, 8, 100, 1));
            additions.add(tombstone(90, 99, 101, 2));
            list.addAll(additions);
        });
    }

    @Test
    public void copiesStayIsolatedAcrossTimestampUpdate()
    {
        assertCopiesStayIsolated(list -> list.updateAllTimestamp(1_000));
    }

    @Test
    public void copiesStayIsolatedAcrossTimestampAndDeletionTimeUpdate()
    {
        assertCopiesStayIsolated(list -> list.updateAllTimestampAndLocalDeletionTime(1_000, 2_000));
    }

    @Test
    public void copyDoesNotRetainArbitraryExcessCapacity()
    {
        RangeTombstoneList padded = new RangeTombstoneList(COMPARATOR, 1_024);
        padded.add(tombstone(10, 19, 10, 1));
        padded.add(tombstone(30, 39, 20, 2));
        padded.add(tombstone(50, 59, 30, 3));

        RangeTombstoneList compact = new RangeTombstoneList(COMPARATOR, padded.size());
        compact.addAll(padded);
        RangeTombstoneList copy = padded.copy();

        assertEquals(snapshot(compact), snapshot(copy));
        assertEquals(compact.unsharedHeapSize(), copy.unsharedHeapSize());
        assertTrue(copy.unsharedHeapSize() < padded.unsharedHeapSize());
    }

    @Test
    public void copyAndMutationRetainMemoryConsistently()
    {
        RangeTombstoneList source = new RangeTombstoneList(COMPARATOR, 4);
        source.add(tombstone(10, 19, 10, 1));
        source.add(tombstone(30, 39, 20, 2));
        source.add(tombstone(50, 59, 30, 3));
        source.add(tombstone(70, 79, 40, 4));

        long sourceHeapSize = source.unsharedHeapSize();
        RangeTombstoneList copy = source.copy();
        copy.add(tombstone(90, 99, 50, 5));

        assertEquals(4, source.size());
        assertEquals(5, copy.size());
        assertEquals(sourceHeapSize, source.unsharedHeapSize());
        assertTrue(copy.unsharedHeapSize() > sourceHeapSize);
    }

    @Test
    public void concurrentAtomicBTreePartitionPublicationPreservesRangeUpdates() throws Exception
    {
        TableMetadata metadata = TableMetadata.builder("copy_isolation", "ranges")
                                              .addPartitionKeyColumn("pk", Int32Type.instance)
                                              .addClusteringColumn("ck", Int32Type.instance)
                                              .build();
        DecoratedKey key = metadata.partitioner.decorateKey(ByteBufferUtil.bytes(0));
        HeapPool pool = new HeapPool(Long.MAX_VALUE, 1.0f, () -> ImmediateFuture.success(Boolean.TRUE));
        MemtableAllocator allocator = pool.newAllocator("copy-isolation");
        AtomicBTreePartition partition = new AtomicBTreePartition(TableMetadataRef.forOfflineTools(metadata), key, allocator);
        OpOrder writeOrder = new OpOrder();
        int rangeCount = 128;
        PartitionUpdate[] updates = new PartitionUpdate[rangeCount];
        for (int i = 0; i < rangeCount; i++)
        {
            updates[i] = new RowUpdateBuilder(metadata, 1, i + 1L, 0)
                         .addRangeTombstone(i * 3, i * 3 + 1)
                         .buildUpdate();
        }

        ExecutorService executor = Executors.newFixedThreadPool(4);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<?>> futures = new ArrayList<>();
        try
        {
            for (int worker = 0; worker < 4; worker++)
            {
                final int workerIndex = worker;
                futures.add(executor.submit(() -> {
                    start.await();
                    for (int i = workerIndex; i < rangeCount; i += 4)
                    {
                        try (OpOrder.Group group = writeOrder.start())
                        {
                            partition.addAll(updates[i], HeapCloner.instance, group, UpdateTransaction.NO_OP);
                        }
                    }
                    return null;
                }));
            }

            start.countDown();
            for (Future<?> future : futures)
                future.get();

            assertEquals(rangeCount, partition.deletionInfo().rangeCount());
            for (int i = 0; i < rangeCount; i++)
                assertNotNull(partition.deletionInfo().rangeCovering(Clustering.make(ByteBufferUtil.bytes(i * 3))));
            assertTrue(allocator.onHeap().owns() >= 0L);
        }
        finally
        {
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(1, TimeUnit.MINUTES));
            allocator.setDiscarding();
            allocator.setDiscarded();
            pool.shutdownAndWait(1, TimeUnit.MINUTES);
        }
    }

    private static void assertCopiesStayIsolated(Consumer<RangeTombstoneList> mutation)
    {
        RangeTombstoneList original = fourRanges();
        RangeTombstoneList parent = original.copy();
        RangeTombstoneList sibling = original.copy();
        List<String> originalBefore = snapshot(original);
        List<String> siblingBefore = snapshot(sibling);

        mutation.accept(parent);
        List<String> parentAfter = snapshot(parent);
        assertEquals(originalBefore, snapshot(original));
        assertEquals(siblingBefore, snapshot(sibling));

        mutation.accept(sibling);
        assertEquals(originalBefore, snapshot(original));
        assertEquals(parentAfter, snapshot(parent));
    }

    private static RangeTombstoneList fourRanges()
    {
        RangeTombstoneList ranges = new RangeTombstoneList(COMPARATOR, 5);
        ranges.add(tombstone(10, 19, 10, 1));
        ranges.add(tombstone(30, 39, 20, 2));
        ranges.add(tombstone(50, 59, 30, 3));
        ranges.add(tombstone(70, 79, 40, 4));
        return ranges;
    }

    private static List<String> snapshot(RangeTombstoneList ranges)
    {
        List<String> result = new ArrayList<>();
        for (RangeTombstone range : ranges)
        {
            DeletionTime deletion = range.deletionTime();
            result.add(range.deletedSlice().toString(COMPARATOR)
                       + '@' + deletion.markedForDeleteAt()
                       + ':' + deletion.localDeletionTimeUnsignedInteger());
        }
        return result;
    }

    private static RangeTombstone tombstone(int start, int end, long timestamp, int localDeletionTime)
    {
        return new RangeTombstone(Slice.make(BufferClusteringBound.inclusiveStartOf(ByteBufferUtil.bytes(start)),
                                             BufferClusteringBound.inclusiveEndOf(ByteBufferUtil.bytes(end))),
                                  DeletionTime.build(timestamp, localDeletionTime));
    }
}
