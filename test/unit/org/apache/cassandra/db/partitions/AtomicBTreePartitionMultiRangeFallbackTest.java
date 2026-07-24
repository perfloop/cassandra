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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringBound;
import org.apache.cassandra.db.DeletionInfo;
import org.apache.cassandra.db.MutableDeletionInfo;
import org.apache.cassandra.db.RangeTombstone;
import org.apache.cassandra.db.Slice;
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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;

/** Verifies multi-range fallback after an AtomicBTreePartition ordered prefix. */
public class AtomicBTreePartitionMultiRangeFallbackTest
{
    private static final long LOCAL_DELETION_TIME = 1;

    @BeforeClass
    public static void setup()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Test
    public void multiRangeFallbackMaterializesAndMatchesCanonicalReconciliation()
    {
        Fixture fixture = new Fixture();
        try
        {
            MutableDeletionInfo expected = MutableDeletionInfo.live();
            for (int i = 0; i < 8; i++)
            {
                PartitionUpdate update = fixture.rangeUpdate(i * 4, i * 4 + 2, i + 1L);
                expected.add(update.deletionInfo());
                fixture.apply(update);
            }
            PartitionUpdate update = fixture.multiRangeUpdate(1, 5, 7, 11, 100L);
            expected.add(update.deletionInfo());
            fixture.apply(update);

            DeletionInfo actual = fixture.partition.deletionInfo();
            if (persistentImplementationIsPresent())
                assertFalse("a multi-range update must materialize canonical mutable deletion info",
                            actual.getClass().getName().endsWith("ImmutableBTreeDeletionInfo"));
            assertSameRanges(expected, actual, Slice.ALL);
            assertSameRanges(expected, actual, Slice.make(ClusteringBound.create(fixture.metadata.comparator, true, true, 1),
                                                          ClusteringBound.create(fixture.metadata.comparator, false, true, 11)));
            for (int key = 0; key <= 34; key++)
                assertSameCoverage(expected, actual, key);
        }
        finally
        {
            fixture.close();
        }
    }

    private static boolean persistentImplementationIsPresent()
    {
        try
        {
            Class.forName("org.apache.cassandra.db.partitions.ImmutableBTreeDeletionInfo");
            return true;
        }
        catch (ClassNotFoundException e)
        {
            return false;
        }
    }

    private static void assertSameRanges(DeletionInfo expected, DeletionInfo actual, Slice slice)
    {
        List<RangeTombstone> forward = collect(expected.rangeIterator(slice, false));
        assertEquals(forward, collect(actual.rangeIterator(slice, false)));
        List<RangeTombstone> reverse = collect(actual.rangeIterator(slice, true));
        Collections.reverse(reverse);
        assertEquals(forward, reverse);
    }

    private static void assertSameCoverage(DeletionInfo expected, DeletionInfo actual, int key)
    {
        RangeTombstone expectedRange = expected.rangeCovering(clustering(key));
        RangeTombstone actualRange = actual.rangeCovering(clustering(key));
        if (expectedRange == null)
            assertNull(actualRange);
        else
            assertEquals(expectedRange, actualRange);
    }

    private static List<RangeTombstone> collect(Iterator<RangeTombstone> iterator)
    {
        List<RangeTombstone> ranges = new ArrayList<>();
        iterator.forEachRemaining(ranges::add);
        return ranges;
    }

    private static Clustering<?> clustering(int value)
    {
        return Clustering.make(Int32Type.instance.decompose(value));
    }

    private static final class Fixture
    {
        private final TableMetadata metadata;
        private final MemtableAllocator allocator;
        private final AtomicBTreePartition partition;
        private final OpOrder order = new OpOrder();

        private Fixture()
        {
            metadata = TableMetadata.builder("range_tombstone_multi_range_fallback_test", "partition")
                                    .addPartitionKeyColumn("pk", Int32Type.instance)
                                    .addClusteringColumn("ck", Int32Type.instance)
                                    .partitioner(ByteOrderedPartitioner.instance)
                                    .build();
            TableMetadataRef metadataRef = TableMetadataRef.forOfflineTools(metadata);
            HeapPool pool = new HeapPool(Long.MAX_VALUE, 1.0f, () -> ImmediateFuture.success(Boolean.TRUE));
            allocator = pool.newAllocator("test");
            partition = new AtomicBTreePartition(metadataRef,
                                                 metadata.partitioner.decorateKey(Int32Type.instance.decompose(0)),
                                                 allocator);
        }

        private void apply(PartitionUpdate update)
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

        private PartitionUpdate multiRangeUpdate(int firstStart, int firstEnd, int secondStart, int secondEnd, long timestamp)
        {
            PartitionUpdate.SimpleBuilder builder = PartitionUpdate.simpleBuilder(metadata, 0);
            builder.timestamp(timestamp).nowInSec(LOCAL_DELETION_TIME);
            builder.addRangeTombstone().start(firstStart).end(firstEnd);
            builder.addRangeTombstone().start(secondStart).end(secondEnd);
            return builder.build();
        }

        private void close()
        {
            allocator.setDiscarding();
            allocator.setDiscarded();
        }
    }
}
