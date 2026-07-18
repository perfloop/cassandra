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

package org.apache.cassandra.db;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Random;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.context.CounterContext;
import org.apache.cassandra.db.filter.ClusteringIndexSliceFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.marshal.ByteBufferAccessor;
import org.apache.cassandra.db.marshal.CounterColumnType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.partitions.SingletonUnfilteredPartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.rows.WrappingUnfilteredRowIterator;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.distributed.test.log.ClusterMetadataTestHelper;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ReadResponseTest
{
    private final Random random = new Random();
    private TableMetadata metadata;

    @BeforeClass
    public static void beforeClass()
    {
        DatabaseDescriptor.daemonInitialization();
        ClusterMetadataTestHelper.setInstanceForTest();
    }
    @Before
    public void setup()
    {

        metadata = TableMetadata.builder("ks", "t1")
                                .offline()
                                .addPartitionKeyColumn("p", Int32Type.instance)
                                .addRegularColumn("v", Int32Type.instance)
                                .partitioner(Murmur3Partitioner.instance)
                                .build();
    }

    @Test
    public void fromCommandWithConclusiveRepairedDigest()
    {
        ByteBuffer digest = digest();
        ReadCommand command = command(key(), metadata);
        StubRepairedDataInfo rdi = new StubRepairedDataInfo(digest, true);
        ReadResponse response = command.createResponse(EmptyIterators.unfilteredPartition(metadata), rdi);
        assertTrue(response.isRepairedDigestConclusive());
        assertEquals(digest, response.repairedDataDigest());
        verifySerDe(response);
    }

    @Test
    public void fromCommandWithInconclusiveRepairedDigest()
    {
        ByteBuffer digest = digest();
        ReadCommand command = command(key(), metadata);
        StubRepairedDataInfo rdi = new StubRepairedDataInfo(digest, false);
        ReadResponse response = command.createResponse(EmptyIterators.unfilteredPartition(metadata), rdi);
        assertFalse(response.isRepairedDigestConclusive());
        assertEquals(digest, response.repairedDataDigest());
        verifySerDe(response);
    }

    @Test
    public void fromCommandWithConclusiveEmptyRepairedDigest()
    {
        ReadCommand command = command(key(), metadata);
        StubRepairedDataInfo rdi = new StubRepairedDataInfo(ByteBufferUtil.EMPTY_BYTE_BUFFER, true);
        ReadResponse response = command.createResponse(EmptyIterators.unfilteredPartition(metadata), rdi);
        assertTrue(response.isRepairedDigestConclusive());
        assertEquals(ByteBufferUtil.EMPTY_BYTE_BUFFER, response.repairedDataDigest());
        verifySerDe(response);
    }

    @Test
    public void fromCommandWithInconclusiveEmptyRepairedDigest()
    {
        ReadCommand command = command(key(), metadata);
        StubRepairedDataInfo rdi = new StubRepairedDataInfo(ByteBufferUtil.EMPTY_BYTE_BUFFER, false);
        ReadResponse response = command.createResponse(EmptyIterators.unfilteredPartition(metadata), rdi);
        assertFalse(response.isRepairedDigestConclusive());
        assertEquals(ByteBufferUtil.EMPTY_BYTE_BUFFER, response.repairedDataDigest());
        verifySerDe(response);
    }

    /*
     * Digest responses should never include repaired data tracking as we only request
     * it in read repair or for range queries
     */
    @Test (expected = UnsupportedOperationException.class)
    public void digestResponseErrorsIfRepairedDataDigestRequested()
    {
        ReadCommand command = digestCommand(key(), metadata);
        StubRepairedDataInfo rdi = new StubRepairedDataInfo(ByteBufferUtil.EMPTY_BYTE_BUFFER, true);
        ReadResponse response = command.createResponse(EmptyIterators.unfilteredPartition(metadata), rdi);
        assertTrue(response.isDigestResponse());
        assertFalse(response.mayIncludeRepairedDigest());
        response.repairedDataDigest();
    }

    @Test (expected = UnsupportedOperationException.class)
    public void digestResponseErrorsIfIsConclusiveRequested()
    {
        ReadCommand command = digestCommand(key(), metadata);
        StubRepairedDataInfo rdi = new StubRepairedDataInfo(ByteBufferUtil.EMPTY_BYTE_BUFFER, true);
        ReadResponse response = command.createResponse(EmptyIterators.unfilteredPartition(metadata), rdi);
        assertTrue(response.isDigestResponse());
        assertFalse(response.mayIncludeRepairedDigest());
        response.isRepairedDigestConclusive();
    }

    @Test (expected = UnsupportedOperationException.class)
    public void digestResponseErrorsIfIteratorRequested()
    {
        ReadCommand command = digestCommand(key(), metadata);
        StubRepairedDataInfo rdi = new StubRepairedDataInfo(ByteBufferUtil.EMPTY_BYTE_BUFFER, true);
        ReadResponse response = command.createResponse(EmptyIterators.unfilteredPartition(metadata), rdi);
        assertTrue(response.isDigestResponse());
        assertFalse(response.mayIncludeRepairedDigest());
        response.makeIterator(command);
    }

    @Test
    public void makeDigestDoesntConsiderRepairedDataInfo()
    {
        // It shouldn't be possible to get false positive DigestMismatchExceptions based
        // on differing repaired data tracking info because it isn't requested on initial
        // requests, only following a digest mismatch. Having a test doesn't hurt though
        int key = key();
        ByteBuffer digest1 = digest();
        ReadCommand command1 = command(key, metadata);
        StubRepairedDataInfo rdi1 = new StubRepairedDataInfo(digest1, true);
        ReadResponse response1 = command1.createResponse(EmptyIterators.unfilteredPartition(metadata), rdi1);

        ByteBuffer digest2 = digest();
        ReadCommand command2 = command(key, metadata);
        StubRepairedDataInfo rdi2 = new StubRepairedDataInfo(digest2, false);
        ReadResponse response2 = command1.createResponse(EmptyIterators.unfilteredPartition(metadata), rdi2);

        assertEquals(response1.digest(command1), response2.digest(command2));
    }

    private void verifySerDe(ReadResponse response) {
        // check that roundtripping through ReadResponse.serializer behaves as expected
        for (MessagingService.Version version : MessagingService.Version.supportedVersions())
            roundTripSerialization(response, version.value);

    }

    private ReadResponse roundTripSerialization(ReadResponse response, int version)
    {
        try
        {
            DataOutputBuffer out = new DataOutputBuffer();
            ReadResponse.serializer.serialize(response, out, version);

            DataInputBuffer in = new DataInputBuffer(out.buffer(), false);
            ReadResponse deser = ReadResponse.serializer.deserialize(in, version);
            assertTrue(version >= MessagingService.VERSION_40);
            assertTrue(deser.mayIncludeRepairedDigest());
            assertEquals(response.repairedDataDigest(), deser.repairedDataDigest());
            assertEquals(response.isRepairedDigestConclusive(), deser.isRepairedDigestConclusive());
            return deser;
        }
        catch (IOException e)
        {
            fail("Caught unexpected IOException during SerDe: " + e.getMessage());
            throw new AssertionError(e);
        }
    }

    @Test
    public void materializesLocalDataBeforeCapturingRepairedDigest()
    {
        int key = key();
        ReadCommand command = command(key, metadata);
        SourceConsumption sourceConsumption = new SourceConsumption();
        ByteBuffer repairedDigest = digest();
        ConsumptionTrackingRepairedDataInfo rdi = new ConsumptionTrackingRepairedDataInfo(repairedDigest, sourceConsumption);
        PartitionUpdate update = new RowUpdateBuilder(metadata, 1L, key).add("v", 1).buildUpdate();

        ReadResponse response = command.createResponse(trackingPartitions(update, sourceConsumption), rdi);

        assertTrue(sourceConsumption.exhausted);
        assertTrue(sourceConsumption.closed);
        assertTrue(rdi.digestCapturedAfterSourceConsumption);
        assertEquals(repairedDigest, response.repairedDataDigest());
        assertFalse(response.isRepairedDigestConclusive());
        assertEquals(1, unfilteredCount(response, command));
        assertEquals(1, unfilteredCount(response, command));

        ByteBuffer responseDigest = response.digest(command);
        assertEquals(responseDigest, response.digest(command));
        for (MessagingService.Version version : MessagingService.Version.supportedVersions())
        {
            ReadResponse deserialized = roundTripSerialization(response, version.value);
            assertEquals(responseDigest, deserialized.digest(command));
            assertEquals(1, unfilteredCount(deserialized, command));
        }
    }

    @Test
    public void clearsMarkedLocalCounterContextsInMaterializedResponse()
    {
        TableMetadata counterMetadata = counterMetadata();
        int key = key();
        ReadCommand command = command(key, counterMetadata);
        ByteBuffer marked = CounterContext.instance().markLocalToBeCleared(CounterContext.instance().createLocal(1));
        assertTrue(CounterContext.instance().shouldClearLocal(marked, ByteBufferAccessor.instance));
        PartitionUpdate update = new RowUpdateBuilder(counterMetadata, 1L, key).add("c", marked).buildUpdate();

        ReadResponse response = command.createResponse(new SingletonUnfilteredPartitionIterator(update.unfilteredIterator()), RepairedDataInfo.NO_OP_REPAIRED_DATA_INFO);

        ByteBuffer cleared = clearedCounterContext(response, command);
        assertEquals(cleared, clearedCounterContext(response, command));
        for (MessagingService.Version version : MessagingService.Version.supportedVersions())
            assertEquals(cleared, clearedCounterContext(roundTripSerialization(response, version.value), command));

        PartitionUpdate remoteUpdate = new RowUpdateBuilder(counterMetadata, 1L, key).add("c", marked).buildUpdate();
        ReadResponse remoteResponse = command.createResponseForRemote(new SingletonUnfilteredPartitionIterator(remoteUpdate.unfilteredIterator()), RepairedDataInfo.NO_OP_REPAIRED_DATA_INFO);
        assertEquals(cleared, clearedCounterContext(remoteResponse, command));
    }

    @Test
    public void mergedLocalDataResponseReplaysMultiplePartitions()
    {
        int first = 1;
        int second = 2;
        if (metadata.partitioner.decorateKey(ByteBufferUtil.bytes(first)).compareTo(metadata.partitioner.decorateKey(ByteBufferUtil.bytes(second))) > 0)
        {
            int swap = first;
            first = second;
            second = swap;
        }

        ReadCommand command = command(first, metadata);
        ReadResponse firstResponse = command.createResponse(new SingletonUnfilteredPartitionIterator(update(metadata, first).unfilteredIterator()), RepairedDataInfo.NO_OP_REPAIRED_DATA_INFO);
        ReadResponse secondResponse = command.createResponse(new SingletonUnfilteredPartitionIterator(update(metadata, second).unfilteredIterator()), RepairedDataInfo.NO_OP_REPAIRED_DATA_INFO);
        ReadResponse merged = ReadResponse.merge(Arrays.asList(firstResponse, secondResponse), command);
        List<ByteBuffer> expectedKeys = Arrays.asList(ByteBufferUtil.bytes(first), ByteBufferUtil.bytes(second));
        List<List<Integer>> expectedRows = Arrays.asList(Arrays.asList(first), Arrays.asList(second));

        assertPartitionContents(merged, command, expectedKeys, expectedRows);
        assertPartitionContents(merged, command, expectedKeys, expectedRows);
        for (MessagingService.Version version : MessagingService.Version.supportedVersions())
            assertPartitionContents(roundTripSerialization(merged, version.value), command, expectedKeys, expectedRows);
    }

    private static UnfilteredPartitionIterator trackingPartitions(PartitionUpdate update, SourceConsumption sourceConsumption)
    {
        UnfilteredRowIterator source = update.unfilteredIterator();
        UnfilteredRowIterator tracked = new WrappingUnfilteredRowIterator()
        {
            public UnfilteredRowIterator wrapped()
            {
                return source;
            }

            public boolean hasNext()
            {
                boolean hasNext = source.hasNext();
                if (!hasNext)
                    sourceConsumption.exhausted = true;
                return hasNext;
            }

            public void close()
            {
                try
                {
                    source.close();
                }
                finally
                {
                    sourceConsumption.closed = true;
                }
            }
        };
        return new SingletonUnfilteredPartitionIterator(tracked);
    }

    private static int unfilteredCount(ReadResponse response, ReadCommand command)
    {
        int count = 0;
        try (UnfilteredPartitionIterator partitions = response.makeIterator(command))
        {
            while (partitions.hasNext())
            {
                try (UnfilteredRowIterator partition = partitions.next())
                {
                    while (partition.hasNext())
                    {
                        partition.next();
                        count++;
                    }
                }
            }
        }
        return count;
    }

    private static ByteBuffer clearedCounterContext(ReadResponse response, ReadCommand command)
    {
        Cell<?> cell = onlyCell(response, command);
        assertFalse(CounterContext.instance().shouldClearLocal(cell.buffer(), ByteBufferAccessor.instance));
        assertEquals(1, CounterContext.instance().total(cell));
        return cell.buffer();
    }

    private static Cell<?> onlyCell(ReadResponse response, ReadCommand command)
    {
        try (UnfilteredPartitionIterator partitions = response.makeIterator(command))
        {
            assertTrue(partitions.hasNext());
            try (UnfilteredRowIterator partition = partitions.next())
            {
                assertTrue(partition.hasNext());
                Row row = (Row) partition.next();
                for (Cell<?> cell : row.cells())
                    return cell;
            }
        }
        throw new AssertionError("Expected a cell");
    }

    private static void assertPartitionContents(ReadResponse response,
                                                ReadCommand command,
                                                List<ByteBuffer> expectedKeys,
                                                List<List<Integer>> expectedRows)
    {
        List<ByteBuffer> keys = new ArrayList<>();
        List<List<Integer>> rows = new ArrayList<>();
        try (UnfilteredPartitionIterator partitions = response.makeIterator(command))
        {
            while (partitions.hasNext())
            {
                try (UnfilteredRowIterator partition = partitions.next())
                {
                    keys.add(partition.partitionKey().getKey());
                    List<Integer> values = new ArrayList<>();
                    while (partition.hasNext())
                    {
                        Row row = (Row) partition.next();
                        Cell<?> cell = row.cells().iterator().next();
                        values.add(Int32Type.instance.compose(cell.buffer()));
                    }
                    rows.add(values);
                }
            }
        }
        assertEquals(expectedKeys, keys);
        assertEquals(expectedRows, rows);
    }

    private static PartitionUpdate update(TableMetadata metadata, int key)
    {
        return new RowUpdateBuilder(metadata, 1L, key).add("v", key).buildUpdate();
    }

    private TableMetadata counterMetadata()
    {
        return TableMetadata.builder("ks", "counter")
                            .flags(EnumSet.of(TableMetadata.Flag.COUNTER, TableMetadata.Flag.COMPOUND))
                            .offline()
                            .addPartitionKeyColumn("p", Int32Type.instance)
                            .addRegularColumn("c", CounterColumnType.instance)
                            .partitioner(Murmur3Partitioner.instance)
                            .build();
    }

    private static class SourceConsumption
    {
        private boolean exhausted;
        private boolean closed;
    }

    private static class ConsumptionTrackingRepairedDataInfo extends RepairedDataInfo
    {
        private final ByteBuffer repairedDigest;
        private final SourceConsumption sourceConsumption;
        private boolean digestCapturedAfterSourceConsumption;

        private ConsumptionTrackingRepairedDataInfo(ByteBuffer repairedDigest, SourceConsumption sourceConsumption)
        {
            super(null);
            this.repairedDigest = repairedDigest;
            this.sourceConsumption = sourceConsumption;
        }

        @Override
        public ByteBuffer getDigest()
        {
            if (!sourceConsumption.exhausted || !sourceConsumption.closed)
                throw new AssertionError("Repaired-data digest captured before source consumption");
            digestCapturedAfterSourceConsumption = true;
            return repairedDigest;
        }

        @Override
        public boolean isConclusive()
        {
            if (!digestCapturedAfterSourceConsumption)
                throw new AssertionError("Repaired-data conclusiveness captured before source consumption");
            return false;
        }
    }

    private int key()
    {
        return random.nextInt();
    }

    private ByteBuffer digest()
    {
        byte[] bytes = new byte[4];
        random.nextBytes(bytes);
        return ByteBuffer.wrap(bytes);
    }

    private ReadCommand digestCommand(int key, TableMetadata metadata)
    {
        return new StubReadCommand(key, metadata, true);
    }

    private ReadCommand command(int key, TableMetadata metadata)
    {
        return new StubReadCommand(key, metadata, false);
    }

    private static class StubRepairedDataInfo extends RepairedDataInfo
    {
        private final ByteBuffer repairedDigest;
        private final boolean conclusive;

        public StubRepairedDataInfo(ByteBuffer repairedDigest, boolean conclusive)
        {
            super(null);
            this.repairedDigest = repairedDigest;
            this.conclusive = conclusive;
        }
        
        @Override
        public ByteBuffer getDigest()
        {
            return repairedDigest;
        }
        
        @Override
        public boolean isConclusive()
        {
            return conclusive;
        }
    }

    private static class StubReadCommand extends SinglePartitionReadCommand
    {
        StubReadCommand(int key, TableMetadata metadata, boolean isDigest)
        {
            super(metadata.epoch,
                  isDigest,
                  0,
                  false,
                  PotentialTxnConflicts.DISALLOW,
                  metadata,
                  FBUtilities.nowInSeconds(),
                  ColumnFilter.all(metadata),
                  RowFilter.none(),
                  DataLimits.NONE,
                  metadata.partitioner.decorateKey(ByteBufferUtil.bytes(key)),
                  new ClusteringIndexSliceFilter(Slices.ALL, false),
                  null,
                  false,
                  null);
           
        }

        @Override
        public boolean selectsFullPartition()
        {
            return true;
        }

        public UnfilteredPartitionIterator executeLocally(ReadExecutionController controller)
        {
            return EmptyIterators.unfilteredPartition(this.metadata());
        }
    }
}
