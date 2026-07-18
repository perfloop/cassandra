/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
import java.nio.charset.StandardCharsets;

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
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.partitions.AbstractUnfilteredPartitionIterator;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.partitions.SingletonUnfilteredPartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.RangeTombstoneMarker;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.rows.WrappingUnfilteredRowIterator;
import org.apache.cassandra.distributed.test.log.ClusterMetadataTestHelper;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.service.reads.ReadCallback;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.transport.Dispatcher;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.CounterId;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.exceptions.RequestFailure;
import org.apache.cassandra.locator.InetAddressAndPort;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class LocalReadResponseTest
{
    private static final int ROWS = 8;
    private static final int VALUES_PER_ROW = 3;

    @BeforeClass
    public static void beforeClass()
    {
        DatabaseDescriptor.daemonInitialization();
        ClusterMetadataTestHelper.setInstanceForTest();
    }

    @Test
    public void localResponseOwnsReusableDataAndPreservesTheWireResponse() throws IOException
    {
        TableMetadata metadata = regularMetadata();
        DecoratedKey key = key(metadata, 1);
        ReadCommand command = SinglePartitionReadCommand.fullPartitionRead(metadata, FBUtilities.nowInSeconds(), key);
        PartitionUpdate partition = regularPartition(metadata, key);
        long expectedChecksum = checksum(new SingletonUnfilteredPartitionIterator(partition.unfilteredIterator()));

        ExhaustionTrackingPartitionIterator data = new ExhaustionTrackingPartitionIterator(new SingletonUnfilteredPartitionIterator(partition.unfilteredIterator()));
        ByteBuffer repairedDigest = ByteBufferUtil.bytes("repaired-data");
        ExhaustionTrackingRepairedDataInfo repairedDataInfo = new ExhaustionTrackingRepairedDataInfo(data, repairedDigest);
        ReadResponse response = command.createLocalResponse(data, repairedDataInfo);

        assertTrue("the response must finish consuming the local input before reading its repaired digest", data.rowsExhausted);
        assertTrue("the repaired digest must be read after the local input is materialized", repairedDataInfo.digestRead);
        assertEquals(repairedDigest, response.repairedDataDigest());
        assertTrue(response.isRepairedDigestConclusive());
        assertEquals(expectedChecksum, checksum(response.makeIterator(command)));
        assertEquals(expectedChecksum, checksum(response.makeIterator(command)));

        for (MessagingService.Version version : MessagingService.Version.supportedVersions())
        {
            try (DataOutputBuffer out = new DataOutputBuffer())
            {
                ReadResponse.serializer.serialize(response, out, version.value);
                assertEquals(ReadResponse.serializer.serializedSize(response, version.value), out.getLength());

                try (DataInputBuffer in = new DataInputBuffer(out.buffer(), false))
                {
                    ReadResponse remoteResponse = ReadResponse.serializer.deserialize(in, version.value);
                    assertEquals(expectedChecksum, checksum(remoteResponse.makeIterator(command)));
                    assertEquals(repairedDigest, remoteResponse.repairedDataDigest());
                    assertTrue(remoteResponse.isRepairedDigestConclusive());
                }
            }
        }
    }

    @Test
    public void localResponsePreservesMultiPartitionWireData() throws IOException
    {
        TableMetadata metadata = regularMetadata();
        DecoratedKey firstKey = key(metadata, 3);
        DecoratedKey secondKey = key(metadata, 4);
        ReadCommand command = SinglePartitionReadCommand.fullPartitionRead(metadata, FBUtilities.nowInSeconds(), firstKey);

        PartitionUpdate.SimpleBuilder firstBuilder = PartitionUpdate.simpleBuilder(metadata, firstKey).timestamp(1L);
        firstBuilder.row().add("static_value", "first static");
        firstBuilder.row(1).add("v0", "first value");
        firstBuilder.addRangeTombstone().start(2).end(4);
        PartitionUpdate first = firstBuilder.build();

        PartitionUpdate.SimpleBuilder secondBuilder = PartitionUpdate.simpleBuilder(metadata, secondKey).timestamp(2L);
        secondBuilder.row(5).add("v1", "second value");
        PartitionUpdate second = secondBuilder.build();

        ReadResponse local = command.createLocalResponse(partitions(first, second), RepairedDataInfo.NO_OP_REPAIRED_DATA_INFO);
        ReadResponse remote = ReadResponse.createRemoteDataResponse(partitions(first, second), ByteBufferUtil.EMPTY_BYTE_BUFFER, true, command, MessagingService.current_version);
        ByteBuffer expectedWireResponse = serialized(remote);

        assertEquals(expectedWireResponse, serialized(local));
        assertEquals(digest(remote, command), digest(local, command));
        assertEquals(digest(remote, command), digest(local, command));
        assertEquals(expectedWireResponse, serialized(local));
    }

    @Test
    public void localResponsePreservesColumnFilterSerializationSemantics()
    {
        TableMetadata metadata = regularMetadata();
        DecoratedKey key = key(metadata, 1);
        ColumnFilter filter = ColumnFilter.selection(metadata,
                                                     RegularAndStaticColumns.builder()
                                                                            .add(metadata.getColumn(ByteBufferUtil.bytes("v0")))
                                                                            .build(),
                                                     false);
        ReadCommand command = SinglePartitionReadCommand.create(metadata,
                                                                 FBUtilities.nowInSeconds(),
                                                                 key,
                                                                 filter,
                                                                 new ClusteringIndexSliceFilter(Slices.ALL, false));
        PartitionUpdate.SimpleBuilder builder = PartitionUpdate.simpleBuilder(metadata, key).timestamp(1L);
        builder.row(1).add("v0", "queried").add("v1", "fetched but not queried");
        PartitionUpdate partition = builder.build();

        ReadResponse serialized = ReadResponse.createDataResponse(new SingletonUnfilteredPartitionIterator(partition.unfilteredIterator()), command);
        ReadResponse local = command.createLocalResponse(new SingletonUnfilteredPartitionIterator(partition.unfilteredIterator()), RepairedDataInfo.NO_OP_REPAIRED_DATA_INFO);

        try (UnfilteredPartitionIterator expectedPartitions = serialized.makeIterator(command);
             UnfilteredPartitionIterator actualPartitions = local.makeIterator(command))
        {
            assertTrue(expectedPartitions.hasNext());
            assertTrue(actualPartitions.hasNext());
            try (UnfilteredRowIterator expected = expectedPartitions.next();
                 UnfilteredRowIterator actual = actualPartitions.next())
            {
                assertEquals(expected.columns(), actual.columns());
                assertEquals(expected.staticRow(), actual.staticRow());
                assertTrue(expected.hasNext());
                assertTrue(actual.hasNext());
                Row expectedRow = (Row) expected.next();
                Row actualRow = (Row) actual.next();
                assertEquals(expectedRow, actualRow);
                assertCellValue(actualRow, metadata, "v0", "queried".getBytes(StandardCharsets.UTF_8));
                Cell<?> unqueried = actualRow.getCell(metadata.getColumn(ByteBufferUtil.bytes("v1")));
                assertNotNull(unqueried);
                assertEquals("fetched but not queried".getBytes(StandardCharsets.UTF_8).length, unqueried.valueSize());
                assertFalse(expected.hasNext());
                assertFalse(actual.hasNext());
            }
            assertFalse(expectedPartitions.hasNext());
            assertFalse(actualPartitions.hasNext());
        }
        assertEquals(serialized.digest(command), local.digest(command));
    }

    @Test
    public void localReadRunnableUsesTheDirectResponseRoute()
    {
        TableMetadata metadata = regularMetadata();
        DecoratedKey key = key(metadata, 1);
        PartitionUpdate partition = regularPartition(metadata, key);
        LocalRunnableReadCommand command = new LocalRunnableReadCommand(metadata, key, partition);
        CapturingReadCallback callback = new CapturingReadCallback(command);

        new StorageProxy.LocalReadRunnable(command, callback, Dispatcher.RequestTime.forImmediateExecution()).run();

        assertTrue(command.createLocalResponseCalled);
        assertNotNull(callback.response);
        assertEquals(checksum(new SingletonUnfilteredPartitionIterator(partition.unfilteredIterator())),
                     checksum(callback.response.makeIterator(command)));
    }

    @Test
    public void localResponseOwnsDataReturnedByEphemeralIterators() throws IOException
    {
        TableMetadata metadata = regularMetadata();
        byte[] keyValue = Int32Type.instance.decompose(5).array();
        DecoratedKey key = metadata.partitioner.decorateKey(ByteBuffer.wrap(keyValue));
        ReadCommand command = SinglePartitionReadCommand.fullPartitionRead(metadata, FBUtilities.nowInSeconds(), key);

        byte[] staticValue = "static value".getBytes(StandardCharsets.UTF_8);
        byte[] firstValue = "first value".getBytes(StandardCharsets.UTF_8);
        byte[] secondValue = "second value".getBytes(StandardCharsets.UTF_8);
        byte[] markerStart = Int32Type.instance.decompose(3).array();
        byte[] markerEnd = Int32Type.instance.decompose(4).array();
        byte[] expectedKey = keyValue.clone();
        byte[] expectedStatic = staticValue.clone();
        byte[] expectedFirst = firstValue.clone();
        byte[] expectedSecond = secondValue.clone();
        byte[] expectedMarkerStart = markerStart.clone();
        byte[] expectedMarkerEnd = markerEnd.clone();

        PartitionUpdate.SimpleBuilder builder = PartitionUpdate.simpleBuilder(metadata, key).timestamp(1L);
        builder.row().add("static_value", ByteBuffer.wrap(staticValue));
        builder.row(1).add("v0", ByteBuffer.wrap(firstValue));
        builder.row(2).add("v0", ByteBuffer.wrap(secondValue));
        builder.addRangeTombstone(new RangeTombstone(Slice.make(BufferClusteringBound.inclusiveStartOf(ByteBuffer.wrap(markerStart)),
                                                                 BufferClusteringBound.inclusiveEndOf(ByteBuffer.wrap(markerEnd))),
                                                      DeletionTime.build(1L, FBUtilities.nowInSeconds())));
        PartitionUpdate partition = builder.build();

        UnfilteredRowIterator ephemeral = new InvalidatingRowIterator(partition.unfilteredIterator(),
                                                                        keyValue,
                                                                        staticValue,
                                                                        firstValue,
                                                                        secondValue,
                                                                        markerStart);
        ReadResponse response = command.createLocalResponse(new SingletonUnfilteredPartitionIterator(ephemeral), RepairedDataInfo.NO_OP_REPAIRED_DATA_INFO);

        assertFalse(ByteBuffer.wrap(expectedKey).equals(ByteBuffer.wrap(keyValue)));
        assertFalse(ByteBuffer.wrap(expectedStatic).equals(ByteBuffer.wrap(staticValue)));
        assertFalse(ByteBuffer.wrap(expectedFirst).equals(ByteBuffer.wrap(firstValue)));
        assertFalse(ByteBuffer.wrap(expectedSecond).equals(ByteBuffer.wrap(secondValue)));
        assertFalse(ByteBuffer.wrap(expectedMarkerStart).equals(ByteBuffer.wrap(markerStart)));
        assertSnapshot(response,
                       command,
                       metadata,
                       expectedKey,
                       expectedStatic,
                       expectedFirst,
                       expectedSecond,
                       expectedMarkerStart,
                       expectedMarkerEnd);
        assertSnapshot(response,
                       command,
                       metadata,
                       expectedKey,
                       expectedStatic,
                       expectedFirst,
                       expectedSecond,
                       expectedMarkerStart,
                       expectedMarkerEnd);
        ByteBuffer digest = response.digest(command);
        assertEquals(digest, response.digest(command));

        ByteBuffer bytes = serialized(response);
        try (DataInputBuffer in = new DataInputBuffer(bytes, false))
        {
            ReadResponse remote = ReadResponse.serializer.deserialize(in, MessagingService.current_version);
            assertSnapshot(remote,
                           command,
                           metadata,
                           expectedKey,
                           expectedStatic,
                           expectedFirst,
                           expectedSecond,
                           expectedMarkerStart,
                           expectedMarkerEnd);
            assertEquals(digest, remote.digest(command));
        }
    }

    @Test
    public void localResponseClearsMarkedCounterContextLikeTheSerializationPath()
    {
        TableMetadata metadata = TableMetadata.builder("ks", "local_response_counter")
                                              .offline()
                                              .addPartitionKeyColumn("pk", Int32Type.instance)
                                              .addClusteringColumn("ck", Int32Type.instance)
                                              .addRegularColumn("count", CounterColumnType.instance)
                                              .partitioner(Murmur3Partitioner.instance)
                                              .build();
        DecoratedKey key = key(metadata, 2);
        ReadCommand command = SinglePartitionReadCommand.fullPartitionRead(metadata, FBUtilities.nowInSeconds(), key);
        CounterContext.ContextState counter = CounterContext.ContextState.allocate(0, 1, 0);
        counter.writeLocal(CounterId.fromInt(1), 1L, 7L);
        ByteBuffer markedCounterContext = CounterContext.instance().markLocalToBeCleared(counter.context);
        assertTrue(CounterContext.instance().shouldClearLocal(markedCounterContext, ByteBufferAccessor.instance));
        PartitionUpdate.SimpleBuilder builder = PartitionUpdate.simpleBuilder(metadata, key).timestamp(1L);
        builder.row(1).add("count", markedCounterContext);
        PartitionUpdate partition = builder.build();

        ReadResponse local = command.createLocalResponse(new SingletonUnfilteredPartitionIterator(partition.unfilteredIterator()), RepairedDataInfo.NO_OP_REPAIRED_DATA_INFO);
        ReadResponse remote = ReadResponse.createRemoteDataResponse(new SingletonUnfilteredPartitionIterator(partition.unfilteredIterator()),
                                                                      null,
                                                                      false,
                                                                      command,
                                                                      MessagingService.current_version);

        ByteBuffer localValue = counterValue(local, command, metadata);
        assertFalse(CounterContext.instance().shouldClearLocal(localValue, ByteBufferAccessor.instance));
        assertEquals(counterValue(remote, command, metadata), localValue);
    }

    private static UnfilteredPartitionIterator partitions(PartitionUpdate... updates)
    {
        return new AbstractUnfilteredPartitionIterator()
        {
            private int next;

            public TableMetadata metadata()
            {
                return updates[0].metadata();
            }

            public boolean hasNext()
            {
                return next < updates.length;
            }

            public UnfilteredRowIterator next()
            {
                return updates[next++].unfilteredIterator();
            }
        };
    }

    private static ByteBuffer serialized(ReadResponse response) throws IOException
    {
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            ReadResponse.serializer.serialize(response, out, MessagingService.current_version);
            assertEquals(ReadResponse.serializer.serializedSize(response, MessagingService.current_version), out.getLength());
            return out.buffer().duplicate();
        }
    }

    private static ByteBuffer digest(ReadResponse response, ReadCommand command)
    {
        return response.digest(command);
    }

    private static void assertSnapshot(ReadResponse response,
                                       ReadCommand command,
                                       TableMetadata metadata,
                                       byte[] key,
                                       byte[] staticValue,
                                       byte[] firstValue,
                                       byte[] secondValue,
                                       byte[] markerStart,
                                       byte[] markerEnd)
    {
        try (UnfilteredPartitionIterator partitions = response.makeIterator(command))
        {
            assertTrue(partitions.hasNext());
            try (UnfilteredRowIterator partition = partitions.next())
            {
                assertEquals(ByteBuffer.wrap(key), partition.partitionKey().getKey());
                assertCellValue(partition.staticRow(), metadata, "static_value", staticValue);
                assertTrue(partition.hasNext());
                assertCellValue((Row) partition.next(), metadata, "v0", firstValue);
                assertTrue(partition.hasNext());
                assertCellValue((Row) partition.next(), metadata, "v0", secondValue);
                assertTrue(partition.hasNext());
                assertMarker((RangeTombstoneMarker) partition.next(), markerStart);
                assertTrue(partition.hasNext());
                assertMarker((RangeTombstoneMarker) partition.next(), markerEnd);
                assertFalse(partition.hasNext());
            }
            assertFalse(partitions.hasNext());
        }
    }

    private static void assertMarker(RangeTombstoneMarker marker, byte[] value)
    {
        assertEquals(ByteBuffer.wrap(value), marker.clustering().bufferAt(0));
    }

    private static void assertCellValue(Row row, TableMetadata metadata, String columnName, byte[] value)
    {
        Cell<?> cell = row.getCell(metadata.getColumn(ByteBufferUtil.bytes(columnName)));
        assertNotNull(cell);
        assertEquals(ByteBuffer.wrap(value), cell.buffer());
    }

    private static TableMetadata regularMetadata()
    {
        return TableMetadata.builder("ks", "local_response")
                            .offline()
                            .addPartitionKeyColumn("pk", Int32Type.instance)
                            .addClusteringColumn("ck", Int32Type.instance)
                            .addStaticColumn("static_value", UTF8Type.instance)
                            .addRegularColumn("v0", UTF8Type.instance)
                            .addRegularColumn("v1", UTF8Type.instance)
                            .addRegularColumn("v2", UTF8Type.instance)
                            .partitioner(Murmur3Partitioner.instance)
                            .build();
    }

    private static DecoratedKey key(TableMetadata metadata, int value)
    {
        return metadata.partitioner.decorateKey(Int32Type.instance.decompose(value));
    }

    private static PartitionUpdate regularPartition(TableMetadata metadata, DecoratedKey key)
    {
        PartitionUpdate.SimpleBuilder builder = PartitionUpdate.simpleBuilder(metadata, key).timestamp(1L);
        builder.row().add("static_value", "static data");
        for (int row = 0; row < ROWS; row++)
        {
            Row.SimpleBuilder rowBuilder = builder.row(row);
            for (int value = 0; value < VALUES_PER_ROW; value++)
                rowBuilder.add("v" + value, "value-" + row + '-' + value);
        }
        return builder.build();
    }

    private static long checksum(UnfilteredPartitionIterator partitions)
    {
        long checksum = 1;
        int partitionsRead = 0;
        int rowsRead = 0;
        int cellsRead = 0;
        try (UnfilteredPartitionIterator iterator = partitions)
        {
            while (iterator.hasNext())
            {
                partitionsRead++;
                try (UnfilteredRowIterator partition = iterator.next())
                {
                    checksum = checksum(checksum, partition.staticRow());
                    cellsRead += partition.staticRow().columnCount();
                    while (partition.hasNext())
                    {
                        Unfiltered unfiltered = partition.next();
                        if (!unfiltered.isRow())
                            continue;

                        Row row = (Row) unfiltered;
                        rowsRead++;
                        cellsRead += row.columnCount();
                        checksum = checksum(checksum, row);
                    }
                }
            }
        }

        assertEquals(1, partitionsRead);
        assertEquals(ROWS, rowsRead);
        assertEquals(1 + ROWS * VALUES_PER_ROW, cellsRead);
        return checksum;
    }

    private static long checksum(long checksum, Row row)
    {
        for (Cell<?> cell : row.cells())
        {
            ByteBuffer value = cell.buffer().duplicate();
            while (value.hasRemaining())
                checksum = 31 * checksum + (value.get() & 0xFF);
        }
        return checksum;
    }

    private static ByteBuffer counterValue(ReadResponse response, ReadCommand command, TableMetadata metadata)
    {
        try (UnfilteredPartitionIterator partitions = response.makeIterator(command))
        {
            assertTrue(partitions.hasNext());
            try (UnfilteredRowIterator partition = partitions.next())
            {
                assertTrue(partition.hasNext());
                Row row = (Row) partition.next();
                Cell<?> cell = row.getCell(metadata.getColumn(ByteBufferUtil.bytes("count")));
                assertNotNull(cell);
                return cell.buffer().duplicate();
            }
        }
    }

    private static class LocalRunnableReadCommand extends SinglePartitionReadCommand
    {
        private final PartitionUpdate result;
        private boolean createLocalResponseCalled;

        private LocalRunnableReadCommand(TableMetadata metadata, DecoratedKey key, PartitionUpdate result)
        {
            super(metadata.epoch,
                  false,
                  MessagingService.current_version,
                  false,
                  PotentialTxnConflicts.ALLOW,
                  metadata,
                  FBUtilities.nowInSeconds(),
                  ColumnFilter.all(metadata),
                  RowFilter.none(),
                  DataLimits.NONE,
                  key,
                  new ClusteringIndexSliceFilter(Slices.ALL, false),
                  null,
                  false,
                  new DataRange(new Bounds<>(key, key), new ClusteringIndexSliceFilter(Slices.ALL, false)));
            this.result = result;
        }

        @Override
        public UnfilteredPartitionIterator executeLocally(ReadExecutionController controller)
        {
            return new SingletonUnfilteredPartitionIterator(result.unfilteredIterator());
        }

        @Override
        public ReadExecutionController executionController(boolean trackRepairedStatus)
        {
            return ReadExecutionController.empty();
        }

        @Override
        public ReadResponse createLocalResponse(UnfilteredPartitionIterator iterator, RepairedDataInfo rdi)
        {
            createLocalResponseCalled = true;
            return super.createLocalResponse(iterator, rdi);
        }
    }

    @SuppressWarnings("rawtypes")
    private static class CapturingReadCallback extends ReadCallback
    {
        private ReadResponse response;

        private CapturingReadCallback(ReadCommand command)
        {
            super(null, command, null, Dispatcher.RequestTime.forImmediateExecution());
        }

        @Override
        public void response(ReadResponse response)
        {
            this.response = response;
        }

        @Override
        public void onFailure(InetAddressAndPort from, RequestFailure failure)
        {
            throw new AssertionError("local read failed: " + failure);
        }
    }

    private static class InvalidatingRowIterator implements WrappingUnfilteredRowIterator
    {
        private final UnfilteredRowIterator wrapped;
        private final byte[] key;
        private final byte[] staticValue;
        private final byte[] firstValue;
        private final byte[] secondValue;
        private final byte[] markerStart;
        private boolean returnedRow;
        private boolean returnedMarker;
        private boolean rowsInvalidated;
        private boolean markerInvalidated;

        private InvalidatingRowIterator(UnfilteredRowIterator wrapped,
                                        byte[] key,
                                        byte[] staticValue,
                                        byte[] firstValue,
                                        byte[] secondValue,
                                        byte[] markerStart)
        {
            this.wrapped = wrapped;
            this.key = key;
            this.staticValue = staticValue;
            this.firstValue = firstValue;
            this.secondValue = secondValue;
            this.markerStart = markerStart;
        }

        public UnfilteredRowIterator wrapped()
        {
            return wrapped;
        }

        public boolean hasNext()
        {
            if (returnedRow && !rowsInvalidated)
            {
                staticValue[0] ^= 1;
                firstValue[0] ^= 1;
                rowsInvalidated = true;
            }
            if (returnedMarker && !markerInvalidated)
            {
                markerStart[markerStart.length - 1] ^= 1;
                markerInvalidated = true;
            }
            return wrapped.hasNext();
        }

        public Unfiltered next()
        {
            Unfiltered next = wrapped.next();
            returnedRow |= next.isRow();
            returnedMarker |= next.isRangeTombstoneMarker();
            return next;
        }

        public void close()
        {
            key[key.length - 1] ^= 1;
            secondValue[0] ^= 1;
            wrapped.close();
        }
    }

    private static class ExhaustionTrackingPartitionIterator extends AbstractUnfilteredPartitionIterator
    {
        private final UnfilteredPartitionIterator wrapped;
        private boolean rowsExhausted;

        private ExhaustionTrackingPartitionIterator(UnfilteredPartitionIterator wrapped)
        {
            this.wrapped = wrapped;
        }

        public TableMetadata metadata()
        {
            return wrapped.metadata();
        }

        public boolean hasNext()
        {
            return wrapped.hasNext();
        }

        public UnfilteredRowIterator next()
        {
            return new ExhaustionTrackingRowIterator(wrapped.next(), this);
        }

        public void close()
        {
            wrapped.close();
        }
    }

    private static class ExhaustionTrackingRowIterator implements WrappingUnfilteredRowIterator
    {
        private final UnfilteredRowIterator wrapped;
        private final ExhaustionTrackingPartitionIterator parent;

        private ExhaustionTrackingRowIterator(UnfilteredRowIterator wrapped, ExhaustionTrackingPartitionIterator parent)
        {
            this.wrapped = wrapped;
            this.parent = parent;
        }

        public UnfilteredRowIterator wrapped()
        {
            return wrapped;
        }

        public boolean hasNext()
        {
            boolean hasNext = wrapped.hasNext();
            if (!hasNext)
                parent.rowsExhausted = true;
            return hasNext;
        }
    }

    private static class ExhaustionTrackingRepairedDataInfo extends RepairedDataInfo
    {
        private final ExhaustionTrackingPartitionIterator data;
        private final ByteBuffer digest;
        private boolean digestRead;

        private ExhaustionTrackingRepairedDataInfo(ExhaustionTrackingPartitionIterator data, ByteBuffer digest)
        {
            super(null);
            this.data = data;
            this.digest = digest;
        }

        @Override
        public ByteBuffer getDigest()
        {
            assertTrue("the repaired digest was requested before the local iterator was fully consumed", data.rowsExhausted);
            digestRead = true;
            return digest;
        }

        @Override
        public boolean isConclusive()
        {
            return true;
        }
    }
}
