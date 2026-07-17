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

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.context.CounterContext;
import org.apache.cassandra.db.marshal.ByteBufferAccessor;
import org.apache.cassandra.db.marshal.CounterColumnType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.partitions.AbstractUnfilteredPartitionIterator;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.partitions.SingletonUnfilteredPartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.rows.WrappingUnfilteredRowIterator;
import org.apache.cassandra.distributed.test.log.ClusterMetadataTestHelper;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.CounterId;
import org.apache.cassandra.utils.FBUtilities;

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
        ReadResponse response = ReadResponse.createDataResponse(data, command, repairedDataInfo);

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

        ReadResponse local = ReadResponse.createDataResponse(new SingletonUnfilteredPartitionIterator(partition.unfilteredIterator()), command);
        ReadResponse remote = ReadResponse.createRemoteDataResponse(new SingletonUnfilteredPartitionIterator(partition.unfilteredIterator()),
                                                                      null,
                                                                      false,
                                                                      command,
                                                                      MessagingService.current_version);

        ByteBuffer localValue = counterValue(local, command, metadata);
        assertFalse(CounterContext.instance().shouldClearLocal(localValue, ByteBufferAccessor.instance));
        assertEquals(counterValue(remote, command, metadata), localValue);
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
