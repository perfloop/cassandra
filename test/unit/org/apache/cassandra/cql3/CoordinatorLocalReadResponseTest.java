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
package org.apache.cassandra.cql3;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.ReadResponse;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.context.CounterContext;
import org.apache.cassandra.db.marshal.ByteBufferAccessor;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.transport.Dispatcher;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.apache.cassandra.config.CassandraRelevantProperties.TEST_LOCAL_RESPONSE_REQUIRE_OFFHEAP;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class CoordinatorLocalReadResponseTest extends CQLTester
{
    private static final int ROWS = 100;
    private static final int COLUMNS = 4;
    private static final long CHECKSUM_SEED = 1_125_899_906_842_597L;

    @BeforeClass
    public static void enableCoordinatorReadExecution()
    {
        enableCoordinatorExecution();
    }

    @Test
    public void readsWholeWidePartitionThroughCoordinatorLocalResponse() throws Throwable
    {
        String keyspace = createKeyspace("CREATE KEYSPACE %s WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 } AND durable_writes = false");
        String table = createTable(keyspace, "CREATE TABLE %s (pk int, ck int, v0 text, v1 text, v2 text, v3 text, PRIMARY KEY (pk, ck))");

        populate(keyspace, table);

        assertCoordinatorRead(keyspace, table, false);
        assertCoordinatorRead(keyspace, table, true);
    }

    @Test
    public void retainsMaterializedResponseAfterSourceReleaseAndMemtableFlush() throws Throwable
    {
        assertOffHeapObjectsIfRequested();
        String keyspace = createKeyspace("CREATE KEYSPACE %s WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 } AND durable_writes = false");
        String table = createTable(keyspace, "CREATE TABLE %s (pk int PRIMARY KEY, v text)");
        executeInternal("INSERT INTO " + keyspace + '.' + table + " (pk, v) VALUES (0, 'before-flush')");

        SinglePartitionReadCommand command = parseReadCommandGroup("SELECT v FROM " + keyspace + '.' + table + " WHERE pk = 0").queries.get(0);
        ReadResponse response = materializeLocalResponse(command);
        ByteBuffer digest = response.digest(command);

        getColumnFamilyStore(keyspace, table).forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);

        assertEquals(ByteBufferUtil.bytes("before-flush"), onlyCell(response, command).buffer());
        assertEquals(digest, response.digest(command));
    }

    @Test
    public void clearsMarkedNativeCounterContextBeforeSourceRelease() throws Throwable
    {
        assertOffHeapObjectsIfRequested();
        String keyspace = createKeyspace("CREATE KEYSPACE %s WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 } AND durable_writes = false");
        String table = createTable(keyspace, "CREATE TABLE %s (pk int PRIMARY KEY, c counter)");
        SinglePartitionReadCommand command = parseReadCommandGroup("SELECT c FROM " + keyspace + '.' + table + " WHERE pk = 0").queries.get(0);
        ByteBuffer marked = CounterContext.instance().markLocalToBeCleared(CounterContext.instance().createLocal(1));
        assertTrue(CounterContext.instance().shouldClearLocal(marked, ByteBufferAccessor.instance));
        PartitionUpdate update = new RowUpdateBuilder(command.metadata(), 1L, 0).add("c", marked).buildUpdate();
        new Mutation(update).apply();

        ReadResponse response = materializeLocalResponse(command);
        ByteBuffer digest = response.digest(command);

        getColumnFamilyStore(keyspace, table).forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);

        Cell<?> cell = onlyCell(response, command);
        assertFalse(CounterContext.instance().shouldClearLocal(cell.buffer(), ByteBufferAccessor.instance));
        assertEquals(1L, CounterContext.instance().total(cell));
        assertEquals(digest, response.digest(command));
    }

    private static void assertOffHeapObjectsIfRequested()
    {
        if (TEST_LOCAL_RESPONSE_REQUIRE_OFFHEAP.getBoolean())
            assertEquals(Config.MemtableAllocationType.offheap_objects, DatabaseDescriptor.getMemtableAllocationType());
    }

    private static ReadResponse materializeLocalResponse(SinglePartitionReadCommand command)
    {
        try (ReadExecutionController controller = command.executionController();
             UnfilteredPartitionIterator source = command.executeLocally(controller))
        {
            return command.createResponseForLocalRead(source, controller.getRepairedDataInfo());
        }
    }

    private static Cell<?> onlyCell(ReadResponse response, SinglePartitionReadCommand command)
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

    private void populate(String keyspace, String table) throws Throwable
    {
        String statement = "INSERT INTO " + keyspace + '.' + table + " (pk, ck, v0, v1, v2, v3) VALUES (?, ?, ?, ?, ?, ?)";
        for (int row = 0; row < ROWS; row++)
        {
            executeInternal(statement,
                            0,
                            row,
                            value(row, 0),
                            value(row, 1),
                            value(row, 2),
                            value(row, 3));
        }
    }

    private void assertCoordinatorRead(String keyspace, String table, boolean reversed) throws Throwable
    {
        String statement = "SELECT ck, v0, v1, v2, v3 FROM " + keyspace + '.' + table + " WHERE pk = 0";
        if (reversed)
            statement += " ORDER BY ck DESC";

        SinglePartitionReadCommand command = parseReadCommandGroup(statement).queries.get(0);
        try (PartitionIterator partitions = StorageProxy.read(SinglePartitionReadCommand.Group.one(command),
                                                              ConsistencyLevel.ONE,
                                                              Dispatcher.RequestTime.forImmediateExecution()))
        {
            assertTrue(partitions.hasNext());

            int rows = 0;
            int cells = 0;
            long checksum = CHECKSUM_SEED;
            try (RowIterator iterator = partitions.next())
            {
                while (iterator.hasNext())
                {
                    Row row = iterator.next();
                    rows++;
                    for (Cell<?> cell : row.cells())
                    {
                        cells++;
                        checksum = updateChecksum(checksum, cell.buffer());
                    }
                }
            }

            assertFalse(partitions.hasNext());
            assertEquals(ROWS, rows);
            assertEquals(ROWS * COLUMNS, cells);
            assertEquals(expectedChecksum(reversed), checksum);
        }
    }

    private static long expectedChecksum(boolean reversed)
    {
        long checksum = CHECKSUM_SEED;
        for (int index = 0; index < ROWS; index++)
        {
            int row = reversed ? ROWS - index - 1 : index;
            for (int column = 0; column < COLUMNS; column++)
                checksum = updateChecksum(checksum, value(row, column).getBytes(StandardCharsets.UTF_8));
        }
        return checksum;
    }

    private static String value(int row, int column)
    {
        return "row-" + row + "-column-" + column + "-0123456789abcdef";
    }

    private static long updateChecksum(long checksum, byte[] value)
    {
        for (byte b : value)
            checksum = 31 * checksum + (b & 0xFF);
        return checksum;
    }

    private static long updateChecksum(long checksum, ByteBuffer value)
    {
        for (int index = value.position(); index < value.limit(); index++)
            checksum = 31 * checksum + (value.get(index) & 0xFF);
        return checksum;
    }
}
