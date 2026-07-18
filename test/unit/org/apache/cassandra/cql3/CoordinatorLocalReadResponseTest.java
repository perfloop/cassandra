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

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.transport.Dispatcher;

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
