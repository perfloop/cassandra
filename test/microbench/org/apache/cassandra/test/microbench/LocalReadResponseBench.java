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

package org.apache.cassandra.test.microbench;

import java.nio.ByteBuffer;
import java.util.concurrent.ThreadLocalRandom;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadResponse;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.partitions.SingletonUnfilteredPartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.FBUtilities;

@Threads(1)
@State(Scope.Benchmark)
public class LocalReadResponseBench
{
    private static final int ROW_COUNT = 100;
    private static final int COLUMN_COUNT = 4;
    private static final int VALUE_BYTES = 32;

    private ReadCommand command;
    private PartitionUpdate partition;
    private long expectedChecksum;

    @Setup(Level.Trial)
    public void setup()
    {
        DatabaseDescriptor.daemonInitialization();

        TableMetadata.Builder table = TableMetadata.builder("ks", "local_read_response_bench")
                                                    .offline()
                                                    .addPartitionKeyColumn("pk", Int32Type.instance)
                                                    .addClusteringColumn("ck", Int32Type.instance);
        for (int column = 0; column < COLUMN_COUNT; column++)
            table.addRegularColumn("v" + column, UTF8Type.instance);

        TableMetadata metadata = table.partitioner(Murmur3Partitioner.instance).build();
        DecoratedKey key = metadata.partitioner.decorateKey(Int32Type.instance.decompose(1));
        command = SinglePartitionReadCommand.fullPartitionRead(metadata, FBUtilities.nowInSeconds(), key);

        PartitionUpdate.SimpleBuilder builder = PartitionUpdate.simpleBuilder(metadata, key).timestamp(1L);
        for (int row = 0; row < ROW_COUNT; row++)
        {
            Row.SimpleBuilder rowBuilder = builder.row(row);
            for (int column = 0; column < COLUMN_COUNT; column++)
            {
                byte[] value = new byte[VALUE_BYTES];
                ThreadLocalRandom.current().nextBytes(value);
                rowBuilder.add("v" + column, ByteBuffer.wrap(value));
            }
        }
        partition = builder.build();
        expectedChecksum = checksum(new SingletonUnfilteredPartitionIterator(partition.unfilteredIterator()));
    }

    @Benchmark
    public long materializeAndConsume()
    {
        ReadResponse response = command.createLocalResponse(new SingletonUnfilteredPartitionIterator(partition.unfilteredIterator()));
        return checksum(response.makeIterator(command));
    }

    private long checksum(UnfilteredPartitionIterator partitions)
    {
        long checksum = 1;
        int rows = 0;
        int cells = 0;
        try (UnfilteredPartitionIterator iterator = partitions)
        {
            while (iterator.hasNext())
            {
                try (UnfilteredRowIterator partition = iterator.next())
                {
                    while (partition.hasNext())
                    {
                        Unfiltered unfiltered = partition.next();
                        if (!unfiltered.isRow())
                            continue;

                        Row row = (Row) unfiltered;
                        rows++;
                        for (Cell<?> cell : row.cells())
                        {
                            cells++;
                            ByteBuffer value = cell.buffer().duplicate();
                            while (value.hasRemaining())
                                checksum = 31 * checksum + (value.get() & 0xFF);
                        }
                    }
                }
            }
        }

        if (rows != ROW_COUNT || cells != ROW_COUNT * COLUMN_COUNT || (expectedChecksum != 0 && checksum != expectedChecksum))
            throw new AssertionError(String.format("expected rows=%d cells=%d checksum=%d but got rows=%d cells=%d checksum=%d",
                                                   ROW_COUNT,
                                                   ROW_COUNT * COLUMN_COUNT,
                                                   expectedChecksum,
                                                   rows,
                                                   cells,
                                                   checksum));
        return checksum;
    }
}
