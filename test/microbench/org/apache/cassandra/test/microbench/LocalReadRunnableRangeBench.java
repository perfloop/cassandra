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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadCommand.PotentialTxnConflicts;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.ReadResponse;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.filter.ClusteringIndexSliceFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.partitions.SingletonUnfilteredPartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterators;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.exceptions.RequestFailure;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.service.reads.ReadCallback;
import org.apache.cassandra.transport.Dispatcher;
import org.apache.cassandra.utils.FBUtilities;

@Threads(1)
@State(Scope.Benchmark)
public class LocalReadRunnableRangeBench
{
    private static final int PARTITION_COUNT = 32;
    private static final int VALUE_BYTES = 32;

    private TableMetadata metadata;
    private List<PartitionUpdate> partitions;
    private long expectedChecksum;

    @Setup(Level.Trial)
    public void setup()
    {
        DatabaseDescriptor.daemonInitialization();

        metadata = TableMetadata.builder("ks", "local_read_runnable_range_bench")
                                .offline()
                                .addPartitionKeyColumn("pk", Int32Type.instance)
                                .addClusteringColumn("ck", Int32Type.instance)
                                .addStaticColumn("static_value", UTF8Type.instance)
                                .addRegularColumn("v0", UTF8Type.instance)
                                .partitioner(Murmur3Partitioner.instance)
                                .build();
        partitions = new ArrayList<>(PARTITION_COUNT);
        for (int partition = 0; partition < PARTITION_COUNT; partition++)
            partitions.add(partition(partition));

        expectedChecksum = checksum(iterators());
    }

    @Benchmark
    public long localRangeResponse()
    {
        LocalRangeReadCommand command = new LocalRangeReadCommand(metadata, partitions);
        if (!command.isRangeRequest())
            throw new AssertionError("benchmark command must be a range request");
        CapturingReadCallback callback = new CapturingReadCallback(command);
        new StorageProxy.LocalReadRunnable(command, callback, Dispatcher.RequestTime.forImmediateExecution()).run();
        if (callback.response == null)
            throw new AssertionError("local read did not produce a response");
        return checksum(callback.response.makeIterator(command));
    }

    private PartitionUpdate partition(int value)
    {
        DecoratedKey key = metadata.partitioner.decorateKey(Int32Type.instance.decompose(value));
        PartitionUpdate.SimpleBuilder builder = PartitionUpdate.simpleBuilder(metadata, key).timestamp(1L);
        builder.row().add("static_value", ByteBuffer.wrap(randomValue()));
        builder.row(value).add("v0", ByteBuffer.wrap(randomValue()));
        builder.addRangeTombstone().start(value + PARTITION_COUNT).end(value + PARTITION_COUNT + 1);
        return builder.build();
    }

    private UnfilteredPartitionIterator iterators()
    {
        List<UnfilteredPartitionIterator> iterators = new ArrayList<>(partitions.size());
        for (PartitionUpdate partition : partitions)
            iterators.add(new SingletonUnfilteredPartitionIterator(partition.unfilteredIterator()));
        return UnfilteredPartitionIterators.concat(iterators);
    }

    private static byte[] randomValue()
    {
        byte[] value = new byte[VALUE_BYTES];
        ThreadLocalRandom.current().nextBytes(value);
        return value;
    }

    private long checksum(UnfilteredPartitionIterator partitions)
    {
        long checksum = 1;
        int partitionCount = 0;
        int rowCount = 0;
        int cellCount = 0;
        try (UnfilteredPartitionIterator iterator = partitions)
        {
            while (iterator.hasNext())
            {
                partitionCount++;
                try (UnfilteredRowIterator partition = iterator.next())
                {
                    checksum = checksum(checksum, partition.staticRow());
                    cellCount += partition.staticRow().columnCount();
                    while (partition.hasNext())
                    {
                        Unfiltered unfiltered = partition.next();
                        if (!unfiltered.isRow())
                            continue;

                        Row row = (Row) unfiltered;
                        rowCount++;
                        cellCount += row.columnCount();
                        checksum = checksum(checksum, row);
                    }
                }
            }
        }

        if (partitionCount != PARTITION_COUNT || rowCount != PARTITION_COUNT || cellCount != 2 * PARTITION_COUNT ||
            (expectedChecksum != 0 && checksum != expectedChecksum))
            throw new AssertionError(String.format("unexpected response contents: partitions=%d/%d, rows=%d/%d, cells=%d/%d, checksum=%d/%d",
                                                   partitionCount,
                                                   PARTITION_COUNT,
                                                   rowCount,
                                                   PARTITION_COUNT,
                                                   cellCount,
                                                   2 * PARTITION_COUNT,
                                                   checksum,
                                                   expectedChecksum));
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

    private static class LocalRangeReadCommand extends SinglePartitionReadCommand
    {
        private final List<PartitionUpdate> partitions;

        private LocalRangeReadCommand(TableMetadata metadata, List<PartitionUpdate> partitions)
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
                  partitions.get(0).partitionKey(),
                  new ClusteringIndexSliceFilter(Slices.ALL, false),
                  null,
                  false,
                  new DataRange(new Bounds<>(partitions.get(0).partitionKey(), partitions.get(0).partitionKey()),
                                new ClusteringIndexSliceFilter(Slices.ALL, false)));
            this.partitions = partitions;
        }

        @Override
        public boolean isRangeRequest()
        {
            return true;
        }

        @Override
        public UnfilteredPartitionIterator executeLocally(ReadExecutionController controller)
        {
            List<UnfilteredPartitionIterator> iterators = new ArrayList<>(partitions.size());
            for (PartitionUpdate partition : partitions)
                iterators.add(new SingletonUnfilteredPartitionIterator(partition.unfilteredIterator()));
            return UnfilteredPartitionIterators.concat(iterators);
        }

        @Override
        public ReadExecutionController executionController(boolean trackRepairedStatus)
        {
            return ReadExecutionController.empty();
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
}
