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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.test.microbench;

import java.io.IOException;
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
import org.apache.cassandra.db.BufferClusteringBound;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.RangeTombstone;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadResponse;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.Slice;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.partitions.SingletonUnfilteredPartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterators;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.FBUtilities;

@Threads(1)
@State(Scope.Benchmark)
public class RemoteReadResponseBench
{
    private static final int WIDE_ROWS = 100;
    private static final int WIDE_COLUMNS = 4;
    private static final int VALUE_BYTES = 32;
    private static final int RANGE_PARTITIONS = 32;

    private ReadCommand command;
    private PartitionUpdate widePartition;
    private List<PartitionUpdate> rangePartitions;

    @Setup(Level.Trial)
    public void setup()
    {
        DatabaseDescriptor.daemonInitialization();

        TableMetadata.Builder table = TableMetadata.builder("ks", "remote_read_response_bench")
                                                    .offline()
                                                    .addPartitionKeyColumn("pk", Int32Type.instance)
                                                    .addClusteringColumn("ck", Int32Type.instance)
                                                    .addStaticColumn("static_value", UTF8Type.instance);
        for (int column = 0; column < WIDE_COLUMNS; column++)
            table.addRegularColumn("v" + column, UTF8Type.instance);

        TableMetadata metadata = table.partitioner(Murmur3Partitioner.instance).build();
        DecoratedKey key = metadata.partitioner.decorateKey(Int32Type.instance.decompose(1));
        command = SinglePartitionReadCommand.fullPartitionRead(metadata, FBUtilities.nowInSeconds(), key);
        widePartition = widePartition(metadata, key);
        rangePartitions = rangePartitions(metadata);
    }

    @Benchmark
    public long serializeWideResponse() throws IOException
    {
        return serialize(new SingletonUnfilteredPartitionIterator(widePartition.unfilteredIterator()));
    }

    @Benchmark
    public long serializeRangeResponse() throws IOException
    {
        List<UnfilteredPartitionIterator> iterators = new ArrayList<>(rangePartitions.size());
        for (PartitionUpdate partition : rangePartitions)
            iterators.add(new SingletonUnfilteredPartitionIterator(partition.unfilteredIterator()));
        return serialize(UnfilteredPartitionIterators.concat(iterators));
    }

    private long serialize(UnfilteredPartitionIterator input) throws IOException
    {
        ReadResponse response = ReadResponse.createDataResponse(input, command);
        long size = ReadResponse.serializer.serializedSize(response, MessagingService.current_version);
        try (DataOutputBuffer out = new DataOutputBuffer(Math.toIntExact(size)))
        {
            ReadResponse.serializer.serialize(response, out, MessagingService.current_version);
            if (out.getLength() != size)
                throw new AssertionError(String.format("expected serialized size %d but got %d", size, out.getLength()));
            return out.getLength();
        }
    }

    private static PartitionUpdate widePartition(TableMetadata metadata, DecoratedKey key)
    {
        PartitionUpdate.SimpleBuilder builder = PartitionUpdate.simpleBuilder(metadata, key).timestamp(1L);
        for (int row = 0; row < WIDE_ROWS; row++)
        {
            Row.SimpleBuilder rowBuilder = builder.row(row);
            for (int column = 0; column < WIDE_COLUMNS; column++)
            {
                byte[] value = new byte[VALUE_BYTES];
                ThreadLocalRandom.current().nextBytes(value);
                rowBuilder.add("v" + column, ByteBuffer.wrap(value));
            }
        }
        return builder.build();
    }

    private static List<PartitionUpdate> rangePartitions(TableMetadata metadata)
    {
        List<PartitionUpdate> partitions = new ArrayList<>(RANGE_PARTITIONS);
        for (int partition = 0; partition < RANGE_PARTITIONS; partition++)
        {
            DecoratedKey key = metadata.partitioner.decorateKey(Int32Type.instance.decompose(partition + 10));
            PartitionUpdate.SimpleBuilder builder = PartitionUpdate.simpleBuilder(metadata, key).timestamp(1L);
            builder.row().add("static_value", ByteBuffer.wrap(new byte[]{ (byte) partition }));
            builder.row(1).add("v0", ByteBuffer.wrap(new byte[]{ (byte) partition, 1 }));
            builder.addRangeTombstone(new RangeTombstone(Slice.make(BufferClusteringBound.inclusiveStartOf(Int32Type.instance.decompose(2)),
                                                                     BufferClusteringBound.inclusiveEndOf(Int32Type.instance.decompose(3))),
                                                          DeletionTime.build(1L, FBUtilities.nowInSeconds())));
            partitions.add(builder.build());
        }
        return partitions;
    }
}
