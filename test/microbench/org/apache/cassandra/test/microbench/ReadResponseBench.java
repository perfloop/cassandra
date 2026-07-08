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

package org.apache.cassandra.test.microbench;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.*;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.*;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.db.partitions.*;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.distributed.test.log.ClusterMetadataTestHelper;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(value = 1, jvmArgsAppend = {
    "-Xmx1G",
    "-Dcassandra.config=file:./test/conf/cassandra.yaml",
    "--add-opens=java.base/java.io=ALL-UNNAMED",
    "--add-opens=java.base/java.lang=ALL-UNNAMED",
    "--add-opens=java.base/java.util=ALL-UNNAMED",
    "--add-opens=java.base/java.nio=ALL-UNNAMED",
    "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED"
})
@Threads(1)
@State(Scope.Benchmark)
public class ReadResponseBench
{
    private TableMetadata metadata;
    private DecoratedKey key;
    private ReadCommand command;

    private static class StubReadCommand extends SinglePartitionReadCommand
    {
        StubReadCommand(DecoratedKey key, TableMetadata metadata)
        {
            super(metadata.epoch,
                  false,
                  0,
                  false,
                  ReadCommand.PotentialTxnConflicts.DISALLOW,
                  metadata,
                  FBUtilities.nowInSeconds(),
                  ColumnFilter.all(metadata),
                  RowFilter.none(),
                  DataLimits.NONE,
                  key,
                  null,
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

    @Setup(Level.Trial)
    public void setup()
    {
        DatabaseDescriptor.daemonInitialization();
        ClusterMetadataTestHelper.setInstanceForTest();

        metadata = TableMetadata.builder("ks", "t1")
                                .offline()
                                .addPartitionKeyColumn("p", Int32Type.instance)
                                .addClusteringColumn("c", Int32Type.instance)
                                .addRegularColumn("v1", UTF8Type.instance)
                                .addRegularColumn("v2", UTF8Type.instance)
                                .addRegularColumn("v3", UTF8Type.instance)
                                .addRegularColumn("v4", UTF8Type.instance)
                                .partitioner(Murmur3Partitioner.instance)
                                .build();

        key = metadata.partitioner.decorateKey(ByteBufferUtil.bytes(42));
        command = new StubReadCommand(key, metadata);
    }

    private UnfilteredPartitionIterator createIterator()
    {
        return new UnfilteredPartitionIterator()
        {
            private int index = 0;

            public TableMetadata metadata() { return metadata; }

            public boolean hasNext() { return index < 1; }

            public UnfilteredRowIterator next()
            {
                index++;
                return new AbstractUnfilteredRowIterator(metadata, key, DeletionTime.LIVE, ColumnFilter.all(metadata).fetchedColumns(), Rows.EMPTY_STATIC_ROW, false, EncodingStats.NO_STATS)
                {
                    private int rowNum = 0;

                    @Override
                    protected Unfiltered computeNext()
                    {
                        if (rowNum >= 100)
                            return endOfData();

                        Row.Builder builder = BTreeRow.unsortedBuilder();
                        builder.newRow(Clustering.make(ByteBufferUtil.bytes(rowNum)));
                        for (int i = 0; i < 4; i++)
                        {
                            builder.addCell(BufferCell.live(metadata.regularColumns().getSimple(i), 123456L, ByteBufferUtil.bytes("value_" + rowNum + "_" + i)));
                        }
                        rowNum++;
                        return builder.build();
                    }
                };
            }

            public void close() {}
        };
    }

    @Benchmark
    public void roundTrip(org.openjdk.jmh.infra.Blackhole bh) throws Exception
    {
        UnfilteredPartitionIterator iter = createIterator();
        ReadResponse response = ReadResponse.createDataResponse(iter, command);
        try (UnfilteredPartitionIterator result = response.makeIterator(command))
        {
            while (result.hasNext())
            {
                try (UnfilteredRowIterator rowIter = result.next())
                {
                    bh.consume(rowIter.partitionKey());
                    while (rowIter.hasNext())
                    {
                        bh.consume(rowIter.next());
                    }
                }
            }
        }
    }
}
