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
package org.apache.cassandra.test.microbench;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import com.sun.management.ThreadMXBean;

import org.openjdk.jmh.annotations.AuxCounters;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;

import org.apache.cassandra.concurrent.ExecutorPlus;
import org.apache.cassandra.concurrent.ImmediateExecutor;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.PartitionRangeReadCommand;
import org.apache.cassandra.db.ReadResponse;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.partitions.SingletonUnfilteredPartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterators;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.service.reads.ReadCoordinator;
import org.apache.cassandra.transport.Dispatcher;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.TestHelper;

@Threads(1)
@State(Scope.Benchmark)
public class RangeReadResponseBench extends CQLTester
{
    private static final int COLUMNS = 4;
    private static final int PARTITIONS = 16;
    private static final int ROWS_PER_PARTITION = 4;
    private static final int ROW_COUNT = PARTITIONS * ROWS_PER_PARTITION;
    private static final long CHECKSUM_SEED = 1_125_899_906_842_597L;
    private static final ThreadMXBean THREADS = (ThreadMXBean) ManagementFactory.getThreadMXBean();

    private PartitionRangeReadCommand rangeTemplate;
    private List<PartitionUpdate> mergeUpdates;
    private long expectedRowChecksum;
    private long expectedMergedResponseChecksum;
    private ExecutorPlus readStage;

    @Setup(Level.Trial)
    public void setup() throws Throwable
    {
        CQLTester.setUpClass();

        String keyspace = createKeyspace("CREATE KEYSPACE %s WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 } AND durable_writes = false");
        String table = createTable(keyspace, "CREATE TABLE %s (pk int, ck int, v0 text, v1 text, v2 text, v3 text, PRIMARY KEY (pk, ck))");
        String statement = "INSERT INTO " + keyspace + '.' + table + " (pk, ck, v0, v1, v2, v3) VALUES (?, ?, ?, ?, ?, ?)";

        for (int partition = 0; partition < PARTITIONS; partition++)
        {
            for (int row = 0; row < ROWS_PER_PARTITION; row++)
            {
                executeInternal(statement,
                                partition,
                                row,
                                value(partition, row, 0),
                                value(partition, row, 1),
                                value(partition, row, 2),
                                value(partition, row, 3));
            }
        }

        TableMetadata metadata = getColumnFamilyStore(keyspace, table).metadata();
        rangeTemplate = PartitionRangeReadCommand.create(metadata,
                                                          FBUtilities.nowInSeconds(),
                                                          ColumnFilter.all(metadata),
                                                          RowFilter.none(),
                                                          DataLimits.cqlLimits(ROW_COUNT),
                                                          DataRange.allData(metadata.partitioner));
        mergeUpdates = mergeUpdates();
        expectedRowChecksum = expectedRowChecksum();
        expectedMergedResponseChecksum = serializedMergedResponseChecksum();
        verifyMergedResponse();

        // The measured paths only read the fixed fixture; stop the fixture's periodic sync thread.
        CommitLog.instance.shutdownBlocking();

        // Run local range reads on the JMH worker so the allocation counter excludes queue workers.
        readStage = Stage.READ.executor();
        Stage.READ.unsafeSetExecutor(ImmediateExecutor.INSTANCE);
    }

    @TearDown(Level.Trial)
    public void teardown() throws Exception
    {
        try
        {
            Stage.READ.unsafeSetExecutor(readStage);
        }
        finally
        {
            TestHelper.teardown();
        }
    }

    @Benchmark
    public long coordinatorLocalRangeRead(AllocationCounter allocation) throws Throwable
    {
        long allocatedBefore = THREADS.getThreadAllocatedBytes(allocation.threadId);
        long checksum = consumeRange(StorageProxy.getRangeSlice(rangeTemplate.copy(),
                                                                 ConsistencyLevel.ONE,
                                                                 ReadCoordinator.DEFAULT,
                                                                 Dispatcher.RequestTime.forImmediateExecution()),
                                     ROW_COUNT);
        if (checksum != expectedRowChecksum)
            throw new AssertionError("Unexpected coordinator-local range read result");

        allocation.threadAllocatedBytes += THREADS.getThreadAllocatedBytes(allocation.threadId) - allocatedBefore;
        allocation.operations++;
        return checksum;
    }

    @Benchmark
    public long mergedRangeResponse(AllocationCounter allocation) throws Throwable
    {
        long allocatedBefore = THREADS.getThreadAllocatedBytes(allocation.threadId);
        long checksum = serializedMergedResponseChecksum();
        if (checksum != expectedMergedResponseChecksum)
            throw new AssertionError("Unexpected merged range response");

        allocation.threadAllocatedBytes += THREADS.getThreadAllocatedBytes(allocation.threadId) - allocatedBefore;
        allocation.operations++;
        return checksum;
    }

    @State(Scope.Thread)
    @AuxCounters(AuxCounters.Type.EVENTS)
    public static class AllocationCounter
    {
        public long threadAllocatedBytes;
        public long operations;
        private long threadId;

        @Setup(Level.Trial)
        public void enableThreadAllocationTracking()
        {
            if (!THREADS.isThreadAllocatedMemorySupported())
                throw new IllegalStateException("Thread allocation tracking is unavailable");

            THREADS.setThreadAllocatedMemoryEnabled(true);
            threadId = Thread.currentThread().getId();
        }

        @Setup(Level.Iteration)
        public void reset()
        {
            threadAllocatedBytes = 0;
            operations = 0;
        }
    }

    private List<PartitionUpdate> mergeUpdates()
    {
        List<PartitionUpdate> updates = new ArrayList<>(PARTITIONS);
        for (int partition = 0; partition < PARTITIONS; partition++)
        {
            RowUpdateBuilder update = new RowUpdateBuilder(rangeTemplate.metadata(), 1L, partition).clustering(0);
            for (int column = 0; column < COLUMNS; column++)
                update.add("v" + column, value(partition, 0, column));
            updates.add(update.buildUpdate());
        }
        updates.sort(Comparator.comparing(PartitionUpdate::partitionKey));
        return updates;
    }

    private long serializedMergedResponseChecksum() throws IOException
    {
        List<ReadResponse> responses = new ArrayList<>(mergeUpdates.size());
        for (PartitionUpdate update : mergeUpdates)
        {
            responses.add(ReadResponse.createDataResponse(new SingletonUnfilteredPartitionIterator(update.unfilteredIterator()),
                                                          rangeTemplate));
        }

        ReadResponse merged = ReadResponse.merge(responses, rangeTemplate);
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            ReadResponse.serializer.serialize(merged, out, MessagingService.current_version);
            return responseChecksum(out.buffer(false));
        }
    }

    private void verifyMergedResponse() throws IOException
    {
        List<ReadResponse> responses = new ArrayList<>(mergeUpdates.size());
        for (PartitionUpdate update : mergeUpdates)
        {
            responses.add(ReadResponse.createDataResponse(new SingletonUnfilteredPartitionIterator(update.unfilteredIterator()),
                                                          rangeTemplate));
        }

        ReadResponse merged = ReadResponse.merge(responses, rangeTemplate);
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            ReadResponse.serializer.serialize(merged, out, MessagingService.current_version);
            ReadResponse decoded = ReadResponse.serializer.deserialize(new DataInputBuffer(out.buffer(false), false), MessagingService.current_version);
            long checksum = consumeRange(UnfilteredPartitionIterators.filter(decoded.makeIterator(rangeTemplate), rangeTemplate.nowInSec()),
                                         PARTITIONS);
            if (checksum != expectedMergeRowChecksum())
                throw new AssertionError("Unexpected decoded merged range response");
        }
    }

    private static long consumeRange(PartitionIterator partitions, int expectedRows)
    {
        try (PartitionIterator iterator = partitions)
        {
            int rows = 0;
            int cells = 0;
            long checksum = 0;
            while (iterator.hasNext())
            {
                try (RowIterator partition = iterator.next())
                {
                    while (partition.hasNext())
                    {
                        Row row = partition.next();
                        rows++;
                        for (Cell<?> cell : row.cells())
                        {
                            cells++;
                            checksum += valueChecksum(cell.buffer());
                        }
                    }
                }
            }

            if (rows != expectedRows || cells != expectedRows * COLUMNS)
                throw new AssertionError("Unexpected range read row or cell count");
            return checksum;
        }
    }

    private long expectedRowChecksum()
    {
        long checksum = 0;
        for (int partition = 0; partition < PARTITIONS; partition++)
        {
            for (int row = 0; row < ROWS_PER_PARTITION; row++)
            {
                for (int column = 0; column < COLUMNS; column++)
                    checksum += valueChecksum(value(partition, row, column).getBytes());
            }
        }
        return checksum;
    }

    private long expectedMergeRowChecksum()
    {
        long checksum = 0;
        for (int partition = 0; partition < PARTITIONS; partition++)
        {
            for (int column = 0; column < COLUMNS; column++)
                checksum += valueChecksum(value(partition, 0, column).getBytes());
        }
        return checksum;
    }

    private static String value(int partition, int row, int column)
    {
        return "partition-" + partition + "-row-" + row + "-column-" + column + "-0123456789abcdef";
    }

    private static long valueChecksum(byte[] value)
    {
        long checksum = CHECKSUM_SEED;
        for (byte b : value)
            checksum = 31 * checksum + (b & 0xFF);
        return checksum;
    }

    private static long valueChecksum(ByteBuffer value)
    {
        long checksum = CHECKSUM_SEED;
        for (int index = value.position(); index < value.limit(); index++)
            checksum = 31 * checksum + (value.get(index) & 0xFF);
        return checksum;
    }

    private static long responseChecksum(ByteBuffer value)
    {
        long checksum = CHECKSUM_SEED;
        for (int index = value.position(); index < value.limit(); index++)
            checksum = 31 * checksum + (value.get(index) & 0xFF);
        return checksum;
    }
}
