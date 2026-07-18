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
package org.apache.cassandra.test.microbench;

import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

import com.sun.management.ThreadMXBean;

import org.openjdk.jmh.annotations.AuxCounters;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

import org.apache.cassandra.concurrent.ExecutorPlus;
import org.apache.cassandra.concurrent.ImmediateExecutor;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.transport.Dispatcher;
import org.apache.cassandra.utils.TestHelper;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 1, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(1)
@Threads(1)
@State(Scope.Benchmark)
public class CoordinatorLocalReadResponseBench extends CQLTester
{
    private static final int COLUMNS = 4;
    private static final long CHECKSUM_SEED = 1_125_899_906_842_597L;
    private static final ThreadMXBean THREADS = (ThreadMXBean) ManagementFactory.getThreadMXBean();

    @Param({ "100" })
    public int rowCount;

    private SinglePartitionReadCommand template;
    private long expectedChecksum;
    private ExecutorPlus readStage;

    @Setup(Level.Trial)
    public void setup() throws Throwable
    {
        CQLTester.setUpClass();

        String keyspace = createKeyspace("CREATE KEYSPACE %s WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 } AND durable_writes = false");
        String table = createTable(keyspace, "CREATE TABLE %s (pk int, ck int, v0 text, v1 text, v2 text, v3 text, PRIMARY KEY (pk, ck))");
        String statement = "INSERT INTO " + keyspace + '.' + table + " (pk, ck, v0, v1, v2, v3) VALUES (?, ?, ?, ?, ?, ?)";

        for (int row = 0; row < rowCount; row++)
        {
            executeInternal(statement,
                            0,
                            row,
                            value(row, 0),
                            value(row, 1),
                            value(row, 2),
                            value(row, 3));
        }

        template = parseReadCommandGroup("SELECT ck, v0, v1, v2, v3 FROM " + keyspace + '.' + table + " WHERE pk = 0").queries.get(0);
        expectedChecksum = expectedChecksum();

        // The measured path only reads the populated partition; stop the fixture's periodic sync thread.
        CommitLog.instance.shutdownBlocking();

        // StorageProxy submits local reads with Stage.READ.maybeExecuteImmediately. This isolated benchmark
        // measures the no-queue path, so run LocalReadRunnable on the JMH worker that owns the allocation counter.
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
    public long coordinatorLocalWideRead(AllocationCounter allocation) throws Throwable
    {
        long allocatedBefore = THREADS.getThreadAllocatedBytes(allocation.threadId);
        SinglePartitionReadCommand command = template.copy();
        long checksum;
        try (PartitionIterator partitions = StorageProxy.read(SinglePartitionReadCommand.Group.one(command),
                                                              ConsistencyLevel.ONE,
                                                              Dispatcher.RequestTime.forImmediateExecution()))
        {
            int rows = 0;
            int cells = 0;
            checksum = CHECKSUM_SEED;
            while (partitions.hasNext())
            {
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
            }

            if (rows != rowCount || cells != rowCount * COLUMNS || checksum != expectedChecksum)
                throw new AssertionError("Unexpected coordinator read result");
        }
        allocation.threadAllocatedBytes += THREADS.getThreadAllocatedBytes(allocation.threadId) - allocatedBefore;
        allocation.operations++;
        return checksum;
    }

    @State(Scope.Thread)
    @AuxCounters(AuxCounters.Type.EVENTS)
    public static class AllocationCounter
    {
        // The GC profiler sums process-wide allocation, including server fixture threads.
        // Count only allocations made by the isolated JMH worker during each read instead.
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

    private long expectedChecksum()
    {
        long checksum = CHECKSUM_SEED;
        for (int row = 0; row < rowCount; row++)
        {
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
