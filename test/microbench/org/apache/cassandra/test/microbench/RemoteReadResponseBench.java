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

import com.sun.management.ThreadMXBean;

import org.openjdk.jmh.annotations.AuxCounters;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ReadCommandVerbHandler;
import org.apache.cassandra.db.ReadResponse;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.TestHelper;

@Threads(1)
@State(Scope.Benchmark)
public class RemoteReadResponseBench extends CQLTester
{
    private static final int COLUMNS = 4;
    private static final int ROW_COUNT = 100;
    private static final long CHECKSUM_SEED = 1_125_899_906_842_597L;
    private static final ThreadMXBean THREADS = (ThreadMXBean) ManagementFactory.getThreadMXBean();

    private SinglePartitionReadCommand template;
    private long expectedChecksum;

    @Setup(Level.Trial)
    public void setup() throws Throwable
    {
        CQLTester.setUpClass();

        String keyspace = createKeyspace("CREATE KEYSPACE %s WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 } AND durable_writes = false");
        String table = createTable(keyspace, "CREATE TABLE %s (pk int, ck int, v0 text, v1 text, v2 text, v3 text, PRIMARY KEY (pk, ck))");
        String statement = "INSERT INTO " + keyspace + '.' + table + " (pk, ck, v0, v1, v2, v3) VALUES (?, ?, ?, ?, ?, ?)";

        for (int row = 0; row < ROW_COUNT; row++)
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
        expectedChecksum = serializedResponseChecksum(template.copy());

        // The measured path only reads the populated partition; stop the fixture's periodic sync thread.
        CommitLog.instance.shutdownBlocking();
    }

    @TearDown(Level.Trial)
    public void teardown() throws Exception
    {
        TestHelper.teardown();
    }

    @Benchmark
    public long remoteWideReadResponse(AllocationCounter allocation) throws Throwable
    {
        long allocatedBefore = THREADS.getThreadAllocatedBytes(allocation.threadId);
        long checksum = serializedResponseChecksum(template.copy());
        if (checksum != expectedChecksum)
            throw new AssertionError("Unexpected remote read response");

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

    private static long serializedResponseChecksum(SinglePartitionReadCommand command) throws IOException
    {
        ReadResponse response = ReadCommandVerbHandler.instance.doRead(command, false);
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            ReadResponse.serializer.serialize(response, out, MessagingService.current_version);
            return updateChecksum(CHECKSUM_SEED, out.buffer(false));
        }
    }

    private static String value(int row, int column)
    {
        return "row-" + row + "-column-" + column + "-0123456789abcdef";
    }

    private static long updateChecksum(long checksum, ByteBuffer value)
    {
        for (int index = value.position(); index < value.limit(); index++)
            checksum = 31 * checksum + (value.get(index) & 0xFF);
        return checksum;
    }
}
