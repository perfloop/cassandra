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

package org.apache.cassandra.test.microbench.instance;

import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.*;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(value = 1)
@Threads(1)
@State(Scope.Benchmark)
public class NamesQueryMultiSSTablesBench extends CQLTester
{
    private String keyspace;
    private String table;
    private ColumnFamilyStore cfs;

    @Param({"5", "20"})
    int numSSTables = 20;

    @Param({"50", "100"})
    int numRows = 100;

    @Setup(Level.Trial)
    public void setup() throws Throwable
    {
        CQLTester.setUpClass();
        DatabaseDescriptor.setAutoSnapshot(false);

        keyspace = createKeyspace("CREATE KEYSPACE %s with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 } and durable_writes = false");
        table = createTable(keyspace,
                            "CREATE TABLE %s ( userid bigint, picid bigint, commentid bigint, PRIMARY KEY(userid, picid)) with compression = {'enabled': false}");
        execute("use " + keyspace + ";");

        cfs = Keyspace.open(keyspace).getColumnFamilyStore(table);
        cfs.disableAutoCompaction();

        // Write rows into multiple SSTables
        for (int s = 0; s < numSSTables; s++)
        {
            for (int r = 0; r < numRows; r++)
            {
                execute("INSERT INTO " + table + " (userid, picid, commentid) VALUES (1, ?, ?)", (long) r, (long) (s * 1000 + r));
            }
            cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.USER_FORCED);
        }

        System.err.println("Setup done. Created " + cfs.getLiveSSTables().size() + " SSTables.");
    }

    @Benchmark
    public Object queryNamesMultiSSTables() throws Throwable
    {
        StringBuilder sb = new StringBuilder("SELECT * FROM " + table + " WHERE userid = 1 AND picid IN (");
        for (int r = 0; r < numRows; r++)
        {
            if (r > 0) sb.append(",");
            sb.append(r);
        }
        sb.append(")");
        return execute(sb.toString());
    }
}
