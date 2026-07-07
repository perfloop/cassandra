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

import java.util.Random;
import java.util.concurrent.TimeUnit;

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
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

import org.apache.cassandra.cql3.CQLTester;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 10, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(value = 1)
@Threads(1)
@State(Scope.Benchmark)
public class TracingOverheadBench extends CQLTester
{
    @Param({"YES"})
    String flush = "YES";

    private String keyspace;
    private String table;
    private Random rand;
    private int count = 50000;

    @Setup(Level.Trial)
    public void setup() throws Throwable
    {
        rand = new Random(1);
        CQLTester.setUpClass(); // This prepares the server
        
        keyspace = createKeyspace("CREATE KEYSPACE %s with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 } and durable_writes = false");
        table = createTable(keyspace, "CREATE TABLE %s ( userid bigint, picid bigint, commentid bigint, PRIMARY KEY(userid, picid)) with compression = {'enabled': false}");
        execute("use " + keyspace + ";");
        
        // Create secondary index on commentid
        createIndex(keyspace, "CREATE INDEX ON %s(commentid)");

        // Write the data
        System.err.println("Writing " + count);
        String writeStatement = "INSERT INTO " + table + "(userid,picid,commentid) VALUES(?,?,?)";
        for (long i = 0; i < count; i++)
        {
            execute(writeStatement, i, i, i);
        }

        if ("YES".equals(flush))
        {
            org.apache.cassandra.db.Keyspace.open(keyspace)
                .getColumnFamilyStore(currentTable())
                .forceBlockingFlush(org.apache.cassandra.db.ColumnFamilyStore.FlushReason.USER_FORCED);
        }
    }

    @Benchmark
    public Object readIndex() throws Throwable
    {
        long target = rand.nextInt(count);
        return execute("SELECT * FROM " + table + " WHERE commentid = ?", target);
    }
}
