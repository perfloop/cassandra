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

import java.lang.reflect.Proxy;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.Warmup;

import org.apache.cassandra.db.ReadCommandTracingHelper;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.schema.IndexMetadata;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(value = 1)
@State(Scope.Benchmark)
public class TracingOverheadBench
{
    private String keyspace = "keyspace1";
    private String tableName = "table1";
    private Index.QueryPlan indexQueryPlan;

    @Setup
    public void setup()
    {
        IndexMetadata indexMetadata = IndexMetadata.fromSchemaMetadata("index1", IndexMetadata.Kind.KEYS, Collections.emptyMap());

        Index mockIndex = (Index) Proxy.newProxyInstance(
            Index.class.getClassLoader(),
            new Class<?>[] { Index.class },
            (proxy, method, args) -> {
                if (method.getName().equals("getIndexMetadata")) {
                    return indexMetadata;
                }
                return null;
            }
        );

        Set<Index> indexes = Collections.singleton(mockIndex);

        indexQueryPlan = (Index.QueryPlan) Proxy.newProxyInstance(
            Index.QueryPlan.class.getClassLoader(),
            new Class<?>[] { Index.QueryPlan.class },
            (proxy, method, args) -> {
                if (method.getName().equals("getIndexes")) {
                    return indexes;
                }
                return null;
            }
        );
    }

    @Benchmark
    public void run()
    {
        if (indexQueryPlan != null)
        {
            ReadCommandTracingHelper.traceIndexQueryPlan(keyspace, tableName, indexQueryPlan);
        }
    }
}
