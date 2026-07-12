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

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Setup;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.RangeTombstone;
import org.apache.cassandra.db.RangeTombstoneList;
import org.apache.cassandra.db.Slice;
import org.apache.cassandra.db.marshal.Int32Type;

/**
 * Shared abstract base class for RangeTombstoneList JMH microbenchmarks.
 * Concrete subclasses define their own @Param parameters and override getSize(),
 * avoiding static field-shadowing bugs during JMH setup.
 */
public abstract class RangeTombstoneListAbstractBench
{
    protected ClusteringComparator comparator;
    protected RangeTombstoneList existing;
    protected RangeTombstone tombstone;

    protected abstract int getSize();

    @Setup(Level.Trial)
    public void setup()
    {
        DatabaseDescriptor.daemonInitialization();
        comparator = new ClusteringComparator(Int32Type.instance);
        int size = getSize();
        existing = new RangeTombstoneList(comparator, size);
        for (int i = 0; i < size; i++)
        {
            existing.add(new RangeTombstone(Slice.make(comparator.make(i * 2), comparator.make(i * 2 + 1)), DeletionTime.build(1, 1)));
        }
        tombstone = new RangeTombstone(Slice.make(comparator.make(100000), comparator.make(100001)), DeletionTime.build(1, 1));
    }

    @Benchmark
    public RangeTombstoneList benchCopyOnly()
    {
        return existing.copy();
    }

    @Benchmark
    public RangeTombstoneList benchCopyAndAdd()
    {
        RangeTombstoneList copy = existing.copy();
        copy.add(tombstone);
        return copy;
    }
}
