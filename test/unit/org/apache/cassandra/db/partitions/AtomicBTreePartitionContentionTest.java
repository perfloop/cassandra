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

package org.apache.cassandra.db.partitions;

import org.junit.Test;

import org.apache.cassandra.test.microbench.btree.AtomicBTreePartitionContentionBench;

import static org.assertj.core.api.Assertions.assertThat;

public class AtomicBTreePartitionContentionTest
{
    @Test
    public void contendedUpdatesActivatePessimisticLockingAndRetainEveryRow()
    {
        AtomicBTreePartitionContentionBench.Fixture fixture = new AtomicBTreePartitionContentionBench.Fixture();
        int activationRounds = fixture.activatePessimisticLocking();
        assertThat(fixture.hasEveryRow()).isFalse();

        long concurrentAllocations = fixture.runConcurrentUpdates();
        assertThat(fixture.hasEveryRow()).isTrue();

        long serialAllocations = fixture.runSerialUpdates();
        assertThat(fixture.hasEveryRow()).isTrue();
        assertThat(concurrentAllocations).isGreaterThanOrEqualTo(serialAllocations);

        System.out.println(String.format("AtomicBTreePartition contention clone allocations: concurrent=%d serial=%d activationRounds=%d",
                                         concurrentAllocations,
                                         serialAllocations,
                                         activationRounds));
    }
}
