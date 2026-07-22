/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.harry.test;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;

public class HarryMemtableRangeDeletionCompactionTest extends CQLTester
{
    private static final int RANGE_COUNT = 128;

    @Test
    public void sequentialMemtableRangeDeletesSurviveFlushAndCompaction() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, value int, PRIMARY KEY (pk, ck)) "
                    + "WITH gc_grace_seconds = 864000");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (int i = 0; i < RANGE_COUNT; i++)
        {
            execute("INSERT INTO %s (pk, ck, value) VALUES (?, ?, ?) USING TIMESTAMP 1", 0, 2 * i, i);
            execute("INSERT INTO %s (pk, ck, value) VALUES (?, ?, ?) USING TIMESTAMP 1", 0, 2 * i + 1, i);
        }

        for (int i = 0; i < RANGE_COUNT; i++)
            execute("DELETE FROM %s USING TIMESTAMP 2 WHERE pk = ? AND ck >= ? AND ck < ?", 0, 2 * i, 2 * i + 1);

        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);
        execute("INSERT INTO %s (pk, ck, value) VALUES (?, ?, ?) USING TIMESTAMP 3", 0, 2 * RANGE_COUNT, RANGE_COUNT);
        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);
        cfs.forceMajorCompaction();

        List<Object[]> expected = new ArrayList<>();
        for (int i = 0; i < RANGE_COUNT; i++)
            expected.add(row(2 * i + 1, i));
        expected.add(row(2 * RANGE_COUNT, RANGE_COUNT));
        assertRows(execute("SELECT ck, value FROM %s WHERE pk = ?", 0), expected);
    }
}
