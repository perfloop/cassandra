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

import java.nio.ByteBuffer;

import org.junit.Test;

import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DeletionInfo;
import org.apache.cassandra.db.MutableDeletionInfo;
import org.apache.cassandra.db.Slice;
import org.apache.cassandra.test.microbench.partitions.DeletionInfoTransitionSupport;

import static org.junit.Assert.assertNotSame;

public class DeletionInfoTransitionTest
{
    @Test
    public void exactBoundTwoRangeTransitionsPreserveSnapshotsAndMaterializations()
    {
        for (int rangeCount : new int[]{ 128, 4096 })
        {
            for (String separation : new String[]{ "ADJACENT", "OPPOSITE" })
            {
                DeletionInfoTransitionSupport.Fixture fixture = DeletionInfoTransitionSupport.fixture(rangeCount, separation);
                for (DeletionInfoTransitionSupport.Transition transition : fixture.transitions(DeletionInfoTransitionSupport.Operation.TWO_EXACT_BOUND))
                {
                    fixture.prepare(transition);
                    fixture.applyPrepared(transition);
                    DeletionInfo actual = fixture.deletionInfo();

                    DeletionInfoTransitionSupport.assertEquivalent(transition.expected(), actual);
                    DeletionInfoTransitionSupport.assertEquivalent(transition.expectedPrefix(), transition.published());
                    assertSliceAndCoverageSemantics(transition, actual);
                    assertIndependentMaterializations(transition, actual);
                }
            }
        }
    }

    @Test
    public void partitionDeletionOverlapAndOutOfOrderTransitionsMatchCanonicalReconciliation()
    {
        for (DeletionInfoTransitionSupport.Operation operation : new DeletionInfoTransitionSupport.Operation[]{
        DeletionInfoTransitionSupport.Operation.PARTITION_DELETE,
        DeletionInfoTransitionSupport.Operation.OVERLAP,
        DeletionInfoTransitionSupport.Operation.OUT_OF_ORDER
        })
        {
            DeletionInfoTransitionSupport.Fixture fixture = DeletionInfoTransitionSupport.fixture(128, "OPPOSITE");
            for (DeletionInfoTransitionSupport.Transition transition : fixture.transitions(operation))
            {
                fixture.prepare(transition);
                fixture.applyPrepared(transition);
                DeletionInfoTransitionSupport.assertEquivalent(transition.expected(), fixture.deletionInfo());
                DeletionInfoTransitionSupport.assertEquivalent(transition.expectedPrefix(), transition.published());
            }
        }
    }

    private static void assertSliceAndCoverageSemantics(DeletionInfoTransitionSupport.Transition transition, DeletionInfo actual)
    {
        for (String position : new String[]{ "EARLY", "MIDDLE", "LATE", "MISS" })
        {
            Slice slice = DeletionInfoTransitionSupport.sliceFor(transition.offset(), transition.rangeCount(), position);
            DeletionInfoTransitionSupport.assertIteratorEquivalent(transition.expected(), actual, slice, false);
            DeletionInfoTransitionSupport.assertIteratorEquivalent(transition.expected(), actual, slice, true);
        }

        for (String coverage : new String[]{ "COVERED", "UNCOVERED" })
        {
            for (Clustering<?> key : DeletionInfoTransitionSupport.clusteringProbes(transition.offset(), transition.rangeCount(), coverage))
                DeletionInfoTransitionSupport.assertRangeEquivalent(transition.expected().rangeCovering(key), actual.rangeCovering(key));
        }
    }

    private static void assertIndependentMaterializations(DeletionInfoTransitionSupport.Transition transition, DeletionInfo actual)
    {
        MutableDeletionInfo expectedMutableCopy = transition.expected().mutableCopy();
        MutableDeletionInfo actualMutableCopy = actual.mutableCopy();
        DeletionInfoTransitionSupport.addDisjointRange(expectedMutableCopy, transition.offset(), transition.rangeCount());
        DeletionInfoTransitionSupport.addDisjointRange(actualMutableCopy, transition.offset(), transition.rangeCount());
        DeletionInfoTransitionSupport.assertEquivalent(expectedMutableCopy, actualMutableCopy);
        DeletionInfoTransitionSupport.assertEquivalent(transition.expected(), actual);

        DeletionInfo cloned = DeletionInfoTransitionSupport.deepClone(actual);
        DeletionInfoTransitionSupport.assertEquivalent(actual, cloned);
        ByteBuffer originalStart = DeletionInfoTransitionSupport.firstStartBuffer(actual);
        ByteBuffer clonedStart = DeletionInfoTransitionSupport.firstStartBuffer(cloned);
        assertNotSame("clone must own its clustering-bound bytes", originalStart, clonedStart);
        DeletionInfoTransitionSupport.assertEquivalent(transition.expected(), actual);
    }
}
