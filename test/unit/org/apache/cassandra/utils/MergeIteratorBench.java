package org.apache.cassandra.utils;

import java.nio.ByteBuffer;
import java.lang.reflect.Proxy;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import com.sun.management.ThreadMXBean;
import java.lang.management.ManagementFactory;

import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.Digest;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.ColumnData;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.utils.memory.Cloner;

public class MergeIteratorBench
{
    static class MockColumnData extends ColumnData
    {
        public MockColumnData(ColumnMetadata column)
        {
            super(column);
        }

        @Override public int dataSize() { return 0; }
        @Override public long unsharedHeapSizeExcludingData() { return 0; }
        @Override public long unsharedHeapSize() { return 0; }
        @Override public void validate() {}
        @Override public boolean hasInvalidDeletions() { return false; }
        @Override public void digest(Digest digest) {}
        @Override public ColumnData clone(Cloner cloner) { return this; }
        @Override public int estimateCloneSize(Cloner cloner) { return 0; }
        @Override public ColumnData updateAllTimestamp(long timestamp) { return this; }
        @Override public ColumnData updateTimesAndPathsForAccord(long timestamp, long rts, long wts) { return this; }
        @Override public ColumnData updateAllTimesWithNewCellPathForComplexColumnData(CellPath path) { return this; }
        @Override public ColumnData markCounterLocalToBeCleared() { return this; }
        @Override public ColumnData purge(DeletionTime activeDeletion, long nowInSec, boolean keepDeletedColumns) { return this; }
        @Override public ColumnData purgeDataOlderThan(long timestamp) { return this; }
        @Override public long maxTimestamp() { return 0; }
    }

    public static Row mockRow(Clustering<?> clustering, List<ColumnData> columnData)
    {
        return (Row) Proxy.newProxyInstance(
            Row.class.getClassLoader(),
            new Class<?>[]{Row.class},
            (proxy, method, args) -> {
                if (method.getName().equals("clustering")) {
                    return clustering;
                } else if (method.getName().equals("primaryKeyLivenessInfo")) {
                    return LivenessInfo.EMPTY;
                } else if (method.getName().equals("deletion")) {
                    return Row.Deletion.LIVE;
                } else if (method.getName().equals("iterator")) {
                    return columnData.iterator();
                } else if (method.getName().equals("columnCount")) {
                    return columnData.size();
                } else if (method.getName().equals("isEmpty")) {
                    return columnData.isEmpty();
                } else if (method.getName().equals("toString") && (args == null || args.length == 0)) {
                    return "MockRow";
                }
                return null;
            }
        );
    }

    public static void main(String[] args)
    {
        ThreadMXBean threadMXBean = (ThreadMXBean) ManagementFactory.getThreadMXBean();
        if (!threadMXBean.isThreadAllocatedMemorySupported())
        {
            System.err.println("Thread allocated memory is not supported");
            System.exit(1);
        }
        threadMXBean.setThreadAllocatedMemoryEnabled(true);

        // Prepare test metadata
        ColumnMetadata col1 = ColumnMetadata.regularColumn("ks", "tbl", "col1", Int32Type.instance, 0);
        ColumnMetadata col2 = ColumnMetadata.regularColumn("ks", "tbl", "col2", Int32Type.instance, 1);
        ColumnMetadata col3 = ColumnMetadata.regularColumn("ks", "tbl", "col3", Int32Type.instance, 2);

        List<ColumnData> data1 = Arrays.asList(new MockColumnData(col1), new MockColumnData(col3));
        List<ColumnData> data2 = Arrays.asList(new MockColumnData(col2), new MockColumnData(col3));

        Row row1 = mockRow(Clustering.EMPTY, data1);
        Row row2 = mockRow(Clustering.EMPTY, data2);

        Row.Merger merger = new Row.Merger(2, false);

        // JIT Warmup
        int warmupRuns = 20000;
        int blackhole = runMerge(merger, row1, row2, warmupRuns);

        // Measurement run
        int count = 50000;
        long startBytes = threadMXBean.getThreadAllocatedBytes(Thread.currentThread().getId());
        blackhole += runMerge(merger, row1, row2, count);
        long endBytes = threadMXBean.getThreadAllocatedBytes(Thread.currentThread().getId());

        double bytesPerOp = (double) (endBytes - startBytes) / count;

        // Prevent dead-code elimination
        if (blackhole == 0) {
            System.out.println("No-op");
        }

        System.out.printf("{\"metric\":\"B/op\",\"value\":%.2f}%n", bytesPerOp);
    }

    private static int runMerge(Row.Merger merger, Row row1, Row row2, int count)
    {
        int blackhole = 0;
        for (int i = 0; i < count; i++)
        {
            merger.clear();
            merger.add(0, row1);
            merger.add(1, row2);
            Row merged = merger.merge(DeletionTime.LIVE);
            if (merged != null)
            {
                blackhole += merged.columnCount();
            }
        }
        return blackhole;
    }
}
