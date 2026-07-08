package org.apache.cassandra.utils;

import java.nio.ByteBuffer;
import java.lang.reflect.Proxy;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import com.google.common.base.Function;
import com.sun.management.ThreadMXBean;
import java.lang.management.ManagementFactory;

import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.Digest;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.DeletionPurger;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.ColumnData;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.ByteBufferAccessor;
import org.apache.cassandra.db.marshal.ValueAccessor;
import org.apache.cassandra.utils.memory.Cloner;

public class MergeIteratorBench
{
    static class MockColumnData extends Cell<ByteBuffer>
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
        @Override public int estimateCloneSize(Cloner cloner) { return 0; }
        @Override public ColumnData updateAllTimestamp(long timestamp) { return this; }
        @Override public ColumnData updateTimesAndPathsForAccord(Function<Cell, CellPath> cellToMaybeNewListPath, long newTimestamp, long newLocalDeletionTime) { return this; }
        @Override public ColumnData updateAllTimesWithNewCellPathForComplexColumnData(CellPath maybeNewPath, long newTimestamp, long newLocalDeletionTime) { return this; }
        @Override public ColumnData markCounterLocalToBeCleared() { return this; }
        @Override public ColumnData purge(DeletionPurger purger, long nowInSec) { return this; }
        @Override public ColumnData purgeDataOlderThan(long timestamp) { return this; }
        @Override public long maxTimestamp() { return 0; }

        @Override public boolean isCounterCell() { return false; }
        @Override public ByteBuffer value() { return ByteBufferUtil.EMPTY_BYTE_BUFFER; }
        @Override public ValueAccessor<ByteBuffer> accessor() { return ByteBufferAccessor.instance; }
        @Override public long timestamp() { return 0L; }
        @Override public int ttl() { return NO_TTL; }
        @Override public boolean isTombstone() { return false; }
        @Override public boolean isExpiring() { return false; }
        @Override public boolean isLive(long nowInSec) { return true; }
        @Override public CellPath path() { return null; }
        @Override public Cell<?> withUpdatedColumn(ColumnMetadata newColumn) { return this; }
        @Override public Cell<?> withUpdatedValue(ByteBuffer newValue) { return this; }
        @Override public Cell<?> withUpdatedTimestamp(long newTimestamp) { return this; }
        @Override public Cell<?> withUpdatedTimestampAndLocalDeletionTime(long newTimestamp, long newLocalDeletionTime) { return this; }
        @Override public Cell<?> withSkippedValue() { return this; }
        @Override protected int localDeletionTimeAsUnsignedInt() { return NO_DELETION_TIME_UNSIGNED_INTEGER; }
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
