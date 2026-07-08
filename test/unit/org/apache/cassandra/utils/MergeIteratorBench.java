package org.apache.cassandra.utils;

import java.nio.ByteBuffer;
import java.lang.reflect.Proxy;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;
import com.google.common.base.Function;
import com.sun.management.ThreadMXBean;
import java.lang.management.ManagementFactory;
import java.util.concurrent.ThreadLocalRandom;

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
import org.apache.cassandra.utils.memory.ByteBufferCloner;

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
        @Override public Cell<?> markCounterLocalToBeCleared() { return this; }
        @Override public Cell<?> purge(DeletionPurger purger, long nowInSec) { return this; }
        @Override public Cell<?> purgeDataOlderThan(long timestamp) { return this; }
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
        @Override public Cell<?> clone(ByteBufferCloner cloner) { return this; }
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

        // Pre-allocate 50 mock rows of different sizes
        Row[] mockRows = new Row[50];
        for (int i = 0; i < 50; i++)
        {
            int cols = 2 + (i % 14); // 2 to 15 columns
            MockColumnData[] data = new MockColumnData[cols];
            for (int c = 0; c < cols; c++)
            {
                ColumnMetadata column = ColumnMetadata.regularColumn("ks", "tbl", "col" + c, Int32Type.instance, c);
                data[c] = new MockColumnData(column);
            }
            mockRows[i] = mockRow(Clustering.EMPTY, Arrays.asList(data));
        }

        Row.Merger merger = new Row.Merger(2, false);

        // JIT Warmup
        int warmupRuns = 20000;
        int blackhole = runMerge(merger, mockRows, warmupRuns);

        // Measurement run
        int count = 50000 + ThreadLocalRandom.current().nextInt(100);
        long startBytes = threadMXBean.getThreadAllocatedBytes(Thread.currentThread().getId());
        blackhole += runMerge(merger, mockRows, count);
        long endBytes = threadMXBean.getThreadAllocatedBytes(Thread.currentThread().getId());

        double bytesPerOp = (double) (endBytes - startBytes) / count;

        // Prevent dead-code elimination by outputting to stderr
        System.err.println("DCE Sentinel: " + blackhole);

        System.out.printf("{\"metric\":\"B/op\",\"value\":%.2f}%n", bytesPerOp);
    }

    private static int runMerge(Row.Merger merger, Row[] mockRows, int count)
    {
        int blackhole = 0;
        ThreadLocalRandom rand = ThreadLocalRandom.current();
        for (int i = 0; i < count; i++)
        {
            int idx1 = rand.nextInt(50);
            int idx2 = rand.nextInt(50);
            merger.clear();
            merger.add(0, mockRows[idx1]);
            merger.add(1, mockRows[idx2]);
            Row merged = merger.merge(DeletionTime.LIVE);
            if (merged != null)
            {
                blackhole += merged.columnCount();
            }
        }
        return blackhole;
    }
}
