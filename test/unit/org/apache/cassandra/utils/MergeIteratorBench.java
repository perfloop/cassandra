package org.apache.cassandra.utils;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import com.google.common.collect.Ordering;
import com.sun.management.ThreadMXBean;
import java.lang.management.ManagementFactory;

public class MergeIteratorBench
{
    private static final List<String> src1 = Arrays.asList("1", "3", "5", "7", "9");
    private static final List<String> src2 = Arrays.asList("2", "4", "6", "8", "10");
    private static final List<String> src3 = Arrays.asList("3", "4", "5", "6", "7");
    private static final List<String> src4 = Arrays.asList("1", "2", "3", "4", "5");

    public static void main(String[] args)
    {
        ThreadMXBean threadMXBean = (ThreadMXBean) ManagementFactory.getThreadMXBean();
        if (!threadMXBean.isThreadAllocatedMemorySupported())
        {
            System.err.println("Thread allocated memory is not supported");
            System.exit(1);
        }
        threadMXBean.setThreadAllocatedMemoryEnabled(true);

        // JIT Warmup
        int warmupRuns = 20000;
        int blackhole = runMerge(warmupRuns);

        // Measurement run
        int count = 50000;
        long startBytes = threadMXBean.getThreadAllocatedBytes(Thread.currentThread().getId());
        blackhole += runMerge(count);
        long endBytes = threadMXBean.getThreadAllocatedBytes(Thread.currentThread().getId());

        double bytesPerOp = (double) (endBytes - startBytes) / count;

        // Prevent dead-code elimination
        if (blackhole == 0) {
            System.out.println("No-op");
        }

        System.out.printf("{\"metric\":\"B/op\",\"value\":%.2f}%n", bytesPerOp);
    }

    private static int runMerge(int count)
    {
        int blackhole = 0;
        for (int i = 0; i < count; i++)
        {
            MergeIterator.Reducer<String, String> reducer = new MergeIterator.Reducer<String, String>()
            {
                String concatted = "";

                @Override
                public void reduce(int idx, String current)
                {
                    concatted += current;
                }

                public String getReduced()
                {
                    String tmp = concatted;
                    concatted = "";
                    return tmp;
                }
            };

            IMergeIterator<String, String> smi = MergeIterator.get(
                Arrays.asList(src1.iterator(), src2.iterator(), src3.iterator(), src4.iterator()),
                Ordering.<String>natural(),
                reducer
            );

            while (smi.hasNext())
            {
                blackhole += smi.next().hashCode();
            }
            smi.close();
        }
        return blackhole;
    }
}
