package org.waterme7on.hbase.procedure2;

import org.apache.hadoop.hbase.metrics.Counter;
import org.apache.hadoop.hbase.metrics.Histogram;

/**
 * With this interface, the procedure framework provides means to collect
 * following set of metrics
 * per procedure type for all procedures:
 * <ul>
 * <li>Count of submitted procedure instances</li>
 * <li>Time histogram for successfully completed procedure instances</li>
 * <li>Count of failed procedure instances</li>
 * </ul>
 * Please implement this interface to return appropriate metrics.
 */
public interface ProcedureMetrics {
    /** Returns Total number of instances submitted for a type of a procedure */
    Counter getSubmittedCounter();

    /**
     * Returns Histogram of runtimes for all successfully completed instances of a
     * type of a procedure
     */
    Histogram getTimeHisto();

    /** Returns Total number of instances failed for a type of a procedure */
    Counter getFailedCounter();
}
