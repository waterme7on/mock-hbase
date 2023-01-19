package org.waterme7on.hbase.regionserver;

import org.apache.hadoop.conf.Configurable;
import org.waterme7on.hbase.regionserver.HeapMemoryManager.TunerContext;
import org.waterme7on.hbase.regionserver.HeapMemoryManager.TunerResult;

/**
 * Makes the decision regarding proper sizing of the heap memory. Decides what
 * percentage of heap
 * memory should be allocated for global memstore and BlockCache.
 */
public interface HeapMemoryTuner extends Configurable {

    /**
     * Perform the heap memory tuning operation.
     * 
     * @return <code>TunerResult</code> including the heap percentage for memstore
     *         and block cache
     */
    TunerResult tune(TunerContext context);
}
