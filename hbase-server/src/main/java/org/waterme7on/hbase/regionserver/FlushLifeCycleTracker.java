package org.waterme7on.hbase.regionserver;

/**
 * Used to track flush execution.
 */
public interface FlushLifeCycleTracker {

    static FlushLifeCycleTracker DUMMY = new FlushLifeCycleTracker() {
    };

    /**
     * Called if the flush request fails for some reason.
     */
    default void notExecuted(String reason) {
    }

    /**
     * Called before flush is executed.
     */
    default void beforeExecution() {
    }

    /**
     * Called after flush is executed.
     */
    default void afterExecution() {
    }
}
