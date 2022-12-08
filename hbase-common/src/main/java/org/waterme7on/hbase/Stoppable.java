package org.waterme7on.hbase;
/**
 * Implementers are Stoppable.
 */
public interface Stoppable {
    /**
     * Stop this service. Implementers should favor logging errors over throwing RuntimeExceptions.
     * @param why Why we're stopping.
     */
    void stop(String why);

    /** Returns True if {@link #stop(String)} has been closed. */
    boolean isStopped();
}
