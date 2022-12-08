package org.waterme7on.hbase;

/**
 * Interface to support the aborting of a given server or client.
 * <p>
 * This is used primarily for ZooKeeper usage when we could get an unexpected and fatal exception,
 * requiring an abort.
 * <p>
 * Implemented by the Master, RegionServer, and TableServers (client).
 */

public interface Abortable {
    /**
     * Abort the server or client.
     * @param why Why we're aborting.
     * @param e   Throwable that caused abort. Can be null.
     */
    void abort(String why, Throwable e);

    /**
     * It just call another abort method and the Throwable parameter is null.
     * @param why Why we're aborting.
     * @see Abortable#abort(String, Throwable)
     */
    default void abort(String why) {
        abort(why, null);
    }

    /**
     * Check if the server or client was aborted.
     * @return true if the server or client was aborted, false otherwise
     */
    boolean isAborted();
}
