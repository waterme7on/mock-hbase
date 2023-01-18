package org.waterme7on.hbase.master.procedure;

import org.apache.hadoop.hbase.ServerName;

/**
 * Procedures that handle servers -- e.g. server crash -- must implement this
 * Interface. It is used
 * by the procedure runner to figure locking and what queuing.
 */
public interface ServerProcedureInterface {
    public enum ServerOperationType {
        CRASH_HANDLER,
        SWITCH_RPC_THROTTLE,

        /**
         * help find a available region server as worker and release worker after task
         * done invoke
         * SPLIT_WAL_REMOTE operation to send real WAL splitting request to worker
         * manage the split wal
         * task flow, will retry if SPLIT_WAL_REMOTE failed
         */
        SPLIT_WAL,

        /**
         * send the split WAL request to region server and handle the response
         */
        SPLIT_WAL_REMOTE,

        /**
         * Get all the replication queues of a crash server and assign them to other
         * region servers
         */
        CLAIM_REPLICATION_QUEUES,

        /**
         * send the claim replication queue request to region server to actually assign
         * it
         */
        CLAIM_REPLICATION_QUEUE_REMOTE
    }

    /** Returns Name of this server instance. */
    ServerName getServerName();

    /** Returns True if this server has an hbase:meta table region. */
    boolean hasMetaTableRegion();

    /**
     * Given an operation type we can take decisions about what to do with pending
     * operations. e.g. if
     * we get a crash handler and we have some assignment operation pending we can
     * abort those
     * operations.
     * 
     * @return the operation type that the procedure is executing.
     */
    ServerOperationType getServerOperationType();
}
