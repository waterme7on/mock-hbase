package org.waterme7on.hbase.executor;

/**
 * The following is a list of all executor types, both those that run in the
 * master and those that
 * run in the regionserver.
 */
public enum ExecutorType {

    // Master executor services
    MASTER_CLOSE_REGION(1),
    MASTER_OPEN_REGION(2),
    MASTER_SERVER_OPERATIONS(3),
    MASTER_TABLE_OPERATIONS(4),
    MASTER_RS_SHUTDOWN(5),
    MASTER_META_SERVER_OPERATIONS(6),
    M_LOG_REPLAY_OPS(7),
    MASTER_SNAPSHOT_OPERATIONS(8),
    MASTER_MERGE_OPERATIONS(9),

    // RegionServer executor services
    RS_OPEN_REGION(20),
    RS_OPEN_ROOT(21),
    RS_OPEN_META(22),
    RS_CLOSE_REGION(23),
    RS_CLOSE_ROOT(24),
    RS_CLOSE_META(25),
    RS_PARALLEL_SEEK(26),
    RS_LOG_REPLAY_OPS(27),
    RS_REGION_REPLICA_FLUSH_OPS(28),
    RS_COMPACTED_FILES_DISCHARGER(29),
    RS_OPEN_PRIORITY_REGION(30),
    RS_REFRESH_PEER(31),
    RS_SWITCH_RPC_THROTTLE(33),
    RS_IN_MEMORY_COMPACTION(34),
    RS_CLAIM_REPLICATION_QUEUE(35);

    ExecutorType(int value) {
    }

    /**
     * Returns Conflation of the executor type and the passed {@code serverName}.
     */
    String getExecutorName(String serverName) {
        return this.toString() + "-" + serverName.replace("%", "%%");
    }
}
