package org.waterme7on.hbase.master;

import org.apache.hadoop.hbase.ServerName;

/**
 * Get notification of server registration events. The invocations are inline so
 * make sure your
 * implementation is fast or else you'll slow hbase.
 */
public interface ServerListener {
    /**
     * Started waiting on RegionServers to check-in.
     */
    default void waiting() {
    };

    /**
     * The server has joined the cluster.
     * 
     * @param serverName The remote servers name.
     */
    default void serverAdded(final ServerName serverName) {
    };

    /**
     * The server was removed from the cluster.
     * 
     * @param serverName The remote servers name.
     */
    default void serverRemoved(final ServerName serverName) {
    };
}
