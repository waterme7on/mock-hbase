package org.waterme7on.hbase.master;

import java.io.IOException;
import java.util.Set;
import org.apache.hadoop.hbase.ServerName;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * For storing the region server list.
 * <p/>
 * Mainly be used when restarting master, to load the previous active region server list.
 */
public interface RegionServerList {

    /**
     * Called when a region server join the cluster.
     */
    void started(ServerName sn);

    /**
     * Called when a region server is dead.
     */
    void expired(ServerName sn);

    /**
     * Get all live region servers.
     */
    Set<ServerName> getAll() throws IOException;
}
