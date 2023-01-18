package org.waterme7on.hbase.master;

import java.io.IOException;

import org.apache.hadoop.hbase.ClusterMetrics;
import org.waterme7on.hbase.Server;

public interface MasterServices extends Server {
    /** Returns Master's filesystem {@link MasterFileSystem} utility class. */
    MasterFileSystem getMasterFileSystem();

    MasterWalManager getMasterWalManager();

    ServerManager getServerManager();

    String getClientIdAuditPrefix();

    ClusterMetrics getClusterMetrics() throws IOException;
}
