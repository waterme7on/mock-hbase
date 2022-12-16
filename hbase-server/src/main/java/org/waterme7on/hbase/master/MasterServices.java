package org.waterme7on.hbase.master;

import org.waterme7on.hbase.Server;

public interface MasterServices extends Server {
    /** Returns Master's filesystem {@link MasterFileSystem} utility class. */
    MasterFileSystem getMasterFileSystem();
    MasterWalManager getMasterWalManager();
}
