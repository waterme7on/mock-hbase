package org.waterme7on.hbase.util;

import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionReplicaUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Similar to {@link RegionReplicaUtil} but for the server side
 */

public class ServerRegionReplicaUtil {
    private static final Logger LOG = LoggerFactory.getLogger(ServerRegionReplicaUtil.class);

    /**
     * Returns the regionInfo object to use for interacting with the file system.
     * 
     * @return An RegionInfo object to interact with the filesystem
     */
    public static RegionInfo getRegionInfoForFs(RegionInfo regionInfo) {
        if (regionInfo == null) {
            return null;
        }
        return RegionReplicaUtil.getRegionInfoForDefaultReplica(regionInfo);
    }
}
