package org.waterme7on.hbase.regionserver;

import org.waterme7on.hbase.Server;

public interface RegionServerServices extends Server {
    boolean isClusterUp();
}
