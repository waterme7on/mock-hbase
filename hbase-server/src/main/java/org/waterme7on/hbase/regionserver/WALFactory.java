package org.waterme7on.hbase.regionserver;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.waterme7on.hbase.Server;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.waterme7on.hbase.wal.WAL;

public class WALFactory {

    public WALFactory(Configuration conf, String factoryId, Server server) throws IOException {
    }

    public WAL getWAL(RegionInfo regionInfo) {
        return null;
    }
}
