package org.waterme7on.hbase.regionserver;

import org.apache.hadoop.conf.Configuration;
import org.waterme7on.hbase.Server;

import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;

/*
* The factory class for creating a region
 * */
public final class HRegionFactory {
    private static final String FLUSH_SIZE_KEY = "hbase.master.store.region.flush.size";
    private static final long DEFAULT_FLUSH_SIZE = 1024 * 1024 * 128L;
    private static final String FLUSH_INTERVAL_MS_KEY = "hbase.master.store.region.flush.interval.ms";

    // default to flush every 15 minutes, for safety
    private static final long DEFAULT_FLUSH_INTERVAL_MS = TimeUnit.MINUTES.toMillis(15);

    public static HRegion create(Server server) throws IOException {
        Configuration conf = server.getConfiguration();
        // long flushSize = conf.getLong(FLUSH_SIZE_KEY, DEFAULT_FLUSH_SIZE);
        // long flushIntervalMs = conf.getLong(FLUSH_INTERVAL_MS_KEY,
        // DEFAULT_FLUSH_INTERVAL_MS);
        // return new HRegion(flushSize, flushIntervalMs);
        return new HRegion(conf);
    }
}
