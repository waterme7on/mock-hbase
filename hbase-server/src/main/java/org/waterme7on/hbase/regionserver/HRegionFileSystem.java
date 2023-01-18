package org.waterme7on.hbase.regionserver;

import java.util.Objects;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HRegionFileSystem {
    private static final Logger LOG = LoggerFactory.getLogger(HRegionFileSystem.class);
    private final RegionInfo regionInfo;

    public HRegionFileSystem(final Configuration conf) {
        this(conf, null);
    }

    public HRegionFileSystem(final Configuration conf, RegionInfo regioninfo) {
        // this.regionInfo = Objects.requireNonNull(regioninfo, "regionInfo is null");
        this.regionInfo = regioninfo;
    }

    /** Returns the {@link RegionInfo} that describe this on-disk region view */
    public RegionInfo getRegionInfo() {
        return this.regionInfo;
    }
}
