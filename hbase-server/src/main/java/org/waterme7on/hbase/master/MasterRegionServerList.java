package org.waterme7on.hbase.master;

import org.apache.hadoop.hbase.ServerName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.waterme7on.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.Abortable;

import java.io.IOException;
import java.util.Set;

public class MasterRegionServerList implements RegionServerList{
    private static final Logger LOG = LoggerFactory.getLogger(MasterRegionServerList.class);

    private final HRegion region;

    private final Abortable abortable;

    public MasterRegionServerList(HRegion region, Abortable abortable) {
        this.region = region;
        this.abortable = abortable;
    }

    @Override
    public void started(ServerName sn) {

    }

    @Override
    public void expired(ServerName sn) {

    }

    @Override
    public Set<ServerName> getAll() throws IOException {
        return null;
    }
}
