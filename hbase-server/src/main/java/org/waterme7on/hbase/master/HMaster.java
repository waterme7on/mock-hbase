package org.waterme7on.hbase.master;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.waterme7on.hbase.regionserver.HRegionServer;
public class HMaster extends HRegionServer implements MasterServices {
    private static final Logger LOG = LoggerFactory.getLogger(HMaster.class);

    // MASTER is name of the webapp and the attribute name used stuffing this
    // instance into a web context !! AND OTHER PLACES !!
    public static final String MASTER = "master";

    @Override
    public void abort(String reason, Throwable cause) {
    }

    @Override
    public boolean isAborted() {
        return false;
    }

    @Override
    public void stop(String why) {

    }

    @Override
    public boolean isStopped() {
        return false;
    }
}
