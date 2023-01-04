package org.waterme7on.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.waterme7on.hbase.master.HMaster;
import org.waterme7on.hbase.regionserver.HRegionServer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

public class HBaseCluster {
    private final static int DEFAULT_NO = 1;
    private static final Logger LOG = LoggerFactory.getLogger(HBaseCluster.class);
    private final Configuration conf;
    private final Class<? extends HMaster> masterClass;
    private final Class<? extends HRegionServer> regionServerClass;

    /**
     * Constructor.
     */
    public HBaseCluster(final Configuration conf) throws IOException {
        this(conf, DEFAULT_NO);
    }

    /**
     * Constructor.
     * 
     * @param conf            Configuration to use. Post construction has the
     *                        master's address.
     * @param noRegionServers Count of regionservers to start.
     */
    public HBaseCluster(final Configuration conf, final int noRegionServers) throws IOException {
        this(conf, 1, 0, noRegionServers, getMasterImplementation(conf),
                getRegionServerImplementation(conf));
    }

    public HBaseCluster(final Configuration conf, final int noMasters,
            final int noAlwaysStandByMasters, final int noRegionServers,
            final Class<? extends HMaster> masterClass,
            final Class<? extends HRegionServer> regionServerClass) throws IOException {
        this.conf = conf;
        // Start the HMasters.
        this.masterClass = (Class<? extends HMaster>) conf.getClass(HConstants.MASTER_IMPL, masterClass);
        for (int j = 0; j < noMasters; j++) {
            addMaster(new Configuration(conf), j);
        }
        // Start the HRegionServers.
        this.regionServerClass = (Class<? extends HRegionServer>) conf.getClass(HConstants.REGION_SERVER_IMPL,
                regionServerClass);
        for (int j = 0; j < noRegionServers; j++) {
            addRegionServer(new Configuration(conf), j);
        }

    }

    private static Class<? extends HMaster> getMasterImplementation(final Configuration conf) {
        return (Class<? extends HMaster>) conf.getClass(HConstants.MASTER_IMPL, HMaster.class);
    }

    private static Class<? extends HRegionServer> getRegionServerImplementation(final Configuration conf) {
        return (Class<? extends HRegionServer>) conf.getClass(HConstants.REGION_SERVER_IMPL, HRegionServer.class);
    }

    public Thread addMaster(Configuration c, final int index)
            throws IOException {
        return null;
    }

    public Thread addRegionServer(Configuration config, final int index)
            throws IOException {
        return null;
    }

    public static void main(String[] args) throws IOException {
        System.out.println("Hello World!");
    }
}
