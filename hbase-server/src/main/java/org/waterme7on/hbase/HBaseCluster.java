package org.waterme7on.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.waterme7on.hbase.master.HMaster;
import org.waterme7on.hbase.regionserver.HRegionServer;
import org.waterme7on.hbase.util.ClusterUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

public class HBaseCluster {
    private final static int DEFAULT_NO = 1;
    private static final Logger LOG = LoggerFactory.getLogger(HBaseCluster.class);
    private final List<ClusterUtil.MasterThread> masterThreads = new CopyOnWriteArrayList<>();
    private final List<ClusterUtil.RegionServerThread> regionThreads = new CopyOnWriteArrayList<>();
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

    @SuppressWarnings("unchecked")
    public HBaseCluster(final Configuration conf, final int noMasters,
            final int noAlwaysStandByMasters, final int noRegionServers,
            final Class<? extends HMaster> masterClass,
            final Class<? extends HRegionServer> regionServerClass) throws IOException {
        this.conf = conf;
        LOG.info("Starting cluster with " + noMasters + " master(s), " + noAlwaysStandByMasters
                + " always standby master(s), " + noRegionServers + " regionserver(s)");
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

    /**
     * Start the cluster.
     */
    public void startup() throws IOException {
        ClusterUtil.startup(this.masterThreads, this.regionThreads);
    }

    /**
     * Shut down the mini HBase cluster
     */
    public void shutdown() {
        ClusterUtil.shutdown(this.masterThreads, this.regionThreads);
    }

    public ClusterUtil.MasterThread addMaster() throws IOException {
        return addMaster(new Configuration(conf), this.masterThreads.size());
    }

    @SuppressWarnings("unchecked")
    public ClusterUtil.MasterThread addMaster(Configuration c, final int index)
            throws IOException {
        // Create each master with its own Configuration instance so each has
        // its Connection instance rather than share (see HBASE_INSTANCES down in
        // the guts of ConnectionManager.
        ClusterUtil.MasterThread mt = ClusterUtil.createMasterThread(c,
                (Class<? extends HMaster>) c.getClass(HConstants.MASTER_IMPL, this.masterClass), index);
        this.masterThreads.add(mt);
        // Refresh the master address config.
        List<String> masterHostPorts = new ArrayList<>();
        getMasters().forEach(masterThread -> masterHostPorts
                .add(masterThread.getMaster().getServerName().getAddress().toString()));
        conf.set(HConstants.MASTER_ADDRS_KEY, String.join(",", masterHostPorts));
        return mt;
    }

    public ClusterUtil.RegionServerThread addRegionServer() throws IOException {
        return addRegionServer(new Configuration(conf), this.regionThreads.size());
    }

    @SuppressWarnings("unchecked")
    public ClusterUtil.RegionServerThread addRegionServer(Configuration config, final int index)
            throws IOException {
        // Create each regionserver with its own Configuration instance so each has
        // its Connection instance rather than share (see HBASE_INSTANCES down in
        // the guts of ConnectionManager).
        ClusterUtil.RegionServerThread rst = ClusterUtil.createRegionServerThread(config,
                (Class<? extends HRegionServer>) conf
                        .getClass(HConstants.REGION_SERVER_IMPL, this.regionServerClass),
                index);

        this.regionThreads.add(rst);
        return rst;
    }

    @SuppressWarnings("unchecked")
    private static Class<? extends HMaster> getMasterImplementation(final Configuration conf) {
        return (Class<? extends HMaster>) conf.getClass(HConstants.MASTER_IMPL, HMaster.class);
    }

    @SuppressWarnings("unchecked")
    private static Class<? extends HRegionServer> getRegionServerImplementation(final Configuration conf) {
        return (Class<? extends HRegionServer>) conf.getClass(HConstants.REGION_SERVER_IMPL, HRegionServer.class);
    }

    /**
     * @param c Configuration to check.
     * @return True if a 'local' address in hbase.master value.
     */
    public static boolean isLocal(final Configuration c) {
        boolean mode = c.getBoolean(HConstants.CLUSTER_DISTRIBUTED, HConstants.DEFAULT_CLUSTER_DISTRIBUTED);
        return (mode == HConstants.CLUSTER_IS_LOCAL);
    }

    /** Returns Read-only list of master threads. */
    public List<ClusterUtil.MasterThread> getMasters() {
        return Collections.unmodifiableList(this.masterThreads);
    }

    /** Returns Read-only list of region server threads. */
    public List<ClusterUtil.RegionServerThread> getRegionServers() {
        return Collections.unmodifiableList(this.regionThreads);
    }
}
