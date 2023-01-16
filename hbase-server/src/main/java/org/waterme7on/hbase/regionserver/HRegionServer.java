package org.waterme7on.hbase.regionserver;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.context.Scope;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.ClusterConnection;
import org.apache.hadoop.hbase.ipc.RpcClient;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.trace.TraceUtil;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.zookeeper.ClusterStatusTracker;
import org.apache.hadoop.hbase.zookeeper.MasterAddressTracker;
import org.apache.hadoop.hbase.zookeeper.ZKClusterId;
import org.apache.hadoop.hbase.zookeeper.ZKNodeTracker;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.commons.lang3.StringUtils;
import org.waterme7on.hbase.fs.HFileSystem;
import org.waterme7on.hbase.ipc.RpcClientFactory;
import org.waterme7on.hbase.util.NettyEventLoopGroupConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

public class HRegionServer extends Thread implements RegionServerServices {
    private static final Logger LOG = LoggerFactory.getLogger(HRegionServer.class);
    protected final Configuration conf;
    // zookeeper connection and watcher
    protected final ZKWatcher zooKeeper;
    protected final RSRpcServices rpcServices;
    private Path dataRootDir;
    protected ServerName serverName;
    /**
     * This servers startcode.
     */
    protected final long startcode;
    /**
     * hostname specified by hostname config
     */
    protected String useThisHostnameInstead;

    /** region server process name */
    public static final String REGIONSERVER = "regionserver";
    /**
     * Map of regions currently being served by this region server. Key is the
     * encoded region name.
     * All access should be synchronized.
     */
    private final Map<String, HRegion> onlineRegions = new ConcurrentHashMap<>();

    /*
     * MemStore components
     */
    private MemStoreFlusher cacheFlusher;

    private HeapMemoryManager hMemManager;
    // master address tracker
    private final MasterAddressTracker masterAddressTracker;
    // Cluster Status Tracker
    protected final ClusterStatusTracker clusterStatusTracker;

    /*
     * HDFS components
     */
    private HFileSystem dataFs;
    private HFileSystem walFs;
    // RPC client. Used to make the stub above that does region server status
    // checking.
    private RpcClient rpcClient;
    /**
     * Unique identifier for the cluster we are a part of.
     */
    protected String clusterId;
    /**
     * Cluster connection to be shared by services. Initialized at server startup
     * and closed when
     * server shuts down. Clients must never close it explicitly. Clients hosted by
     * this Server should
     * make use of this clusterConnection rather than create their own; if they
     * create their own,
     * there is no way for the hosting server to shutdown ongoing client RPCs.
     */
    protected ClusterConnection clusterConnection;
    /**
     * True if this RegionServer is coming up in a cluster where there is no Master;
     * means it needs to
     * just come up and make do without a Master to talk to: e.g. in test or
     * HRegionServer is doing
     * other than its usual duties: e.g. as an hollowed-out host whose only purpose
     * is as a
     * Replication-stream sink; see HBASE-18846 for more. TODO: can this replace
     * {@link #TEST_SKIP_REPORTING_TRANSITION} ?
     */
    private final boolean masterless;
    private final NettyEventLoopGroupConfig eventLoopGroupConfig;
    private static final String MASTERLESS_CONFIG_NAME = "hbase.masterless";
    // Set when a report to the master comes back with a message asking us to
    // shutdown. Also set by call to stop when debugging or running unit tests
    // of HRegionServer in isolation.
    private volatile boolean stopped = false;
    // Go down hard. Used if file system becomes unavailable and also in
    // debugging and unit tests.
    private AtomicBoolean abortRequested;
    final int msgInterval;

    public HRegionServer(final Configuration conf) throws IOException {
        super("RegionServer"); // thread name
        final Span span = TraceUtil.createSpan("HRegionServer.cxtor");
        try (Scope ignored = span.makeCurrent()) {
            this.startcode = EnvironmentEdgeManager.currentTime();
            this.conf = conf;
            this.masterless = conf.getBoolean(MASTERLESS_CONFIG_NAME, false);
            this.eventLoopGroupConfig = setupNetty(this.conf);
            // initialize hdfs
            this.dataRootDir = CommonFSUtils.getRootDir(this.conf);
            // Some unit tests don't need a cluster, so no zookeeper at all
            // Open connection to zookeeper and set primary watcher
            this.rpcServices = createRpcServices();
            this.zooKeeper = new ZKWatcher(conf, getProcessName() + ":" + rpcServices.isa.getPort(), this,
                    canCreateBaseZNode());
            String hostName = StringUtils.isBlank(useThisHostnameInstead)
                    ? this.rpcServices.isa.getHostName()
                    : this.useThisHostnameInstead;
            this.serverName = ServerName.valueOf(hostName, this.rpcServices.isa.getPort(), this.startcode);
            this.stopped = false;
            this.abortRequested = new AtomicBoolean(false);
            this.msgInterval = conf.getInt("hbase.regionserver.msginterval", 3 * 1000);
            // If no master in cluster, skip trying to track one or look for a cluster
            // status.
            if (!this.masterless) {
                // if (conf.getBoolean(HBASE_SPLIT_WAL_COORDINATED_BY_ZK,
                // DEFAULT_HBASE_SPLIT_COORDINATED_BY_ZK)) {
                // this.csm = new ZkCoordinatedStateManager(this);
                // }

                masterAddressTracker = new MasterAddressTracker(getZooKeeper(), this);
                masterAddressTracker.start();

                clusterStatusTracker = new ClusterStatusTracker(zooKeeper, this);
                clusterStatusTracker.start();
            } else {
                masterAddressTracker = null;
                clusterStatusTracker = null;
            }
        } catch (Throwable t) {
            // Make sure we log the exception. HRegionServer is often started via reflection
            // and the
            // cause of failed startup is lost.
            TraceUtil.setError(span, t);
            LOG.error("Failed construction RegionServer", t.getMessage());
            throw t;
        } finally {
            span.end();
        }
    }

    @Override
    public ZKWatcher getZooKeeper() {
        return zooKeeper;
    }

    public NettyEventLoopGroupConfig getEventLoopGroupConfig() {
        return this.eventLoopGroupConfig;
    }

    public Configuration getConfiguration() {
        return conf;
    }

    protected Path getDataRootDir() {
        return dataRootDir;
    }

    protected String getProcessName() {
        return REGIONSERVER;
    }

    protected boolean canCreateBaseZNode() {
        return this.masterless;
    }

    protected RSRpcServices createRpcServices() throws IOException {
        return new RSRpcServices(this);
    }

    public void abort(String why, Throwable e) {

    }

    /**
     * Sets the abort state if not already set.
     * 
     * @return True if abortRequested set to True successfully, false if an abort is
     *         already in progress.
     */
    protected boolean setAbortRequested() {
        return abortRequested.compareAndSet(false, true);
    }

    @Override
    public boolean isAborted() {
        return abortRequested.get();
    }

    @Override
    public void stop(String why) {

    }

    @Override
    public boolean isStopped() {
        return this.stopped;
    }

    protected void initializeMemStoreChunkCreator() {
        // TODO: MemStoreLAB, simply leave empty now
        LOG.debug("initializeMemStoreChunkCreator");
    }

    private static NettyEventLoopGroupConfig setupNetty(Configuration conf) {
        // Initialize netty event loop group at start as we may use it for rpc server,
        // rpc client & WAL.
        NettyEventLoopGroupConfig nelgc = new NettyEventLoopGroupConfig(conf, "RS-EventLoopGroup");
        // NettyRpcClientConfigHelper.setEventLoopConfig(conf, nelgc.group(),
        // nelgc.clientChannelClass());
        // NettyAsyncFSWALConfigHelper.setEventLoopConfig(conf, nelgc.group(),
        // nelgc.clientChannelClass());
        return nelgc;
    }

    @Override
    public void run() {
        LOG.debug("HRegionServer running");
        if (isStopped()) {
            LOG.info("Skipping run; stopped");
            return;
        }

        try {
            // Do pre-registration initializations; zookeeper, lease threads, etc.
            preRegistrationInitialization();
        } catch (Throwable e) {
            abort("Fatal exception during initialization", e);
        }

        try {
            if (!isStopped() && !isAborted()) {
                // Try and register with the Master; tell it we are here. Break if server is
                // stopped or
                // the clusterup flag is down or hdfs went wacky. Once registered successfully,
                // go ahead and
                // start up all Services. Use RetryCounter to get backoff in case Master is
                // struggling to
                // come up.
            }
        } catch (Throwable e) {
            abort("Fatal exception during registration", e);
        }

        for (int i = 0; i < 3; i++) {
            try {
                if (!isStopped() && !isAborted()) {
                    LOG.debug("running...");
                    Thread.sleep(1000);
                }
            } catch (InterruptedException e) {
                LOG.error("Interrupted", e);
            }
        }
    }

    /**
     * All initialization needed before we go register with Master.<br>
     * Do bare minimum. Do bulk of initializations AFTER we've connected to the
     * Master.<br>
     * In here we just put up the RpcServer, setup Connection, and ZooKeeper.
     */
    private void preRegistrationInitialization() {
        final Span span = TraceUtil.createSpan("HRegionServer.preRegistrationInitialization");
        try (Scope ignored = span.makeCurrent()) {
            initializeZooKeeper();
            setupClusterConnection();
            // Setup RPC client for master communication
            this.rpcClient = RpcClientFactory.createClient(conf, clusterId,
                    new InetSocketAddress(this.rpcServices.isa.getAddress(), 0),
                    null);
            span.setStatus(StatusCode.OK);
        } catch (Throwable t) {
            // Call stop if error or process will stick around for ever since server
            // puts up non-daemon threads.
            TraceUtil.setError(span, t);
            t.printStackTrace();
            LOG.debug("Error:" + t.toString());
            this.rpcServices.stop();
            abort("Initialization of RS failed.  Hence aborting RS.", t);
        } finally {
            span.end();
        }
    }

    /**
     * Bring up connection to zk ensemble and then wait until a master for this
     * cluster and then after
     * that, wait until cluster 'up' flag has been set. This is the order in which
     * master does things.
     * <p>
     * Finally open long-living server short-circuit connection.
     */
    private void initializeZooKeeper() throws IOException, InterruptedException {
        // Nothing to do in here if no Master in the mix.
        if (this.masterless) {
            return;
        }
        // Create the master address tracker, register with zk, and start it. Then
        // block until a master is available. No point in starting up if no master
        // running.
        blockAndCheckIfStopped(this.masterAddressTracker);
        LOG.debug("Master address tracker is up.");

        // Wait on cluster being up. Master will set this flag up in zookeeper
        // when ready.
        blockAndCheckIfStopped(this.clusterStatusTracker);
        LOG.debug("Cluster status tracker is up.");

        // If we are HMaster then the cluster id should have already been set.
        if (clusterId == null) {
            // Retrieve clusterId
            // Since cluster status is now up
            // ID should have already been set by HMaster
            try {
                clusterId = ZKClusterId.readClusterIdZNode(this.zooKeeper);
                if (clusterId == null) {
                    this.abort("Cluster ID has not been set");
                }
                LOG.info("ClusterId : " + clusterId);
            } catch (Exception e) {
                this.abort("Failed to retrieve Cluster ID", e);
            }
        }

        waitForMasterActive();
        if (isStopped() || isAborted()) {
            return; // No need for further initialization
        }
    }

    /**
     * Utilty method to wait indefinitely on a znode availability while checking if
     * the region server
     * is shut down
     * 
     * @param tracker znode tracker to use
     * @throws IOException          any IO exception, plus if the RS is stopped
     * @throws InterruptedException if the waiting thread is interrupted
     */
    private void blockAndCheckIfStopped(ZKNodeTracker tracker)
            throws IOException, InterruptedException {
        while (tracker.blockUntilAvailable(this.msgInterval, false) == null) {
            if (this.stopped) {
                throw new IOException("Received the shutdown message while waiting.");
            }
        }
    }

    /** Returns True if the cluster is up. */
    @Override
    public boolean isClusterUp() {
        return this.masterless
                || (this.clusterStatusTracker != null && this.clusterStatusTracker.isClusterUp());
    }

    /**
     * Wait for an active Master. See override in Master superclass for how it is
     * used.
     */
    protected void waitForMasterActive() {
    }

    /**
     * Setup our cluster connection if not already initialized.
     */
    protected synchronized void setupClusterConnection() throws IOException {
        return;
    }
}
