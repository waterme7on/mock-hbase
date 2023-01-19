package org.waterme7on.hbase.regionserver;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.context.Scope;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.ChoreService;
import org.apache.hadoop.hbase.CoordinatedStateManager;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.exceptions.RegionMovedException;
import org.apache.hadoop.hbase.exceptions.RegionOpeningException;
import org.waterme7on.hbase.TableDescriptors;
import org.waterme7on.hbase.client.ClusterConnection;
import org.apache.hadoop.hbase.ipc.RpcClient;
import org.apache.hadoop.hbase.ipc.ServerNotRunningYetException;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.RegionServerInfo;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.RegionServerStartupRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.RegionServerStartupResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.RegionServerStatusService;
import org.apache.hadoop.hbase.trace.TraceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.RetryCounter;
import org.apache.hadoop.hbase.util.RetryCounterFactory;
import org.apache.hadoop.hbase.util.Sleeper;
import org.apache.hadoop.hbase.zookeeper.ClusterStatusTracker;
import org.apache.hadoop.hbase.zookeeper.MasterAddressTracker;
import org.apache.hadoop.hbase.zookeeper.ZKClusterId;
import org.apache.hadoop.hbase.zookeeper.ZKNodeTracker;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hbase.thirdparty.com.google.common.cache.Cache;
import org.apache.hbase.thirdparty.com.google.common.cache.CacheBuilder;
import org.apache.hbase.thirdparty.com.google.protobuf.BlockingRpcChannel;
import org.apache.zookeeper.KeeperException;
import org.apache.commons.lang3.StringUtils;
import org.waterme7on.hbase.client.ServerConnectionUtils;
import org.waterme7on.hbase.executor.ExecutorService;
import org.waterme7on.hbase.executor.ExecutorType;
import org.waterme7on.hbase.fs.HFileSystem;
import org.waterme7on.hbase.ipc.RpcClientFactory;
import org.waterme7on.hbase.master.HMaster;
import org.waterme7on.hbase.master.MasterRpcServices;
import org.waterme7on.hbase.util.FSTableDescriptors;
import org.waterme7on.hbase.util.NettyEventLoopGroupConfig;

import com.google.protobuf.Service;

import org.apache.hbase.thirdparty.com.google.protobuf.ServiceException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class HRegionServer extends Thread implements RegionServerServices {
    private static final Logger LOG = LoggerFactory.getLogger(HRegionServer.class);
    protected final Configuration conf;
    // zookeeper connection and watcher
    protected final ZKWatcher zooKeeper;
    protected final RSRpcServices rpcServices;

    protected ServerName serverName;

    // Instance of the hbase executor executorService.
    protected ExecutorService executorService;

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
    // A sleeper that sleeps for msgInterval.
    protected final Sleeper sleeper;
    private final int shortOperationTimeout;
    // flag set after we're done setting up server threads
    final AtomicBoolean online = new AtomicBoolean(false);
    /*
     * MemStore components
     */
    private MemStoreFlusher cacheFlusher;

    private HeapMemoryManager hMemManager;

    private final RegionServerAccounting regionServerAccounting;

    // Stub to do region server status calls against the master.
    private volatile RegionServerStatusService.BlockingInterface rssStub;
    // RPC client. Used to make the stub above that does region server status
    // checking.
    protected RpcClient rpcClient;

    // master address tracker
    private final MasterAddressTracker masterAddressTracker;
    // Cluster Status Tracker
    protected final ClusterStatusTracker clusterStatusTracker;
    /**
     * A map from RegionName to current action in progress. Boolean value indicates:
     * true - if open
     * region action in progress false - if close region action in progress
     */
    private final ConcurrentMap<byte[], Boolean> regionsInTransitionInRS = new ConcurrentSkipListMap<>(
            Bytes.BYTES_COMPARATOR);

    /**
     * Used to cache the moved-out regions
     */
    private final Cache<String, MovedRegionInfo> movedRegionInfoCache = CacheBuilder.newBuilder()
            .expireAfterWrite(movedRegionCacheExpiredTime(), TimeUnit.MILLISECONDS).build();

    /*
     * HDFS components
     */
    private volatile boolean dataFsOk;
    private HFileSystem dataFs;
    private Path dataRootDir;
    private HFileSystem walFs;
    private Path walRootDir;
    /**
     * Go here to get table descriptors.
     */
    protected TableDescriptors tableDescriptors;

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
            this.dataFsOk = true;
            this.masterless = conf.getBoolean(MASTERLESS_CONFIG_NAME, false);
            this.eventLoopGroupConfig = setupNetty(this.conf);
            // initialize hdfs
            this.dataRootDir = CommonFSUtils.getRootDir(this.conf);
            // Some unit tests don't need a cluster, so no zookeeper at all
            // Open connection to zookeeper and set primary watcher
            this.rpcServices = createRpcServices();

            initializeFileSystem();

            this.zooKeeper = new ZKWatcher(conf, getProcessName() + ":" + rpcServices.isa.getPort(), this,
                    canCreateBaseZNode());
            String hostName = StringUtils.isBlank(useThisHostnameInstead)
                    ? this.rpcServices.isa.getHostName()
                    : this.useThisHostnameInstead;
            this.serverName = ServerName.valueOf(hostName, this.rpcServices.isa.getPort(), this.startcode);
            this.stopped = false;
            this.abortRequested = new AtomicBoolean(false);
            this.msgInterval = conf.getInt("hbase.regionserver.msginterval", 3 * 1000);
            this.sleeper = new Sleeper(this.msgInterval, this);
            this.shortOperationTimeout = conf.getInt(HConstants.HBASE_RPC_SHORTOPERATION_TIMEOUT_KEY,
                    HConstants.DEFAULT_HBASE_RPC_SHORTOPERATION_TIMEOUT);
            this.executorService = new ExecutorService(getName());
            regionServerAccounting = new RegionServerAccounting(conf);
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

            this.rpcServices.start(zooKeeper);
            // init filesystem
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

    private void initializeFileSystem() throws IOException {
        // initialize fs
        boolean useHBaseChecksum = conf.getBoolean(HConstants.HBASE_CHECKSUM_VERIFICATION, true);
        String walDirUri = CommonFSUtils.getDirUri(this.conf,
                new Path(conf.get(CommonFSUtils.HBASE_WAL_DIR, conf.get(HConstants.HBASE_DIR))));
        // set WAL's uri
        if (walDirUri != null) {
            CommonFSUtils.setFsDefault(this.conf, walDirUri);
        }
        // init the WALFs
        this.walFs = new HFileSystem(this.conf, useHBaseChecksum);
        this.walRootDir = CommonFSUtils.getWALRootDir(this.conf);
        // Set 'fs.defaultFS' to match the filesystem on hbase.rootdir else
        // underlying hadoop hdfs accessors will be going against wrong filesystem
        // (unless all is set to defaults).
        String rootDirUri = CommonFSUtils.getDirUri(this.conf, new Path(conf.get(HConstants.HBASE_DIR)));
        if (rootDirUri != null) {
            CommonFSUtils.setFsDefault(this.conf, rootDirUri);
        }
        // init the filesystem
        this.dataFs = new HFileSystem(this.conf, useHBaseChecksum);
        this.dataRootDir = CommonFSUtils.getRootDir(this.conf);
        this.tableDescriptors = new FSTableDescriptors(this.dataFs, this.dataRootDir,
                false, false);

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

    @Override
    public Connection getConnection() {
        return getClusterConnection();
    }

    @Override
    public ClusterConnection getClusterConnection() {
        return this.clusterConnection;
    }

    @Override
    public Connection createConnection(Configuration conf) throws IOException {
        return ServerConnectionUtils.createShortCircuitConnection(conf, User.getCurrent(), this.serverName,
                this.rpcServices, this.rpcServices, null);
    }

    /**
     * Cause the server to exit without closing the regions it is serving, the log
     * it is using and
     * without notifying the master. Used unit testing and on catastrophic events
     * such as HDFS is
     * yanked out from under hbase or we OOME. the reason we are aborting the
     * exception that caused
     * the abort, or null
     */
    @Override
    public void abort(String why, Throwable e) {
        if (!setAbortRequested()) {
            // Abort already in progress, ignore the new request.
            LOG.debug("Abort already in progress. Ignoring the current request.");
            return;
        }
        String msg = "***** ABORTING region server " + this + " *****";
        // Do our best to report our abort to the master, but this may not work

        // scheduleAbortTimer();
        // shutdown should be run as the internal user
        stop(msg);
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
        if (!this.stopped) {
            LOG.info("***** STOPPING region server '" + this + "' *****");
            this.stopped = true;
            LOG.info("STOPPED: " + why);
            // Wakes run() if it is sleeping
            sleeper.skipSleepCycle();
        }

    }

    @Override
    public boolean isStopped() {
        return this.stopped;
    }

    protected void initializeMemStoreChunkCreator() {
        // TODO: MemStoreLAB, simply leave empty now
        LOG.debug("initializeMemStoreChunkCreator");
    }

    @Override
    public ConcurrentMap<byte[], Boolean> getRegionsInTransitionInRS() {
        return this.regionsInTransitionInRS;
    }

    /**
     * We need a timeout. If not there is a risk of giving a wrong information: this
     * would double the
     * number of network calls instead of reducing them.
     */
    private static final int TIMEOUT_REGION_MOVED = (2 * 60 * 1000);

    private void addToMovedRegions(String encodedName, ServerName destination, long closeSeqNum,
            boolean selfMove) {
        if (selfMove) {
            LOG.warn("Not adding moved region record: " + encodedName + " to self.");
            return;
        }
        LOG.info("Adding " + encodedName + " move to " + destination + " record at close sequenceid="
                + closeSeqNum);
        movedRegionInfoCache.put(encodedName, new MovedRegionInfo(destination, closeSeqNum));
    }

    void removeFromMovedRegions(String encodedName) {
        movedRegionInfoCache.invalidate(encodedName);
    }

    public MovedRegionInfo getMovedRegion(String encodedRegionName) {
        return movedRegionInfoCache.getIfPresent(encodedRegionName);
    }

    public int movedRegionCacheExpiredTime() {
        return TIMEOUT_REGION_MOVED;
    }

    private static class MovedRegionInfo {
        private final ServerName serverName;
        private final long seqNum;

        MovedRegionInfo(ServerName serverName, long closeSeqNum) {
            this.serverName = serverName;
            this.seqNum = closeSeqNum;
        }

        public ServerName getServerName() {
            return serverName;
        }

        public long getSeqNum() {
            return seqNum;
        }
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
        LOG.info("HRegionServer running");
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
            // TODO: register with master
            if (!isStopped() && !isAborted()) {
                // Try and register with the Master; tell it we are here. Break if server is
                // stopped or
                // the clusterup flag is down or hdfs went wacky. Once registered successfully,
                // go ahead and
                // start up all Services. Use RetryCounter to get backoff in case Master is
                // struggling to
                // come up.
                LOG.debug("About to register with Master.");
                TraceUtil.trace(() -> {
                    RetryCounterFactory rcf = new RetryCounterFactory(Integer.MAX_VALUE, this.sleeper.getPeriod(),
                            1000 * 60 * 5);
                    RetryCounter rc = rcf.create();
                    while (keepLooping()) {
                        RegionServerStartupResponse w = reportForDuty();
                        if (w == null) {
                            long sleepTime = rc.getBackoffTimeAndIncrementAttempts();
                            LOG.warn("reportForDuty failed; sleeping {} ms and then retrying.", sleepTime);
                            this.sleeper.sleep(sleepTime);
                        } else {
                            LOG.debug("reportForDuty succeeded; continuing.");
                            handleReportForDutyResponse(w);
                            break;
                        }
                    }
                }, "HRegionServer.registerWithMaster");

            }
            // TODO: start handlers

            // TODO: main run loop
            // We registered with the Master. Go into run mode.
            long lastMsg = EnvironmentEdgeManager.currentTime();
            while (!isStopped() && isHealthy()) {
                if (!isClusterUp()) {
                    LOG.info("Cluster down, regionserver shutting down");
                    break;
                }
                LOG.debug("Running, Server status - " + this.rpcServices.getRpcServer().toString());
                // LOG.debug(this.rpcServices.getServices().toString());
                // the main run loop
                long now = EnvironmentEdgeManager.currentTime();
                if ((now - lastMsg) >= msgInterval) {
                    // tryRegionServerReport(lastMsg, now);
                    lastMsg = EnvironmentEdgeManager.currentTime();
                }
                if (!isStopped() && !isAborted()) {
                    this.sleeper.sleep();
                }
            }
            // TODO: cleanup
            this.sleeper.sleep();
        } catch (Throwable e) {
            abort("Fatal exception during registration", e);
        }

        try {
            deleteMyEphemeralNode();
        } catch (KeeperException.NoNodeException nn) {
            // pass
        } catch (KeeperException e) {
            LOG.warn("Failed deleting my ephemeral node", e);
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
        if (clusterConnection == null) {
            clusterConnection = createClusterConnection();
        }
    }

    protected ClusterConnection createClusterConnection() throws IOException {
        // Create a cluster connection that when appropriate, can short-circuit and go
        // directly to the
        // local server if the request is to the local server bypassing RPC. Can be used
        // for both local
        // and remote invocations.
        return ServerConnectionUtils.createShortCircuitConnection(conf, null,
                serverName, rpcServices, rpcServices, null);
    }

    /*
     * Verify that server is healthy
     */
    private boolean isHealthy() {
        if (!dataFsOk) {
            // File system problem
            return false;
        }
        // Verify that all threads are alive
        boolean healthy = true;
        if (!healthy) {
            stop("One or more threads are no longer alive -- stop");
        }
        return healthy;
    }

    /**
     * Checks to see if the file system is still accessible. If not, sets
     * abortRequested and
     * stopRequested
     * 
     * @return false if file system is not available
     */
    boolean checkFileSystem() {
        // if (this.dataFsOk && this.dataFs != null) {
        // try {
        // FSUtils.checkFileSystemAvailable(this.dataFs);
        // } catch (IOException e) {
        // abort("File System not available", e);
        // this.dataFsOk = false;
        // }
        // }
        return this.dataFsOk;
    }

    /**
     * @return True if we should break loop because cluster is going down or this
     *         server has been
     *         stopped or hdfs has gone bad.
     */
    private boolean keepLooping() {
        return !this.stopped && isClusterUp();
    }

    /*
     * Let the master know we're here Run initialization using parameters passed us
     * by the master.
     * 
     * @return A Map of key/value configurations we got from the Master else null if
     * we failed to
     * register.
     */
    private RegionServerStartupResponse reportForDuty() throws IOException {
        if (this.masterless) {
            return RegionServerStartupResponse.getDefaultInstance();
        }
        ServerName masterServerName = createRegionServerStatusStub(true);
        RegionServerStatusService.BlockingInterface rss = rssStub;
        if (masterServerName == null || rss == null) {
            return null;
        }
        RegionServerStartupResponse result = null;
        try {
            LOG.info("reportForDuty to master=" + masterServerName + " with isa=" + rpcServices.isa
                    + ", startcode=" + this.startcode);
            long now = EnvironmentEdgeManager.currentTime();
            int port = rpcServices.isa.getPort();
            RegionServerStartupRequest.Builder request = RegionServerStartupRequest.newBuilder();
            if (!StringUtils.isBlank(useThisHostnameInstead)) {
                request.setUseThisHostnameInstead(useThisHostnameInstead);
            }
            request.setPort(port);
            request.setServerStartCode(this.startcode);
            request.setServerCurrentTime(now);
            LOG.debug("Report request: \n" + request.toString().replaceAll("[\r ]", ""));
            result = rss.regionServerStartup(null, request.build());
        } catch (Exception e) {
            LOG.debug("Error talking to master", e);
            rssStub = null;
        }
        return result;
    }

    /**
     * Get the current master from ZooKeeper and open the RPC connection to it. To
     * get a fresh
     * connection, the current rssStub must be null. Method will block until a
     * master is available.
     * You can break from this block by requesting the server stop.
     * 
     * @param refresh If true then master address will be read from ZK, otherwise
     *                use cached data
     * @return master + port, or null if server has been stopped
     */
    protected synchronized ServerName createRegionServerStatusStub(boolean refresh) {
        if (rssStub != null) {
            return masterAddressTracker.getMasterAddress();
        }
        ServerName sn = null;
        long previousLogTime = 0;
        RegionServerStatusService.BlockingInterface intRssStub = null;
        boolean interrupted = false;
        try {
            while (keepLooping()) {
                sn = this.masterAddressTracker.getMasterAddress(refresh);
                LOG.debug("createRegionServerStatusStub: master servername=" + sn + ", refresh=" + refresh);
                if (sn == null) {
                    if (!keepLooping()) {
                        // give up with no connection.
                        LOG.debug("No master found and cluster is stopped; bailing out");
                        return null;
                    }
                    if (EnvironmentEdgeManager.currentTime() > (previousLogTime + 1000)) {
                        LOG.debug("No master found; retry");
                        previousLogTime = EnvironmentEdgeManager.currentTime();
                    }
                    refresh = true; // let's try pull it from ZK directly
                    if (sleepInterrupted(200)) {
                        interrupted = true;
                    }
                    continue;
                }

                // If we are on the active master, use the shortcut
                if (this instanceof HMaster && sn.equals(getServerName())) {
                    intRssStub = ((HMaster) this).getMasterRpcServices();
                    LOG.debug("this server is the master, using shortcut");
                    break;
                }
                // If we are not the active master, then create using the RPC client
                try {
                    BlockingRpcChannel channel = createChannelToServerName(sn);
                    LOG.debug(channel.toString());
                    LOG.debug(this.rpcClient.toString() + "," + this.rpcClient.getClass().getName());
                    intRssStub = RegionServerStatusService.newBlockingStub(channel);
                    break;
                } catch (IOException e) {
                    if (EnvironmentEdgeManager.currentTime() > (previousLogTime + 1000)) {
                        e = e instanceof RemoteException ? ((RemoteException) e).unwrapRemoteException() : e;
                        if (e instanceof ServerNotRunningYetException) {
                            LOG.info("Master isn't available yet, retrying");
                        } else {
                            LOG.warn("Unable to connect to master. Retrying. Error was:", e);
                        }
                        previousLogTime = EnvironmentEdgeManager.currentTime();
                    }
                    if (sleepInterrupted(200)) {
                        interrupted = true;
                    }
                }
            }
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
        this.rssStub = intRssStub;
        LOG.debug("rssStub:" + intRssStub);
        return sn;
    }

    private static boolean sleepInterrupted(long millis) {
        boolean interrupted = false;
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            LOG.warn("Interrupted while sleeping");
            interrupted = true;
        }
        return interrupted;
    }

    /*
     * Run init. Sets up wal and starts up all server threads.
     * 
     * @param c Extra configuration.
     */
    protected void handleReportForDutyResponse(final RegionServerStartupResponse c) throws IOException {
        // Set our ephemeral znode up in zookeeper now we have a name.
        // LOG.debug("handleReportForDutyResponse: " + c.toString().replaceAll("[\r ]",
        // ""));
        try {
            createMyEphemeralNode();
        } catch (Throwable e) {
            stop("Failed initialization");
            throw convertThrowableToIOE(cleanup(e, "Failed init"), "Region server startup failed");
        } finally {
            sleeper.skipSleepCycle();
        }
        // TODO ...

        startServices();

        // Set up ZK
        LOG.info(
                "Serving as " + this.serverName + ", RpcServer on " + rpcServices.isa + ", sessionid=0x"
                        + Long.toHexString(this.zooKeeper.getRecoverableZooKeeper().getSessionId()));

        // Wake up anyone waiting for this server to online
        synchronized (online) {
            online.set(true);
            online.notifyAll();
        }
    }

    /**
     * Start maintenance Threads, Server, Worker and lease checker threads. Start
     * all threads we need
     * to run. This is called after we've successfully registered with the Master.
     * Install an
     * UncaughtExceptionHandler that calls abort of RegionServer if we get an
     * unhandled exception. We
     * cannot set the handler on all threads. Server's internal Listener thread is
     * off limits. For
     * Server, if an OOME, it waits a while then retries. Meantime, a flush or a
     * compaction that tries
     * to run should trigger same critical condition and the shutdown will run. On
     * its way out, this
     * server will shut down Server. Leases are sort of inbetween. It has an
     * internal thread that
     * while it inherits from Chore, it keeps its own internal stop mechanism so
     * needs to be stopped
     * by this hosting server. Worker logs the exception and exits.
     */
    private void startServices() throws IOException {
        if (!isStopped() && !isAborted()) {
            initializeThreads();
        }
        // Start executor services
        final int openRegionThreads = conf.getInt("hbase.regionserver.executor.openregion.threads", 3);
        executorService.startExecutorService(executorService.new ExecutorConfig()
                .setExecutorType(ExecutorType.RS_OPEN_REGION).setCorePoolSize(openRegionThreads));
        final int openMetaThreads = conf.getInt("hbase.regionserver.executor.openmeta.threads", 1);
        executorService.startExecutorService(executorService.new ExecutorConfig()
                .setExecutorType(ExecutorType.RS_OPEN_META).setCorePoolSize(openMetaThreads));
    }

    private void initializeThreads() {
        this.cacheFlusher = new MemStoreFlusher(this.conf, this);
    }

    @Override
    public ServerName getServerName() {
        return serverName;
    }

    /**
     * @param msg Message to put in new IOE if passed <code>t</code> is not an IOE
     * @return Make <code>t</code> an IOE if it isn't already.
     */
    private IOException convertThrowableToIOE(final Throwable t, final String msg) {
        return (t instanceof IOException ? (IOException) t
                : msg == null || msg.length() == 0 ? new IOException(t)
                        : new IOException(msg, t));
    }

    private void createMyEphemeralNode() throws KeeperException {
        RegionServerInfo.Builder rsInfo = RegionServerInfo.newBuilder();
        // rsInfo.setInfoPort(infoServer != null ? infoServer.getPort() : -1);
        // rsInfo.setVersionInfo(ProtobufUtil.getVersionInfo());
        byte[] data = ProtobufUtil.prependPBMagic(rsInfo.build().toByteArray());
        ZKUtil.createEphemeralNodeAndWatch(this.zooKeeper, getMyEphemeralNodePath(), data);
    }

    private void deleteMyEphemeralNode() throws KeeperException {
        ZKUtil.deleteNode(this.zooKeeper, getMyEphemeralNodePath());
    }

    private String getMyEphemeralNodePath() {
        return zooKeeper.getZNodePaths().getRsPath(serverName);
    }

    /**
     * Cleanup after Throwable caught invoking method. Converts <code>t</code> to
     * IOE if it isn't
     * already.
     * 
     * @param t   Throwable
     * @param msg Message to log in error. Can be null.
     * @return Throwable converted to an IOE; methods can only let out IOEs.
     */
    private Throwable cleanup(final Throwable t, final String msg) {
        // Don't log as error if NSRE; NSRE is 'normal' operation.
        if (t instanceof NotServingRegionException) {
            LOG.debug("NotServingRegionException; " + t.getMessage());
            return t;
        }
        Throwable e = t instanceof RemoteException ? ((RemoteException) t).unwrapRemoteException() : t;
        if (msg == null) {
            LOG.error("", e);
        } else {
            LOG.error(msg, e);
        }
        // if (!rpcServices.checkOOME(t)) {
        checkFileSystem();
        // }
        return t;
    }

    /**
     * Returns {@code true} when the data file system is available, {@code false}
     * otherwise.
     */
    boolean isDataFileSystemOk() {
        return this.dataFsOk;
    }

    /**
     * Report the status of the server. A server is online once all the startup is
     * completed (setting
     * up filesystem, starting executorService threads, etc.). This method is
     * designed mostly to be
     * useful in tests.
     * 
     * @return true if online, false if not.
     */
    public boolean isOnline() {
        return online.get();
    }

    public RSRpcServices getRSRpcServices() {
        return this.rpcServices;
    }

    @Override
    public void addRegion(HRegion region) {
        this.onlineRegions.put(region.getRegionInfo().getEncodedName(), region);
    }

    @Override
    public boolean removeRegion(HRegion r, ServerName destination) {
        HRegion toReturn = this.onlineRegions.remove(r.getRegionInfo().getEncodedName());
        // TODO: flush or other stuff before remove...
        return toReturn != null;
    }

    @Override
    public HRegion getRegion(final String encodedRegionName) {
        return this.onlineRegions.get(encodedRegionName);
    }

    /**
     * Protected Utility method for safely obtaining an HRegion handle.
     * 
     * @param regionName Name of online {@link HRegion} to return
     * @return {@link HRegion} for <code>regionName</code>
     */
    protected HRegion getRegion(final byte[] regionName) throws NotServingRegionException {
        String encodedRegionName = RegionInfo.encodeRegionName(regionName);
        return getRegionByEncodedName(regionName, encodedRegionName);
    }

    public HRegion getRegionByEncodedName(String encodedRegionName) throws NotServingRegionException {
        return getRegionByEncodedName(null, encodedRegionName);
    }

    private HRegion getRegionByEncodedName(byte[] regionName, String encodedRegionName)
            throws NotServingRegionException {
        HRegion region = this.onlineRegions.get(encodedRegionName);
        if (region == null) {
            MovedRegionInfo moveInfo = getMovedRegion(encodedRegionName);
            if (moveInfo != null) {
                throw new RegionMovedException(moveInfo.getServerName(), moveInfo.getSeqNum());
            }
            Boolean isOpening = this.regionsInTransitionInRS.get(Bytes.toBytes(encodedRegionName));
            String regionNameStr = regionName == null ? encodedRegionName : Bytes.toStringBinary(regionName);
            if (isOpening != null && isOpening) {
                throw new RegionOpeningException(
                        "Region " + regionNameStr + " is opening on " + this.serverName);
            }
            throw new NotServingRegionException(
                    "" + regionNameStr + " is not online on " + this.serverName);
        }
        return region;
    }

    public MemStoreFlusher getMemStoreFlusher() {
        return cacheFlusher;
    }

    /**
     * return all online regions
     */
    @Override
    public List<HRegion> getRegions(TableName tableName) {
        List<HRegion> tableRegions = new ArrayList<>();
        synchronized (this.onlineRegions) {
            for (HRegion region : this.onlineRegions.values()) {
                RegionInfo regionInfo = region.getRegionInfo();
                if (regionInfo.getTable().equals(tableName)) {
                    tableRegions.add(region);
                }
            }
        }
        return tableRegions;
    }

    /**
     * return all regions
     */
    @Override
    public List<HRegion> getRegions() {
        List<HRegion> allRegions;
        synchronized (this.onlineRegions) {
            // Return a clone copy of the onlineRegions
            allRegions = new ArrayList<>(onlineRegions.values());
        }
        return allRegions;
    }

    public void registerService(Service service) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'registerService'");
    }

    protected BlockingRpcChannel createChannelToServerName(ServerName sn) throws IOException {
        return this.rpcClient.createBlockingRpcChannel(sn, User.getCurrent(), shortOperationTimeout);
    }

    @Override
    public RegionServerAccounting getRegionServerAccounting() {
        return this.regionServerAccounting;
    }
}
