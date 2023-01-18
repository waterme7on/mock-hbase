package org.waterme7on.hbase.master;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.context.Scope;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ClusterId;
import org.apache.hadoop.hbase.ClusterMetrics;
import org.apache.hadoop.hbase.ClusterMetricsBuilder;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.ipc.ServerNotRunningYetException;
import org.apache.hadoop.hbase.log.HBaseMarkers;
import org.apache.hadoop.hbase.ServerMetrics;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.RegionServerInfo;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.RegionServerStatusService.BlockingInterface;
import org.apache.hadoop.hbase.trace.TraceUtil;
import org.apache.hadoop.hbase.util.Addressing;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.util.VersionInfo;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.waterme7on.hbase.regionserver.HRegion;
import org.waterme7on.hbase.regionserver.HRegionFactory;
import org.waterme7on.hbase.regionserver.HRegionServer;
import org.waterme7on.hbase.regionserver.RSRpcServices;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.lang.management.MemoryType;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

import org.apache.hbase.thirdparty.org.eclipse.jetty.server.Handler;
import org.apache.hbase.thirdparty.org.eclipse.jetty.server.Server;
import org.apache.hbase.thirdparty.org.eclipse.jetty.server.ServerConnector;
import org.apache.hadoop.hbase.zookeeper.ZKClusterId;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.waterme7on.hbase.ipc.RpcServer;
import org.waterme7on.hbase.monitoring.MonitoredTask;
import org.waterme7on.hbase.monitoring.TaskMonitor;

public class HMaster extends HRegionServer implements MasterServices {
    private static final Logger LOG = LoggerFactory.getLogger(HMaster.class);

    /** jetty server for master to redirect requests to regionserver infoServer */
    private Server masterJettyServer;
    // file system manager for the master FS operations
    private MasterFileSystem fileSystemManager;
    private MasterWalManager walManager;

    // Manager and zk listener for master election
    private final ActiveMasterManager activeMasterManager;

    // MASTER is name of the webapp and the attribute name used stuffing this
    // instance into a web context !! AND OTHER PLACES !!
    public static final String MASTER = "master";
    // flag set after we become the active master (used for testing)
    private volatile boolean activeMaster = false;
    private HRegion masterRegion;
    private RegionServerList rsListStorage;
    // server manager to deal with region server info
    private volatile ServerManager serverManager;
    // flag set after master services are started,
    // initialization may have not completed yet.
    volatile boolean serviceStarted = false;
    // Time stamps for when a hmaster became active
    private long masterActiveTime;
    // private final RegionServerTracker regionServerTracker;

    public HMaster(final Configuration conf) throws IOException {
        super(conf);
        final Span span = TraceUtil.createSpan("HMaster.cxtor");
        try (Scope ignored = span.makeCurrent()) {
            LOG.info("hbase.rootdir={}, hbase.cluster.distributed={}", getDataRootDir(),
                    this.conf.getBoolean(HConstants.CLUSTER_DISTRIBUTED, false));
            this.conf.setBoolean(HConstants.USE_META_REPLICAS, false);
            this.activeMasterManager = createActiveMasterManager(zooKeeper, serverName, this);
            span.setStatus(StatusCode.OK);
        } catch (Throwable t) {
            // Make sure we log the exception. HMaster is often started via reflection and
            // the
            // cause of failed startup is lost.
            TraceUtil.setError(span, t);
            LOG.error("Failed construction of Master", t);
            throw t;
        } finally {
            span.end();
        }
    }

    @Override
    public void run() {
        try {
            Threads.setDaemonThreadRunning(new Thread(() -> TraceUtil.trace(() -> {
                try {
                    // TODO
                    // int infoPort = putUpJettyServer();
                    startActiveMasterManager(-1);
                } catch (Throwable t) {
                    // Make sure we log the exception.
                    String error = "Failed to become Active Master";
                    LOG.error(error, t);
                    // Abort should have been called already.
                    if (!isAborted()) {
                        abort(error, t);
                    }
                }
            }, "HMaster.becomeActiveMaster")), getName() + ":becomeActiveMaster");
            // Fall in here even if we have been aborted. Need to run the shutdown services
            // and
            // the super run call will do this for us.
            super.run();
            LOG.debug("master exiting main loop");
        } finally {
            final Span span = TraceUtil.createSpan("HMaster exiting main loop");
            try (Scope ignored = span.makeCurrent()) {
                this.activeMaster = false;
                span.setStatus(StatusCode.OK);
            } finally {
                span.end();
            }
        }
    }

    public void startActiveMasterManager(int infoPort) throws KeeperException {
        // TODO
        // omit details such as backup nodes, currently, we only support one master

        this.activeMasterManager.setInfoPort(infoPort);
        int timeout = conf.getInt(HConstants.ZK_SESSION_TIMEOUT, HConstants.DEFAULT_ZK_SESSION_TIMEOUT);
        MonitoredTask status = TaskMonitor.get().createStatus("Master startup"); //
        status.setDescription("Master startup");

        try {
            if (activeMasterManager.blockUntilBecomingActiveMaster(timeout, status)) {
                finishActiveMasterInitialization(status);
            }
        } catch (Throwable t) {
            status.setStatus("Failed to become active: " + t.getMessage());
            LOG.error(HBaseMarkers.FATAL, "Failed to become active master", t);
            // HBASE-5680: Likely hadoop23 vs hadoop 20.x/1.x incompatibility
            if (t instanceof NoClassDefFoundError
                    && t.getMessage().contains("org/apache/hadoop/hdfs/protocol/HdfsConstants$SafeModeAction")) {
                // improved error message for this special case
                abort("HBase is having a problem with its Hadoop jars.  You may need to recompile "
                        + "HBase against Hadoop version " + org.apache.hadoop.util.VersionInfo.getVersion()
                        + " or change your hadoop jars to start properly", t);
            } else {
                abort("Unhandled exception. Starting shutdown.", t);
            }
        } finally {
            status.cleanup();
        }
    }

    private int putUpJettyServer() throws IOException {
        final int infoPort = conf.getInt("hbase.master.info.port.orig", HConstants.DEFAULT_MASTER_INFOPORT);
        if (infoPort < 0) {
            return -1;
        }
        final String addr = conf.get("hbase.master.info.bindAddress", "0.0.0.0");
        if (!Addressing.isLocalAddress(InetAddress.getByName(addr))) {
            String msg = "Failed to start redirecting jetty server. Address " + addr
                    + " does not belong to this host. Correct configuration parameter: "
                    + "hbase.master.info.bindAddress";
            LOG.error(msg);
            throw new IOException(msg);
        }

        LOG.debug("Jetty Server start up, address " + addr + ":" + infoPort);

        // TODO simply return the infoport now, later we need to start a jetty server
        // for information

        masterJettyServer = new Server();

        final ServerConnector connector = new ServerConnector(masterJettyServer);
        connector.setHost(addr);
        connector.setPort(infoPort);
        masterJettyServer.addConnector(connector);
        masterJettyServer.setStopAtShutdown(true);
        masterJettyServer
                .setHandler(org.apache.hadoop.hbase.http.HttpServer.buildGzipHandler(masterJettyServer.getHandler()));

        try {
            masterJettyServer.start();
        } catch (Exception e) {
            throw new IOException("Failed to start redirecting jetty server", e);
        }
        return connector.getLocalPort();
    }

    /**
     * Finish initialization of HMaster after becoming the primary master.
     *
     * The startup order is a bit complicated but very important, do not change it
     * unless you know
     * what you are doing.
     *
     * Publish cluster id
     *
     * Here comes the most complicated part - initialize server manager, assignment
     * manager and
     * region server tracker
     * - Create master local region
     * - Create server manager
     * - Wait for meta to be initialized if necessary, start table state manager.
     * - Wait for enough region servers to check-in
     * - Let assignment manager load data from meta and construct region states
     * - Start all other things such as chore services, etc
     *
     *
     * Notice that now we will not schedule a special procedure to make meta
     * online(unless the first
     * time where meta has not been created yet), we will rely on SCP to bring meta
     * online.
     */
    private void finishActiveMasterInitialization(MonitoredTask status)
            throws IOException, InterruptedException, KeeperException {
        /*
         * We are active master now... go initialize components we need to run.
         */
        status.setStatus("Initializing Master file system");
        this.masterActiveTime = EnvironmentEdgeManager.currentTime();

        // always initialize the MemStoreLAB as we use a region to store data in master
        initializeMemStoreChunkCreator(); // TODO
        this.fileSystemManager = new MasterFileSystem(conf); // do file read/write
        this.walManager = new MasterWalManager(this); // wal read/write into filesystem

        // Publish cluster ID; set it in Master too. The superclass RegionServer does
        // this later but
        // only after it has checked in with the Master. At least a few tests ask Master
        // for clusterId
        // before it has called its run method and before RegionServer has done the
        // reportForDuty.
        ClusterId clusterId = fileSystemManager.getClusterId();
        status.setStatus("Publishing Cluster ID " + clusterId + " in ZooKeeper");
        ZKClusterId.setClusterId(this.zooKeeper,
                clusterId);
        this.clusterId = clusterId.toString();
        LOG.debug("zookeeper:" + this.zooKeeper.toString());
        LOG.debug("clusterId:" + this.clusterId);

        status.setStatus("Initialize ServerManager and schedule SCP for crash servers");
        // The below two managers must be created before loading procedures, as they
        // will be used during loading.
        // initialize master local region
        masterRegion = HRegionFactory.create(this);
        rsListStorage = new MasterRegionServerList(masterRegion, this);
        this.serverManager = createServerManager(this, rsListStorage);
        // TODO...

        status.setStatus("Initializing ZK system trackers");
        initializeZKBasedSystemTrackers();
        // Set ourselves as active Master now our claim has succeeded up in zk.
        this.activeMaster = true;

        // TODO...

        // start up all service threads.
        status.setStatus("Initializing master service threads");
        startServiceThreads();

        // TODO...

        // Set master as 'initialized'.
        status.markComplete("Initialization successful");
        setInitialized(true);

        // TODO...
        LOG.info("Master finish ActiveMasterInitialization and become active");
    }

    private void initializeZKBasedSystemTrackers()
            throws IOException, KeeperException {
        // TODO
        LOG.debug("initializeZKBasedSystemTrackers");

        // Set the cluster as up. If new RSs, they'll be waiting on this before
        // going ahead with their startup.
        boolean wasUp = this.clusterStatusTracker.isClusterUp();
        if (!wasUp) {
            this.clusterStatusTracker.setClusterUp();
            LOG.debug("cluster is up");
        } else {
            LOG.debug("cluster is already up");
        }
    }

    public void shutdown() throws IOException {
        TraceUtil.trace(() -> {
            // Tell the servermanager cluster shutdown has been called. This makes it so
            // when Master is
            // last running server, it'll stop itself. Next, we broadcast the cluster
            // shutdown by setting
            // the cluster status as down. RegionServers will notice this change in state
            // and will start
            // shutting themselves down. When last has exited, Master can go down.
            if (this.serverManager != null) {
                this.serverManager.shutdownCluster();
            }
            if (this.clusterStatusTracker != null) {
                try {
                    this.clusterStatusTracker.setClusterDown();
                } catch (KeeperException e) {
                    LOG.error("ZooKeeper exception trying to set cluster as down in ZK", e);
                }
            }

        }, "HMaster.shutdown");
    }

    private void startServiceThreads() throws IOException {
        // TODO
        LOG.debug("startServiceThreads");
    }

    private ServerManager createServerManager(MasterServices master, RegionServerList storage) {
        // TODO
        LOG.debug("createServerManager");
        return new ServerManager(master, storage);
    }

    /** Returns cluster status */
    public ClusterMetrics getClusterMetrics() throws IOException {
        return getClusterMetrics(EnumSet.allOf(ClusterMetrics.Option.class));
    }

    public ClusterMetrics getClusterMetrics(EnumSet<ClusterMetrics.Option> options) throws IOException {
        // if (cpHost != null) {
        // cpHost.preGetClusterMetrics();
        // }
        ClusterMetrics status = getClusterMetricsWithoutCoprocessor(options);
        // if (cpHost != null) {
        // cpHost.postGetClusterMetrics(status);
        // }
        return status;
    }

    public ClusterMetrics getClusterMetricsWithoutCoprocessor(EnumSet<ClusterMetrics.Option> options)
            throws InterruptedIOException {
        ClusterMetricsBuilder builder = ClusterMetricsBuilder.newBuilder();
        // given that hbase1 can't submit the request with Option,
        // we return all information to client if the list of Option is empty.
        if (options.isEmpty()) {
            options = EnumSet.allOf(ClusterMetrics.Option.class);
        }
        for (ClusterMetrics.Option opt : options) {
            switch (opt) {
                case HBASE_VERSION:
                    builder.setHBaseVersion(VersionInfo.getVersion());
                    break;
                case CLUSTER_ID:
                    builder.setClusterId(getClusterId());
                    break;
                case MASTER:
                    builder.setMasterName(getServerName());
                    break;
                case BACKUP_MASTERS:
                    // builder.setBackerMasterNames(getBackupMasters());
                    break;
                case TASKS: {
                    // // Master tasks
                    // builder.setMasterTasks(TaskMonitor.get().getTasks().stream()
                    // .map(task ->
                    // ServerTaskBuilder.newBuilder().setDescription(task.getDescription())
                    // .setStatus(task.getStatus())
                    // .setState(ServerTask.State.valueOf(task.getState().name()))
                    // .setStartTime(task.getStartTime()).setCompletionTime(task.getCompletionTimestamp())
                    // .build())
                    // .collect(Collectors.toList()));
                    // // TASKS is also synonymous with LIVE_SERVERS for now because task
                    // information
                    // // for
                    // // regionservers is carried in ServerLoad.
                    // // Add entries to serverMetricsMap for all live servers, if we haven't
                    // already
                    // // done so
                    // if (serverMetricsMap == null) {
                    // serverMetricsMap = getOnlineServers();
                    // }
                    break;
                }
                case LIVE_SERVERS: {
                    // Add entries to serverMetricsMap for all live servers, if we haven't already
                    // done so
                    // if (serverMetricsMap == null) {
                    // serverMetricsMap = getOnlineServers();
                    // }
                    builder.setLiveServerMetrics(getOnlineServers());
                    break;
                }
                case DEAD_SERVERS: {
                    // if (serverManager != null) {
                    // builder.setDeadServerNames(
                    // new ArrayList<>(serverManager.getDeadServers().copyServerNames()));
                    // }
                    break;
                }
                case UNKNOWN_SERVERS: {
                    // if (serverManager != null) {
                    // builder.setUnknownServerNames(getUnknownServers());
                    // }
                    break;
                }
                case MASTER_COPROCESSORS: {
                    // if (cpHost != null) {
                    // builder.setMasterCoprocessorNames(Arrays.asList(getMasterCoprocessors()));
                    // }
                    break;
                }
                case REGIONS_IN_TRANSITION: {
                    // if (assignmentManager != null) {
                    // builder.setRegionsInTransition(
                    // assignmentManager.getRegionStates().getRegionsStateInTransition());
                    // }
                    break;
                }
                case BALANCER_ON: {
                    // if (loadBalancerTracker != null) {
                    // builder.setBalancerOn(loadBalancerTracker.isBalancerOn());
                    // }
                    break;
                }
                case MASTER_INFO_PORT: {
                    // if (infoServer != null) {
                    // builder.setMasterInfoPort(infoServer.getPort());
                    // }
                    break;
                }
                case SERVERS_NAME: {
                    if (serverManager != null) {
                        builder.setServerNames(serverManager.getOnlineServersList());
                    }
                    break;
                }
                case TABLE_TO_REGIONS_COUNT: {
                    // if (isActiveMaster() && isInitialized() && assignmentManager != null) {
                    // try {
                    // Map<TableName, RegionStatesCount> tableRegionStatesCountMap = new
                    // HashMap<>();
                    // Map<String, TableDescriptor> tableDescriptorMap =
                    // getTableDescriptors().getAll();
                    // for (TableDescriptor tableDescriptor : tableDescriptorMap.values()) {
                    // TableName tableName = tableDescriptor.getTableName();
                    // RegionStatesCount regionStatesCount =
                    // assignmentManager.getRegionStatesCount(tableName);
                    // tableRegionStatesCountMap.put(tableName, regionStatesCount);
                    // }
                    // builder.setTableRegionStatesCount(tableRegionStatesCountMap);
                    // } catch (IOException e) {
                    // LOG.error("Error while populating TABLE_TO_REGIONS_COUNT for Cluster
                    // Metrics..", e);
                    // }
                    // }
                    break;
                }
            }
        }
        return builder.build();
    }

    private Map<ServerName, ServerMetrics> getOnlineServers() {
        if (serverManager != null) {
            final Map<ServerName, ServerMetrics> map = new HashMap<>();
            serverManager.getOnlineServers().entrySet().forEach(e -> map.put(e.getKey(), e.getValue()));
            return map;
        }
        return null;
    }

    public void setInitialized(boolean isInitialized) {
        // procedureExecutor.getEnvironment().setEventReady(initialized, isInitialized);
        // TODO
        serviceStarted = true;
    }

    protected ActiveMasterManager createActiveMasterManager(ZKWatcher zk, ServerName sn,
            org.waterme7on.hbase.Server server) throws InterruptedIOException {
        return new ActiveMasterManager(zk, sn, server);
    }

    @Override
    public void abort(String reason, Throwable cause) {
        if (!setAbortRequested() || isStopped()) {
            LOG.debug("Abort called but aborted={}, stopped={}", isAborted(), isStopped());
            return;
        }

        try {
            stopMaster();
        } catch (IOException e) {
            LOG.error("Exception occurred while stopping master", e);
        }
    }

    @Override
    public void stop(String why) {
        if (!isStopped()) {
            super.stop(why);
            if (this.activeMasterManager != null) {
                this.activeMasterManager.stop();
            }
        }
    }

    @Override
    public boolean isStopped() {
        return false;
    }

    @Override
    public MasterFileSystem getMasterFileSystem() {
        return this.fileSystemManager;
    }

    @Override
    public ServerManager getServerManager() {
        return this.serverManager;
    }

    @Override
    public MasterWalManager getMasterWalManager() {
        return this.walManager;
    }

    @Override
    protected RSRpcServices createRpcServices() throws IOException {
        return new MasterRpcServices(this);
    }

    public MasterRpcServices getMasterRpcServices() {
        return (MasterRpcServices) rpcServices;
    }

    /**
     * @return true if this is the active master, for testing only
     */
    public boolean isActiveMaster() {
        return activeMaster;
    }

    public void stopMaster() throws IOException {
        stop("Stopped by " + Thread.currentThread().getName());
    }

    protected void checkServiceStarted() throws ServerNotRunningYetException {
        if (!serviceStarted) {
            throw new ServerNotRunningYetException("Server is not running yet");
        }
    }

    /**
     * Returns Client info for use as prefix on an audit log string; who did an
     * action
     */
    public String getClientIdAuditPrefix() {
        return "Client=" + RpcServer.getRequestUserName().orElse(null) + "/"
                + RpcServer.getRemoteAddress().orElse(null);
    }

    /** Returns Get remote side's InetAddress */
    InetAddress getRemoteInetAddress(final int port, final long serverStartCode)
            throws UnknownHostException {
        // Do it out here in its own little method so can fake an address when
        // mocking up in tests.
        InetAddress ia = RpcServer.getRemoteIp();

        // The call could be from the local regionserver,
        // in which case, there is no remote address.
        if (ia == null && serverStartCode == startcode) {
            InetSocketAddress isa = rpcServices.getSocketAddress();
            if (isa != null && isa.getPort() == port) {
                ia = isa.getAddress();
            }
        }
        return ia;
    }

    /**
     * Utility for constructing an instance of the passed HMaster class.
     * 
     * @return HMaster instance.
     */
    public static HMaster constructMaster(Class<? extends HMaster> masterClass,
            final Configuration conf) {
        try {
            Constructor<? extends HMaster> c = masterClass.getConstructor(Configuration.class);
            return c.newInstance(conf);
        } catch (Exception e) {
            Throwable err = e;
            if (e instanceof InvocationTargetException
                    && ((InvocationTargetException) e).getTargetException() != null) {
                err = ((InvocationTargetException) e).getTargetException();
            }
            throw new RuntimeException("Failed construction of Master: " + masterClass.toString() + ". ",
                    err);
        }
    }

    public String getClusterId() {
        return clusterId;
    }

    /**
     * @see org.waterme7on.hbase.master.HMasterCommandLine
     */
    public static void main(String[] args) {
        LOG.info("STARTING service " + HMaster.class.getSimpleName());
        if (args.length == 0) {
            new HMasterCommandLine(HMaster.class).doMain(new String[] { "start" });
        } else {
            new HMasterCommandLine(HMaster.class).doMain(args);
        }
    }

}
