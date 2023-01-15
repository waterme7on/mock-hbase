package org.waterme7on.hbase.regionserver;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.context.Scope;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.trace.TraceUtil;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.hadoop.util.StringUtils;
import org.waterme7on.hbase.fs.HFileSystem;
import org.waterme7on.hbase.util.NettyEventLoopGroupConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
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

    /*
     * HDFS components
     */
    private HFileSystem dataFs;
    private HFileSystem walFs;

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
            String hostName = org.apache.commons.lang3.StringUtils.isBlank(useThisHostnameInstead)
                    ? this.rpcServices.isa.getHostName()
                    : this.useThisHostnameInstead;
            this.serverName = ServerName.valueOf(hostName, this.rpcServices.isa.getPort(), this.startcode);
            this.stopped = false;
            this.abortRequested = new AtomicBoolean(false);
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

        for (int i = 0; i < 10; i++) {
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
    };
}
