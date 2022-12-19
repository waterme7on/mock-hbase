package org.waterme7on.hbase.master;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.context.Scope;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.log.HBaseMarkers;
import org.apache.hadoop.hbase.trace.TraceUtil;
import org.apache.hadoop.hbase.util.Addressing;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.waterme7on.hbase.regionserver.HRegion;
import org.waterme7on.hbase.regionserver.HRegionFactory;
import org.waterme7on.hbase.regionserver.HRegionServer;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.lang.management.MemoryType;
import java.net.InetAddress;
import org.apache.hbase.thirdparty.org.eclipse.jetty.server.Handler;
import org.apache.hbase.thirdparty.org.eclipse.jetty.server.Server;
import org.apache.hbase.thirdparty.org.eclipse.jetty.server.ServerConnector;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
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
    protected ServerName serverName;
    // flag set after we become the active master (used for testing)
    private volatile boolean activeMaster = false;
    private HRegion masterRegion;
    private RegionServerList rsListStorage;
    // server manager to deal with region server info
    private volatile ServerManager serverManager;

    public HMaster(final Configuration conf) throws IOException {
        super(conf);
        final Span span = TraceUtil.createSpan("HMaster.cxtor");
        try (Scope ignored = span.makeCurrent()) {
            LOG.info("hbase.rootdir={}, hbase.cluster.distributed={}", getDataRootDir(),
                    this.conf.getBoolean(HConstants.CLUSTER_DISTRIBUTED, false));
            this.conf.setBoolean(HConstants.USE_META_REPLICAS, false);
            this.activeMasterManager = createActiveMasterManager(zooKeeper, serverName, this);
            span.setStatus(StatusCode.OK);
        }catch (Throwable t) {
            // Make sure we log the exception. HMaster is often started via reflection and the
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
                    int infoPort = putUpJettyServer();
                    startActiveMasterManager(infoPort);
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
            // Fall in here even if we have been aborted. Need to run the shutdown services and
            // the super run call will do this for us.
            super.run();
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
        // omit details such as backup nodes

        this.activeMasterManager.setInfoPort(infoPort);
        int timeout = conf.getInt(HConstants.ZK_SESSION_TIMEOUT, HConstants.DEFAULT_ZK_SESSION_TIMEOUT);
        // If we're a backup master, stall until a primary to write this address
        if (conf.getBoolean(HConstants.MASTER_TYPE_BACKUP, HConstants.DEFAULT_MASTER_TYPE_BACKUP)) {
            LOG.debug("HMaster started in backup mode. Stalling until master znode is written.");
            // This will only be a minute or so while the cluster starts up,
            // so don't worry about setting watches on the parent znode
            while (!activeMasterManager.hasActiveMaster()) {
                LOG.debug("Waiting for master address and cluster state znode to be written.");
                Threads.sleep(timeout);
            }
        }

        MonitoredTask status = TaskMonitor.get().createStatus("Master startup"); // log
        status.setDescription("Master startup");

        try {
            if (activeMasterManager.blockUntilBecomingActiveMaster(timeout, status)) {
                finishActiveMasterInitialization(status);
            }
        } catch (Throwable t) {
            status.setStatus("Failed to become active: " + t.getMessage());
            LOG.error(HBaseMarkers.FATAL, "Failed to become active master", t);
            // HBASE-5680: Likely hadoop23 vs hadoop 20.x/1.x incompatibility
            if (
                    t instanceof NoClassDefFoundError
                            && t.getMessage().contains("org/apache/hadoop/hdfs/protocol/HdfsConstants$SafeModeAction")
            ) {
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
        final int infoPort =
                conf.getInt("hbase.master.info.port.orig", HConstants.DEFAULT_MASTER_INFOPORT);
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

        // TODO
        masterJettyServer = new Server();

        final ServerConnector connector = new ServerConnector(masterJettyServer);
        connector.setHost(addr);
        connector.setPort(infoPort);
        masterJettyServer.addConnector(connector);
        masterJettyServer.setStopAtShutdown(true);
        masterJettyServer.setHandler(org.apache.hadoop.hbase.http.HttpServer.buildGzipHandler(masterJettyServer.getHandler()));

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
     * The startup order is a bit complicated but very important, do not change it unless you know
     * what you are doing.
     *
     * Publish cluster id
     *
     * Here comes the most complicated part - initialize server manager, assignment manager and
     * region server tracker
     * - Create master local region
     * - Create server manager
     * - Wait for meta to be initialized if necessary, start table state manager.
     * - Wait for enough region servers to check-in
     * - Let assignment manager load data from meta and construct region states
     * - Start all other things such as chore services, etc
     *
     *
     * Notice that now we will not schedule a special procedure to make meta online(unless the first
     * time where meta has not been created yet), we will rely on SCP to bring meta online.
     */
    private void finishActiveMasterInitialization(MonitoredTask status)
            throws IOException, InterruptedException, KeeperException {
        /*
         * We are active master now... go initialize components we need to run.
         */
        status.setStatus("Initializing Master file system");
        // always initialize the MemStoreLAB as we use a region to store data in master now, see
        // localStore.
        initializeMemStoreChunkCreator(); // TODO
        this.fileSystemManager = new MasterFileSystem(conf); // do file read/write
        this.walManager = new MasterWalManager(this); // wal read/write into filesystem

        // The below two managers must be created before loading procedures, as they will be used during
        // loading.
        // initialize master local region
        masterRegion = HRegionFactory.create(this);
        rsListStorage = new MasterRegionServerList(masterRegion, this);
        this.serverManager = createServerManager(this, rsListStorage);
        this.activeMaster = true;
    }

    private ServerManager createServerManager(MasterServices master, RegionServerList storage) {
        // TODO
        return new ServerManager();
    }

    protected ActiveMasterManager createActiveMasterManager(ZKWatcher zk, ServerName sn,
                                                        org.waterme7on.hbase.Server server) throws InterruptedIOException {
        return new ActiveMasterManager(zk, sn, server);
    }

    protected void initializeMemStoreChunkCreator() {
        // TODO: MemStoreLAB, simply leave empty now
    }


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

    @Override
    public MasterFileSystem getMasterFileSystem() {
        return this.fileSystemManager;
    }

    @Override
    public MasterWalManager getMasterWalManager() {
        return this.walManager;
    }
}
