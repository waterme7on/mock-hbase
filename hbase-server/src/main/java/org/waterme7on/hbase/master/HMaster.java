package org.waterme7on.hbase.master;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.context.Scope;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.trace.TraceUtil;
import org.apache.hadoop.hbase.util.Addressing;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.waterme7on.hbase.regionserver.HRegionServer;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.InetAddress;
import org.apache.hbase.thirdparty.org.eclipse.jetty.server.Handler;
import org.apache.hbase.thirdparty.org.eclipse.jetty.server.Server;
import org.apache.hbase.thirdparty.org.eclipse.jetty.server.ServerConnector;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;

public class HMaster extends HRegionServer implements MasterServices {
    private static final Logger LOG = LoggerFactory.getLogger(HMaster.class);

    /** jetty server for master to redirect requests to regionserver infoServer */
    private Server masterJettyServer;

    // Manager and zk listener for master election
    private final ActiveMasterManager activeMasterManager;

    // MASTER is name of the webapp and the attribute name used stuffing this
    // instance into a web context !! AND OTHER PLACES !!
    public static final String MASTER = "master";
    protected ServerName serverName;
    // flag set after we become the active master (used for testing)
    private volatile boolean activeMaster = false;

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


    protected ActiveMasterManager createActiveMasterManager(ZKWatcher zk, ServerName sn,
                                                            org.waterme7on.hbase.Server server) throws InterruptedIOException {
        return new ActiveMasterManager(zk, sn, server);
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
}
