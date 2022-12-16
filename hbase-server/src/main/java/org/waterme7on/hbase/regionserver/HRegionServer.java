package org.waterme7on.hbase.regionserver;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.context.Scope;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.ChoreService;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.conf.ConfigurationManager;
import org.apache.hadoop.hbase.security.Superusers;
import org.apache.hadoop.hbase.security.UserProvider;
import org.apache.hadoop.hbase.trace.TraceUtil;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.Sleeper;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.hadoop.hbase.HConstants.DEFAULT_HBASE_SPLIT_COORDINATED_BY_ZK;
import static org.apache.hadoop.hbase.HConstants.HBASE_SPLIT_WAL_COORDINATED_BY_ZK;

public class HRegionServer extends Thread implements RegionServerServices {
    private static final Logger LOG = LoggerFactory.getLogger(HRegionServer.class);
    protected final Configuration conf;
    // zookeeper connection and watcher
    protected final ZKWatcher zooKeeper;
    protected final RSRpcServices rpcServices;
    private Path dataRootDir;
    /** region server process name */
    public static final String REGIONSERVER = "regionserver";
    /**
     * True if this RegionServer is coming up in a cluster where there is no Master; means it needs to
     * just come up and make do without a Master to talk to: e.g. in test or HRegionServer is doing
     * other than its usual duties: e.g. as an hollowed-out host whose only purpose is as a
     * Replication-stream sink; see HBASE-18846 for more. TODO: can this replace
     * {@link #TEST_SKIP_REPORTING_TRANSITION} ?
     */
    private final boolean masterless;
    private static final String MASTERLESS_CONFIG_NAME = "hbase.masterless";

    public HRegionServer(final Configuration conf) throws IOException {
        super("RegionServer"); // thread name
        final Span span = TraceUtil.createSpan("HRegionServer.cxtor");
        try (Scope ignored = span.makeCurrent()){
            this.conf = conf;
            this.masterless = conf.getBoolean(MASTERLESS_CONFIG_NAME, false);
            // initialize hdfs
            this.dataRootDir = CommonFSUtils.getRootDir(this.conf);
            // Some unit tests don't need a cluster, so no zookeeper at all
            // Open connection to zookeeper and set primary watcher
            this.rpcServices = createRpcServices();
            this.zooKeeper = new ZKWatcher(conf, getProcessName() + ":" + rpcServices.isa.getPort(), this,
                    canCreateBaseZNode());

        } catch (Throwable t) {
            // Make sure we log the exception. HRegionServer is often started via reflection and the
            // cause of failed startup is lost.
            TraceUtil.setError(span, t);
            LOG.error("Failed construction RegionServer", t);
            throw t;
        } finally {
            span.end();
        }
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
    public void abort(String why, Throwable e) {

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
