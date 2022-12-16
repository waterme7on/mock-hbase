package org.waterme7on.hbase.regionserver;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.context.Scope;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ChoreService;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.conf.ConfigurationManager;
import org.apache.hadoop.hbase.security.Superusers;
import org.apache.hadoop.hbase.security.UserProvider;
import org.apache.hadoop.hbase.trace.TraceUtil;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.Sleeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.hadoop.hbase.HConstants.DEFAULT_HBASE_SPLIT_COORDINATED_BY_ZK;
import static org.apache.hadoop.hbase.HConstants.HBASE_SPLIT_WAL_COORDINATED_BY_ZK;

public class HRegionServer extends Thread {
    private static final Logger LOG = LoggerFactory.getLogger(HRegionServer.class);
    protected final Configuration conf;

    public HRegionServer(final Configuration conf) throws IOException {
        super("RegionServer"); // thread name
        final Span span = TraceUtil.createSpan("HRegionServer.cxtor");
        try (Scope ignored = span.makeCurrent()){
            this.conf = conf;
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
}
