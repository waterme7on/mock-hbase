package org.waterme7on.hbase.regionserver;

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.trace.TraceUtil;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.waterme7on.hbase.Server;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.context.Scope;
import org.apache.hadoop.util.StringUtils.TraditionalBinaryPrefix;

public class MemStoreFlusher implements FlushRequester {

    private final HRegionServer server;
    private final Configuration conf;
    private final long threadWakeFrequency;
    private long blockingWaitTime;
    private static final Logger LOG = LoggerFactory.getLogger(MemStoreFlusher.class);

    /**
     *   */
    public MemStoreFlusher(final Configuration conf, final HRegionServer server) {
        super();
        this.conf = conf;
        this.server = server;
        this.threadWakeFrequency = conf.getLong(HConstants.THREAD_WAKE_FREQUENCY, 10 * 1000);
        this.blockingWaitTime = conf.getInt("hbase.hstore.blockingWaitTime", 90000);
        int handlerCount = conf.getInt("hbase.hstore.flusher.count", 2);
        if (server != null) {
            if (handlerCount < 1) {
                LOG.warn(
                        "hbase.hstore.flusher.count was configed to {} which is less than 1, " + "corrected to 1",
                        handlerCount);
                handlerCount = 1;
            }
            LOG.info("globalMemStoreLimit="
                    + TraditionalBinaryPrefix
                            .long2String(this.server.getRegionServerAccounting().getGlobalMemStoreLimit(), "", 1)
                    + ", globalMemStoreLimitLowMark="
                    + TraditionalBinaryPrefix.long2String(
                            this.server.getRegionServerAccounting().getGlobalMemStoreLimitLowMark(), "", 1)
                    + ", Offheap=" + (this.server.getRegionServerAccounting().isOffheap()));
        }
        // this.flushHandlers = new FlushHandler[handlerCount];
    }

    /**
     * Check if the regionserver's memstore memory usage is greater than the limit.
     * If so, flush
     * regions with the biggest memstores until we're down to the lower limit. This
     * method blocks
     * callers until we're down to a safe amount of memstore consumption.
     */
    // public void reclaimMemStoreMemory() {
    // Span span =
    // TraceUtil.getGlobalTracer().spanBuilder("MemStoreFluser.reclaimMemStoreMemory").startSpan();
    // try (Scope scope = span.makeCurrent()) {
    // FlushType flushType = isAboveHighWaterMark();
    // if (flushType != FlushType.NORMAL) {
    // span.addEvent("Force Flush. We're above high water mark.");
    // long start = EnvironmentEdgeManager.currentTime();
    // long nextLogTimeMs = start;
    // synchronized (this.blockSignal) {
    // boolean blocked = false;
    // long startTime = 0;
    // boolean interrupted = false;
    // try {
    // flushType = isAboveHighWaterMark();
    // while (flushType != FlushType.NORMAL && !server.isStopped()) {
    // if (!blocked) {
    // startTime = EnvironmentEdgeManager.currentTime();
    // if (!server.getRegionServerAccounting().isOffheap()) {
    // logMsg("global memstore heapsize",
    // server.getRegionServerAccounting().getGlobalMemStoreHeapSize(),
    // server.getRegionServerAccounting().getGlobalMemStoreLimit());
    // } else {
    // switch (flushType) {
    // case ABOVE_OFFHEAP_HIGHER_MARK:
    // logMsg("the global offheap memstore datasize",
    // server.getRegionServerAccounting().getGlobalMemStoreOffHeapSize(),
    // server.getRegionServerAccounting().getGlobalMemStoreLimit());
    // break;
    // case ABOVE_ONHEAP_HIGHER_MARK:
    // logMsg("global memstore heapsize",
    // server.getRegionServerAccounting().getGlobalMemStoreHeapSize(),
    // server.getRegionServerAccounting().getGlobalOnHeapMemStoreLimit());
    // break;
    // default:
    // break;
    // }
    // }
    // }
    // blocked = true;
    // wakeupFlushThread();
    // try {
    // // we should be able to wait forever, but we've seen a bug where
    // // we miss a notify, so put a 5 second bound on it at least.
    // blockSignal.wait(5 * 1000);
    // } catch (InterruptedException ie) {
    // LOG.warn("Interrupted while waiting");
    // interrupted = true;
    // }
    // long nowMs = EnvironmentEdgeManager.currentTime();
    // if (nowMs >= nextLogTimeMs) {
    // LOG.warn("Memstore is above high water mark and block {} ms", nowMs - start);
    // nextLogTimeMs = nowMs + 1000;
    // }
    // flushType = isAboveHighWaterMark();
    // }
    // } finally {
    // if (interrupted) {
    // Thread.currentThread().interrupt();
    // }
    // }

    // if (blocked) {
    // final long totalTime = EnvironmentEdgeManager.currentTime() - startTime;
    // if (totalTime > 0) {
    // this.updatesBlockedMsHighWater.add(totalTime);
    // }
    // LOG.info("Unblocking updates for server " + server.toString());
    // }
    // }
    // } else {
    // flushType = isAboveLowWaterMark();
    // if (flushType != FlushType.NORMAL) {
    // wakeupFlushThread();
    // }
    // span.end();
    // }
    // }
    // }

    @Override
    public boolean requestFlush(HRegion region, FlushLifeCycleTracker tracker) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'requestFlush'");
    }

    @Override
    public boolean requestFlush(HRegion region, List<byte[]> families, FlushLifeCycleTracker tracker) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'requestFlush'");
    }

    @Override
    public boolean requestDelayedFlush(HRegion region, long delay) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'requestDelayedFlush'");
    }

    @Override
    public void registerFlushRequestListener(FlushRequestListener listener) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'registerFlushRequestListener'");
    }

    @Override
    public boolean unregisterFlushRequestListener(FlushRequestListener listener) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'unregisterFlushRequestListener'");
    }

    @Override
    public void setGlobalMemStoreLimit(long globalMemStoreSize) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'setGlobalMemStoreLimit'");
    }

    // TODO
}
