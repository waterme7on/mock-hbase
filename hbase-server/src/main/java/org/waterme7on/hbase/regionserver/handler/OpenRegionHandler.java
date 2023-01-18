package org.waterme7on.hbase.regionserver.handler;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hadoop.hbase.HConstants;
import org.waterme7on.hbase.Server;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.waterme7on.hbase.executor.EventHandler;
import org.waterme7on.hbase.executor.EventType;
// import org.apache.hadoop.hbase.procedure2.Procedure;
import org.waterme7on.hbase.regionserver.HRegion;
import org.waterme7on.hbase.regionserver.RegionServerServices;
import org.waterme7on.hbase.regionserver.RegionServerServices.PostOpenDeployContext;
import org.waterme7on.hbase.regionserver.RegionServerServices.RegionStateTransitionContext;
import org.waterme7on.hbase.util.CancelableProgressable;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OpenRegionHandler extends EventHandler {
    private static final Logger LOG = LoggerFactory.getLogger(OpenRegionHandler.class);

    protected final RegionServerServices rsServices;

    private final RegionInfo regionInfo;
    private final TableDescriptor htd;
    private final long masterSystemTime;

    public OpenRegionHandler(final Server server, final RegionServerServices rsServices,
            RegionInfo regionInfo, TableDescriptor htd, long masterSystemTime) {
        this(server, rsServices, regionInfo, htd, masterSystemTime, EventType.M_RS_OPEN_REGION);
    }

    protected OpenRegionHandler(final Server server, final RegionServerServices rsServices,
            final RegionInfo regionInfo, final TableDescriptor htd, long masterSystemTime,
            EventType eventType) {
        super(server, eventType);
        this.rsServices = rsServices;
        this.regionInfo = regionInfo;
        this.htd = htd;
        this.masterSystemTime = masterSystemTime;
    }

    @Override
    public void process() throws IOException {
        boolean openSuccessful = false;
        final String regionName = regionInfo.getRegionNameAsString();
        LOG.debug("todo: OpenRegionHandler.process: " + regionName);

    }
}
