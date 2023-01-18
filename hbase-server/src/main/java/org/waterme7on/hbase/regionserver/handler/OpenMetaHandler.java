package org.waterme7on.hbase.regionserver.handler;

import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.waterme7on.hbase.Server;
import org.waterme7on.hbase.executor.EventType;
import org.waterme7on.hbase.regionserver.RegionServerServices;

public class OpenMetaHandler extends OpenRegionHandler {
    public OpenMetaHandler(final Server server, final RegionServerServices rsServices,
            RegionInfo regionInfo, final TableDescriptor htd, long masterSystemTime) {
        super(server, rsServices, regionInfo, htd, masterSystemTime, EventType.M_RS_OPEN_META);
    }

}
