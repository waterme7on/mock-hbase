package org.waterme7on.hbase.master;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.RegionInfo;

public class LoadBalancer {

    private final Configuration conf;

    private int serverIndex = 0;

    LoadBalancer(Configuration conf) {
        this.conf = conf;
    }

    Map<ServerName, List<RegionInfo>> roundRobinAssignment(List<RegionInfo> hris, List<ServerName> servers) {
        Map<ServerName, List<RegionInfo>> assignments = new HashMap<>();
        for (RegionInfo hri : hris) {
            ServerName sn = servers.get(serverIndex);
            List<RegionInfo> regions = assignments.get(sn);
            if (regions == null) {
                regions = new ArrayList<>();
                assignments.put(sn, regions);
            }
            regions.add(hri);
            serverIndex = (serverIndex + 1) % servers.size();
        }
        return assignments;
    }
}
