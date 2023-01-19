package org.waterme7on.hbase.regionserver;

import java.io.IOException;
import java.util.concurrent.ConcurrentMap;
import org.waterme7on.hbase.Server;
import org.waterme7on.hbase.wal.WAL;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.RegionStateTransition.TransitionCode;

public interface RegionServerServices extends Server, OnlineRegions {
    boolean isClusterUp();

    WAL getWAL(RegionInfo regionInfo) throws IOException;

    public ConcurrentMap<byte[], Boolean> getRegionsInTransitionInRS();

    public RegionServerAccounting getRegionServerAccounting();

    /**
     * Context for postOpenDeployTasks().
     */
    class PostOpenDeployContext {
        private final HRegion region;
        private final long openProcId;
        private final long masterSystemTime;

        public PostOpenDeployContext(HRegion region, long openProcId, long masterSystemTime) {
            this.region = region;
            this.openProcId = openProcId;
            this.masterSystemTime = masterSystemTime;
        }

        public HRegion getRegion() {
            return region;
        }

        public long getOpenProcId() {
            return openProcId;
        }

        public long getMasterSystemTime() {
            return masterSystemTime;
        }
    }

    class RegionStateTransitionContext {
        private final TransitionCode code;
        private final long openSeqNum;
        private final long masterSystemTime;
        private final long[] procIds;
        private final RegionInfo[] hris;

        public RegionStateTransitionContext(TransitionCode code, long openSeqNum, long masterSystemTime,
                RegionInfo... hris) {
            this.code = code;
            this.openSeqNum = openSeqNum;
            this.masterSystemTime = masterSystemTime;
            this.hris = hris;
            this.procIds = new long[hris.length];
        }

        public RegionStateTransitionContext(TransitionCode code, long openSeqNum, long procId,
                long masterSystemTime, RegionInfo hri) {
            this.code = code;
            this.openSeqNum = openSeqNum;
            this.masterSystemTime = masterSystemTime;
            this.hris = new RegionInfo[] { hri };
            this.procIds = new long[] { procId };
        }

        public TransitionCode getCode() {
            return code;
        }

        public long getOpenSeqNum() {
            return openSeqNum;
        }

        public long getMasterSystemTime() {
            return masterSystemTime;
        }

        public RegionInfo[] getHris() {
            return hris;
        }

        public long[] getProcIds() {
            return procIds;
        }
    }

    FileSystem getFileSystem();

}
