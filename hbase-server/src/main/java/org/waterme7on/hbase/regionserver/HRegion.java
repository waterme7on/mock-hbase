package org.waterme7on.hbase.regionserver;


import java.util.HashMap;

/*
*  * <pre>
* hbase
*   |
*   --region dir
*       |
*       --data
*       |  |
*       |  --ns/table/encoded-region-name <---- The region data
*       |      |
*       |      --replay <---- The edits to replay
*       |
*       --WALs
*          |
*          --server-name <---- The WAL dir
* */
public class HRegion implements Region {
    private long flushSize;
    private long flushIntervalMs;
    // TODO
    public HRegion(long flushSize, long flushIntervalMs) {
        this.flushSize = flushSize;
        this.flushIntervalMs = flushIntervalMs;
    }
}
