package org.waterme7on.hbase.regionserver;

import java.io.IOException;

/**
 * This interface denotes a scanner as one which can ship cells. Scan operation
 * do many RPC requests
 * to server and fetch N rows/RPC. These are then shipped to client. At the end
 * of every such batch
 * {@link #shipped()} will get called.
 */
public interface Shipper {

    /**
     * Called after a batch of rows scanned and set to be returned to client. Any in
     * between cleanup
     * can be done here.
     */
    void shipped() throws IOException;
}
