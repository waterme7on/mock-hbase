
package org.waterme7on.hbase.ipc;

import java.io.IOException;

import org.waterme7on.hbase.regionserver.RegionScanner;

/**
 * Denotes a callback action that has to be executed at the end of an Rpc Call.
 * 
 * @see RpcCallContext#setCallBack(RpcCallback)
 */

public interface RpcCallback {
    /**
     * Called at the end of an Rpc Call {@link RpcCallContext}
     */
    void run() throws IOException;

    void addScanner(RegionScanner scanner);
}
