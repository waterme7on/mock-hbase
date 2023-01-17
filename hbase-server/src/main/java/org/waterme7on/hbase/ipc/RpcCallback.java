
package org.waterme7on.hbase.ipc;

import java.io.IOException;

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
}
