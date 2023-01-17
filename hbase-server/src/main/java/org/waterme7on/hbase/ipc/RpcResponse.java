package org.waterme7on.hbase.ipc;

/**
 * An interface represent the response of an rpc call.
 */
interface RpcResponse {

    BufferChain getResponse();

    default void done() {
        // nothing
    }
}
