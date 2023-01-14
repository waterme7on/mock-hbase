package org.waterme7on.hbase.ipc;

public interface RpcResponse {
    BufferChain getResponse();

    default void done() {
        // nothing
    }
}
