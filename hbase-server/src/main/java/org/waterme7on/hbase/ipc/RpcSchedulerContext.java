package org.waterme7on.hbase.ipc;

import java.net.InetSocketAddress;

public class RpcSchedulerContext extends RpcScheduler.Context {
    private final RpcServer rpcServer;

    /**
     *   */
    RpcSchedulerContext(final RpcServer rpcServer) {
        this.rpcServer = rpcServer;
    }

    @Override
    public InetSocketAddress getListenerAddress() {
        return this.rpcServer.getListenerAddress();
    }
}
