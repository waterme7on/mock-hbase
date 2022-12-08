package org.waterme7on.hbase.regionserver;

import org.waterme7on.hbase.ipc.RpcServerInterface;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RSRpcServices {
    protected static final Logger LOG = LoggerFactory.getLogger(RSRpcServices.class);
    // TODO: HRegionServer
    protected final HRegionServer regionServer;
    // final RpcServerInterface rpcServer;

    public RSRpcServices(HRegionServer regionServer) {
        this.regionServer = regionServer;
        // this.rpcServer = createRpcServer();
    }

    // protected RpcServerInterface createRpcServer() {
    //     return new RpcServerInterface();
    // }
}
