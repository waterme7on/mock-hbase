package org.waterme7on.hbase.ipc;

import org.waterme7on.hbase.Server;
import org.apache.hadoop.conf.Configuration;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.waterme7on.hbase.regionserver.RSRpcServices;

public class NettyRpcServer extends RpcServer {
    public static final Logger LOG = LoggerFactory.getLogger(NettyRpcServer.class);
    public static final String HBASE_NETTY_ALLOCATOR_KEY = "hbase.netty.rpcserver.allocator";

    public NettyRpcServer(Server server, String name,
                          InetSocketAddress bindAddress, Configuration conf, RpcScheduler scheduler,
                          boolean reservoirEnabled) throws IOException {

    }
    public void start(){
        // TODO
    };

    public boolean isStarted() {
        // TODO
        return true;
    };

    public void stop(){

    }

    @Override
    public void setRsRpcServices(RSRpcServices rsRpcServices) {

    }

    @Override
    public InetSocketAddress getListenerAddress() {
        return null;
    }

    @Override
    public void setErrorHandler(HBaseRPCErrorHandler handler) {

    }

    ;
}
