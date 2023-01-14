package org.waterme7on.hbase.ipc;

import org.waterme7on.hbase.Server;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.ReflectionUtils;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.waterme7on.hbase.ipc.RpcServer.BlockingServiceAndInterface;

public class RpcServerFactory {
        public static final Logger LOG = LoggerFactory.getLogger(RpcServerFactory.class);

        public static final String CUSTOM_RPC_SERVER_IMPL_CONF_KEY = "hbase.rpc.server.impl";

        /**
         * Private Constructor
         */
        private RpcServerFactory() {
        }

        public static RpcServer createRpcServer(final Server server, final String name,
                        final List<BlockingServiceAndInterface> services,
                        final InetSocketAddress bindAddress,
                        Configuration conf, RpcScheduler scheduler) throws IOException {
                return createRpcServer(server, name, services, bindAddress, conf, scheduler, true);
        }

        public static RpcServer createRpcServer(final Server server, final String name,
                        final List<BlockingServiceAndInterface> services,
                        final InetSocketAddress bindAddress,
                        Configuration conf, RpcScheduler scheduler, boolean reservoirEnabled) throws IOException {
                String rpcServerClass = conf.get(CUSTOM_RPC_SERVER_IMPL_CONF_KEY, NettyRpcServer.class.getName());
                return ReflectionUtils.instantiateWithCustomCtor(rpcServerClass,
                                new Class[] { Server.class, String.class, InetSocketAddress.class,
                                                Configuration.class, RpcScheduler.class, boolean.class },
                                new Object[] { server, name, bindAddress, conf, scheduler, reservoirEnabled });
        }
}