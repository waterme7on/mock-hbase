package org.waterme7on.hbase.ipc;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import org.apache.hadoop.conf.Configuration;
import org.waterme7on.hbase.Server;
import org.waterme7on.hbase.regionserver.RSRpcServices;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.protobuf.BlockingService;

public abstract class RpcServer implements RpcServerInterface {
    public static final Logger LOG = LoggerFactory.getLogger(RpcServer.class);
    public static final String MAX_REQUEST_SIZE = "hbase.ipc.max.request.size";
    protected static final int DEFAULT_MAX_CALLQUEUE_SIZE = 1024 * 1024 * 1024;
    public static final int DEFAULT_MAX_REQUEST_SIZE = DEFAULT_MAX_CALLQUEUE_SIZE / 4; // 256M

    /* Attributes */
    protected final Server server;
    protected final RpcScheduler scheduler;
    protected final InetSocketAddress bindAddress;
    protected final Configuration conf;
    protected final boolean tcpNoDelay; // if T then disable Nagle's Algorithm
    protected final boolean tcpKeepAlive; // if T then use keepalives
    protected final int maxRequestSize;
    private RSRpcServices rsRpcServices;

    public RpcServer(final Server server, final String name, final InetSocketAddress bindAddress,
            Configuration conf, RpcScheduler scheduler, boolean reservoirEnabled) throws IOException {
        this.server = server;
        this.bindAddress = bindAddress;
        this.conf = conf;
        this.scheduler = scheduler;
        this.tcpNoDelay = conf.getBoolean("hbase.ipc.server.tcpnodelay", true);
        this.tcpKeepAlive = conf.getBoolean("hbase.ipc.server.tcpkeepalive", true);
        this.maxRequestSize = conf.getInt(MAX_REQUEST_SIZE, DEFAULT_MAX_REQUEST_SIZE);

        LOG.debug(RpcServer.class.getName() + "," + server);
    }

    @Override
    public void setRsRpcServices(RSRpcServices rsRpcServices) {
        this.rsRpcServices = rsRpcServices;
    }

    @Override
    public void setErrorHandler(HBaseRPCErrorHandler handler) {
    }

    /**
     * Datastructure for passing a {@link BlockingService} and its associated class
     * of protobuf
     * service interface. For example, a server that fielded what is defined in the
     * client protobuf
     * service would pass in an implementation of the client blocking service and
     * then its
     * ClientService.BlockingInterface.class. Used checking connection setup.
     */
    public static class BlockingServiceAndInterface {
        private final BlockingService service;
        private final Class<?> serviceInterface;

        public BlockingServiceAndInterface(final BlockingService service,
                final Class<?> serviceInterface) {
            this.service = service;
            this.serviceInterface = serviceInterface;
        }

        public Class<?> getServiceInterface() {
            return this.serviceInterface;
        }

        public BlockingService getBlockingService() {
            return this.service;
        }
    }
}
