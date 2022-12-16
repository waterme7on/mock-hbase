package org.waterme7on.hbase.ipc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.protobuf.BlockingService;

public abstract class RpcServer implements RpcServerInterface {
    public static final Logger LOG = LoggerFactory.getLogger(RpcServer.class);

    /**
     * Datastructure for passing a {@link BlockingService} and its associated class of protobuf
     * service interface. For example, a server that fielded what is defined in the client protobuf
     * service would pass in an implementation of the client blocking service and then its
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
