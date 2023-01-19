package org.waterme7on.hbase.client;

import java.io.IOException;
import java.util.Optional;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.ConnectionRegistry;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.UserProvider;
import org.apache.hbase.thirdparty.com.google.protobuf.RpcController;
import org.apache.hbase.thirdparty.com.google.protobuf.ServiceException;

import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.AdminService;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.BulkLoadHFileRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.BulkLoadHFileResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.CleanupBulkLoadRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.CleanupBulkLoadResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.ClientService;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.CoprocessorServiceRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.CoprocessorServiceResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.GetRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.GetResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.MultiRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.MultiResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.MutateRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.MutateResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.PrepareBulkLoadRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.PrepareBulkLoadResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.ScanRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.ScanResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.MasterService;
import org.waterme7on.hbase.ipc.RpcCall;
import org.waterme7on.hbase.ipc.RpcServer;
import org.waterme7on.hbase.regionserver.RSRpcServices;

public final class ServerConnectionUtils {
    private ServerConnectionUtils() {
    }

    public final static class ShortCircuitingClusterConnection extends ConnectionImplementation {
        private final ServerName serverName;

        private final AdminService.BlockingInterface localHostAdmin;
        private final ClientService.BlockingInterface localHostClient;
        private final ClientService.BlockingInterface localClientServiceBlockingInterfaceWrapper;

        @Override
        public ClientService.BlockingInterface getClient(ServerName sn) throws IOException {
            return serverName.equals(sn)
                    ? this.localClientServiceBlockingInterfaceWrapper
                    : super.getClient(sn);
        }


        private ShortCircuitingClusterConnection(Configuration conf, User user, ServerName serverName,
                AdminService.BlockingInterface admin, ClientService.BlockingInterface client,
                ConnectionRegistry registry) throws IOException {
            super(conf, null, user, registry);
            this.serverName = serverName;
            this.localHostAdmin = admin;
            this.localHostClient = client;
            this.localClientServiceBlockingInterfaceWrapper = new ClientServiceBlockingInterfaceWrapper(
                    this.localHostClient);
        }

        interface Operation<REQUEST, RESPONSE> {
            RESPONSE call(RpcController controller, REQUEST request) throws ServiceException;
        }

        private <REQUEST, RESPONSE> RESPONSE doCall(RpcController controller, REQUEST request,
                Operation<REQUEST, RESPONSE> operation) throws ServiceException {
            Optional<RpcCall> rpcCallOptional = RpcServer.unsetCurrentCall();
            try {
                return operation.call(controller, request);
            } finally {
                rpcCallOptional.ifPresent(RpcServer::setCurrentCall);
            }
        }
    }

    /**
     * Creates a short-circuit connection that can bypass the RPC layer
     * (serialization,
     * deserialization, networking, etc..) when talking to a local server.
     * 
     * @param conf         the current configuration
     * @param user         the user the connection is for
     * @param serverName   the local server name
     * @param rpcServices  the admin interface of the local server
     * @param rpcServices2 the client interface of the local server
     * @param registry     the connection registry to be used, can be null
     * @return an short-circuit connection.
     * @throws IOException if IO failure occurred
     */
    public static ClusterConnection createShortCircuitConnection(final Configuration conf, User user,
            final ServerName serverName, final RSRpcServices rpcServices,
            final RSRpcServices rpcServices2, ConnectionRegistry registry) throws IOException {
        if (user == null) {
            user = UserProvider.instantiate(conf).getCurrent();
        }
        return new ShortCircuitingClusterConnection(conf, User.getCurrent(), serverName,
                rpcServices,
                rpcServices2,
                registry);
    }

    /**
     * When we directly invoke {@link RSRpcServices#get} on the same RegionServer
     * through
     * {@link ShortCircuitingClusterConnection} in region CPs such as
     * {@link RegionObserver#postScannerOpen} to get other rows, the
     * {@link RegionScanner} created
     * for the directly {@link RSRpcServices#get} may not be closed until the
     * outmost rpc call is
     * completed if there is an outmost {@link RpcCall}, and even worse , the
     * {@link ServerCall#rpcCallback} may be override which would cause serious
     * problem,so for
     * {@link ShortCircuitingClusterConnection#getClient}, if return
     * {@link ShortCircuitingClusterConnection#localHostClient},we would add a
     * wrapper class to wrap
     * it , which using {@link RpcServer#unsetCurrentCall} and
     * {RpcServer#setCurrentCall} to
     * surround the scan and get method call,so the {@link RegionScanner} created
     * for the directly
     * {@link RSRpcServices#get} could be closed immediately,see HBASE-26812 for
     * more.
     */
    static class ClientServiceBlockingInterfaceWrapper implements ClientService.BlockingInterface {

        private ClientService.BlockingInterface target;

        ClientServiceBlockingInterfaceWrapper(ClientService.BlockingInterface target) {
            this.target = target;
        }

        @Override
        public GetResponse get(RpcController controller, GetRequest request) throws ServiceException {
            return this.doCall(controller, request, (c, r) -> {
                return target.get(c, r);
            });
        }

        @Override
        public MultiResponse multi(RpcController controller, MultiRequest request)
                throws ServiceException {
            /**
             * Here is for multiGet
             */
            return this.doCall(controller, request, (c, r) -> {
                return target.multi(c, r);
            });
        }

        @Override
        public ScanResponse scan(RpcController controller, ScanRequest request)
                throws ServiceException {
            return this.doCall(controller, request, (c, r) -> {
                return target.scan(c, r);
            });
        }

        interface Operation<REQUEST, RESPONSE> {
            RESPONSE call(RpcController controller, REQUEST request) throws ServiceException;
        }

        private <REQUEST, RESPONSE> RESPONSE doCall(RpcController controller, REQUEST request,
                Operation<REQUEST, RESPONSE> operation) throws ServiceException {
            Optional<RpcCall> rpcCallOptional = RpcServer.unsetCurrentCall();
            try {
                return operation.call(controller, request);
            } finally {
                rpcCallOptional.ifPresent(RpcServer::setCurrentCall);
            }
        }

        @Override
        public MutateResponse mutate(RpcController controller, MutateRequest request)
                throws ServiceException {
            return target.mutate(controller, request);
        }

        @Override
        public BulkLoadHFileResponse bulkLoadHFile(RpcController controller,
                BulkLoadHFileRequest request) throws ServiceException {
            return target.bulkLoadHFile(controller, request);
        }

        @Override
        public PrepareBulkLoadResponse prepareBulkLoad(RpcController controller,
                PrepareBulkLoadRequest request) throws ServiceException {
            return target.prepareBulkLoad(controller, request);
        }

        @Override
        public CleanupBulkLoadResponse cleanupBulkLoad(RpcController controller,
                CleanupBulkLoadRequest request) throws ServiceException {
            return target.cleanupBulkLoad(controller, request);
        }

        @Override
        public CoprocessorServiceResponse execService(RpcController controller,
                CoprocessorServiceRequest request) throws ServiceException {
            return target.execService(controller, request);
        }

        @Override
        public CoprocessorServiceResponse execRegionServerService(RpcController controller,
                CoprocessorServiceRequest request) throws ServiceException {
            return target.execRegionServerService(controller, request);
        }
    }
}
