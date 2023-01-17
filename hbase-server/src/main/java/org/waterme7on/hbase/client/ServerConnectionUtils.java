package org.waterme7on.hbase.client;

import java.io.IOException;
import java.util.Optional;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.ConnectionRegistry;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.UserProvider;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.AdminService;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.ClientService;
import org.apache.hbase.thirdparty.com.google.protobuf.RpcController;
import org.apache.hbase.thirdparty.com.google.protobuf.ServiceException;
import org.waterme7on.hbase.ipc.RpcCall;
import org.waterme7on.hbase.ipc.RpcServer;
import org.waterme7on.hbase.regionserver.RSRpcServices;

public final class ServerConnectionUtils {
    private ServerConnectionUtils() {
    }

    public final static class ShortCircuitingClusterConnection extends ConnectionImplementation {
        private final ServerName serverName;

        private ShortCircuitingClusterConnection(Configuration conf, User user, ServerName serverName,
                AdminService.BlockingInterface admin, ClientService.BlockingInterface client,
                ConnectionRegistry registry) throws IOException {
            super(conf, null, user, registry);
            this.serverName = serverName;
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
        return new ShortCircuitingClusterConnection(conf, User.getCurrent(), serverName, null, null,
                registry);
    }
}
