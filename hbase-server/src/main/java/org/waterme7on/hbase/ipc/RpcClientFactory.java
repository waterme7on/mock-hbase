package org.waterme7on.hbase.ipc;

import java.net.SocketAddress;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.MetricsConnection;
import org.apache.hadoop.hbase.ipc.RpcClient;
import org.apache.hadoop.hbase.util.ReflectionUtils;

import org.apache.hbase.thirdparty.com.google.common.collect.ImmutableMap;

public final class RpcClientFactory {
    public static final String CUSTOM_RPC_CLIENT_IMPL_CONF_KEY = "hbase.rpc.client.impl";
    private static final ImmutableMap<String, String> DEPRECATED_NAME_MAPPING = ImmutableMap.of(
            // "org.apache.hadoop.hbase.ipc.RpcClientImpl",
            // BlockingRpcClient.class.getName(),
            "org.apache.hadoop.hbase.ipc.AsyncRpcClient",
            NettyRpcClient.class.getName());

    /**
     * Private Constructor (static member-only classes)
     */
    private RpcClientFactory() {
    }

    /**
     * Helper method for tests only. Creates an {@code RpcClient} without metrics.
     */
    public static RpcClient createClient(Configuration conf, String clusterId) {
        return createClient(conf, clusterId, null);
    }

    /**
     * Creates a new RpcClient by the class defined in the configuration or falls
     * back to
     * RpcClientImpl
     * 
     * @param conf      configuration
     * @param clusterId the cluster id
     * @param metrics   the connection metrics
     * @return newly created RpcClient
     */
    public static RpcClient createClient(Configuration conf, String clusterId,
            MetricsConnection metrics) {
        return createClient(conf, clusterId, null, metrics);
    }

    private static String getRpcClientClass(Configuration conf) {
        String rpcClientClass = conf.get(CUSTOM_RPC_CLIENT_IMPL_CONF_KEY);
        if (rpcClientClass == null) {
            return NettyRpcClient.class.getName();
        }
        String mappedName = DEPRECATED_NAME_MAPPING.get(rpcClientClass);
        return mappedName == null ? rpcClientClass : mappedName;
    }

    /**
     * Creates a new RpcClient by the class defined in the configuration or falls
     * back to
     * RpcClientImpl
     * 
     * @param conf      configuration
     * @param clusterId the cluster id
     * @param localAddr client socket bind address.
     * @param metrics   the connection metrics
     * @return newly created RpcClient
     */
    public static RpcClient createClient(Configuration conf, String clusterId,
            SocketAddress localAddr, MetricsConnection metrics) {
        String rpcClientClass = getRpcClientClass(conf);
        return ReflectionUtils.instantiateWithCustomCtor(rpcClientClass, new Class[] {
                Configuration.class, String.class, SocketAddress.class, MetricsConnection.class },
                new Object[] { conf, clusterId, localAddr, metrics });
    }
}
