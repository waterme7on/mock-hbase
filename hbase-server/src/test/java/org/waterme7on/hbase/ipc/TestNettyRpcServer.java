package org.waterme7on.hbase.ipc;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.ipc.RpcClient;
import org.apache.hadoop.hbase.util.DNS;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.hbase.thirdparty.com.google.protobuf.BlockingRpcChannel;
import org.junit.Test;

import org.waterme7on.hbase.ipc.RpcServer.BlockingServiceAndInterface;
import org.waterme7on.hbase.regionserver.HRegionServer;
import org.waterme7on.hbase.regionserver.SimpleRpcSchedulerFactory;
import org.waterme7on.hbase.HBaseCommonTestingUtility;
import org.waterme7on.hbase.Server;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.RegionServerStartupRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.RegionServerStatusService;

import com.google.common.collect.ImmutableList;

public class TestNettyRpcServer {
    private static final HBaseCommonTestingUtility TESTING_UTIL = new HBaseCommonTestingUtility();

    protected List<BlockingServiceAndInterface> getServices() {
        List<BlockingServiceAndInterface> bssi = new ArrayList<>();
        // if (client) {
        // bssi.add(new
        // BlockingServiceAndInterface(ClientService.newReflectiveBlockingService(this),
        // ClientService.BlockingInterface.class));
        // }
        // if (admin) {
        // bssi.add(new
        // BlockingServiceAndInterface(AdminService.newReflectiveBlockingService(this),
        // AdminService.BlockingInterface.class));
        // }
        // if (clientMeta) {
        // bssi.add(new
        // BlockingServiceAndInterface(ClientMetaService.newReflectiveBlockingService(this),
        // ClientMetaService.BlockingInterface.class));
        // }
        return new ImmutableList.Builder<BlockingServiceAndInterface>().addAll(bssi).build();
    }

    @Test
    public void testServerRun() throws Exception {
        Configuration conf = HBaseConfiguration.create();
        String hostname = DNS.getHostname(conf, DNS.ServerType.REGIONSERVER);
        int port = 17173;
        final RpcSchedulerFactory rpcSchedulerFactory = conf.getClass("REGION_SERVER_RPC_SCHEDULER_FACTORY_CLASS",
                SimpleRpcSchedulerFactory.class).asSubclass(RpcSchedulerFactory.class)
                .getDeclaredConstructor().newInstance();
        InetSocketAddress bindAddress = new InetSocketAddress(
                conf.get("hbase.regionserver.ipc.address", hostname), port);

        // Server rs = new TestServer();

        // RpcServer rpcServer = RpcServerFactory.createRpcServer(rs, "test",
        // getServices(), bindAddress,
        // conf, rpcSchedulerFactory.create(conf, rs), false);

        // ServerName sn = ServerName.parseServerName("172.17.0.3" + ":" + port);

        // RpcClient rpcClient = new NettyRpcClient(conf);

        // BlockingRpcChannel channel = rpcClient.createBlockingRpcChannel(sn, null,
        // port);
        // RegionServerStatusService.BlockingInterface rss =
        // RegionServerStatusService.newBlockingStub(channel);

        // RegionServerStartupRequest.Builder request =
        // RegionServerStartupRequest.newBuilder();
        // request.setPort(port);
        // request.setServerStartCode(77777777);
        // request.setServerCurrentTime(EnvironmentEdgeManager.currentTime());
        // rss.regionServerStartup(null, request.build());

        // System.out.println(channel);
        // System.out.println(rpcClient);
        // System.out.println(rpcServer);

    }

}
