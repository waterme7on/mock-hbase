package org.waterme7on.hbase.regionserver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hbase.thirdparty.com.google.common.collect.ImmutableList;
import org.waterme7on.hbase.ipc.RpcServer.BlockingServiceAndInterface;
import org.waterme7on.hbase.ipc.RpcServerInterface;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class RSRpcServices {
    public static final String REGIONSERVER_ADMIN_SERVICE_CONFIG =
            "hbase.regionserver.admin.executorService";
    public static final String REGIONSERVER_CLIENT_SERVICE_CONFIG =
            "hbase.regionserver.client.executorService";
    public static final String REGIONSERVER_CLIENT_META_SERVICE_CONFIG =
            "hbase.regionserver.client.meta.executorService";

    protected static final Logger LOG = LoggerFactory.getLogger(RSRpcServices.class);
    // TODO: HRegionServer
    protected final HRegionServer regionServer;
    // final RpcServerInterface rpcServer;

    public RSRpcServices(HRegionServer regionServer) {
        this.regionServer = regionServer;
        // this.rpcServer = createRpcServer();
    }

    public Configuration getConfiguration() {
        return regionServer.getConfiguration();
    }

    protected List<BlockingServiceAndInterface> getServices() {
        boolean admin = getConfiguration().getBoolean(REGIONSERVER_ADMIN_SERVICE_CONFIG, true);
        boolean client = getConfiguration().getBoolean(REGIONSERVER_CLIENT_SERVICE_CONFIG, true);
        boolean clientMeta =
                getConfiguration().getBoolean(REGIONSERVER_CLIENT_META_SERVICE_CONFIG, true);
        List<BlockingServiceAndInterface> bssi = new ArrayList<>();
//        if (client) {
//            bssi.add(new BlockingServiceAndInterface(ClientService.newReflectiveBlockingService(this),
//                    ClientService.BlockingInterface.class));
//        }
//        if (admin) {
//            bssi.add(new BlockingServiceAndInterface(AdminService.newReflectiveBlockingService(this),
//                    AdminService.BlockingInterface.class));
//        }
//        if (clientMeta) {
//            bssi.add(new BlockingServiceAndInterface(ClientMetaService.newReflectiveBlockingService(this),
//                    ClientMetaService.BlockingInterface.class));
//        }
        return new ImmutableList.Builder<BlockingServiceAndInterface>().addAll(bssi).build();
    }
}
