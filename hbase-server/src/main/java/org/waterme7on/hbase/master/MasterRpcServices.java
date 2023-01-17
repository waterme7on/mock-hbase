package org.waterme7on.hbase.master;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.io.ByteBuffAllocator;
import org.waterme7on.hbase.ipc.RpcServerInterface;
import org.waterme7on.hbase.ipc.RpcServer.BlockingServiceAndInterface;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.waterme7on.hbase.regionserver.RSRpcServices;
import org.waterme7on.hbase.Server;
import org.waterme7on.hbase.ipc.RpcSchedulerFactory;
import org.waterme7on.hbase.ipc.RpcServerFactory;

import java.io.IOException;
import java.net.BindException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import org.waterme7on.hbase.protobuf.generated.RegionServerStatusProtos.RegionServerStartupRequest;
import org.waterme7on.hbase.protobuf.generated.RegionServerStatusProtos.RegionServerStartupResponse;
import org.waterme7on.hbase.protobuf.generated.RegionServerStatusProtos.RegionServerStatusService;
import org.waterme7on.hbase.protobuf.generated.HBaseProtos.NameStringPair;
import org.apache.hbase.thirdparty.com.google.protobuf.RpcController;
import org.apache.hbase.thirdparty.com.google.protobuf.ServiceException;

public class MasterRpcServices extends RSRpcServices
        implements RegionServerStatusService.BlockingInterface {
    private static final Logger LOG = LoggerFactory.getLogger(MasterRpcServices.class.getName());

    private final HMaster master;

    public MasterRpcServices(HMaster m) throws IOException {
        super(m);
        master = m;
    }

    protected RpcServerInterface createRpcServer(final Server server,
            final RpcSchedulerFactory rpcSchedulerFactory, final InetSocketAddress bindAddress,
            final String name) throws IOException {
        final Configuration conf = regionServer.getConfiguration();
        LOG.info("Starting Master RPC server on " + bindAddress);
        // RpcServer at HM by default enable ByteBufferPool iff HM having user table
        // region in it
        // boolean reservoirEnabled =
        // conf.getBoolean(ByteBuffAllocator.ALLOCATOR_POOL_ENABLED_KEY,
        // LoadBalancer.isMasterCanHostUserRegions(conf));
        boolean reservoirEnabled = false;
        try {
            return RpcServerFactory.createRpcServer(server, name, getServices(), bindAddress, // use final
                    // bindAddress
                    // for this
                    // server.
                    conf, rpcSchedulerFactory.create(conf, server), reservoirEnabled);
        } catch (BindException be) {
            throw new IOException(be.getMessage() + ". To switch ports use the '" + HConstants.MASTER_PORT
                    + "' configuration property.", be.getCause() != null ? be.getCause() : be);
        }
    }

    protected List<BlockingServiceAndInterface> getServices() {
        List<BlockingServiceAndInterface> bssi = new ArrayList<>(5);
        bssi.add(new BlockingServiceAndInterface(RegionServerStatusService.newReflectiveBlockingService(this),
                RegionServerStatusService.BlockingInterface.class));
        bssi.addAll(super.getServices());
        return bssi;
    }

    /**
     * @return Subset of configuration to pass initializing regionservers: e.g. the
     *         filesystem to use
     *         and root directory to use.
     */
    private RegionServerStartupResponse.Builder createConfigurationSubset() {
        RegionServerStartupResponse.Builder resp = addConfig(RegionServerStartupResponse.newBuilder(),
                HConstants.HBASE_DIR);
        resp = addConfig(resp, "fs.defaultFS");
        return addConfig(resp, "hbase.master.info.port");
    }

    private RegionServerStartupResponse.Builder addConfig(final RegionServerStartupResponse.Builder resp,
            final String key) {
        NameStringPair.Builder entry = NameStringPair.newBuilder().setName(key)
                .setValue(master.getConfiguration().get(key));
        resp.addMapEntries(entry.build());
        return resp;
    }

    @Override
    public RegionServerStartupResponse regionServerStartup(RpcController controller, RegionServerStartupRequest request)
            throws ServiceException {
        // Register with server manager
        try {
            master.checkServiceStarted();
            int versionNumber = 0;
            String version = "0.0.0";
            InetAddress ia = master.getRemoteInetAddress(request.getPort(), request.getServerStartCode());

            // if regionserver passed hostname to use,
            // then use it instead of doing a reverse DNS lookup
            ServerName rs = master.getServerManager().regionServerStartup(request, versionNumber, version, ia);

            StringBuilder sb = new StringBuilder();
            sb.append("Current online regionservers:");
            for (ServerName server : master.getServerManager().getOnlineServers().keySet()) {
                sb.append("[").append(server).append("],");
            }
            LOG.debug(sb.toString());

            // Send back some config info
            RegionServerStartupResponse.Builder resp = createConfigurationSubset();
            NameStringPair.Builder entry = NameStringPair.newBuilder()
                    .setName(HConstants.KEY_FOR_HOSTNAME_SEEN_BY_MASTER).setValue(rs.getHostname());
            resp.addMapEntries(entry.build());

            return resp.build();
        } catch (IOException ioe) {
            throw new ServiceException(ioe);
        }
    }
}
