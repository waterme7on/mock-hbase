package org.waterme7on.hbase.regionserver;

import com.google.protobuf.Message;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.io.ByteBuffAllocator;
import org.apache.hadoop.hbase.net.Address;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.DNS;
import org.apache.hbase.thirdparty.com.google.common.collect.ImmutableList;
import org.apache.hbase.thirdparty.com.google.protobuf.RpcController;
import org.apache.hbase.thirdparty.com.google.protobuf.ServiceException;
import org.apache.hadoop.hbase.Abortable;
import org.waterme7on.hbase.ipc.*;
import org.waterme7on.hbase.ipc.RpcServer.BlockingServiceAndInterface;
import org.waterme7on.hbase.Server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.waterme7on.hbase.master.MasterRpcServices;
import org.waterme7on.hbase.master.HMaster;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.AdminService;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.ClearCompactionQueuesRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.ClearCompactionQueuesResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.ClearRegionBlockCacheRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.ClearRegionBlockCacheResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.ClearSlowLogResponseRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.ClearSlowLogResponses;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.CloseRegionRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.CloseRegionResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.CompactRegionRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.CompactRegionResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.CompactionSwitchRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.CompactionSwitchResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.ExecuteProceduresRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.ExecuteProceduresResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.FlushRegionRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.FlushRegionResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.GetOnlineRegionRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.GetOnlineRegionResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.GetRegionInfoRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.GetRegionInfoResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.GetRegionLoadRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.GetRegionLoadResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.GetServerInfoRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.GetServerInfoResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.GetStoreFileRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.GetStoreFileResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.OpenRegionRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.OpenRegionResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.ReplicateWALEntryRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.ReplicateWALEntryResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.RollWALWriterRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.RollWALWriterResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.SlowLogResponseRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.SlowLogResponses;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.StopServerRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.StopServerResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.UpdateConfigurationRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.UpdateConfigurationResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.UpdateFavoredNodesRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.UpdateFavoredNodesResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.WarmupRegionRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.WarmupRegionResponse;
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
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.LogEntry;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.LogRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.QuotaProtos.GetSpaceQuotaSnapshotsRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.QuotaProtos.GetSpaceQuotaSnapshotsResponse;

public class RSRpcServices
        implements HBaseRPCErrorHandler, AdminService.BlockingInterface, ClientService.BlockingInterface {
    public static final String REGIONSERVER_ADMIN_SERVICE_CONFIG = "hbase.regionserver.admin.executorService";
    public static final String REGIONSERVER_CLIENT_SERVICE_CONFIG = "hbase.regionserver.client.executorService";
    public static final String REGIONSERVER_CLIENT_META_SERVICE_CONFIG = "hbase.regionserver.client.meta.executorService";
    /** RPC scheduler to use for the region server. */
    public static final String REGION_SERVER_RPC_SCHEDULER_FACTORY_CLASS = "hbase.region.server.rpc.scheduler.factory.class";

    protected static final Logger LOG = LoggerFactory.getLogger(RSRpcServices.class);
    // TODO: HRegionServer
    protected final HRegionServer regionServer;
    // Server to handle client requests.
    final RpcServerInterface rpcServer;
    // The reference to the priority extraction function

    public RSRpcServices(final HRegionServer rs) throws IOException {
        this.regionServer = rs;
        final Configuration conf = rs.getConfiguration();

        final RpcSchedulerFactory rpcSchedulerFactory;
        try {
            rpcSchedulerFactory = getRpcSchedulerFactoryClass().asSubclass(RpcSchedulerFactory.class)
                    .getDeclaredConstructor().newInstance();
        } catch (NoSuchMethodException | InvocationTargetException | InstantiationException
                | IllegalAccessException e) {
            throw new IllegalArgumentException(e);
        }
        // Server to handle client requests.
        final InetSocketAddress initialIsa;
        final InetSocketAddress bindAddress;
        if (this instanceof MasterRpcServices) {
            String hostname = DNS.getHostname(conf, DNS.ServerType.MASTER);
            int port = conf.getInt(HConstants.MASTER_PORT, HConstants.DEFAULT_MASTER_PORT);
            // Creation of a HSA will force a resolve.
            initialIsa = new InetSocketAddress(hostname, port);
            bindAddress = new InetSocketAddress(conf.get("hbase.master.ipc.address", hostname), port);
        } else {
            String hostname = DNS.getHostname(conf, DNS.ServerType.REGIONSERVER);
            int port = conf.getInt(HConstants.REGIONSERVER_PORT, HConstants.DEFAULT_REGIONSERVER_PORT);
            // Creation of a HSA will force a resolve.
            initialIsa = new InetSocketAddress(hostname, port);
            bindAddress = new InetSocketAddress(conf.get("hbase.regionserver.ipc.address", hostname), port);
        }
        if (initialIsa.getAddress() == null) {
            throw new IllegalArgumentException("Failed resolve of " + initialIsa);
        }
        // Using Address means we don't get the IP too. Shorten it more even to just the
        // host name
        // w/o the domain.
        final String name = rs.getProcessName() + "/"
                + Address.fromParts(initialIsa.getHostName(), initialIsa.getPort()).toStringWithoutDomain();
        LOG.info("Starting RPC server on " + initialIsa + " with bindAddress=" + bindAddress + " and name=" + name);
        rpcServer = createRpcServer((Server) rs, rpcSchedulerFactory, bindAddress, name);
        rpcServer.setRsRpcServices(this);
        // // TODO
        // if (!(rs instanceof HMaster)) {
        // rpcServer.setNamedQueueRecorder(rs.getNamedQueueRecorder());
        // }

        final InetSocketAddress address = rpcServer.getListenerAddress();
        if (address == null) {
            throw new IOException("Listener channel is closed");
        }
        // Set our address, however we need the final port that was given to rpcServer
        isa = new InetSocketAddress(initialIsa.getHostName(), address.getPort());
        rpcServer.setErrorHandler((HBaseRPCErrorHandler) this);
        rs.setName(name);

    }

    public Configuration getConfiguration() {
        return regionServer.getConfiguration();
    }

    protected List<BlockingServiceAndInterface> getServices() {
        boolean admin = getConfiguration().getBoolean(REGIONSERVER_ADMIN_SERVICE_CONFIG, true);
        boolean client = getConfiguration().getBoolean(REGIONSERVER_CLIENT_SERVICE_CONFIG, true);
        boolean clientMeta = getConfiguration().getBoolean(REGIONSERVER_CLIENT_META_SERVICE_CONFIG, true);
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

    final InetSocketAddress isa;

    protected RpcServerInterface createRpcServer(final Server server,
            final RpcSchedulerFactory rpcSchedulerFactory, final InetSocketAddress bindAddress,
            final String name) throws IOException {
        LOG.info("Creating RegionServer RpcServer for " + name + " on " + bindAddress);
        final Configuration conf = server.getConfiguration();
        boolean reservoirEnabled = conf.getBoolean(ByteBuffAllocator.ALLOCATOR_POOL_ENABLED_KEY, true);
        try {
            RpcServerInterface ret = RpcServerFactory.createRpcServer(server, name, getServices(), bindAddress, // use
                                                                                                                // final
                    // bindAddress
                    // for this
                    // server.
                    conf, rpcSchedulerFactory.create(conf, (Abortable) server),
                    reservoirEnabled);
            return ret;
        } catch (BindException be) {
            throw new IOException(be.getMessage() + ". To switch ports use the '"
                    + HConstants.REGIONSERVER_PORT + "' configuration property.",
                    be.getCause() != null ? be.getCause() : be);
        }
    }

    protected Class<?> getRpcSchedulerFactoryClass() {
        final Configuration conf = regionServer.getConfiguration();
        return conf.getClass(REGION_SERVER_RPC_SCHEDULER_FACTORY_CLASS,
                SimpleRpcSchedulerFactory.class);
    }

    /*
     * Check if an OOME and, if so, abort immediately to avoid creating more
     * objects.
     * 
     * @return True if we OOME'd and are aborting.
     */
    @Override
    public boolean checkOOME(final Throwable e) {
        return exitIfOOME(e);
    }

    public static boolean exitIfOOME(final Throwable e) {
        boolean stop = false;
        try {
            if (e instanceof OutOfMemoryError
                    || (e.getCause() != null && e.getCause() instanceof OutOfMemoryError)
                    || (e.getMessage() != null && e.getMessage().contains("java.lang.OutOfMemoryError"))) {
                stop = true;
                LOG.error("FATAL Run out of memory; " + RSRpcServices.class.getSimpleName()
                        + " will abort itself immediately", e);
            }
        } finally {
            if (stop) {
                Runtime.getRuntime().halt(1);
            }
        }
        return stop;
    }

    void stop() {
        // if (zkPermissionWatcher != null) {
        // zkPermissionWatcher.close();
        // }
        // closeAllScanners();
        rpcServer.stop();
    }

    public InetSocketAddress getSocketAddress() {
        return isa;
    }

    @Override
    public GetResponse get(RpcController controller, GetRequest request) throws ServiceException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public MutateResponse mutate(RpcController controller, MutateRequest request) throws ServiceException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ScanResponse scan(RpcController controller, ScanRequest request) throws ServiceException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public BulkLoadHFileResponse bulkLoadHFile(RpcController controller, BulkLoadHFileRequest request)
            throws ServiceException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public PrepareBulkLoadResponse prepareBulkLoad(RpcController controller, PrepareBulkLoadRequest request)
            throws ServiceException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public CleanupBulkLoadResponse cleanupBulkLoad(RpcController controller, CleanupBulkLoadRequest request)
            throws ServiceException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public CoprocessorServiceResponse execService(RpcController controller, CoprocessorServiceRequest request)
            throws ServiceException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public CoprocessorServiceResponse execRegionServerService(RpcController controller,
            CoprocessorServiceRequest request) throws ServiceException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public MultiResponse multi(RpcController controller, MultiRequest request) throws ServiceException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ClearCompactionQueuesResponse clearCompactionQueues(RpcController arg0, ClearCompactionQueuesRequest arg1)
            throws ServiceException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public GetRegionInfoResponse getRegionInfo(RpcController controller, GetRegionInfoRequest request)
            throws ServiceException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public GetStoreFileResponse getStoreFile(RpcController controller, GetStoreFileRequest request)
            throws ServiceException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public GetOnlineRegionResponse getOnlineRegion(RpcController controller, GetOnlineRegionRequest request)
            throws ServiceException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public CloseRegionResponse closeRegion(RpcController controller, CloseRegionRequest request)
            throws ServiceException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public FlushRegionResponse flushRegion(RpcController controller, FlushRegionRequest request)
            throws ServiceException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public CompactionSwitchResponse compactionSwitch(RpcController controller, CompactionSwitchRequest request)
            throws ServiceException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public CompactRegionResponse compactRegion(RpcController controller, CompactRegionRequest request)
            throws ServiceException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public GetServerInfoResponse getServerInfo(RpcController controller, GetServerInfoRequest request)
            throws ServiceException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public GetRegionLoadResponse getRegionLoad(RpcController controller, GetRegionLoadRequest request)
            throws ServiceException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ClearRegionBlockCacheResponse clearRegionBlockCache(RpcController controller,
            ClearRegionBlockCacheRequest request) throws ServiceException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ExecuteProceduresResponse executeProcedures(RpcController controller, ExecuteProceduresRequest request)
            throws ServiceException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public SlowLogResponses getSlowLogResponses(RpcController controller, SlowLogResponseRequest request)
            throws ServiceException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public SlowLogResponses getLargeLogResponses(RpcController controller, SlowLogResponseRequest request)
            throws ServiceException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ClearSlowLogResponses clearSlowLogsResponses(RpcController controller, ClearSlowLogResponseRequest request)
            throws ServiceException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public LogEntry getLogEntries(RpcController controller, LogRequest request) throws ServiceException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public GetSpaceQuotaSnapshotsResponse getSpaceQuotaSnapshots(RpcController arg0, GetSpaceQuotaSnapshotsRequest arg1)
            throws ServiceException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public OpenRegionResponse openRegion(RpcController controller, OpenRegionRequest request) throws ServiceException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public WarmupRegionResponse warmupRegion(RpcController controller, WarmupRegionRequest request)
            throws ServiceException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ReplicateWALEntryResponse replicateWALEntry(RpcController controller, ReplicateWALEntryRequest request)
            throws ServiceException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ReplicateWALEntryResponse replay(RpcController controller, ReplicateWALEntryRequest request)
            throws ServiceException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public RollWALWriterResponse rollWALWriter(RpcController controller, RollWALWriterRequest request)
            throws ServiceException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public StopServerResponse stopServer(RpcController controller, StopServerRequest request) throws ServiceException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public UpdateFavoredNodesResponse updateFavoredNodes(RpcController controller, UpdateFavoredNodesRequest request)
            throws ServiceException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public UpdateConfigurationResponse updateConfiguration(RpcController controller, UpdateConfigurationRequest request)
            throws ServiceException {
        // TODO Auto-generated method stub
        return null;
    }

    public RpcServerInterface getRpcServer() {
        return rpcServer;
    }
}
