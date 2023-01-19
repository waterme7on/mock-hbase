package org.waterme7on.hbase.regionserver;

import org.apache.hbase.thirdparty.com.google.protobuf.Message;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.CheckAndMutate;
import org.apache.hadoop.hbase.client.CheckAndMutateResult;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.io.ByteBuffAllocator;
import org.apache.hadoop.hbase.ipc.HBaseRpcController;
import org.apache.hadoop.hbase.ipc.ServerNotRunningYetException;
import org.apache.hadoop.hbase.net.Address;
import org.apache.hadoop.hbase.quotas.ActivePolicyEnforcement;
import org.apache.hadoop.hbase.quotas.OperationQuota;
import org.apache.hadoop.hbase.regionserver.RegionServerAbortedException;
import org.apache.hadoop.hbase.regionserver.RegionServerStoppedException;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.LogEntry;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.LogRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.RegionSpecifier;
import org.apache.hadoop.hbase.shaded.protobuf.generated.QuotaProtos.GetSpaceQuotaSnapshotsRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.QuotaProtos.GetSpaceQuotaSnapshotsResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RPCProtos.RequestHeader;
import org.apache.hadoop.hbase.util.DNS;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.hbase.thirdparty.com.google.common.collect.ImmutableList;
import org.apache.hbase.thirdparty.com.google.protobuf.RpcController;
import org.apache.hbase.thirdparty.com.google.protobuf.ServiceException;
import org.apache.hadoop.hbase.Abortable;
import org.waterme7on.hbase.ipc.*;
import org.waterme7on.hbase.ipc.RpcServer.BlockingServiceAndInterface;
import org.waterme7on.hbase.ByteBufferExtendedCell;
import org.waterme7on.hbase.Cell;
import org.waterme7on.hbase.PrivateCellUtil;
import org.waterme7on.hbase.Server;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
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
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.RemoteProcedureRequest;
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
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.OpenRegionRequest.RegionOpenInfo;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.OpenRegionResponse.RegionOpeningState;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.BulkLoadHFileRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.BulkLoadHFileResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.CleanupBulkLoadRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.CleanupBulkLoadResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.ClientService;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.Condition;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.CoprocessorServiceRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.CoprocessorServiceResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.GetRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.GetResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.MultiRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.MultiResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.MutateRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.MutateResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.MutationProto;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.PrepareBulkLoadRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.PrepareBulkLoadResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.ScanRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.ScanResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.MutationProto.MutationType;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegistryProtos.ClientMetaService;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegistryProtos.GetActiveMasterRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegistryProtos.GetActiveMasterResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegistryProtos.GetBootstrapNodesRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegistryProtos.GetBootstrapNodesResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegistryProtos.GetClusterIdRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegistryProtos.GetClusterIdResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegistryProtos.GetMastersRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegistryProtos.GetMastersResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegistryProtos.GetMetaRegionLocationsRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegistryProtos.GetMetaRegionLocationsResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.waterme7on.hbase.master.MasterRpcServices;
import org.waterme7on.hbase.regionserver.handler.OpenMetaHandler;
import org.waterme7on.hbase.regionserver.handler.OpenRegionHandler;
import org.waterme7on.hbase.util.CancelableProgressable;
import org.waterme7on.hbase.master.HMaster;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RSRpcServices implements HBaseRPCErrorHandler, PriorityFunction, AdminService.BlockingInterface,
        ClientService.BlockingInterface, ClientMetaService.BlockingInterface {
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
    private final PriorityFunction priority;
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
        priority = createPriority();

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
        List<BlockingServiceAndInterface> bssi = new ArrayList<>();
        boolean client = getConfiguration().getBoolean(REGIONSERVER_CLIENT_SERVICE_CONFIG, true);
        if (client) {
            bssi.add(new BlockingServiceAndInterface(ClientService.newReflectiveBlockingService(this),
                    ClientService.BlockingInterface.class));
        }
        boolean admin = getConfiguration().getBoolean(REGIONSERVER_ADMIN_SERVICE_CONFIG, true);
        if (admin) {
            bssi.add(new BlockingServiceAndInterface(AdminService.newReflectiveBlockingService(this),
                    AdminService.BlockingInterface.class));
        }
        // boolean clientMeta =
        // getConfiguration().getBoolean(REGIONSERVER_CLIENT_META_SERVICE_CONFIG, true);
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
                    conf, rpcSchedulerFactory.create(conf, this, (Abortable) server),
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

    void start(ZKWatcher zkWatcher) {
        rpcServer.start();
    }

    void stop() {
        // if (zkPermissionWatcher != null) {
        // zkPermissionWatcher.close();
        // }
        // closeAllScanners();
        rpcServer.stop();
    }

    /**
     * Called to verify that this server is up and running.
     */
    // TODO : Rename this and HMaster#checkInitialized to isRunning() (or a better
    // name).
    protected void checkOpen() throws IOException {
        if (regionServer.isAborted()) {
            throw new RegionServerAbortedException("Server " + regionServer.serverName + " aborting");
        }
        if (regionServer.isStopped()) {
            throw new RegionServerStoppedException("Server " + regionServer.serverName + " stopping");
        }
        if (!regionServer.isDataFileSystemOk()) {
            throw new RegionServerStoppedException("File system not available");
        }
        if (!regionServer.isOnline()) {
            throw new ServerNotRunningYetException(
                    "Server " + regionServer.serverName + " is not running yet");
        }
    }

    public InetSocketAddress getSocketAddress() {
        return isa;
    }

    public RpcServerInterface getRpcServer() {
        return rpcServer;
    }

    @Override
    public boolean checkOOME(Throwable e) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'checkOOME'");
    }

    public PriorityFunction getPriority() {
        return priority;
    }

    @Override
    public long getDeadline(RequestHeader header, Message param) {
        return priority.getDeadline(header, param);
    }

    protected PriorityFunction createPriority() {
        return new AnnotationReadingPriorityFunction(this);
    }

    @Override
    public int getPriority(RequestHeader header, Message param, User user) {
        return priority.getPriority(header, param, user);
    }

    @Override
    public GetClusterIdResponse getClusterId(RpcController controller, GetClusterIdRequest request)
            throws ServiceException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getClusterId'");
    }

    @Override
    public GetActiveMasterResponse getActiveMaster(RpcController controller, GetActiveMasterRequest request)
            throws ServiceException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getActiveMaster'");
    }

    @Override
    public GetMastersResponse getMasters(RpcController controller, GetMastersRequest request) throws ServiceException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getMasters'");
    }

    @Override
    public GetMetaRegionLocationsResponse getMetaRegionLocations(RpcController controller,
            GetMetaRegionLocationsRequest request) throws ServiceException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getMetaRegionLocations'");
    }

    @Override
    public GetBootstrapNodesResponse getBootstrapNodes(RpcController controller, GetBootstrapNodesRequest request)
            throws ServiceException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getBootstrapNodes'");
    }

    @Override
    public GetResponse get(RpcController controller, GetRequest request) throws ServiceException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'get'");
    }

    /**
     * Execute an append mutation.
     * 
     * @return result to return to client if default operation should be bypassed as
     *         indicated by
     *         RegionObserver, null otherwise
     */
    private Result append(final HRegion region, final OperationQuota quota,
            final MutationProto mutation, final CellScanner cellScanner, long nonceGroup,
            ActivePolicyEnforcement spaceQuota) throws IOException {
        // long before = EnvironmentEdgeManager.currentTime();
        Append append = ProtobufUtil.toAppend(mutation, cellScanner);
        checkCellSizeLimit(region, append);
        if (spaceQuota != null) {
            // spaceQuota.getPolicyEnforcement(region).check(append);
        }
        if (quota != null) {
            // quota.addMutation(append);
        }
        long nonce = mutation.hasNonce() ? mutation.getNonce() : HConstants.NO_NONCE;
        Result r = region.append(append, nonceGroup, nonce);
        // if (regionServer.getMetrics() != null) {
        // regionServer.getMetrics().updateAppend(region.getTableDescriptor().getTableName(),
        // EnvironmentEdgeManager.currentTime() - before);
        // }
        return r == null ? Result.EMPTY_RESULT : r;
    }

    /**
     * Execute an increment mutation.
     */
    private Result increment(final HRegion region, final OperationQuota quota,
            final MutationProto mutation, final CellScanner cells, long nonceGroup,
            ActivePolicyEnforcement spaceQuota) throws IOException {
        // long before = EnvironmentEdgeManager.currentTime();
        Increment increment = ProtobufUtil.toIncrement(mutation, cells);
        checkCellSizeLimit(region, increment);
        if (spaceQuota != null) {
            // spaceQuota.getPolicyEnforcement(region).check(increment);
        }
        if (quota != null) {
            // quota.addMutation(increment);
        }
        long nonce = mutation.hasNonce() ? mutation.getNonce() : HConstants.NO_NONCE;
        Result r = region.increment(increment, nonceGroup, nonce);
        // final MetricsRegionServer metricsRegionServer = regionServer.getMetrics();
        // if (metricsRegionServer != null) {
        // metricsRegionServer.updateIncrement(region.getTableDescriptor().getTableName(),
        // EnvironmentEdgeManager.currentTime() - before);
        // }
        return r == null ? Result.EMPTY_RESULT : r;
    }

    private void delete(HRegion region, OperationQuota quota, MutationProto mutation,
            CellScanner cellScanner, ActivePolicyEnforcement spaceQuota) throws IOException {
        // long before = EnvironmentEdgeManager.currentTime();
        Delete delete = ProtobufUtil.toDelete(mutation, cellScanner);
        checkCellSizeLimit(region, delete);
        if (spaceQuota != null) {
            // spaceQuota.getPolicyEnforcement(region).check(delete);
        }
        if (quota != null) {
            // quota.addMutation(delete);
        }
        region.delete(delete);

        // MetricsRegionServer metricsRegionServer = regionServer.getMetrics();
        // if (metricsRegionServer != null) {
        // long after = EnvironmentEdgeManager.currentTime();
        // metricsRegionServer.updateDelete(region.getRegionInfo().getTable(), after -
        // before);
        // }
    }

    private void put(HRegion region, OperationQuota quota, MutationProto mutation,
            CellScanner cellScanner, ActivePolicyEnforcement spaceQuota) throws IOException {
        // long before = EnvironmentEdgeManager.currentTime();
        Put put = ProtobufUtil.toPut(mutation, cellScanner);
        checkCellSizeLimit(region, put);

        if (spaceQuota != null) {
            // spaceQuota.getPolicyEnforcement(region).check(put);
        }
        if (quota != null) {
            // quota.addMutation(put);
        }
        region.put(put);

        // MetricsRegionServer metricsRegionServer = regionServer.getMetrics();
        // if (metricsRegionServer != null) {
        // long after = EnvironmentEdgeManager.currentTime();
        // metricsRegionServer.updatePut(region.getRegionInfo().getTable(), after -
        // before);
        // }
    }

    @Override
    public MutateResponse mutate(RpcController controller, MutateRequest request) throws ServiceException {
        // TODO Auto-generated method stub
        HBaseRpcController rpcc = (HBaseRpcController) controller;
        CellScanner cellScanner = controller != null ? rpcc.cellScanner() : null;
        RpcCallContext context = RpcServer.getCurrentCall().orElse(null);

        // Clear scanner so we are not holding on to reference across call.
        if (rpcc != null) {
            rpcc.setCellScanner(null);
        }
        try {
            checkOpen();
            HRegion region = getRegion(request.getRegion());
            MutationProto mutation = request.getMutation();
            MutateResponse.Builder builder = MutateResponse.newBuilder();
            OperationQuota quota = null;
            ActivePolicyEnforcement spaceQuotaEnforcement = null;
            long nonceGroup = request.hasNonceGroup() ? request.getNonceGroup() : HConstants.NO_NONCE;

            if (!region.getRegionInfo().isMetaRegion()) {
                // regionServer.getMemStoreFlusher().reclaimMemStoreMemory();
            }
            if (request.hasCondition()) {
                CheckAndMutateResult result = checkAndMutate(region, quota, mutation, cellScanner,
                        request.getCondition(), nonceGroup, spaceQuotaEnforcement);
                builder.setProcessed(result.isSuccess());
                boolean clientCellBlockSupported = isClientCellBlockSupport(context);
                addResult(builder, result.getResult(), (HBaseRpcController) controller, clientCellBlockSupported);
                if (clientCellBlockSupported) {
                    addSize(context, result.getResult(), null);
                }
            } else {
                Result r = null;
                Boolean processed = null;
                MutationType type = mutation.getMutateType();
                switch (type) {
                    case APPEND:
                        // TODO: this doesn't actually check anything.
                        r = append(region, quota, mutation, cellScanner, nonceGroup,
                                spaceQuotaEnforcement);
                        break;
                    case INCREMENT:
                        // TODO: this doesn't actually check anything.
                        r = increment(region, quota, mutation, cellScanner, nonceGroup,
                                spaceQuotaEnforcement);
                        break;
                    case PUT:
                        put(region, quota, mutation, cellScanner, spaceQuotaEnforcement);
                        processed = Boolean.TRUE;
                        break;
                    case DELETE:
                        delete(region, quota, mutation, cellScanner, spaceQuotaEnforcement);
                        processed = Boolean.TRUE;
                        break;
                    default:
                        throw new DoNotRetryIOException("Unsupported mutate type: " + type.name());
                }
                if (processed != null) {
                    builder.setProcessed(processed);
                }
                boolean clientCellBlockSupported = isClientCellBlockSupport(context);
                addResult(builder, r, (HBaseRpcController) controller, clientCellBlockSupported);
                if (clientCellBlockSupported) {
                    addSize(context, r, null);
                }
            }
            return builder.build();
        } catch (IOException e) {
            throw new ServiceException(e);
        }

    }

    private CheckAndMutateResult checkAndMutate(HRegion region, OperationQuota quota,
            MutationProto mutation, CellScanner cellScanner, Condition condition, long nonceGroup,
            ActivePolicyEnforcement spaceQuota) throws IOException {
        long before = EnvironmentEdgeManager.currentTime();
        CheckAndMutate checkAndMutate = ProtobufUtil.toCheckAndMutate(condition, mutation, cellScanner);
        long nonce = mutation.hasNonce() ? mutation.getNonce() : HConstants.NO_NONCE;
        checkCellSizeLimit(region, (Mutation) checkAndMutate.getAction());
        CheckAndMutateResult result = null;
        result = region.checkAndMutate(checkAndMutate, nonceGroup, nonce);
        return result;
    }

    private void checkCellSizeLimit(final HRegion r, final Mutation m) throws IOException {
        if (r.maxCellSize > 0) {
            CellScanner cells = m.cellScanner();
            while (cells.advance()) {
                int size = PrivateCellUtil.estimatedSerializedSizeOf((Cell) cells.current());
                if (size > r.maxCellSize) {
                    String msg = "Cell[" + cells.current() + "] with size " + size + " exceeds limit of "
                            + r.maxCellSize + " bytes";
                    LOG.debug(msg);
                    throw new DoNotRetryIOException(msg);
                }
            }
        }

    }

    /**
     * Find the HRegion based on a region specifier
     * 
     * @param regionSpecifier the region specifier
     * @return the corresponding region
     * @throws IOException if the specifier is not null, but failed to find the
     *                     region
     */
    public HRegion getRegion(final RegionSpecifier regionSpecifier) throws IOException {
        return regionServer.getRegion(regionSpecifier.getValue().toByteArray());
    }

    @Override
    public ScanResponse scan(RpcController controller, ScanRequest request) throws ServiceException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'scan'");
    }

    @Override
    public BulkLoadHFileResponse bulkLoadHFile(RpcController controller, BulkLoadHFileRequest request)
            throws ServiceException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'bulkLoadHFile'");
    }

    @Override
    public PrepareBulkLoadResponse prepareBulkLoad(RpcController controller, PrepareBulkLoadRequest request)
            throws ServiceException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'prepareBulkLoad'");
    }

    @Override
    public CleanupBulkLoadResponse cleanupBulkLoad(RpcController controller, CleanupBulkLoadRequest request)
            throws ServiceException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'cleanupBulkLoad'");
    }

    @Override
    public CoprocessorServiceResponse execService(RpcController controller, CoprocessorServiceRequest request)
            throws ServiceException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'execService'");
    }

    @Override
    public CoprocessorServiceResponse execRegionServerService(RpcController controller,
            CoprocessorServiceRequest request) throws ServiceException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'execRegionServerService'");
    }

    @Override
    public MultiResponse multi(RpcController controller, MultiRequest request) throws ServiceException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'multi'");
    }

    @Override
    public ClearCompactionQueuesResponse clearCompactionQueues(RpcController arg0, ClearCompactionQueuesRequest arg1)
            throws ServiceException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'clearCompactionQueues'");
    }

    @Override
    public GetRegionInfoResponse getRegionInfo(RpcController controller, GetRegionInfoRequest request)
            throws ServiceException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getRegionInfo'");
    }

    @Override
    public GetStoreFileResponse getStoreFile(RpcController controller, GetStoreFileRequest request)
            throws ServiceException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getStoreFile'");
    }

    @Override
    public GetOnlineRegionResponse getOnlineRegion(RpcController controller, GetOnlineRegionRequest request)
            throws ServiceException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getOnlineRegion'");
    }

    @Override
    public OpenRegionResponse openRegion(RpcController controller, OpenRegionRequest request) throws ServiceException {
        OpenRegionResponse.Builder builder = OpenRegionResponse.newBuilder();
        final int regionCount = request.getOpenInfoCount();
        final Map<TableName, TableDescriptor> htds = new HashMap<>(regionCount);
        final boolean isBulkAssign = regionCount > 1;

        // meta initialization
        try {
            checkOpen();
        } catch (IOException ie) {
            TableName tableName = null;
            if (regionCount == 1) {
                org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.RegionInfo ri = request.getOpenInfo(0)
                        .getRegion();
                if (ri != null) {
                    tableName = ProtobufUtil.toTableName(ri.getTableName());
                }
            }
            if (!TableName.META_TABLE_NAME.equals(tableName)) {
                throw new ServiceException(ie);
            }
            // We are assigning meta, wait a little for regionserver to finish
            // initialization.
            // Default to quarter of RPC timeout
            int timeout = regionServer.getConfiguration().getInt(HConstants.HBASE_RPC_TIMEOUT_KEY,
                    HConstants.DEFAULT_HBASE_RPC_TIMEOUT) >> 2;
            long endTime = EnvironmentEdgeManager.currentTime() + timeout;
            synchronized (regionServer.online) {
                try {
                    while (EnvironmentEdgeManager.currentTime() <= endTime && !regionServer.isStopped()
                            && !regionServer.isOnline()) {
                        regionServer.online.wait(regionServer.msgInterval);
                    }
                    checkOpen();
                } catch (InterruptedException t) {
                    Thread.currentThread().interrupt();
                    throw new ServiceException(t);
                } catch (IOException e) {
                    throw new ServiceException(e);
                }
            }
        }

        long masterSystemTime = request.hasMasterSystemTime() ? request.getMasterSystemTime() : -1;
        for (RegionOpenInfo regionOpenInfo : request.getOpenInfoList()) {
            final RegionInfo region = ProtobufUtil.toRegionInfo(regionOpenInfo.getRegion());
            TableDescriptor htd;
            try {
                String encodedName = region.getEncodedName();
                byte[] encodedNameBytes = region.getEncodedNameAsBytes();
                final HRegion onlineRegion = regionServer.getRegion(encodedName);
                if (onlineRegion != null) {
                    // The region is already online. This should not happen any more.
                    String error = "Received OPEN for the region:" + region.getRegionNameAsString()
                            + ", which is already online";
                    LOG.warn(error);
                    // regionServer.abort(error);
                    // throw new IOException(error);
                    builder.addOpeningState(RegionOpeningState.OPENED);
                    continue;
                }
                LOG.info("Open " + region.getRegionNameAsString());

                final Boolean previous = regionServer.getRegionsInTransitionInRS().putIfAbsent(encodedNameBytes,
                        Boolean.TRUE);

                if (Boolean.FALSE.equals(previous)) {
                    if (regionServer.getRegion(encodedName) != null) {
                        // There is a close in progress. This should not happen any more.
                        String error = "Received OPEN for the region:" + region.getRegionNameAsString()
                                + ", which we are already trying to CLOSE";
                        regionServer.abort(error);
                        throw new IOException(error);
                    }
                    regionServer.getRegionsInTransitionInRS().put(encodedNameBytes, Boolean.TRUE);
                }
                if (Boolean.TRUE.equals(previous)) {
                    // An open is in progress. This is supported, but let's log this.
                    LOG.info("Receiving OPEN for the region:" + region.getRegionNameAsString()
                            + ", which we are already trying to OPEN"
                            + " - ignoring this new request for this region.");
                }

                // We are opening this region. If it moves back and forth for whatever reason,
                // we don't
                // want to keep returning the stale moved record while we are opening/if we
                // close again.
                regionServer.removeFromMovedRegions(region.getEncodedName());
                if (previous == null || !previous.booleanValue()) {
                    htd = htds.get(region.getTable());
                    if (htd == null) {
                        htd = regionServer.tableDescriptors.get(region.getTable());
                        htds.put(region.getTable(), htd);
                    }
                    if (htd == null) {
                        throw new IOException("Missing table descriptor for " + region.getEncodedName());
                    }
                    // If there is no action in progress, we can submit a specific handler.
                    // Need to pass the expected version in the constructor.
                    if (regionServer.executorService == null) {
                        LOG.info("No executor executorService; skipping open request");
                    } else {
                        HRegion r = null;
                        try {
                            // Instantiate the region. This also periodically tickles OPENING
                            // state so master doesn't timeout this region in transition.
                            r = HRegion.openHRegion(region, htd,
                                    this.regionServer.getWAL(region),
                                    this.regionServer.getConfiguration(), this.regionServer);

                            updateMeta(r, masterSystemTime);
                            this.regionServer.addRegion(r);
                        } catch (Throwable t) {
                            // We failed open. Our caller will see the 'null' return value
                            // and transition the node back to FAILED_OPEN. If that fails,
                            // we rely on the Timeout Monitor in the master to reassign.
                            LOG.error("Failed open of region=" + r, t);
                        }
                    }
                }

                builder.addOpeningState(RegionOpeningState.OPENED);
            } catch (IOException ie) {
                LOG.warn("Failed opening region " + region.getRegionNameAsString(), ie);
                if (isBulkAssign) {
                    builder.addOpeningState(RegionOpeningState.FAILED_OPENING);
                } else {
                    throw new ServiceException(ie);
                }

            }
        }
        return builder.build();
    }

    boolean updateMeta(HRegion region, long masterSystemTime) {
        // TODO
        return true;
    }

    /**
     * Method to account for the size of retained cells and retained data blocks.
     * 
     * @param context   rpc call context
     * @param r         result to add size.
     * @param lastBlock last block to check whether we need to add the block size in
     *                  context.
     * @return an object that represents the last referenced block from this
     *         response.
     */
    Object addSize(RpcCallContext context, Result r, Object lastBlock) {
        if (context != null && r != null && !r.isEmpty()) {
            for (org.apache.hadoop.hbase.Cell c : r.rawCells()) {
                context.incrementResponseCellSize(PrivateCellUtil.estimatedSerializedSizeOf((Cell) c));

                // Since byte buffers can point all kinds of crazy places it's harder to keep
                // track
                // of which blocks are kept alive by what byte buffer.
                // So we make a guess.
                if (c instanceof ByteBufferExtendedCell) {
                    ByteBufferExtendedCell bbCell = (ByteBufferExtendedCell) c;
                    ByteBuffer bb = bbCell.getValueByteBuffer();
                    if (bb != lastBlock) {
                        context.incrementResponseBlockSize(bb.capacity());
                        lastBlock = bb;
                    }
                } else {
                    // We're using the last block being the same as the current block as
                    // a proxy for pointing to a new block. This won't be exact.
                    // If there are multiple gets that bounce back and forth
                    // Then it's possible that this will over count the size of
                    // referenced blocks. However it's better to over count and
                    // use two rpcs than to OOME the regionserver.
                    byte[] valueArray = c.getValueArray();
                    if (valueArray != lastBlock) {
                        context.incrementResponseBlockSize(valueArray.length);
                        lastBlock = valueArray;
                    }
                }

            }
        }
        return lastBlock;
    }

    private boolean isClientCellBlockSupport(RpcCallContext context) {
        return context != null && context.isClientCellBlockSupported();
    }

    private void addResult(final MutateResponse.Builder builder, final Result result,
            final HBaseRpcController rpcc, boolean clientCellBlockSupported) {
        if (result == null)
            return;
        if (clientCellBlockSupported) {
            builder.setResult(ProtobufUtil.toResultNoData(result));
            rpcc.setCellScanner(result.cellScanner());
        } else {
            org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.Result pbr = ProtobufUtil.toResult(result);
            builder.setResult(pbr);
        }
    }

    @Override
    public CloseRegionResponse closeRegion(RpcController controller, CloseRegionRequest request)
            throws ServiceException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'closeRegion'");
    }

    @Override
    public FlushRegionResponse flushRegion(RpcController controller, FlushRegionRequest request)
            throws ServiceException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'flushRegion'");
    }

    @Override
    public CompactionSwitchResponse compactionSwitch(RpcController controller, CompactionSwitchRequest request)
            throws ServiceException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'compactionSwitch'");
    }

    @Override
    public CompactRegionResponse compactRegion(RpcController controller, CompactRegionRequest request)
            throws ServiceException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'compactRegion'");
    }

    @Override
    public ReplicateWALEntryResponse replicateWALEntry(RpcController controller, ReplicateWALEntryRequest request)
            throws ServiceException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'replicateWALEntry'");
    }

    @Override
    public ReplicateWALEntryResponse replay(RpcController controller, ReplicateWALEntryRequest request)
            throws ServiceException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'replay'");
    }

    @Override
    public GetServerInfoResponse getServerInfo(RpcController controller, GetServerInfoRequest request)
            throws ServiceException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getServerInfo'");
    }

    @Override
    public GetRegionLoadResponse getRegionLoad(RpcController controller, GetRegionLoadRequest request)
            throws ServiceException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getRegionLoad'");
    }

    @Override
    public ClearRegionBlockCacheResponse clearRegionBlockCache(RpcController controller,
            ClearRegionBlockCacheRequest request) throws ServiceException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'clearRegionBlockCache'");
    }

    @Override
    public ExecuteProceduresResponse executeProcedures(RpcController controller, ExecuteProceduresRequest request)
            throws ServiceException {
        try {
            checkOpen();
            if (request.getOpenRegionCount() > 0) {
                // Avoid reading from the TableDescritor every time(usually it will read from
                // the file
                // system)
                Map<TableName, TableDescriptor> tdCache = new HashMap<>();
                request.getOpenRegionList().forEach(req -> executeOpenRegionProcedures(req, tdCache));
            }
            if (request.getCloseRegionCount() > 0) {
                request.getCloseRegionList().forEach(this::executeCloseRegionProcedures);
            }
            if (request.getProcCount() > 0) {
                request.getProcList().forEach(this::executeProcedures);
            }
            return ExecuteProceduresResponse.getDefaultInstance();
        } catch (IOException e) {
            throw new ServiceException(e);
        }
    }

    private void executeOpenRegionProcedures(OpenRegionRequest request,
            Map<TableName, TableDescriptor> tdCache) {
    }

    private void executeCloseRegionProcedures(CloseRegionRequest request) {
    }

    private void executeProcedures(RemoteProcedureRequest request) {
    }

    @Override
    public SlowLogResponses getSlowLogResponses(RpcController controller, SlowLogResponseRequest request)
            throws ServiceException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getSlowLogResponses'");
    }

    @Override
    public SlowLogResponses getLargeLogResponses(RpcController controller, SlowLogResponseRequest request)
            throws ServiceException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getLargeLogResponses'");
    }

    @Override
    public ClearSlowLogResponses clearSlowLogsResponses(RpcController controller, ClearSlowLogResponseRequest request)
            throws ServiceException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'clearSlowLogsResponses'");
    }

    @Override
    public LogEntry getLogEntries(RpcController controller, LogRequest request) throws ServiceException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getLogEntries'");
    }

    @Override
    public GetSpaceQuotaSnapshotsResponse getSpaceQuotaSnapshots(RpcController arg0, GetSpaceQuotaSnapshotsRequest arg1)
            throws ServiceException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getSpaceQuotaSnapshots'");
    }

    @Override
    public WarmupRegionResponse warmupRegion(RpcController controller, WarmupRegionRequest request)
            throws ServiceException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'warmupRegion'");
    }

    @Override
    public RollWALWriterResponse rollWALWriter(RpcController controller, RollWALWriterRequest request)
            throws ServiceException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'rollWALWriter'");
    }

    @Override
    public StopServerResponse stopServer(RpcController controller, StopServerRequest request) throws ServiceException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'stopServer'");
    }

    @Override
    public UpdateFavoredNodesResponse updateFavoredNodes(RpcController controller, UpdateFavoredNodesRequest request)
            throws ServiceException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'updateFavoredNodes'");
    }

    @Override
    public UpdateConfigurationResponse updateConfiguration(RpcController controller, UpdateConfigurationRequest request)
            throws ServiceException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'updateConfiguration'");
    }
}
