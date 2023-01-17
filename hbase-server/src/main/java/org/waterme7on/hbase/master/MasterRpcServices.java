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

import org.apache.hadoop.hbase.shaded.protobuf.generated.AccessControlProtos.GetUserPermissionsRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AccessControlProtos.GetUserPermissionsResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AccessControlProtos.GrantRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AccessControlProtos.GrantResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AccessControlProtos.HasUserPermissionsRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AccessControlProtos.HasUserPermissionsResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AccessControlProtos.RevokeRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AccessControlProtos.RevokeResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.CoprocessorServiceRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.CoprocessorServiceResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.LogEntry;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.LogRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.LockServiceProtos.LockHeartbeatRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.LockServiceProtos.LockHeartbeatResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.LockServiceProtos.LockRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.LockServiceProtos.LockResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.LockServiceProtos.LockService;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.*;
import org.apache.hadoop.hbase.shaded.protobuf.generated.QuotaProtos.GetQuotaStatesRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.QuotaProtos.GetQuotaStatesResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.QuotaProtos.GetSpaceQuotaRegionSizesRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.QuotaProtos.GetSpaceQuotaRegionSizesResponse;
import org.waterme7on.hbase.protobuf.generated.RegionServerStatusProtos.RegionServerStartupRequest;
import org.waterme7on.hbase.protobuf.generated.RegionServerStatusProtos.RegionServerStartupResponse;
import org.waterme7on.hbase.protobuf.generated.RegionServerStatusProtos.RegionServerStatusService;
import org.waterme7on.hbase.protobuf.generated.HBaseProtos.NameStringPair;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ReplicationProtos.AddReplicationPeerRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ReplicationProtos.AddReplicationPeerResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ReplicationProtos.DisableReplicationPeerRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ReplicationProtos.DisableReplicationPeerResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ReplicationProtos.EnableReplicationPeerRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ReplicationProtos.EnableReplicationPeerResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ReplicationProtos.GetReplicationPeerConfigRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ReplicationProtos.GetReplicationPeerConfigResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ReplicationProtos.ListReplicationPeersRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ReplicationProtos.ListReplicationPeersResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ReplicationProtos.RemoveReplicationPeerRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ReplicationProtos.RemoveReplicationPeerResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ReplicationProtos.UpdateReplicationPeerConfigRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ReplicationProtos.UpdateReplicationPeerConfigResponse;
import org.apache.hbase.thirdparty.com.google.protobuf.RpcController;
import org.apache.hbase.thirdparty.com.google.protobuf.ServiceException;

public class MasterRpcServices extends RSRpcServices
        implements MasterService.BlockingInterface, RegionServerStatusService.BlockingInterface,
        LockService.BlockingInterface {
    private static final Logger LOG = LoggerFactory.getLogger(MasterRpcServices.class.getName());
    private static final Logger AUDITLOG = LoggerFactory
            .getLogger("SecurityLogger." + MasterRpcServices.class.getName());

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
    public LockResponse requestLock(RpcController controller, LockRequest request) throws ServiceException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public LockHeartbeatResponse lockHeartbeat(RpcController controller, LockHeartbeatRequest request)
            throws ServiceException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public RegionServerStartupResponse regionServerStartup(RpcController controller, RegionServerStartupRequest request)
            throws ServiceException {
        LOG.debug("Received region server startup request locally:\n" + request.toString());
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

    @Override
    public MajorCompactionTimestampResponse getLastMajorCompactionTimestamp(RpcController controller,
            MajorCompactionTimestampRequest request) throws ServiceException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public MajorCompactionTimestampResponse getLastMajorCompactionTimestampForRegion(RpcController controller,
            MajorCompactionTimestampForRegionRequest request) throws ServiceException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public GetProcedureResultResponse getProcedureResult(RpcController controller, GetProcedureResultRequest request)
            throws ServiceException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public SecurityCapabilitiesResponse getSecurityCapabilities(RpcController controller,
            SecurityCapabilitiesRequest request) throws ServiceException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public AbortProcedureResponse abortProcedure(RpcController controller, AbortProcedureRequest request)
            throws ServiceException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public GetProceduresResponse getProcedures(RpcController controller, GetProceduresRequest request)
            throws ServiceException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public GetLocksResponse getLocks(RpcController controller, GetLocksRequest request) throws ServiceException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public AddReplicationPeerResponse addReplicationPeer(RpcController controller, AddReplicationPeerRequest request)
            throws ServiceException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public EnableReplicationPeerResponse enableReplicationPeer(RpcController controller,
            EnableReplicationPeerRequest request) throws ServiceException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public DisableReplicationPeerResponse disableReplicationPeer(RpcController controller,
            DisableReplicationPeerRequest request) throws ServiceException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public GetReplicationPeerConfigResponse getReplicationPeerConfig(RpcController controller,
            GetReplicationPeerConfigRequest request) throws ServiceException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public DecommissionRegionServersResponse decommissionRegionServers(RpcController controller,
            DecommissionRegionServersRequest request) throws ServiceException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ClearDeadServersResponse clearDeadServers(RpcController controller, ClearDeadServersRequest request)
            throws ServiceException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public IsRpcThrottleEnabledResponse isRpcThrottleEnabled(RpcController controller,
            IsRpcThrottleEnabledRequest request) throws ServiceException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public GrantResponse grant(RpcController controller, GrantRequest request) throws ServiceException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public GetUserPermissionsResponse getUserPermissions(RpcController controller, GetUserPermissionsRequest request)
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
    public GetQuotaStatesResponse getQuotaStates(RpcController arg0, GetQuotaStatesRequest arg1)
            throws ServiceException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public GetSpaceQuotaRegionSizesResponse getSpaceQuotaRegionSizes(RpcController arg0,
            GetSpaceQuotaRegionSizesRequest arg1) throws ServiceException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ModifyColumnResponse modifyColumn(RpcController controller, ModifyColumnRequest request)
            throws ServiceException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public MoveRegionResponse moveRegion(RpcController controller, MoveRegionRequest request) throws ServiceException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public MergeTableRegionsResponse mergeTableRegions(RpcController controller, MergeTableRegionsRequest request)
            throws ServiceException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public UnassignRegionResponse unassignRegion(RpcController controller, UnassignRegionRequest request)
            throws ServiceException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public OfflineRegionResponse offlineRegion(RpcController controller, OfflineRegionRequest request)
            throws ServiceException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public SplitTableRegionResponse splitRegion(RpcController controller, SplitTableRegionRequest request)
            throws ServiceException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public TruncateTableResponse truncateTable(RpcController controller, TruncateTableRequest request)
            throws ServiceException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ModifyTableResponse modifyTable(RpcController controller, ModifyTableRequest request)
            throws ServiceException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ShutdownResponse shutdown(RpcController controller, ShutdownRequest request) throws ServiceException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public StopMasterResponse stopMaster(RpcController controller, StopMasterRequest request) throws ServiceException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public SetBalancerRunningResponse setBalancerRunning(RpcController controller, SetBalancerRunningRequest request)
            throws ServiceException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public SetSplitOrMergeEnabledResponse setSplitOrMergeEnabled(RpcController controller,
            SetSplitOrMergeEnabledRequest request) throws ServiceException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public IsSplitOrMergeEnabledResponse isSplitOrMergeEnabled(RpcController controller,
            IsSplitOrMergeEnabledRequest request) throws ServiceException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public NormalizeResponse normalize(RpcController controller, NormalizeRequest request) throws ServiceException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public SetNormalizerRunningResponse setNormalizerRunning(RpcController controller,
            SetNormalizerRunningRequest request) throws ServiceException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public RunCatalogScanResponse runCatalogScan(RpcController controller, RunCatalogScanRequest request)
            throws ServiceException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public RunCleanerChoreResponse runCleanerChore(RpcController controller, RunCleanerChoreRequest request)
            throws ServiceException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public SetCleanerChoreRunningResponse setCleanerChoreRunning(RpcController controller,
            SetCleanerChoreRunningRequest request) throws ServiceException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public SetQuotaResponse setQuota(RpcController arg0, SetQuotaRequest arg1) throws ServiceException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public SnapshotResponse snapshot(RpcController controller, SnapshotRequest request) throws ServiceException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public IsSnapshotDoneResponse isSnapshotDone(RpcController controller, IsSnapshotDoneRequest request)
            throws ServiceException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public RestoreSnapshotResponse restoreSnapshot(RpcController controller, RestoreSnapshotRequest request)
            throws ServiceException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public SwitchExceedThrottleQuotaResponse switchExceedThrottleQuota(RpcController arg0,
            SwitchExceedThrottleQuotaRequest arg1) throws ServiceException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public SetSnapshotCleanupResponse switchSnapshotCleanup(RpcController controller, SetSnapshotCleanupRequest request)
            throws ServiceException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public IsSnapshotCleanupEnabledResponse isSnapshotCleanupEnabled(RpcController controller,
            IsSnapshotCleanupEnabledRequest request) throws ServiceException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ModifyNamespaceResponse modifyNamespace(RpcController controller, ModifyNamespaceRequest request)
            throws ServiceException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ListNamespaceDescriptorsResponse listNamespaceDescriptors(RpcController controller,
            ListNamespaceDescriptorsRequest request) throws ServiceException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ListTableDescriptorsByNamespaceResponse listTableDescriptorsByNamespace(RpcController controller,
            ListTableDescriptorsByNamespaceRequest request) throws ServiceException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ListTableNamesByNamespaceResponse listTableNamesByNamespace(RpcController controller,
            ListTableNamesByNamespaceRequest request) throws ServiceException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public RemoveReplicationPeerResponse removeReplicationPeer(RpcController controller,
            RemoveReplicationPeerRequest request) throws ServiceException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public UpdateReplicationPeerConfigResponse updateReplicationPeerConfig(RpcController controller,
            UpdateReplicationPeerConfigRequest request) throws ServiceException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ListReplicationPeersResponse listReplicationPeers(RpcController controller,
            ListReplicationPeersRequest request) throws ServiceException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ListDecommissionedRegionServersResponse listDecommissionedRegionServers(RpcController controller,
            ListDecommissionedRegionServersRequest request) throws ServiceException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public RecommissionRegionServerResponse recommissionRegionServer(RpcController controller,
            RecommissionRegionServerRequest request) throws ServiceException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public SwitchRpcThrottleResponse switchRpcThrottle(RpcController controller, SwitchRpcThrottleRequest request)
            throws ServiceException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public RevokeResponse revoke(RpcController controller, RevokeRequest request) throws ServiceException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public HasUserPermissionsResponse hasUserPermissions(RpcController controller, HasUserPermissionsRequest request)
            throws ServiceException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ListNamespacesResponse listNamespaces(RpcController controller, ListNamespacesRequest request)
            throws ServiceException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ModifyTableStoreFileTrackerResponse modifyTableStoreFileTracker(RpcController controller,
            ModifyTableStoreFileTrackerRequest request) throws ServiceException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ModifyColumnStoreFileTrackerResponse modifyColumnStoreFileTracker(RpcController controller,
            ModifyColumnStoreFileTrackerRequest request) throws ServiceException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public FlushMasterStoreResponse flushMasterStore(RpcController controller, FlushMasterStoreRequest request)
            throws ServiceException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public AddColumnResponse addColumn(RpcController controller, AddColumnRequest request) throws ServiceException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public AssignRegionResponse assignRegion(RpcController controller, AssignRegionRequest request)
            throws ServiceException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public BalanceResponse balance(RpcController controller, BalanceRequest request) throws ServiceException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public CreateNamespaceResponse createNamespace(RpcController controller, CreateNamespaceRequest request)
            throws ServiceException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public CreateTableResponse createTable(RpcController controller, CreateTableRequest request)
            throws ServiceException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public DeleteColumnResponse deleteColumn(RpcController controller, DeleteColumnRequest request)
            throws ServiceException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public DeleteNamespaceResponse deleteNamespace(RpcController controller, DeleteNamespaceRequest request)
            throws ServiceException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public DeleteSnapshotResponse deleteSnapshot(RpcController controller, DeleteSnapshotRequest request)
            throws ServiceException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public DeleteTableResponse deleteTable(RpcController controller, DeleteTableRequest request)
            throws ServiceException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public DisableTableResponse disableTable(RpcController controller, DisableTableRequest request)
            throws ServiceException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public EnableCatalogJanitorResponse enableCatalogJanitor(RpcController controller,
            EnableCatalogJanitorRequest request) throws ServiceException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public EnableTableResponse enableTable(RpcController controller, EnableTableRequest request)
            throws ServiceException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public CoprocessorServiceResponse execMasterService(RpcController controller, CoprocessorServiceRequest request)
            throws ServiceException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ExecProcedureResponse execProcedure(RpcController controller, ExecProcedureRequest request)
            throws ServiceException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ExecProcedureResponse execProcedureWithRet(RpcController controller, ExecProcedureRequest request)
            throws ServiceException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public GetClusterStatusResponse getClusterStatus(RpcController controller, GetClusterStatusRequest request)
            throws ServiceException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public GetCompletedSnapshotsResponse getCompletedSnapshots(RpcController controller,
            GetCompletedSnapshotsRequest request) throws ServiceException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public GetNamespaceDescriptorResponse getNamespaceDescriptor(RpcController controller,
            GetNamespaceDescriptorRequest request) throws ServiceException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public GetSchemaAlterStatusResponse getSchemaAlterStatus(RpcController controller,
            GetSchemaAlterStatusRequest request) throws ServiceException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public GetTableDescriptorsResponse getTableDescriptors(RpcController controller, GetTableDescriptorsRequest request)
            throws ServiceException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public GetTableNamesResponse getTableNames(RpcController controller, GetTableNamesRequest request)
            throws ServiceException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public GetTableStateResponse getTableState(RpcController controller, GetTableStateRequest request)
            throws ServiceException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public IsBalancerEnabledResponse isBalancerEnabled(RpcController controller, IsBalancerEnabledRequest request)
            throws ServiceException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public IsCatalogJanitorEnabledResponse isCatalogJanitorEnabled(RpcController controller,
            IsCatalogJanitorEnabledRequest request) throws ServiceException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public IsCleanerChoreEnabledResponse isCleanerChoreEnabled(RpcController controller,
            IsCleanerChoreEnabledRequest request) throws ServiceException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public IsInMaintenanceModeResponse isMasterInMaintenanceMode(RpcController controller,
            IsInMaintenanceModeRequest request) throws ServiceException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public IsMasterRunningResponse isMasterRunning(RpcController controller, IsMasterRunningRequest request)
            throws ServiceException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public IsNormalizerEnabledResponse isNormalizerEnabled(RpcController controller, IsNormalizerEnabledRequest request)
            throws ServiceException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public IsProcedureDoneResponse isProcedureDone(RpcController controller, IsProcedureDoneRequest request)
            throws ServiceException {
        // TODO Auto-generated method stub
        return null;
    }
}
