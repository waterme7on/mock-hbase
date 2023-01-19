package org.waterme7on.hbase.client;

import java.io.Closeable;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.lang.reflect.UndeclaredThrowableException;
import java.util.concurrent.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.ipc.RpcClient;
import org.apache.hadoop.hbase.ipc.RpcClientFactory;
import org.apache.hadoop.hbase.regionserver.RegionServerStoppedException;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.ipc.RpcControllerFactory;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.RequestConverter;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos;
import org.apache.hadoop.hbase.util.ConcurrentMapUtils;
import org.apache.hbase.thirdparty.com.google.common.base.Throwables;
import org.apache.hbase.thirdparty.com.google.protobuf.BlockingRpcChannel;
import org.apache.hbase.thirdparty.com.google.protobuf.ServiceException;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConnectionImplementation implements ClusterConnection {
    private static final Logger LOG = LoggerFactory.getLogger(ConnectionImplementation.class);

    private final Configuration conf;
    // cache the configuration value for tables so that we can avoid calling
    // the expensive Configuration to fetch the value multiple times.
    private final ConnectionConfiguration connectionConfig;
    final MasterServiceState masterServiceState = new MasterServiceState(this);
    private final Object masterLock = new Object();

    private final int metaReplicaCallTimeoutScanInMicroSecond;
    private final int numTries;

    // Client rpc instance.
    private final RpcClient rpcClient;

    private final User user;
    // thread executor shared by all Table instances created
    // by this connection
    private volatile ThreadPoolExecutor batchPool = null;
    private ConnectionRegistry registry;
    private volatile boolean closed;
    private volatile boolean aborted;
    final int rpcTimeout;
    // Map keyed by service name + regionserver to service stub implementation
    private final ConcurrentMap<String, Object> stubs = new ConcurrentHashMap<>();

    private final ServerStatisticTracker stats;

    private final RpcRetryingCallerFactory rpcCallerFactory;

    private final RpcControllerFactory rpcControllerFactory;
    /**
     * constructor
     * 
     * @param conf Configuration object
     */
    ConnectionImplementation(Configuration conf, ExecutorService pool, User user) throws IOException {
        this(conf, pool, user, null);
    }

    /**
     * Constructor, for creating cluster connection with provided
     * ConnectionRegistry.
     */
    ConnectionImplementation(Configuration conf, ExecutorService pool, User user,
            ConnectionRegistry registry) throws IOException {
        this.conf = conf;
        this.user = user;
        this.closed = false;
        this.batchPool = (ThreadPoolExecutor) pool;
        this.registry = registry;
        this.connectionConfig = new ConnectionConfiguration(conf);
        this.metaReplicaCallTimeoutScanInMicroSecond = connectionConfig.getMetaReplicaCallTimeoutMicroSecondScan();
        this.rpcTimeout =
                conf.getInt(HConstants.HBASE_RPC_TIMEOUT_KEY, HConstants.DEFAULT_HBASE_RPC_TIMEOUT);

        // how many times to try, one more than max *retry* time
        this.numTries = (connectionConfig.getRetriesNumber());
        this.stats = ServerStatisticTracker.create(conf);
        this.rpcControllerFactory = RpcControllerFactory.instantiate(conf);
        this.rpcCallerFactory = RpcRetryingCallerFactory.instantiate(conf, null, this.stats);
        this.rpcClient = RpcClientFactory.createClient(this.conf, this.clusterId, null);

    }

    protected String clusterId = null;
    protected void retrieveClusterId() {
        if (clusterId != null) {
            return;
        }
        try {
            this.clusterId = this.registry.getClusterId().get();
        } catch (InterruptedException | ExecutionException e) {
            LOG.warn("Retrieve cluster id failed", e);
        }
        if (clusterId == null) {
            clusterId = HConstants.CLUSTER_ID_DEFAULT;
            LOG.debug("clusterid came back null, using default " + clusterId);
        }
    }
    public ConnectionConfiguration getConnectionConfiguration() {
        return connectionConfig;
    }

    @Override
    public Configuration getConfiguration() {
        return conf;
    }

    @Override
    public BufferedMutator getBufferedMutator(TableName tableName) throws IOException {
        return null;
    }

    @Override
    public BufferedMutator getBufferedMutator(BufferedMutatorParams params) throws IOException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public RegionLocator getRegionLocator(TableName tableName) throws IOException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void clearRegionLocationCache() {
        // TODO Auto-generated method stub

    }

    @Override
    public Admin getAdmin() throws IOException {
        // TODO Auto-generated method stub
        return null;
    }
    @Override
    public void close() throws IOException {
        // TODO Auto-generated method stub
        this.batchPool.shutdown();
    }

    @Override
    public boolean isClosed() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public TableBuilder getTableBuilder(TableName tableName, ExecutorService pool) {
        return new TableBuilderBase(tableName, connectionConfig) {

            @Override
            public Table build() {
                return new HTable(ConnectionImplementation.this, this, rpcCallerFactory,
                        rpcControllerFactory, pool);
            }
        };

    }

    @Override
    public void abort(String why, Throwable e) {
        // TODO Auto-generated method stub
        this.aborted = true;
        try {

            close();
        } catch (Exception xx) {
        }
        this.closed = true;
    }

    @Override
    public boolean isAborted() {
        // TODO Auto-generated method stub
        return this.aborted;
    }

    @Override
    public MasterProtos.MasterService.BlockingInterface getMaster() throws IOException {
        synchronized (masterLock) {
            if (!isKeepAliveMasterConnectedAndRunning(this.masterServiceState)) {
                MasterServiceStubMaker stubMaker = new MasterServiceStubMaker();
                this.masterServiceState.stub = stubMaker.makeStub();
            }
            resetMasterServiceState(this.masterServiceState);
        }
        return this.masterServiceState.stub;
    }

    private void resetMasterServiceState(final MasterServiceState mss) {
        mss.userCount++;
    }


    /**
     * State of the MasterService connection/setup.
     */
    static class MasterServiceState {
        Connection connection;

        MasterProtos.MasterService.BlockingInterface stub;
        int userCount;

        MasterServiceState(final Connection connection) {
            super();
            this.connection = connection;
        }

        @Override
        public String toString() {
            return "MasterService";
        }

        Object getStub() {
            return this.stub;
        }

        void clearStub() {
            this.stub = null;
        }

        boolean isMasterRunning() throws IOException {
            MasterProtos.IsMasterRunningResponse response = null;
            try {
                response = this.stub.isMasterRunning(null, RequestConverter.buildIsMasterRunningRequest());
            } catch (Exception e) {
                throw ProtobufUtil.handleRemoteException(e);
            }
            return response != null ? response.getIsMasterRunning() : false;
        }
    }

    /**
     * Class to make a MasterServiceStubMaker stub.
     */
    private final class MasterServiceStubMaker {

        private void isMasterRunning(MasterProtos.MasterService.BlockingInterface stub)
                throws IOException {
            try {
                stub.isMasterRunning(null, RequestConverter.buildIsMasterRunningRequest());
            } catch (ServiceException e) {
                throw ProtobufUtil.handleRemoteException(e);
            }
        }

        /**
         * Create a stub. Try once only. It is not typed because there is no common type to protobuf
         * services nor their interfaces. Let the caller do appropriate casting.
         * @return A stub for master services.
         */
        private MasterProtos.MasterService.BlockingInterface makeStubNoRetries()
                throws IOException, KeeperException {
            ServerName sn = get(registry.getActiveMaster());
            if (sn == null) {
                String msg = "ZooKeeper available but no active master location found";
                LOG.info(msg);
                throw new MasterNotRunningException(msg);
            }
            // Use the security info interface name as our stub key
            String key = getStubKey(MasterProtos.MasterService.getDescriptor().getName(), sn);
            MasterProtos.MasterService.BlockingInterface stub =
                    (MasterProtos.MasterService.BlockingInterface) computeIfAbsentEx(stubs, key, () -> {
                        BlockingRpcChannel channel = rpcClient.createBlockingRpcChannel(sn, user, rpcTimeout);
                        return MasterProtos.MasterService.newBlockingStub(channel);
                    });
            isMasterRunning(stub);
            return stub;
        }

        /**
         * Create a stub against the master. Retry if necessary.
         * @return A stub to do <code>intf</code> against the master
         * @throws org.apache.hadoop.hbase.MasterNotRunningException if master is not running
         */
        MasterProtos.MasterService.BlockingInterface makeStub() throws IOException {
            // The lock must be at the beginning to prevent multiple master creations
            // (and leaks) in a multithread context
            synchronized (masterLock) {
                Exception exceptionCaught = null;
                if (!closed) {
                    try {
                        return makeStubNoRetries();
                    } catch (IOException e) {
                        exceptionCaught = e;
                    } catch (KeeperException e) {
                        exceptionCaught = e;
                    }
                    throw new MasterNotRunningException(exceptionCaught);
                } else {
                    throw new DoNotRetryIOException("Connection was closed while trying to get master");
                }
            }
        }
    }


    private boolean isKeepAliveMasterConnectedAndRunning(MasterServiceState mss) {
        if (mss.getStub() == null) {
            return false;
        }
        try {
            return mss.isMasterRunning();
        } catch (UndeclaredThrowableException e) {
            // It's somehow messy, but we can receive exceptions such as
            // java.net.ConnectException but they're not declared. So we catch it...
            LOG.info("Master connection is not running anymore", e.getUndeclaredThrowable());
            return false;
        } catch (IOException se) {
            LOG.warn("Checking master connection", se);
            return false;
        }
    }

    private static <T> T get(CompletableFuture<T> future) throws IOException {
        try {
            return future.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw (IOException) new InterruptedIOException().initCause(e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            Throwables.propagateIfPossible(cause, IOException.class);
            throw new IOException(cause);
        }
    }


    /**
     * Get a unique key for the rpc stub to the given server.
     */
    static String getStubKey(String serviceName, ServerName serverName) {
        return String.format("%s@%s", serviceName, serverName);
    }

    /**
     * In HBASE-16648 we found that ConcurrentHashMap.get is much faster than computeIfAbsent if the
     * value already exists. So here we copy the implementation of
     * {@link ConcurrentMap#computeIfAbsent(Object, java.util.function.Function)}. It uses get and
     * putIfAbsent to implement computeIfAbsent. And notice that the implementation does not guarantee
     * that the supplier will only be executed once.
     */
    public static <K, V> V computeIfAbsentEx(ConcurrentMap<K, V> map, K key,
                                             ConcurrentMapUtils.IOExceptionSupplier<V> supplier) throws IOException {
        V v, newValue;
        return ((v = map.get(key)) == null && (newValue = supplier.get()) != null
                && (v = map.putIfAbsent(key, newValue)) == null) ? newValue : v;
    }
}
