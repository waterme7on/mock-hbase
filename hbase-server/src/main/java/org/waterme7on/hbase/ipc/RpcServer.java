package org.waterme7on.hbase.ipc;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.LongAdder;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CallQueueTooBigException;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.conf.ConfigurationObserver;
import org.apache.hadoop.hbase.io.ByteBuffAllocator;
import org.apache.hadoop.hbase.ipc.HBaseRpcController;
import org.apache.hadoop.hbase.ipc.HBaseRpcControllerImpl;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.Pair;
import org.waterme7on.hbase.Server;
import org.waterme7on.hbase.regionserver.RSRpcServices;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hbase.thirdparty.com.google.protobuf.BlockingService;
import org.apache.hbase.thirdparty.com.google.protobuf.Message;
import org.apache.hbase.thirdparty.com.google.protobuf.ServiceException;
import org.apache.hbase.thirdparty.com.google.protobuf.TextFormat;
import org.apache.hbase.thirdparty.com.google.protobuf.Descriptors.MethodDescriptor;

public abstract class RpcServer implements RpcServerInterface, ConfigurationObserver {
    public static final Logger LOG = LoggerFactory.getLogger(RpcServer.class);
    public static final String MAX_REQUEST_SIZE = "hbase.ipc.max.request.size";
    protected static final int DEFAULT_MAX_CALLQUEUE_SIZE = 1024 * 1024 * 1024;
    public static final int DEFAULT_MAX_REQUEST_SIZE = DEFAULT_MAX_CALLQUEUE_SIZE / 4; // 256M
    /**
     * How many calls/handler are allowed in the queue.
     */
    public static final int DEFAULT_MAX_CALLQUEUE_LENGTH_PER_HANDLER = 10;

    /* Attributes */
    /**
     * This is set to Call object before Handler invokes an RPC and ybdie after the
     * call returns.
     */

    protected static final ThreadLocal<RpcCall> CurCall = new ThreadLocal<>();
    protected final Server server;
    protected final List<BlockingServiceAndInterface> services;
    protected final RpcScheduler scheduler;
    protected final InetSocketAddress bindAddress;
    protected final Configuration conf;
    protected final boolean tcpNoDelay; // if T then disable Nagle's Algorithm
    protected final boolean tcpKeepAlive; // if T then use keepalives
    protected final int maxRequestSize;
    private RSRpcServices rsRpcServices;
    protected final ByteBuffAllocator bbAllocator;
    protected final CellBlockBuilder cellBlockBuilder;
    /**
     * This is a running count of the size in bytes of all outstanding calls whether
     * currently
     * executing or queued waiting to be run.
     */
    protected final LongAdder callQueueSizeInBytes = new LongAdder();
    protected final long maxQueueSizeInBytes;
    protected static final CallQueueTooBigException CALL_QUEUE_TOO_BIG_EXCEPTION = new CallQueueTooBigException();
    protected final int minClientRequestTimeout;
    /**
     * Minimum allowable timeout (in milliseconds) in rpc request's header. This
     * configuration exists
     * to prevent the rpc service regarding this request as timeout immediately.
     */
    protected static final String MIN_CLIENT_REQUEST_TIMEOUT = "hbase.ipc.min.client.request.timeout";
    protected static final int DEFAULT_MIN_CLIENT_REQUEST_TIMEOUT = 20;

    public RpcServer(final Server server, final String name, final List<BlockingServiceAndInterface> services,
            final InetSocketAddress bindAddress,
            Configuration conf, RpcScheduler scheduler, boolean reservoirEnabled) throws IOException {
        // See declaration above for documentation on what this size is.
        this.server = server;
        this.conf = conf;
        this.bbAllocator = ByteBuffAllocator.create(conf, reservoirEnabled);
        this.cellBlockBuilder = new CellBlockBuilder(conf);
        this.services = services;
        this.bindAddress = bindAddress;
        this.maxQueueSizeInBytes = this.conf.getLong("hbase.ipc.server.max.callqueue.size", DEFAULT_MAX_CALLQUEUE_SIZE);
        this.scheduler = scheduler;
        this.tcpNoDelay = conf.getBoolean("hbase.ipc.server.tcpnodelay", true);
        this.tcpKeepAlive = conf.getBoolean("hbase.ipc.server.tcpkeepalive", true);
        this.maxRequestSize = conf.getInt(MAX_REQUEST_SIZE, DEFAULT_MAX_REQUEST_SIZE);
        this.minClientRequestTimeout = conf.getInt(MIN_CLIENT_REQUEST_TIMEOUT, DEFAULT_MIN_CLIENT_REQUEST_TIMEOUT);

        LOG.debug(RpcServer.class.getName() + "," + server);
    }

    @Override
    public void setRsRpcServices(RSRpcServices rsRpcServices) {
        this.rsRpcServices = rsRpcServices;
    }

    @Override
    public void setErrorHandler(HBaseRPCErrorHandler handler) {
    }

    /**
     * Datastructure for passing a {@link BlockingService} and its associated class
     * of protobuf
     * service interface. For example, a server that fielded what is defined in the
     * client protobuf
     * service would pass in an implementation of the client blocking service and
     * then its
     * ClientService.BlockingInterface.class. Used checking connection setup.
     */
    public static class BlockingServiceAndInterface {
        private final BlockingService service;
        private final Class<?> serviceInterface;

        public BlockingServiceAndInterface(final BlockingService service,
                final Class<?> serviceInterface) {
            this.service = service;
            this.serviceInterface = serviceInterface;
        }

        public Class<?> getServiceInterface() {
            return this.serviceInterface;
        }

        public BlockingService getBlockingService() {
            return this.service;
        }
    }

    /**
     * Returns the remote side ip address when invoked inside an RPC Returns null
     * incase of an error.
     */
    public static InetAddress getRemoteIp() {
        RpcCall call = CurCall.get();
        if (call != null) {
            return call.getRemoteAddress();
        }
        return null;
    }

    @FunctionalInterface
    protected interface CallCleanup {
        void run();
    }

    /**
     * Used by
     * {@link org.apache.hadoop.hbase.procedure2.store.region.RegionProcedureStore}.
     * For
     * master's rpc call, it may generate new procedure and mutate the region which
     * store procedure.
     * There are some check about rpc when mutate region, such as rpc timeout check.
     * So unset the rpc
     * call to avoid the rpc check.
     * 
     * @return the currently ongoing rpc call
     */
    public static Optional<RpcCall> unsetCurrentCall() {
        Optional<RpcCall> rpcCall = getCurrentCall();
        CurCall.set(null);
        return rpcCall;
    }

    /**
     * Set the rpc call back after mutate region.
     */
    public static void setCurrentCall(RpcCall rpcCall) {
        CurCall.set(rpcCall);
    }

    /**
     * Needed for features such as delayed calls. We need to be able to store the
     * current call so that
     * we can complete it later or ask questions of what is supported by the current
     * ongoing call.
     * 
     * @return An RpcCallContext backed by the currently ongoing call (gotten from a
     *         thread local)
     */
    public static Optional<RpcCall> getCurrentCall() {
        return Optional.ofNullable(CurCall.get());
    }

    /**
     * @param serviceName Some arbitrary string that represents a 'service'.
     * @param services    Available services and their service interfaces.
     * @return Service interface class for <code>serviceName</code>
     */
    protected static Class<?> getServiceInterface(final List<BlockingServiceAndInterface> services,
            final String serviceName) {
        BlockingServiceAndInterface bsasi = getServiceAndInterface(services, serviceName);
        return bsasi == null ? null : bsasi.getServiceInterface();
    }

    /**
     * @param serviceName Some arbitrary string that represents a 'service'.
     * @param services    Available services and their service interfaces.
     * @return BlockingService that goes with the passed <code>serviceName</code>
     */
    protected static BlockingService getService(final List<BlockingServiceAndInterface> services,
            final String serviceName) {
        BlockingServiceAndInterface bsasi = getServiceAndInterface(services, serviceName);
        return bsasi == null ? null : bsasi.getBlockingService();
    }

    /**
     * @param serviceName Some arbitrary string that represents a 'service'.
     * @param services    Available service instances
     * @return Matching BlockingServiceAndInterface pair
     */
    protected static BlockingServiceAndInterface getServiceAndInterface(
            final List<BlockingServiceAndInterface> services, final String serviceName) {
        for (BlockingServiceAndInterface bs : services) {
            if (bs.getBlockingService().getDescriptorForType().getName().equals(serviceName)) {
                return bs;
            }
        }
        return null;
    }

    public void addCallSize(final long diff) {
        this.callQueueSizeInBytes.add(diff);
    }

    @Override
    public void onConfigurationChange(Configuration newConf) {
        initReconfigurable(newConf);
        if (scheduler instanceof ConfigurationObserver) {
            ((ConfigurationObserver) scheduler).onConfigurationChange(newConf);
        }
    }

    protected void initReconfigurable(Configuration confToLoad) {
    }

    /**
     * This is a server side method, which is invoked over RPC. On success the
     * return response has
     * protobuf response payload. On failure, the exception name and the stack trace
     * are returned in
     * the protobuf response.
     */
    @Override
    public Pair<Message, CellScanner> call(RpcCall call)
            throws IOException {
        try {
            MethodDescriptor md = call.getMethod();
            Message param = call.getParam();
            // status.setRPC(md.getName(), new Object[] { param }, call.getReceiveTime());
            // status.setRPCPacket(param);
            // status.resume("Servicing call");
            // get an instance of the method arg type
            HBaseRpcController controller = new HBaseRpcControllerImpl(call.getCellScanner());
            controller.setCallTimeout(call.getTimeout());
            Message result = call.getService().callBlockingMethod(md, controller, param);
            long receiveTime = call.getReceiveTime();
            long startTime = call.getStartTime();
            long endTime = EnvironmentEdgeManager.currentTime();
            int processingTime = (int) (endTime - startTime);
            int qTime = (int) (startTime - receiveTime);
            int totalTime = (int) (endTime - receiveTime);
            if (LOG.isTraceEnabled()) {
                LOG.trace(
                        "{}, response: {}, receiveTime: {}, queueTime: {}, processingTime: {}, totalTime: {}",
                        CurCall.get().toString(), TextFormat.shortDebugString(result),
                        CurCall.get().getReceiveTime(), qTime, processingTime, totalTime);
            }
            return new Pair<>(result, controller.cellScanner());
        } catch (Throwable e) {
            // The above callBlockingMethod will always return a SE. Strip the SE wrapper
            // before
            // putting it on the wire. Its needed to adhere to the pb Service Interface but
            // we don't
            // need to pass it over the wire.
            if (e instanceof ServiceException) {
                if (e.getCause() == null) {
                    LOG.debug("Caught a ServiceException with null cause", e);
                } else {
                    e = e.getCause();
                }
            }
            LOG.error("Unexpected throwable object ", e);
            throw new IOException(e.getMessage(), e);
        }
    }

    public static boolean isInRpcCallContext() {
        return CurCall.get() != null;
    }

    /**
     * Returns the username for any user associated with the current RPC request or
     * not present if no
     * user is set.
     */
    public static Optional<String> getRequestUserName() {
        return getRequestUser().map(User::getShortName);
    }

    /** Returns Address of remote client if a request is ongoing, else null */
    public static Optional<InetAddress> getRemoteAddress() {
        return getCurrentCall().map(RpcCall::getRemoteAddress);
    }

    /**
     * Returns the user credentials associated with the current RPC request or not
     * present if no
     * credentials were provided.
     * 
     * @return A User
     */
    public static Optional<User> getRequestUser() {
        Optional<RpcCall> ctx = getCurrentCall();
        return ctx.isPresent() ? ctx.get().getRequestUser() : Optional.empty();
    }

}
