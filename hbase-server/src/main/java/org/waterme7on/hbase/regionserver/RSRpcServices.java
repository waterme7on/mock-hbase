package org.waterme7on.hbase.regionserver;

import org.apache.hbase.thirdparty.com.google.protobuf.Message;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.io.ByteBuffAllocator;
import org.apache.hadoop.hbase.net.Address;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.RegionInfo;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.RegionSpecifier;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RPCProtos.RequestHeader;
import org.apache.hadoop.hbase.util.DNS;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
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

public class RSRpcServices implements HBaseRPCErrorHandler, PriorityFunction {
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

    public Region getRegion(RegionSpecifier specifier) {
        // TODO
        return null;
    }

    @Override
    public int getPriority(RequestHeader header, Message param, User user) {
        return priority.getPriority(header, param, user);
    }
}
