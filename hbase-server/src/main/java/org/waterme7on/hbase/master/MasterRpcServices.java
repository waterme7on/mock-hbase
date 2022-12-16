package org.waterme7on.hbase.master;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.io.ByteBuffAllocator;
import org.waterme7on.hbase.ipc.PriorityFunction;
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
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

public class MasterRpcServices extends RSRpcServices {
    private static final Logger LOG = LoggerFactory.getLogger(MasterRpcServices.class.getName());
    private static final Logger AUDITLOG =
            LoggerFactory.getLogger("SecurityLogger." + MasterRpcServices.class.getName());

    private final HMaster master;
    public MasterRpcServices(HMaster m) throws IOException {
        super(m);
        master = m;
    }

    protected RpcServerInterface createRpcServer(final Server server,
                                                final RpcSchedulerFactory rpcSchedulerFactory, final InetSocketAddress bindAddress,
                                                final String name) throws IOException {
        final Configuration conf = regionServer.getConfiguration();
        // RpcServer at HM by default enable ByteBufferPool iff HM having user table region in it
//        boolean reservoirEnabled = conf.getBoolean(ByteBuffAllocator.ALLOCATOR_POOL_ENABLED_KEY,
//                LoadBalancer.isMasterCanHostUserRegions(conf));
        boolean reservoirEnabled = false;
        try {
            return RpcServerFactory.createRpcServer(server, name, getServices(), bindAddress, // use final
                    // bindAddress
                    // for this
                    // server.
                    conf, rpcSchedulerFactory.create(conf, (PriorityFunction) this, server), reservoirEnabled);
        } catch (BindException be) {
            throw new IOException(be.getMessage() + ". To switch ports use the '" + HConstants.MASTER_PORT
                    + "' configuration property.", be.getCause() != null ? be.getCause() : be);
        }
    }
    protected List<BlockingServiceAndInterface> getServices() {
        List<BlockingServiceAndInterface> bssi = new ArrayList<>(5);
//        bssi.add(new BlockingServiceAndInterface(MasterService.newReflectiveBlockingService(this),
//                MasterService.BlockingInterface.class));
        bssi.addAll(super.getServices());
        return bssi;
    }
}
