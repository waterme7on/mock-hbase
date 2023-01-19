package org.waterme7on.hbase.master;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.master.assignment.RegionStateNode;
import org.apache.hadoop.hbase.master.assignment.RegionStates;
import org.apache.hadoop.hbase.regionserver.StoreFileInfo;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.AdminService;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.ExecuteProceduresRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.AdminService.BlockingInterface;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hbase.thirdparty.com.google.protobuf.RpcController;
import org.apache.zookeeper.server.admin.AdminServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.waterme7on.hbase.coprocessor.StringUtils;
import org.waterme7on.hbase.regionserver.HRegion;
import org.waterme7on.hbase.regionserver.HRegionFactory.MasterRegion;
import org.waterme7on.hbase.util.FSTableDescriptors;

public class AssignmentManager {
    private static final Logger LOG = LoggerFactory.getLogger(AssignmentManager.class);

    public static final String ASSIGN_DISPATCH_WAIT_MSEC_CONF_KEY = "hbase.assignment.dispatch.wait.msec";
    private static final int DEFAULT_ASSIGN_DISPATCH_WAIT_MSEC = 150;

    public static final String ASSIGN_DISPATCH_WAITQ_MAX_CONF_KEY = "hbase.assignment.dispatch.wait.queue.max.size";
    private static final int DEFAULT_ASSIGN_DISPATCH_WAITQ_MAX = 100;
    public static final String ASSIGN_MAX_ATTEMPTS = "hbase.assignment.maximum.attempts";
    private static final int DEFAULT_ASSIGN_MAX_ATTEMPTS = Integer.MAX_VALUE;
    public static final String ASSIGN_RETRY_IMMEDIATELY_MAX_ATTEMPTS = "hbase.assignment.retry.immediately.maximum.attempts";
    private static final int DEFAULT_ASSIGN_RETRY_IMMEDIATELY_MAX_ATTEMPTS = 3;

    private final int assignDispatchWaitQueueMaxSize;
    private final int assignDispatchWaitMillis;
    private final int assignMaxAttempts;
    private final int assignRetryImmediatelyMaxAttempts;

    private final AtomicBoolean running = new AtomicBoolean(false);

    private final MasterRegion masterRegion;

    private final MasterServices master;

    private final Configuration conf;

    public final RegionStates regionStates = new RegionStates();
    private Thread assignThread;

    // ============================================================================================
    // Assign Queue (Assign/Balance)
    // ============================================================================================
    private final ArrayList<RegionStateNode> pendingAssignQueue = new ArrayList<RegionStateNode>();
    private final ReentrantLock assignQueueLock = new ReentrantLock();
    private final Condition assignQueueFullCond = assignQueueLock.newCondition();

    public AssignmentManager(MasterServices master, MasterRegion masterRegion) {
        this.master = master;
        this.masterRegion = masterRegion;
        Configuration conf = master.getConfiguration();
        this.conf = conf;
        this.assignDispatchWaitMillis = conf.getInt(ASSIGN_DISPATCH_WAIT_MSEC_CONF_KEY,
                DEFAULT_ASSIGN_DISPATCH_WAIT_MSEC);
        this.assignDispatchWaitQueueMaxSize = conf.getInt(ASSIGN_DISPATCH_WAITQ_MAX_CONF_KEY,
                DEFAULT_ASSIGN_DISPATCH_WAITQ_MAX);

        this.assignMaxAttempts = Math.max(1, conf.getInt(ASSIGN_MAX_ATTEMPTS, DEFAULT_ASSIGN_MAX_ATTEMPTS));
        this.assignRetryImmediatelyMaxAttempts = conf.getInt(ASSIGN_RETRY_IMMEDIATELY_MAX_ATTEMPTS,
                DEFAULT_ASSIGN_RETRY_IMMEDIATELY_MAX_ATTEMPTS);

    }

    public void initMeta() throws IOException {
        Path rootDir = CommonFSUtils.getRootDir(conf);
        TableDescriptor td = writeFsLayout(rootDir, conf);
        this.master.getTableDescriptors().update(td, true);
        RegionStateNode rnode = regionStates.getOrCreateRegionStateNode(RegionInfoBuilder.FIRST_META_REGIONINFO);
        queueAssign(rnode);
    }

    public void joinCluster() throws IOException {
        long startTime = System.nanoTime();
        LOG.debug("Joining cluster...");

        while (master.getServerManager().countOfRegionServers() < 1) {
            LOG.info("Waiting for RegionServers to join; current count={}",
                    master.getServerManager().countOfRegionServers());
            Threads.sleep(250);
        }
        LOG.info("Number of RegionServers={}", master.getServerManager().countOfRegionServers());

        long costMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime);
        LOG.info("Joined the cluster in {}", StringUtils.humanTimeDiff(costMs));
    }

    private void openRegion(RegionStateNode regionNode) throws IOException {
        RegionInfo regionInfo = regionNode.getRegionInfo();
        ServerName serverName = regionNode.getRegionLocation();
        if (serverName == null) {
            LOG.warn("No server to open region " + regionInfo.getRegionNameAsString());
            return;
        }
        regionStates.addRegionToServer(regionNode);
        LOG.debug("Current region states: " + regionStates.getRegionAssignments());
        LOG.info("Opening region " + regionInfo.getRegionNameAsString() + " on " + serverName);
        RpcController controller = null;
        AdminService.BlockingInterface admin = AdminService.newBlockingStub(this.master.getRsStub(serverName));
        ProtobufUtil.openRegion(controller,
                admin, serverName,
                regionInfo);
    }

    /**
     * 
     * Add the assign operation to the assignment queue. The pending assignment
     * operation will be
     * processed, and each region will be assigned by a server using the balancer.
     */
    protected void queueAssign(final RegionStateNode regionNode) {
        regionNode.getProcedureEvent().suspend();

        // TODO: quick-start for meta and the other sys-tables?
        assignQueueLock.lock();
        try {
            pendingAssignQueue.add(regionNode);
            if (regionNode.isSystemTable() || pendingAssignQueue.size() == 1
                    || pendingAssignQueue.size() >= assignDispatchWaitQueueMaxSize) {
                assignQueueFullCond.signal();
            }
        } finally {
            assignQueueLock.unlock();
        }
    }

    public void start() {
        if (!running.compareAndSet(false, true)) {
            return;
        }
        startAssignmentThread();
        // todo: read from snapshot
    }

    public void stop() {
        if (!running.compareAndSet(true, false)) {
            return;
        }
    }

    private void startAssignmentThread() {
        assignThread = new Thread(master.getServerName().toShortString()) {
            @Override
            public void run() {
                while (isRunning()) {
                    processAssignQueue();
                }
                pendingAssignQueue.clear();
            }
        };
        assignThread.setDaemon(true);
        assignThread.start();
    }

    private void processAssignQueue() {
        final HashMap<RegionInfo, RegionStateNode> regions = waitOnAssignQueue();
        if (regions == null || regions.size() == 0 || !isRunning()) {
            return;
        }
        List<ServerName> servers = master.getServerManager().createDestinationServersList();
        for (int i = 0; servers.size() < 1; ++i) {
            // Report every fourth time around this loop; try not to flood log.
            if (i % 4 == 0) {
                LOG.warn("No servers available; cannot place " + regions.size() + " unassigned regions.");
            }

            if (!isRunning()) {
                LOG.debug("Stopped! Dropping assign of " + regions.size() + " queued regions.");
                return;
            }
            Threads.sleep(250);
            servers = master.getServerManager().createDestinationServersList();
        }
        final List<RegionInfo> userHRIs = new ArrayList<>(regions.size());
        // Regions for system tables requiring reassignment
        final List<RegionInfo> systemHRIs = new ArrayList<>();
        for (RegionStateNode regionStateNode : regions.values()) {
            boolean sysTable = regionStateNode.isSystemTable();
            final List<RegionInfo> hris = sysTable ? systemHRIs : userHRIs;
            hris.add(regionStateNode.getRegionInfo());
        }

        if (!systemHRIs.isEmpty()) {
            try {
                // Assign user regions
                Map<ServerName, List<RegionInfo>> plan = this.master.getLoadBalancer().roundRobinAssignment(
                        systemHRIs,
                        servers);

                if (plan.isEmpty()) {
                    throw new IOException("unable to compute plans for regions=" + regions.size());
                }
                for (Map.Entry<ServerName, List<RegionInfo>> entry : plan.entrySet()) {
                    final ServerName server = entry.getKey();
                    for (RegionInfo hri : entry.getValue()) {
                        final RegionStateNode regionNode = regions.get(hri);
                        LOG.debug("assign {} to {}", hri.getRegionNameAsString(), server);
                        try {
                            regionNode.setRegionLocation(server);
                            openRegion(regionNode);
                        } catch (Exception e) {
                            queueAssign(regionNode);
                            LOG.error("Failed to open region " + hri.getRegionNameAsString(), e);
                        }
                    }
                }
            } catch (Exception e) {
                LOG.error("Failed to assign regions", e);
            }
        }
        if (!userHRIs.isEmpty()) {
            try {
                // Assign user regions
                Map<ServerName, List<RegionInfo>> plan = this.master.getLoadBalancer().roundRobinAssignment(
                        userHRIs,
                        servers);

                if (plan.isEmpty()) {
                    throw new IOException("unable to compute plans for regions=" + regions.size());
                }
                for (Map.Entry<ServerName, List<RegionInfo>> entry : plan.entrySet()) {
                    final ServerName server = entry.getKey();
                    for (RegionInfo hri : entry.getValue()) {
                        final RegionStateNode regionNode = regions.get(hri);
                        LOG.debug("assign {} to {}", hri.getRegionNameAsString(), server);
                        try {
                            regionNode.setRegionLocation(server);
                            openRegion(regionNode);

                        } catch (Exception e) {
                            LOG.error("Failed to open region " + hri.getRegionNameAsString(), e);
                            queueAssign(regionNode);
                        }
                    }
                }
            } catch (Exception e) {
                LOG.error("Failed to assign regions", e);
            }
        }
    }

    private HashMap<RegionInfo, RegionStateNode> waitOnAssignQueue() {
        HashMap<RegionInfo, RegionStateNode> regions = null;

        assignQueueLock.lock();
        try {
            if (pendingAssignQueue.isEmpty() && isRunning()) {
                assignQueueFullCond.await();
            }

            if (!isRunning()) {
                return null;
            }
            assignQueueFullCond.await(assignDispatchWaitMillis, TimeUnit.MILLISECONDS);
            regions = new HashMap<RegionInfo, RegionStateNode>(pendingAssignQueue.size());
            for (RegionStateNode regionNode : pendingAssignQueue) {
                regions.put(regionNode.getRegionInfo(), regionNode);
            }
            pendingAssignQueue.clear();
        } catch (InterruptedException e) {
            LOG.warn("got interrupted ", e);
            Thread.currentThread().interrupt();
        } finally {
            assignQueueLock.unlock();
        }
        return regions;
    }

    public boolean isRunning() {
        return running.get();
    }

    private static TableDescriptor writeFsLayout(Path rootDir, Configuration conf)
            throws IOException {
        LOG.info("BOOTSTRAP: creating hbase:meta region");
        LOG.debug("writeFsLayout at {}", rootDir);
        FileSystem fs = rootDir.getFileSystem(conf);
        Path tableDir = CommonFSUtils.getTableDir(rootDir, TableName.META_TABLE_NAME);
        if (fs.exists(tableDir) && !deleteMetaTableDirectoryIfPartial(fs, tableDir)) {
            LOG.warn("Can not delete partial created meta table, continue...");
        }
        // Bootstrapping, make sure blockcache is off. Else, one will be
        // created here in bootstrap and it'll need to be cleaned up. Better to
        // not make it in first place. Turn off block caching for bootstrap.
        // Enable after.
        TableDescriptor metaDescriptor = FSTableDescriptors.tryUpdateAndGetMetaTableDescriptor(conf, fs, rootDir);
        // HRegion.createHRegion(RegionInfoBuilder.FIRST_META_REGIONINFO, rootDir, conf,
        // metaDescriptor, null)
        // .close();
        return metaDescriptor;
    }

    private static boolean deleteMetaTableDirectoryIfPartial(FileSystem rootDirectoryFs,
            Path metaTableDir) throws IOException {
        boolean shouldDelete = true;
        try {
            TableDescriptor metaDescriptor = FSTableDescriptors.getTableDescriptorFromFs(rootDirectoryFs, metaTableDir);
            // when entering the state of INIT_META_WRITE_FS_LAYOUT, if a meta table
            // directory is found,
            // the meta table should not have any useful data and considers as partial.
            // if we find any valid HFiles, operator should fix the meta e.g. via HBCK.
            if (metaDescriptor != null && metaDescriptor.getColumnFamilyCount() > 0) {
                RemoteIterator<LocatedFileStatus> iterator = rootDirectoryFs.listFiles(metaTableDir, true);
                while (iterator.hasNext()) {
                    LocatedFileStatus status = iterator.next();
                    if (StoreFileInfo.isHFile(status.getPath())
                            && HFile.isHFileFormat(rootDirectoryFs, status.getPath())) {
                        shouldDelete = false;
                        break;
                    }
                }
            }
        } catch (Exception e) {
            if (!shouldDelete) {
                throw new IOException("Meta table is not partial, please sideline this meta directory "
                        + "or run HBCK to fix this meta table, e.g. rebuild the server hostname in ZNode for the "
                        + "meta region");
            }
        }
        return rootDirectoryFs.delete(metaTableDir, true);
    }

}
