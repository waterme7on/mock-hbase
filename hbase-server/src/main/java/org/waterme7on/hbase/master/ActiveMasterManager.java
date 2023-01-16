package org.waterme7on.hbase.master;

import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hbase.thirdparty.com.google.common.collect.ImmutableList;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.waterme7on.hbase.Server;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.hadoop.hbase.zookeeper.ZKListener;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.MasterAddressTracker;
import org.waterme7on.hbase.monitoring.MonitoredTask;
import org.waterme7on.hbase.monitoring.TaskMonitor;

public class ActiveMasterManager extends ZKListener {
    /*
     * ActiveMasterManager manages masters' status
     *
     * when master is active,
     */

    private static final Logger LOG = LoggerFactory.getLogger(ActiveMasterManager.class);

    final AtomicBoolean clusterHasActiveMaster = new AtomicBoolean(false);
    final AtomicBoolean clusterShutDown = new AtomicBoolean(false);

    // This server's information. Package-private for child implementations.
    int infoPort;
    final ServerName sn;
    final Server master;
    // Active master's server name. Invalidated anytime active master changes (based
    // on ZK
    // notifications) and lazily fetched on-demand.
    // ServerName is immutable, so we don't need heavy synchronization around it.
    volatile ServerName activeMasterServerName;
    // Registered backup masters. List is kept up to date based on ZK change
    // notifications to
    // backup znode.
    private volatile ImmutableList<ServerName> backupMasters;

    // will be set after jetty server is started
    public void setInfoPort(int infoPort) {
        this.infoPort = infoPort;
    }

    ActiveMasterManager(ZKWatcher watcher, ServerName sn, Server master)
            throws InterruptedIOException {
        super(watcher);
        watcher.registerListener(this);
        this.sn = sn;
        this.master = master;
        updateBackupMasters();
    }

    private void updateBackupMasters() throws InterruptedIOException {
        backupMasters = ImmutableList.copyOf(MasterAddressTracker.getBackupMastersAndRenewWatch(watcher));
    }

    /** Returns True if cluster has an active master. */
    boolean hasActiveMaster() {
        try {
            if (ZKUtil.checkExists(watcher, watcher.getZNodePaths().masterAddressZNode) >= 0) {
                return true;
            }
        } catch (KeeperException ke) {
            LOG.info("Received an unexpected KeeperException when checking " + "isActiveMaster : " + ke);
        }
        return false;
    }

    /**
     * Block until becoming the active master. Method blocks until there is not
     * another active master
     * and our attempt to become the new active master is successful. This also
     * makes sure that we are
     * watching the master znode so will be notified if another master dies.
     * 
     * @param checkInterval the interval to check if the master is stopped
     * @param startupStatus the monitor status to track the progress
     * @return True if no issue becoming active master else false if another master
     *         was running or if
     *         some other problem (zookeeper, stop flag has been set on this Master)
     */
    boolean blockUntilBecomingActiveMaster(int checkInterval, MonitoredTask startupStatus) {
        while (!(master.isAborted() || master.isStopped())) {
            LOG.debug("Checking if we are the active master");
            startupStatus.setStatus("Trying to register in ZK as active master");
            try {
                LOG.debug(this.watcher.toString());
                LOG.debug(this.watcher.getZNodePaths().masterAddressZNode);
                LOG.debug(this.sn.toString());
                LOG.debug(String.valueOf(this.infoPort));
                if (MasterAddressTracker.setMasterAddress(this.watcher,
                        this.watcher.getZNodePaths().masterAddressZNode, this.sn, infoPort)) {

                    // We are the master, return
                    startupStatus.setStatus("Successfully registered as active master.");
                    this.clusterHasActiveMaster.set(true);
                    activeMasterServerName = sn;
                    LOG.info("Registered as active master=" + this.sn);
                    return true;
                }
                // Invalidate the active master name so that subsequent requests do not get any
                // stale
                // master information. Will be re-fetched if needed.
                activeMasterServerName = null;
                // There is another active master running elsewhere or this is a restart
                // and the master ephemeral node has not expired yet.
                this.clusterHasActiveMaster.set(true);

                String msg;
                byte[] bytes = ZKUtil.getDataAndWatch(this.watcher, this.watcher.getZNodePaths().masterAddressZNode);
                if (bytes == null) {
                    msg = ("A master was detected, but went down before its address "
                            + "could be read.  Attempting to become the next active master");
                } else {
                    ServerName currentMaster;
                    try {
                        currentMaster = ProtobufUtil.parseServerNameFrom(bytes);
                    } catch (DeserializationException e) {
                        LOG.warn("Failed parse", e);
                        // Hopefully next time around we won't fail the parse. Dangerous.
                        continue;
                    }

                    if (ServerName.isSameAddress(currentMaster, this.sn)) {
                        msg = ("Current master has this master's address, " + currentMaster
                                + "; master was restarted? Deleting node.");
                        // Hurry along the expiration of the znode.
                        ZKUtil.deleteNode(this.watcher, this.watcher.getZNodePaths().masterAddressZNode);
                        continue;
                    } else {
                        msg = "Another master is the active master, " + currentMaster
                                + "; waiting to become the next active master";
                    }
                }
                LOG.info(msg);
                startupStatus.setStatus(msg);
            } catch (Exception e) {
                LOG.warn("Failed to set master address in ZK, retrying", e);
                return false;
            }
            synchronized (this.clusterHasActiveMaster) {
                while (clusterHasActiveMaster.get() && !master.isStopped()) {
                    try {
                        clusterHasActiveMaster.wait(checkInterval);
                    } catch (InterruptedException e) {
                        // We expect to be interrupted when a master dies,
                        // will fall out if so
                        LOG.debug("Interrupted waiting for master to die", e);
                    }
                }
                if (clusterShutDown.get()) {
                    this.master.stop("Cluster went down before this master became active");
                }
            }
        }
        return false;
    }

    public void stop() {
        try {
            synchronized (clusterHasActiveMaster) {
                // Master is already stopped, wake up the manager
                // thread so that it can shutdown soon.
                clusterHasActiveMaster.notifyAll();
            }
            // If our address is in ZK, delete it on our way out
            ServerName activeMaster = null;
            try {
                activeMaster = MasterAddressTracker.getMasterAddress(this.watcher);
            } catch (IOException e) {
                LOG.warn("Failed get of master address: " + e.toString());
            }
            if (activeMaster != null && activeMaster.equals(this.sn)) {
                ZKUtil.deleteNode(watcher, watcher.getZNodePaths().masterAddressZNode);
                // // We may have failed to delete the znode at the previous step, but
                // // we delete the file anyway: a second attempt to delete the znode is likely
                // to
                // // fail again.
                // ZNodeClearer.deleteMyEphemeralNodeOnDisk();
            }
        } catch (KeeperException e) {
            LOG.debug(this.watcher.prefix("Failed delete of our master address node; " + e.getMessage()));
        }
    }
}