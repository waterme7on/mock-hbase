package org.waterme7on.hbase.master;

import org.apache.hadoop.hbase.ServerName;
import org.apache.hbase.thirdparty.com.google.common.collect.ImmutableList;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    * */


    private static final Logger LOG = LoggerFactory.getLogger(ActiveMasterManager.class);


    final AtomicBoolean clusterHasActiveMaster = new AtomicBoolean(false);
    final AtomicBoolean clusterShutDown = new AtomicBoolean(false);

    // This server's information. Package-private for child implementations.
    int infoPort;
    final ServerName sn;
    final Server master;
    // Active master's server name. Invalidated anytime active master changes (based on ZK
    // notifications) and lazily fetched on-demand.
    // ServerName is immutable, so we don't need heavy synchronization around it.
    volatile ServerName activeMasterServerName;
    // Registered backup masters. List is kept up to date based on ZK change notifications to
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
        backupMasters =
                ImmutableList.copyOf(MasterAddressTracker.getBackupMastersAndRenewWatch(watcher));
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
     * Block until becoming the active master. Method blocks until there is not another active master
     * and our attempt to become the new active master is successful. This also makes sure that we are
     * watching the master znode so will be notified if another master dies.
     * @param checkInterval the interval to check if the master is stopped
     * @param startupStatus the monitor status to track the progress
     * @return True if no issue becoming active master else false if another master was running or if
     *         some other problem (zookeeper, stop flag has been set on this Master)
     */
    boolean blockUntilBecomingActiveMaster(int checkInterval, MonitoredTask startupStatus) {
        return true;
    }
}
