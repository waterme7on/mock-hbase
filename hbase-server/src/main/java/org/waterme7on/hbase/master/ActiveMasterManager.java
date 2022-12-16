package org.waterme7on.hbase.master;

import org.apache.hadoop.hbase.ServerName;
import org.apache.hbase.thirdparty.com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InterruptedIOException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.waterme7on.hbase.Server;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.hadoop.hbase.zookeeper.ZKListener;
import org.apache.hadoop.hbase.zookeeper.MasterAddressTracker;
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
}
