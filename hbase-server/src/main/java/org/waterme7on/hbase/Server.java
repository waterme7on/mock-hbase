package org.waterme7on.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;

public interface Server extends org.apache.hadoop.hbase.Abortable, org.apache.hadoop.hbase.Stoppable {

    Configuration getConfiguration();

    ZKWatcher getZooKeeper();

    ServerName getServerName();
}
