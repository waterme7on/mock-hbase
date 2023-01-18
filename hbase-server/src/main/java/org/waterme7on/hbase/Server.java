package org.waterme7on.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.waterme7on.hbase.client.ClusterConnection;

import com.google.protobuf.Service;

public interface Server extends org.apache.hadoop.hbase.Abortable, org.apache.hadoop.hbase.Stoppable {

    Configuration getConfiguration();

    ZKWatcher getZooKeeper();

    ServerName getServerName();

    // MasterCoprocessorHost getMasterCoprocessorHost();

    Connection getConnection();

    Connection createConnection(Configuration conf) throws IOException;

    void registerService(Service service);

    ClusterConnection getClusterConnection();
}
