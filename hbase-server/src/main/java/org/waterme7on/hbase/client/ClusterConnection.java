package org.waterme7on.hbase.client;

import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos;

import java.io.IOException;

public interface ClusterConnection extends Connection {
    /**
     * Key for configuration in Configuration whose value is the class we implement
     * making a new
     * Connection instance.
     */
    String HBASE_CLIENT_CONNECTION_IMPL = "hbase.client.connection.impl";

    MasterProtos.MasterService.BlockingInterface getMaster() throws IOException;

}
