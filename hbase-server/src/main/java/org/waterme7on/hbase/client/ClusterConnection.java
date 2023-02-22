package org.waterme7on.hbase.client;

import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.RegionLocations;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos;
import org.waterme7on.hbase.protobuf.generated.TableMapProtos;

import java.io.IOException;

public interface ClusterConnection extends Connection {
    /**
     * Key for configuration in Configuration whose value is the class we implement
     * making a new
     * Connection instance.
     */
    String HBASE_CLIENT_CONNECTION_IMPL = "hbase.client.connection.impl";

    TableMapProtos.TableLocationService.BlockingInterface getTableMapService() throws IOException;

    MasterProtos.MasterService.BlockingInterface getMaster() throws IOException;

    AdminProtos.AdminService.BlockingInterface getMasterAdmin() throws IOException;

    AdminProtos.AdminService.BlockingInterface getAdmin(final ServerName serverName) throws IOException;

    ClientProtos.ClientService.BlockingInterface getClient(final ServerName serverName) throws IOException;

    /**
     * Gets the locations of the region in the specified table, <i>tableName</i>,
     * for a given row.
     * 
     * @param tableName table to get regions of
     * @param row       the row
     * @param useCache  Should we use the cache to retrieve the region information.
     * @param retry     do we retry
     * @param replicaId the replicaId for the region
     * @return region locations for this row.
     * @throws IOException if IO failure occurs
     */
    RegionLocations locateRegion(TableName tableName, byte[] row, boolean useCache, boolean retry,
            int replicaId) throws IOException;

    RegionLocations locateRegion(TableName tableName) throws IOException;

}
