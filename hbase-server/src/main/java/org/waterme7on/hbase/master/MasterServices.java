package org.waterme7on.hbase.master;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ClusterMetrics;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.waterme7on.hbase.Server;
import org.waterme7on.hbase.TableDescriptors;
import org.waterme7on.hbase.client.ClusterConnection;
import org.waterme7on.hbase.procedure2.ProcedureExecutor;

import com.google.protobuf.Service;

public interface MasterServices extends Server {
    /** Returns Master's filesystem {@link MasterFileSystem} utility class. */
    MasterFileSystem getMasterFileSystem();

    MasterWalManager getMasterWalManager();

    ServerManager getServerManager();

    String getClientIdAuditPrefix();

    ClusterMetrics getClusterMetrics() throws IOException;

    long createTable(final TableDescriptor tableDescriptor, final byte[][] splitKeys,
            final long nonceGroup, final long nonce) throws IOException;

    TableDescriptors getTableDescriptors();

    /** Returns Master's instance of {@link ClusterSchema} */
    ClusterSchema getClusterSchema();

    /** Returns true if master is initialized */
    boolean isInitialized();

    void createSystemTable(TableDescriptor td);

    /** Returns Master's instance of {@link ProcedureExecutor} */
    // ProcedureExecutor<MasterProcedureEnv> getMasterProcedureExecutor();
    /** Returns Master's instance of {@link TableStateManager} */
    TableStateManager getTableStateManager();

}
