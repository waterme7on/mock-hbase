package org.waterme7on.hbase.client;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.ipc.RpcControllerFactory;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.RequestConverter;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos;
import org.apache.hbase.thirdparty.com.google.protobuf.RpcController;

import java.io.Closeable;
import java.io.IOException;

public class HBaseAdmin {

    static TableDescriptor getTableDescriptor(final TableName tableName, ClusterConnection connection,
                                              RpcRetryingCallerFactory rpcCallerFactory, final RpcControllerFactory rpcControllerFactory) throws IOException {
        if (tableName == null) return null;
        try {
            MasterProtos.GetTableDescriptorsRequest req = RequestConverter.buildGetTableDescriptorsRequest(tableName);
            MasterProtos.GetTableDescriptorsResponse htds = connection.getMaster().getTableDescriptors((RpcController) rpcControllerFactory.newController(), req);
            if (!htds.getTableSchemaList().isEmpty()) {
                return ProtobufUtil.toTableDescriptor(htds.getTableSchemaList().get(0));
            }
        } catch (Exception e) {
            throw new IOException(e);
        }
        return null;
    }
}
