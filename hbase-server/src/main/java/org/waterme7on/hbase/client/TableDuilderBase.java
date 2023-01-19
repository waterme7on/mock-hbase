package org.waterme7on.hbase.client;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.TableBuilder;

/**
 * Base class for all table builders.
 */
abstract class TableBuilderBase implements TableBuilder {

    protected TableName tableName;

    protected int operationTimeout;

    protected int rpcTimeout;

    protected int readRpcTimeout;

    protected int writeRpcTimeout;
    protected final int scanReadRpcTimeout;
    protected int scanTimeout;

    TableBuilderBase(TableName tableName, ConnectionConfiguration connConf) {
        if (tableName == null) {
            throw new IllegalArgumentException("Given table name is null");
        }
        this.tableName = tableName;
        this.operationTimeout = tableName.isSystemTable()
                ? connConf.getMetaOperationTimeout()
                : connConf.getOperationTimeout();
        this.rpcTimeout = connConf.getRpcTimeout();
        this.readRpcTimeout = connConf.getReadRpcTimeout();
        this.scanReadRpcTimeout = tableName.isSystemTable() ? connConf.getMetaReadRpcTimeout() : readRpcTimeout;
        this.scanTimeout = tableName.isSystemTable() ? connConf.getMetaScanTimeout() : connConf.getScanTimeout();
        this.writeRpcTimeout = connConf.getWriteRpcTimeout();
    }

    @Override
    public TableBuilderBase setOperationTimeout(int timeout) {
        this.operationTimeout = timeout;
        return this;
    }

    @Override
    public TableBuilderBase setRpcTimeout(int timeout) {
        this.rpcTimeout = timeout;
        return this;
    }

    @Override
    public TableBuilderBase setReadRpcTimeout(int timeout) {
        this.readRpcTimeout = timeout;
        return this;
    }

    @Override
    public TableBuilderBase setWriteRpcTimeout(int timeout) {
        this.writeRpcTimeout = timeout;
        return this;
    }
}
