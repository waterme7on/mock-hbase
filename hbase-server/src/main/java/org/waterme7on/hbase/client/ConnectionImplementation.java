package org.waterme7on.hbase.client;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.BufferedMutatorParams;
import org.apache.hadoop.hbase.client.ConnectionRegistry;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.TableBuilder;
import org.apache.hadoop.hbase.security.User;

public class ConnectionImplementation implements ClusterConnection {

    private final Configuration conf;
    private final User user;
    // thread executor shared by all Table instances created
    // by this connection
    private volatile ThreadPoolExecutor batchPool = null;
    private ConnectionRegistry registry;

    /**
     * constructor
     * 
     * @param conf Configuration object
     */
    ConnectionImplementation(Configuration conf, ExecutorService pool, User user) throws IOException {
        this(conf, pool, user, null);
    }

    /**
     * Constructor, for creating cluster connection with provided
     * ConnectionRegistry.
     */
    ConnectionImplementation(Configuration conf, ExecutorService pool, User user,
            ConnectionRegistry registry) throws IOException {
        this.conf = conf;
        this.user = user;
        this.batchPool = (ThreadPoolExecutor) pool;
        this.registry = registry;
    }

    @Override
    public Configuration getConfiguration() {
        return conf;
    }

    @Override
    public BufferedMutator getBufferedMutator(TableName tableName) throws IOException {
        return null;
    }

    @Override
    public BufferedMutator getBufferedMutator(BufferedMutatorParams params) throws IOException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public RegionLocator getRegionLocator(TableName tableName) throws IOException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void clearRegionLocationCache() {
        // TODO Auto-generated method stub

    }

    @Override
    public Admin getAdmin() throws IOException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void close() throws IOException {
        // TODO Auto-generated method stub

    }

    @Override
    public boolean isClosed() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public TableBuilder getTableBuilder(TableName tableName, ExecutorService pool) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void abort(String why, Throwable e) {
        // TODO Auto-generated method stub

    }

    @Override
    public boolean isAborted() {
        // TODO Auto-generated method stub
        return false;
    }

}
