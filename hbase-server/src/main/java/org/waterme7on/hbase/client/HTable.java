package org.waterme7on.hbase.client;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.client.trace.TableOperationSpanBuilder;
import org.apache.hadoop.hbase.shaded.protobuf.RequestConverter;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.trace.TraceUtil;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hbase.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.apache.hadoop.hbase.ipc.RpcControllerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HTable implements Table {

    private static final Logger LOG = LoggerFactory.getLogger(HTable.class);
    private final ClusterConnection connection;
    private final TableName tableName;
    private final Configuration configuration;
    private final ConnectionConfiguration connConfiguration;
    private final ExecutorService pool; // For Multi & Scan
    private final RpcRetryingCallerFactory rpcCallerFactory;
    private final RpcControllerFactory rpcControllerFactory;



    public static ThreadPoolExecutor getDefaultExecutor(Configuration conf) {
        int maxThreads = conf.getInt("hbase.htable.threads.max", Integer.MAX_VALUE);
        if (maxThreads == 0) {
            maxThreads = 1; // is there a better default?
        }
        int corePoolSize = conf.getInt("hbase.htable.threads.coresize", 1);
        long keepAliveTime = conf.getLong("hbase.htable.threads.keepalivetime", 60);

        // Using the "direct handoff" approach, new threads will only be created
        // if it is necessary and will grow unbounded. This could be bad but in HCM
        // we only create as many Runnables as there are region servers. It means
        // it also scales when new region servers are added.
        ThreadPoolExecutor pool = new ThreadPoolExecutor(corePoolSize, maxThreads, keepAliveTime, TimeUnit.SECONDS,
                new SynchronousQueue<>(), new ThreadFactoryBuilder().setNameFormat("htable-pool-%d")
                        .setUncaughtExceptionHandler(Threads.LOGGING_EXCEPTION_HANDLER).build());
        pool.allowCoreThreadTimeOut(true);
        return pool;
    }

    protected HTable(final ConnectionImplementation connection, final TableBuilderBase builder,
            final RpcRetryingCallerFactory rpcCallerFactory,
            final RpcControllerFactory rpcControllerFactory, final ExecutorService pool) {
        this.connection = connection;
        this.configuration = connection.getConfiguration();
        this.connConfiguration = connection.getConnectionConfiguration();
        this.pool = getDefaultExecutor(this.configuration);
        this.rpcCallerFactory = rpcCallerFactory;
        this.rpcControllerFactory = rpcControllerFactory;
        this.tableName = builder.tableName;

    }

    @Override
    public TableName getName() {
        return this.tableName;
    }

    @Override
    public Configuration getConfiguration() {
        return configuration;
    }

    @Override
    public TableDescriptor getDescriptor() throws IOException {
        return HBaseAdmin.getTableDescriptor(tableName, connection, rpcCallerFactory, rpcControllerFactory);
    }

    @Override
    public RegionLocator getRegionLocator() throws IOException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getRegionLocator'");
    }

    @Override
    public void put(final Put put) throws IOException {
        TraceUtil.trace(() -> {
            LOG.debug(put.toString());
        }, "HTable.put");
    }

    @Override
    public void close() {

    }

}
