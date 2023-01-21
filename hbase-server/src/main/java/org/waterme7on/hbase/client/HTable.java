package org.waterme7on.hbase.client;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.RegionLocations;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.client.trace.TableOperationSpanBuilder;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.RequestConverter;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.trace.TraceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ReflectionUtils;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hbase.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hbase.thirdparty.com.google.protobuf.RpcController;
import org.apache.hadoop.hbase.ipc.HBaseRpcController;
import org.apache.hadoop.hbase.ipc.RpcControllerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.waterme7on.hbase.protobuf.generated.TableMapProtos.*;

public class HTable implements Table {

    private static final Logger LOG = LoggerFactory.getLogger(HTable.class);

    private static final Consistency DEFAULT_CONSISTENCY = Consistency.STRONG;
    private final ClusterConnection connection;
    private final TableName tableName;
    private final Configuration configuration;
    private final ConnectionConfiguration connConfiguration;
    private final ExecutorService pool; // For Multi & Scan
    private final RpcRetryingCallerFactory rpcCallerFactory;
    private final RpcControllerFactory rpcControllerFactory;
    private int operationTimeoutMs; // global timeout for each blocking method with retrying rpc
    private final int rpcTimeoutMs; // FIXME we should use this for rpc like batch and checkAndXXX
    private int readRpcTimeoutMs; // timeout for each read rpc request
    private int writeRpcTimeoutMs; // timeout for each write rpc request
    private String regionName;
    private String serverName;

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
        this.operationTimeoutMs = builder.operationTimeout;
        this.rpcTimeoutMs = builder.rpcTimeout;
        this.readRpcTimeoutMs = builder.readRpcTimeout;
        this.writeRpcTimeoutMs = builder.writeRpcTimeout;
        this.regionName = null;
        this.serverName = null;

        for (int i = 0; i < 10; i++) {
            try {
                TableLocationRequest tableMapRequest = TableLocationRequest.newBuilder()
                        .setTableName(tableName.getNameAsString()).build();
                TableLocationService.BlockingInterface rs = (TableLocationService.BlockingInterface) this.connection
                        .getTableMapService();
                TableLocationResponse tableMapResponse = rs.tableLocation(rpcControllerFactory.newController(),
                        tableMapRequest);
                this.regionName = tableMapResponse.getRegionName();
                this.serverName = tableMapResponse.getServerName();
                break;
            } catch (Exception e) {
                continue;
            }
        }
        if (this.regionName == null || this.serverName == null) {
            throw new RuntimeException("Failed to get region name and server name");
        }

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
        try {

            TraceUtil.trace(() -> {
                validatePut(put);
                ClientProtos.MutateRequest request = RequestConverter
                        .buildMutateRequest(Bytes.toBytes(regionName), put);
                ClientProtos.ClientService.BlockingInterface stub = (ClientProtos.ClientService.BlockingInterface) this.connection
                        .getClient(ServerName.parseServerName(serverName));
                ClientProtos.MutateResponse res = stub.mutate(rpcControllerFactory.newController(), request);
                LOG.debug("HTable.put - {}, {}", put.toString(), res.getResult().toString());
            }, "HTable.put");
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    @Override
    public Result get(final Get get) throws IOException {
        try {
            return TraceUtil.trace(() -> get(get, get.isCheckExistenceOnly()), "HTable.get");
        } catch (Exception e) {
            e.printStackTrace();
        }
        return new Result();
    }

    private Result get(final Get get, final boolean checkExistenceOnly) throws Exception {
        return TraceUtil.trace(() -> {
            ClientProtos.GetRequest request = RequestConverter
                    .buildGetRequest(Bytes.toBytes(regionName), get);
            ClientProtos.ClientService.BlockingInterface stub = (ClientProtos.ClientService.BlockingInterface) this.connection
                    .getClient(ServerName.parseServerName(serverName));
            ClientProtos.GetResponse response;
            HBaseRpcController rpcController = rpcControllerFactory.newController();
            response = stub.get(rpcController, request);

            LOG.debug("HTable.get - {}, {}", get.toString(), response.getResult().toString());
            return response == null ? null
                    : ProtobufUtil.toResult(response.getResult(), (rpcController).cellScanner());
        }, "HTable.put");
    }

    @Override
    public void delete(Delete delete) throws IOException {
        try {
            TraceUtil.trace(() -> {
                ClientProtos.MutateRequest request = RequestConverter
                        .buildMutateRequest(Bytes.toBytes(regionName), delete);
                ClientProtos.ClientService.BlockingInterface stub = (ClientProtos.ClientService.BlockingInterface) this.connection
                        .getClient(ServerName.parseServerName(serverName));
                ClientProtos.MutateResponse res = stub.mutate(rpcControllerFactory.newController(), request);
                LOG.debug("HTable.delete - {}, {}", delete.toString(), res.getResult().toString());

            }, "HTable.delete");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // @Override
    // public Result[] get(List<Get> gets) throws IOException {
    // return TraceUtil.trace(() -> {
    // if (gets.size() == 1) {
    // return new Result[] { get(gets.get(0)) };
    // }
    // try {
    // Object[] r1 = new Object[gets.size()];
    // batch((List<? extends Row>) gets, r1, readRpcTimeoutMs);
    // // Translate.
    // Result[] results = new Result[r1.length];
    // int i = 0;
    // for (Object obj : r1) {
    // // Batch ensures if there is a failure we get an exception instead
    // results[i++] = (Result) obj;
    // }
    // return results;
    // } catch (InterruptedException e) {
    // throw (InterruptedIOException) new InterruptedIOException().initCause(e);
    // }
    // }, "Htable.get");
    // }

    @Override
    public void close() {

    }

    // validate for well-formedness
    private void validatePut(final Put put) throws IllegalArgumentException {
        // TODO
    }

}
