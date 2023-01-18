package org.waterme7on.hbase.master;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hbase.thirdparty.com.google.protobuf.BlockingRpcChannel;
import org.apache.hbase.thirdparty.org.apache.commons.cli.CommandLine;
import org.apache.hbase.thirdparty.org.apache.commons.cli.GnuParser;
import org.apache.hbase.thirdparty.org.apache.commons.cli.Options;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.ipc.RpcClient;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.trace.TraceUtil;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import org.waterme7on.hbase.ipc.RpcClientFactory;
import io.opentelemetry.context.Scope;
import org.apache.hbase.thirdparty.org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.waterme7on.hbase.HBaseCluster;
import org.waterme7on.hbase.ServerCommandLine;
import org.waterme7on.hbase.regionserver.HRegionServer;
import org.waterme7on.hbase.util.ClusterUtil;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Scanner;

public class HMasterCommandLine extends ServerCommandLine {
    private static final Logger LOG = LoggerFactory.getLogger(HMasterCommandLine.class);
    private static final String USAGE = "//TODO";

    private int startMaster() {
        Configuration conf = getConf();
        conf.addResource(null, "hbase-site.xml");
        conf.addResource("hbase-site.xml");

        final Span span = TraceUtil.createSpan("HMasterCommandLine.startMaster");
        try (Scope ignored = span.makeCurrent()) {
            if (HBaseCluster.isLocal(conf)) {
                LOG.debug("Running in local mode");
                logProcessInfo(conf);
                // Need to have the zk cluster shutdown when master is shutdown.
                // Run a subclass that does the zk cluster shutdown on its way out.
                int regionServersCount = conf.getInt("hbase.regionservers", 1);
                HBaseCluster cluster = new HBaseCluster(conf, regionServersCount);
                cluster.startup();
                waitOnMasterThreads(cluster);
                // HMaster master = HMaster.constructMaster(masterClass, conf);
                // if (master.isStopped()) {
                // LOG.info("Won't bring the Master up as a shutdown is requested");
                // return 1;
                // }
                // master.start();
                // master.join();
                // if (master.isAborted()) {
                // throw new RuntimeException("HMaster Aborted");
                // }
            } else {
                LOG.debug("Running in distributed mode (unsupported yet)");
            }
            span.setStatus(StatusCode.OK);
        } catch (Throwable t) {
            TraceUtil.setError(span, t);
            LOG.error("Master exiting", t);
            return 1;
        } finally {
            span.end();
        }

        return 0;
    }

    private int stopMaster() {
        Configuration conf = getConf();
        // Don't try more than once
        conf.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 0);
        try {
            Connection connection = ConnectionFactory.createConnection(conf);
            // String clusterId = connection.getClusterId();
            // ServerName sn = connection.getAdmin().getClusterMetrics().getMasterName();
            // RpcClient rpcClient = RpcClientFactory.createClient(conf, clusterId,
            // new InetSocketAddress("127.0.0.1", 0),
            // null);
            // BlockingRpcChannel channel = rpcClient.createBlockingRpcChannel(sn,
            // User.getCurrent(), 10000);
            // LOG.debug("clusterId:" + clusterId);
            // LOG.debug("sn:" + sn.toString());
            try {
                Admin admin = connection.getAdmin();
                admin.shutdown();
            } catch (Throwable t) {
                LOG.error("Failed to stop master", t);
                return 1;
            }
        } catch (MasterNotRunningException e) {
            LOG.error("Master not running");
            return 1;
        } catch (ZooKeeperConnectionException e) {
            LOG.error("ZooKeeper not available");
            return 1;
        } catch (IOException e) {
            LOG.error("Got IOException: " + e.getMessage(), e);
            return 1;
        }
        return 0;
    }

    private int createTable(String tableName) {
        Configuration conf = getConf();
        // Don't try more than once
        conf.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 0);
        try {
            Connection connection = ConnectionFactory.createConnection(conf);
            Admin admin = connection.getAdmin();
            TableDescriptor td = TableDescriptorBuilder.newBuilder(TableName.valueOf(tableName))
                    .setColumnFamily(ColumnFamilyDescriptorBuilder.of("cf"))
                    .build();
            admin.createTable(td);
            return 1;
        } catch (IOException e) {
            LOG.error("Got IOException: " + e.getMessage(), e);
        }
        return 0;
    }

    private int deleteTable(String tableName) {
        Configuration conf = getConf();
        // Don't try more than once
        conf.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 0);
        try {
            Connection connection = ConnectionFactory.createConnection(conf);

            Admin admin = connection.getAdmin();
            // TableDescriptor td =
            // TableDescriptorBuilder.newBuilder(TableName.valueOf(tableName))
            // .setColumnFamily(ColumnFamilyDescriptorBuilder.of("cf"))
            // .build();
            // Table t = connection.getTable(TableName.valueOf(tableName));
            admin.deleteTable(TableName.valueOf(tableName));
            return 1;
        } catch (IOException e) {
            LOG.error("Got IOException: " + e.getMessage(), e);
        }
        return 0;
    }

    private final Class<? extends HMaster> masterClass;

    public HMasterCommandLine(Class<? extends HMaster> masterClass) {
        this.masterClass = masterClass;
    }

    private void waitOnMasterThreads(HBaseCluster cluster) throws InterruptedException {
        List<ClusterUtil.MasterThread> masters = cluster.getMasters();
        List<ClusterUtil.RegionServerThread> regionservers = cluster.getRegionServers();

        if (masters != null) {
            for (ClusterUtil.MasterThread t : masters) {
                t.join();
                if (t.getMaster().isAborted()) {
                    closeAllRegionServerThreads(regionservers);
                    throw new RuntimeException("HMaster Aborted");
                }
            }
        }
    }

    private static void closeAllRegionServerThreads(List<ClusterUtil.RegionServerThread> regionservers) {
        for (ClusterUtil.RegionServerThread t : regionservers) {
            t.getRegionServer().stop("HMaster Aborted; Bringing down regions servers");
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        boolean shutDownCluster = false;
        Options opt = new Options();
        opt.addOption("localRegionServers", true, "RegionServers to start in master process when running standalone");
        opt.addOption("masters", true, "Masters to start in this process");
        opt.addOption("shutDownCluster", false, "`hbase master stop --shutDownCluster` shuts down cluster");

        // get input
        CommandLine cmd;
        try {
            cmd = new GnuParser().parse(opt, args);
        } catch (ParseException e) {
            LOG.error("Could not parse: ", e);
            usage(null);
            return 1;
        }
        // How many regionservers to startup in this process (we run regionservers in
        // same process as
        // master when we are in local/standalone mode. Useful testing)
        if (cmd.hasOption("localRegionServers")) {
            String val = cmd.getOptionValue("localRegionServers");
            getConf().setInt("hbase.regionservers", Integer.parseInt(val));
            LOG.debug("localRegionServers set to " + val);
        }
        // How many masters to startup inside this process; useful testing
        if (cmd.hasOption("masters")) {
            String val = cmd.getOptionValue("masters");
            getConf().setInt("hbase.masters", Integer.parseInt(val));
            LOG.debug("masters set to " + val);
        }
        // Checking whether to shut down cluster or not
        if (cmd.hasOption("shutDownCluster")) {
            shutDownCluster = true;
        }

        // Resolve remain arguments
        List<String> remainingArgs = cmd.getArgList();
        // if (remainingArgs.size() != 1) {
        // usage(null);
        // return 1;
        // }
        LOG.debug("remainingArgs: {}", (remainingArgs.size()));

        String command = remainingArgs.get(0);

        if ("start".equals(command)) {
            return startMaster();
        } else if ("stop".equals(command)) {
            return stopMaster();
        } else if ("createTable".equals(command)) {
            if (remainingArgs.size() != 2) {
                return 0;
            }
            return createTable(remainingArgs.get(1));
        } else if ("deleteTable".equals(command)) {
            if (remainingArgs.size() != 2) {
                return 0;
            }
            return deleteTable(remainingArgs.get(1));
        } else if ("shell".equals(command)) {
            shell();
        } else {
            usage("Invalid command: " + command);
            return 1;
        }
        return 0;
    }

    void shell() throws IOException {
        Configuration conf = getConf();
        conf.addResource(null, "hbase-site.xml");
        conf.addResource("hbase-site.xml");

        Scanner scanner = new Scanner(System.in);
        String inputCommand;
        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();

        while (true) {
            System.out.print("hbase(main):001:0> ");
            inputCommand = scanner.nextLine();
            if (inputCommand.equals("exit")) {
                break;
            }
            if (inputCommand.equals("createTable")) {
                System.out.print("Enter table name: ");
                String tableName = scanner.nextLine();
                TableDescriptor td = TableDescriptorBuilder.newBuilder(TableName.valueOf(tableName))
                        .setColumnFamily(ColumnFamilyDescriptorBuilder.of("cf"))
                        .build();
                admin.createTable(td);
            } else if (inputCommand.equals("deleteTable")) {
                System.out.print("Enter table name: ");
                String tableName = scanner.nextLine();
                admin.deleteTable(TableName.valueOf(tableName));
            } else {
                System.out.println("[" + inputCommand + "] is not a valid command. Please try again.");
            }
        }
    }

    @Override
    protected String getUsage() {
        return USAGE;
    }
}
