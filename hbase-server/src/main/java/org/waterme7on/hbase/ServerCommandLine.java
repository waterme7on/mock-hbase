package org.waterme7on.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;

import static org.apache.hadoop.hbase.util.Threads.isNonDaemonThreadRunning;

public abstract class ServerCommandLine extends Configured implements Tool {
    private static final Logger LOG = LoggerFactory.getLogger(ServerCommandLine.class);

    /**
     * Implementing subclasses should return a usage string to print out.
     */
    protected abstract String getUsage();

    /**
     * Print usage information for this command line.
     * 
     * @param message if not null, print this message before the usage info.
     */
    protected void usage(String message) {
        if (message != null) {
            System.err.println(message);
            System.err.println("");
        }

        System.err.println(getUsage());
    }

    /**
     * Log information about the currently running JVM.
     */
    public static void logJVMInfo() {
        // Print out vm stats before starting up.
        RuntimeMXBean runtime = ManagementFactory.getRuntimeMXBean();
        if (runtime != null) {
            LOG.info("vmName=" + runtime.getVmName() + ", vmVendor=" + runtime.getVmVendor()
                    + ", vmVersion=" + runtime.getVmVersion());
            LOG.info("vmInputArguments=" + runtime.getInputArguments());
        }
    }

    /**
     * Print into log some of the important hbase attributes.
     */
    private static void logHBaseConfigs(Configuration conf) {
        final String[] keys = new String[] {
                // Expand this list as you see fit.
                "hbase.tmp.dir", HConstants.HBASE_DIR, HConstants.CLUSTER_DISTRIBUTED,
                HConstants.ZOOKEEPER_QUORUM,

        };
        for (String key : keys) {
            LOG.info(key + ": " + conf.get(key));
        }
    }

    public static void logProcessInfo(Configuration conf) {
        logHBaseConfigs(conf);
        // log processes info
        // todo

        // and JVM info
        logJVMInfo();
    }

    /**
     * Parse and run the given command line. This will exit the JVM with the exit
     * code returned from
     * <code>run()</code>. If return code is 0, wait for atmost 30 seconds for all
     * non-daemon threads
     * to quit, otherwise exit the jvm
     */
    public void doMain(String args[]) {
        try {
            Configuration conf = HBaseConfiguration.create();
            Path rootDir = new Path(System.getProperty("user.dir"));
            LOG.debug("doMain:" + rootDir.toString());
            // conf.set("hbase.zookeeper.quorum", "172.17.0.2");
            conf.addResource(new Path(rootDir, "conf/hbase-site.xml"));
            logHBaseConfigs(conf);
            int ret = ToolRunner.run(conf, this, args);
            if (ret != 0) {
                System.exit(ret);
            }
            // Return code is 0 here.
            boolean forceStop = false;
            long startTime = EnvironmentEdgeManager.currentTime();
            while (isNonDaemonThreadRunning()) {
                if (EnvironmentEdgeManager.currentTime() - startTime > 30 * 1000) {
                    forceStop = true;
                    break;
                }
                Thread.sleep(1000);
            }
            if (forceStop) {
                LOG.error("Failed to stop all non-daemon threads, so terminating JVM");
                System.exit(-1);
            }
        } catch (Exception e) {
            LOG.error("Failed to run", e);
            System.exit(-1);
        }
    }
}
