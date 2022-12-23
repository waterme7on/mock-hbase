package org.waterme7on.hbase.master;

import org.apache.hadoop.conf.Configuration;
import org.apache.hbase.thirdparty.org.apache.commons.cli.CommandLine;
import org.apache.hbase.thirdparty.org.apache.commons.cli.GnuParser;
import org.apache.hbase.thirdparty.org.apache.commons.cli.Options;
import org.apache.hbase.thirdparty.org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.waterme7on.hbase.ServerCommandLine;

import java.util.List;

public class HMasterCommandLine extends ServerCommandLine {
    private static final Logger LOG = LoggerFactory.getLogger(HMasterCommandLine.class);
    private static final String USAGE = "//TODO";
    private int startMaster(){
        Configuration conf = getConf();
        System.out.println(conf);
        return 0;
    }
    private int stopMaster(){
        return 0;
    }
    private final Class<? extends HMaster> masterClass;

    public HMasterCommandLine(Class<? extends HMaster> masterClass) {
        this.masterClass = masterClass;
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
        // How many regionservers to startup in this process (we run regionservers in same process as
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
        if (remainingArgs.size() != 1) {
            usage(null);
            return 1;
        }

        String command = remainingArgs.get(0);

        if ("start".equals(command)) {
            return startMaster();
        } else if ("stop".equals(command)) {
            if (shutDownCluster) {
                return stopMaster();
            }
            System.err.println("To shutdown the master run "
                    + "hbase-daemon.sh stop master or send a kill signal to the HMaster pid, "
                    + "and to stop HBase Cluster run \"stop-hbase.sh\" or \"hbase master "
                    + "stop --shutDownCluster\"");
            return 1;
//        } else if ("clear".equals(command)) {
//            return (ZNodeClearer.clear(getConf()) ? 0 : 1);
        } else {
            usage("Invalid command: " + command);
            return 1;
        }

    }

    @Override
    protected String getUsage() {
        return null;
    }
}
