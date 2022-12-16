package org.waterme7on.hbase.master;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class MasterWalManager {
    private static final Logger LOG = LoggerFactory.getLogger(MasterWalManager.class);
    // Keep around for convenience.
    private final MasterServices services;
    private final Configuration conf;
    private final FileSystem fs;

    // The Path to the old logs dir
    private final Path oldLogDir;

    private final Path rootDir;

    public MasterWalManager(MasterServices services) throws IOException {
        this(services.getConfiguration(), services.getMasterFileSystem().getWALFileSystem(), services);
    }

    public MasterWalManager(Configuration conf, FileSystem fs, MasterServices services)
            throws IOException {
        this.fs = fs;
        this.conf = conf;
        this.rootDir = CommonFSUtils.getWALRootDir(conf);
        this.services = services;
//        this.splitLogManager = new SplitLogManager(services, conf);
        this.oldLogDir = new Path(rootDir, HConstants.HREGION_OLDLOGDIR_NAME);
    }

}
