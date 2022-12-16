package org.waterme7on.hbase.master;


import java.io.FileNotFoundException;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hbase.ClusterId;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.log.HBaseMarkers;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class abstracts a bunch of operations the HMaster needs to interact with the underlying file
 * system like creating the initial layout, checking file system status, etc.
 */
public class MasterFileSystem {
    private static final Logger LOG = LoggerFactory.getLogger(MasterFileSystem.class);

    // HBase configuration
    private final Configuration conf;
    // Persisted unique cluster ID
    private ClusterId clusterId;
    // Keep around for convenience.
    private final FileSystem fs;
    // Keep around for convenience.
    private final FileSystem walFs;
    // root log directory on the FS
    private final Path rootdir;
    // hbase temp directory used for table construction and deletion
    private final Path tempdir;
    // root hbase directory on the FS
    private final Path walRootDir;


    public MasterFileSystem(Configuration conf) throws IOException {
        this.conf = conf;
        // Set filesystem to be that of this.rootdir else we get complaints about
        // mismatched filesystems if hbase.rootdir is hdfs and fs.defaultFS is
        // default localfs. Presumption is that rootdir is fully-qualified before
        // we get to here with appropriate fs scheme.
        this.rootdir = CommonFSUtils.getRootDir(conf);
        this.tempdir = new Path(this.rootdir, HConstants.HBASE_TEMP_DIRECTORY);
        // Cover both bases, the old way of setting default fs and the new.
        this.fs = this.rootdir.getFileSystem(conf);
        this.walRootDir = CommonFSUtils.getWALRootDir(conf);
        this.walFs = CommonFSUtils.getWALFileSystem(conf);
        CommonFSUtils.setFsDefault(conf, new Path(this.walFs.getUri()));
        walFs.setConf(conf);
        CommonFSUtils.setFsDefault(conf, new Path(this.fs.getUri()));
        // make sure the fs has the same conf
        fs.setConf(conf);
        // setup the filesystem variable
        createInitialFileSystemLayout();
    }

    public void createInitialFileSystemLayout(){

    }
    public FileSystem getWALFileSystem() {
        return this.walFs;
    }

}
