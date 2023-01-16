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
import org.waterme7on.hbase.util.FSUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class abstracts a bunch of operations the HMaster needs to interact with
 * the underlying file
 * system like creating the initial layout, checking file system status, etc.
 */
public class MasterFileSystem {
    private static final Logger LOG = LoggerFactory.getLogger(MasterFileSystem.class);
    /** Parameter name for HBase instance root directory permission */
    public static final String HBASE_DIR_PERMS = "hbase.rootdir.perms";

    /** Parameter name for HBase WAL directory permission */
    public static final String HBASE_WAL_DIR_PERMS = "hbase.wal.dir.perms";
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
    // Permissions for bulk load staging directory under rootDir
    private final FsPermission HiddenDirPerms = FsPermission.valueOf("-rwx--x--x");

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

    public void createInitialFileSystemLayout() throws IOException {
        LOG.debug("createInitialFileSystemLayout");
        final String[] protectedSubDirs = new String[] { HConstants.BASE_NAMESPACE_DIR,
                HConstants.HFILE_ARCHIVE_DIRECTORY,
                HConstants.HBCK_SIDELINEDIR_NAME };

        // With the introduction of RegionProcedureStore,
        // there's no need to create MasterProcWAL dir here anymore. See HBASE-23715
        final String[] protectedSubLogDirs = new String[] { HConstants.HREGION_LOGDIR_NAME,
                HConstants.HREGION_OLDLOGDIR_NAME, HConstants.CORRUPT_DIR_NAME };
        // check if the root directory exists
        checkRootDir(this.rootdir, conf, this.fs);

        // Check the directories under rootdir.
        checkTempDir(this.tempdir, conf, this.fs);
        for (String subDir : protectedSubDirs) {
            checkSubDir(new Path(this.rootdir, subDir), HBASE_DIR_PERMS);
        }

        final String perms;
        if (!this.walRootDir.equals(this.rootdir)) {
            perms = HBASE_WAL_DIR_PERMS;
        } else {
            perms = HBASE_DIR_PERMS;
        }
        for (String subDir : protectedSubLogDirs) {
            checkSubDir(new Path(this.walRootDir, subDir), perms);
        }

        checkStagingDir();
    }

    public FileSystem getWALFileSystem() {
        return this.walFs;
    }

    public ClusterId getClusterId() {
        return clusterId;
    }

    /**
     * Get the rootdir. Make sure its wholesome and exists before returning.
     * 
     * @return hbase.rootdir (after checks for existence and bootstrapping if needed
     *         populating the
     *         directory with necessary bootup files).
     */
    private void checkRootDir(final Path rd, final Configuration c, final FileSystem fs)
            throws IOException {
        int threadWakeFrequency = c.getInt(HConstants.THREAD_WAKE_FREQUENCY, 10 * 1000);
        // If FS is in safe mode wait till out of it.
        FSUtils.waitOnSafeMode(c, threadWakeFrequency);

        // Filesystem is good. Go ahead and check for hbase.rootdir.
        FileStatus status;
        try {
            status = fs.getFileStatus(rd);
        } catch (FileNotFoundException e) {
            status = null;
        }
        int versionFileWriteAttempts = c.getInt(HConstants.VERSION_FILE_WRITE_ATTEMPTS,
                HConstants.DEFAULT_VERSION_FILE_WRITE_ATTEMPTS);
        try {
            if (status == null) {
                if (!fs.mkdirs(rd)) {
                    throw new IOException("Can not create configured '" + HConstants.HBASE_DIR + "' " + rd);
                }
                // DFS leaves safe mode with 0 DNs when there are 0 blocks.
                // We used to handle this by checking the current DN count and waiting until
                // it is nonzero. With security, the check for datanode count doesn't work --
                // it is a privileged op. So instead we adopt the strategy of the jobtracker
                // and simply retry file creation during bootstrap indefinitely. As soon as
                // there is one datanode it will succeed. Permission problems should have
                // already been caught by mkdirs above.
                FSUtils.setVersion(fs, rd, threadWakeFrequency, versionFileWriteAttempts);
            } else {
                if (!status.isDirectory()) {
                    throw new IllegalArgumentException(
                            "Configured '" + HConstants.HBASE_DIR + "' " + rd + " is not a directory.");
                }
                // as above
                FSUtils.checkVersion(fs, rd, true, threadWakeFrequency, versionFileWriteAttempts);
            }
        } catch (DeserializationException de) {
            LOG.error(HBaseMarkers.FATAL, "Please fix invalid configuration for '{}' {}",
                    HConstants.HBASE_DIR, rd, de);
            throw new IOException(de);
        } catch (IllegalArgumentException iae) {
            LOG.error(HBaseMarkers.FATAL, "Please fix invalid configuration for '{}' {}",
                    HConstants.HBASE_DIR, rd, iae);
            throw iae;
        }
        // Make sure cluster ID exists
        if (!FSUtils.checkClusterIdExists(fs, rd, threadWakeFrequency)) {
            FSUtils.setClusterId(fs, rd, new ClusterId(), threadWakeFrequency);
        }
        clusterId = FSUtils.getClusterId(fs, rd);
    }

    /**
     * Make sure the hbase temp directory exists and is empty. NOTE that this method
     * is only executed
     * once just after the master becomes the active one.
     */
    void checkTempDir(final Path tmpdir, final Configuration c, final FileSystem fs)
            throws IOException {
        if (fs.exists(tmpdir)) {
            fs.delete(tmpdir, true);
        }

        // Create the temp directory
        if (!fs.exists(tmpdir)) {
            if (!fs.mkdirs(tmpdir)) {
                throw new IOException("HBase temp directory '" + tmpdir + "' creation failure.");
            }
        }
    }

    /**
     * Make sure the directories under rootDir have good permissions. Create if
     * necessary.
     */
    private void checkSubDir(final Path p, final String dirPermsConfName) throws IOException {
        FileSystem fs = p.getFileSystem(conf);
        if (!fs.exists(p)) {
            if (!fs.mkdirs(p)) {
                throw new IOException("HBase directory '" + p + "' creation failure.");
            }
        }
    }

    /**
     * Check permissions for bulk load staging directory. This directory has special
     * hidden
     * permissions. Create it if necessary.
     */
    private void checkStagingDir() throws IOException {
        Path p = new Path(this.rootdir, HConstants.BULKLOAD_STAGING_DIR_NAME);
        try {
            if (!this.fs.exists(p)) {
                if (!this.fs.mkdirs(p, HiddenDirPerms)) {
                    throw new IOException("Failed to create staging directory " + p.toString());
                }
            }
            this.fs.setPermission(p, HiddenDirPerms);

        } catch (IOException e) {
            LOG.error("Failed to create or set permission on staging directory " + p.toString());
            throw new IOException(
                    "Failed to create or set permission on staging directory " + p.toString(), e);
        }
    }
}
