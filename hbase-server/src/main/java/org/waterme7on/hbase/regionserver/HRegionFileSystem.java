package org.waterme7on.hbase.regionserver;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.Objects;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.waterme7on.hbase.util.FSUtils;
import org.waterme7on.hbase.util.ServerRegionReplicaUtil;

/**
 * to store the region info
 */

public class HRegionFileSystem {
    private static final Logger LOG = LoggerFactory.getLogger(HRegionFileSystem.class);

    /**
     * Name of the region info file that resides just under the region directory.
     */
    public final static String REGION_INFO_FILE = ".regioninfo";

    /** Temporary subdirectory of the region directory used for merges. */
    public static final String REGION_MERGES_DIR = ".merges";

    /** Temporary subdirectory of the region directory used for splits. */
    public static final String REGION_SPLITS_DIR = ".splits";

    /**
     * Temporary subdirectory of the region directory used for compaction output.
     */
    static final String REGION_TEMP_DIR = ".tmp";

    private final RegionInfo regionInfo;
    // regionInfo for interacting with FS (getting encodedName, etc)
    final RegionInfo regionInfoForFs;
    final Configuration conf;
    private final Path tableDir;
    final FileSystem fs;
    private final Path regionDir;

    /**
     * In order to handle NN connectivity hiccups, one need to retry non-idempotent
     * operation at the
     * client level.
     */
    private final int hdfsClientRetriesNumber;
    private final int baseSleepBeforeRetries;
    private static final int DEFAULT_HDFS_CLIENT_RETRIES_NUMBER = 10;
    private static final int DEFAULT_BASE_SLEEP_BEFORE_RETRIES = 1000;

    /**
     * Create a view to the on-disk region
     * 
     * @param conf       the {@link Configuration} to use
     * @param fs         {@link FileSystem} that contains the region
     * @param tableDir   {@link Path} to where the table is being stored
     * @param regionInfo {@link RegionInfo} for region
     */
    public HRegionFileSystem(final Configuration conf, final FileSystem fs, final Path tableDir,
            final RegionInfo regionInfo) {
        this.fs = fs;
        this.conf = conf;
        this.tableDir = Objects.requireNonNull(tableDir, "tableDir is null");
        this.regionInfo = Objects.requireNonNull(regionInfo, "regionInfo is null");
        this.regionInfoForFs = ServerRegionReplicaUtil.getRegionInfoForFs(regionInfo);
        this.regionDir = FSUtils.getRegionDirFromTableDir(tableDir, regionInfo);
        this.hdfsClientRetriesNumber = conf.getInt("hdfs.client.retries.number", DEFAULT_HDFS_CLIENT_RETRIES_NUMBER);
        this.baseSleepBeforeRetries = conf.getInt("hdfs.client.sleep.before.retries",
                DEFAULT_BASE_SLEEP_BEFORE_RETRIES);
    }

    /** Returns the underlying {@link FileSystem} */
    public FileSystem getFileSystem() {
        return this.fs;
    }

    /** Returns the {@link RegionInfo} that describe this on-disk region view */
    public RegionInfo getRegionInfo() {
        return this.regionInfo;
    }

    public RegionInfo getRegionInfoForFS() {
        return this.regionInfoForFs;
    }

    /** Returns {@link Path} to the region's root directory. */
    public Path getTableDir() {
        return this.tableDir;
    }

    /** Returns {@link Path} to the region directory. */
    public Path getRegionDir() {
        return regionDir;
    }

    /**
     * Returns the directory path of the specified family
     * 
     * @param familyName Column Family Name
     * @return {@link Path} to the directory of the specified family
     */
    public Path getStoreDir(final String familyName) {
        return new Path(this.getRegionDir(), familyName);
    }

    /**
     * Create the store directory for the specified family name
     * 
     * @param familyName Column Family Name
     * @return {@link Path} to the directory of the specified family
     * @throws IOException if the directory creation fails.
     */
    Path createStoreDir(final String familyName) throws IOException {
        Path storeDir = getStoreDir(familyName);
        if (!fs.exists(storeDir) && !createDir(storeDir))
            throw new IOException("Failed creating " + storeDir);
        return storeDir;
    }

    /**
     * Creates a directory. Assumes the user has already checked for this directory
     * existence.
     * 
     * @return the result of fs.mkdirs(). In case underlying fs throws an
     *         IOException, it checks
     *         whether the directory exists or not, and returns true if it exists.
     */
    boolean createDir(Path dir) throws IOException {
        int i = 0;
        IOException lastIOE = null;
        do {
            try {
                return mkdirs(fs, conf, dir);
            } catch (IOException ioe) {
                lastIOE = ioe;
                if (fs.exists(dir))
                    return true; // directory is present
                try {
                    sleepBeforeRetry("Create Directory", i + 1);
                } catch (InterruptedException e) {
                    throw (InterruptedIOException) new InterruptedIOException().initCause(e);
                }
            }
        } while (++i <= hdfsClientRetriesNumber);
        throw new IOException("Exception in createDir", lastIOE);
    }

    static boolean mkdirs(FileSystem fs, Configuration conf, Path dir) throws IOException {
        FsPermission perms = CommonFSUtils.getFilePermissions(fs, conf, HConstants.DATA_FILE_UMASK_KEY);
        return fs.mkdirs(dir, perms);
    }

    // ===========================================================================
    // Temp Helpers
    // ===========================================================================
    /**
     * Returns {@link Path} to the region's temp directory, used for file creations
     */
    public Path getTempDir() {
        return new Path(getRegionDir(), REGION_TEMP_DIR);
    }

    /**
     * Clean up any temp detritus that may have been left around from previous
     * operation attempts.
     */
    void cleanupTempDir() throws IOException {
        deleteDir(getTempDir());
    }

    /**
     * Create a new Region on file-system.
     * 
     * @param conf       the {@link Configuration} to use
     * @param fs         {@link FileSystem} from which to add the region
     * @param tableDir   {@link Path} to where the table is being stored
     * @param regionInfo {@link RegionInfo} for region to be added
     * @throws IOException if the region creation fails due to a FileSystem
     *                     exception.
     */
    public static HRegionFileSystem createRegionOnFileSystem(final Configuration conf,
            final FileSystem fs, final Path tableDir, final RegionInfo regionInfo) throws IOException {
        HRegionFileSystem regionFs = new HRegionFileSystem(conf, fs, tableDir, regionInfo);

        // We only create a .regioninfo and the region directory if this is the default
        // region replica
        if (regionInfo.getReplicaId() == RegionInfo.DEFAULT_REPLICA_ID) {
            Path regionDir = regionFs.getRegionDir();
            if (fs.exists(regionDir)) {
                LOG.warn("Trying to create a region that already exists on disk: " + regionDir);
            } else {
                // Create the region directory
                if (!createDirOnFileSystem(fs, conf, regionDir)) {
                    LOG.warn("Unable to create the region directory: " + regionDir);
                    throw new IOException("Unable to create region directory: " + regionDir);
                }
            }

            // Write HRI to a file in case we need to recover hbase:meta
            regionFs.writeRegionInfoOnFilesystem(false);
        } else {
            if (LOG.isDebugEnabled())
                LOG.debug("Skipping creation of .regioninfo file for " + regionInfo);
        }
        return regionFs;
    }

    /**
     * Creates a directory for a filesystem and configuration object. Assumes the
     * user has already
     * checked for this directory existence.
     * 
     * @return the result of fs.mkdirs(). In case underlying fs throws an
     *         IOException, it checks
     *         whether the directory exists or not, and returns true if it exists.
     */
    private static boolean createDirOnFileSystem(FileSystem fs, Configuration conf, Path dir)
            throws IOException {
        int i = 0;
        IOException lastIOE = null;
        int hdfsClientRetriesNumber = conf.getInt("hdfs.client.retries.number", DEFAULT_HDFS_CLIENT_RETRIES_NUMBER);
        int baseSleepBeforeRetries = conf.getInt("hdfs.client.sleep.before.retries", DEFAULT_BASE_SLEEP_BEFORE_RETRIES);
        do {
            try {
                return fs.mkdirs(dir);
            } catch (IOException ioe) {
                lastIOE = ioe;
                if (fs.exists(dir))
                    return true; // directory is present
                try {
                    sleepBeforeRetry("Create Directory", i + 1, baseSleepBeforeRetries,
                            hdfsClientRetriesNumber);
                } catch (InterruptedException e) {
                    throw (InterruptedIOException) new InterruptedIOException().initCause(e);
                }
            }
        } while (++i <= hdfsClientRetriesNumber);

        throw new IOException("Exception in createDir", lastIOE);
    }

    /**
     * Write out an info file under the region directory. Useful recovering mangled
     * regions.
     * 
     * @param useTempDir indicate whether or not using the region .tmp dir for a
     *                   safer file creation.
     */
    private void writeRegionInfoOnFilesystem(boolean useTempDir) throws IOException {
        byte[] content = getRegionInfoFileContent(regionInfoForFs);
        writeRegionInfoOnFilesystem(content, useTempDir);
    }

    /**
     * Write out an info file under the region directory. Useful recovering mangled
     * regions.
     * 
     * @param regionInfoContent serialized version of the {@link RegionInfo}
     * @param useTempDir        indicate whether or not using the region .tmp dir
     *                          for a safer file
     *                          creation.
     */
    private void writeRegionInfoOnFilesystem(final byte[] regionInfoContent, final boolean useTempDir)
            throws IOException {
        Path regionInfoFile = new Path(getRegionDir(), REGION_INFO_FILE);
        if (useTempDir) {
            // Create in tmpDir and then move into place in case we crash after
            // create but before close. If we don't successfully close the file,
            // subsequent region reopens will fail the below because create is
            // registered in NN.

            // And then create the file
            Path tmpPath = new Path(getTempDir(), REGION_INFO_FILE);

            // If datanode crashes or if the RS goes down just before the close is called
            // while trying to
            // close the created regioninfo file in the .tmp directory then on next
            // creation we will be getting AlreadyCreatedException.
            // Hence delete and create the file if exists.
            if (CommonFSUtils.isExists(fs, tmpPath)) {
                CommonFSUtils.delete(fs, tmpPath, true);
            }

            // Write HRI to a file in case we need to recover hbase:meta
            writeRegionInfoFileContent(conf, fs, tmpPath, regionInfoContent);

            // Move the created file to the original path
            if (fs.exists(tmpPath) && !rename(tmpPath, regionInfoFile)) {
                throw new IOException("Unable to rename " + tmpPath + " to " + regionInfoFile);
            }
        } else {
            // Write HRI to a file in case we need to recover hbase:meta
            writeRegionInfoFileContent(conf, fs, regionInfoFile, regionInfoContent);
        }
    }

    // ===========================================================================
    // Create/Open/Delete Helpers
    // ===========================================================================

    /** Returns Content of the file we write out to the filesystem under a region */
    private static byte[] getRegionInfoFileContent(final RegionInfo hri) throws IOException {
        return RegionInfo.toDelimitedByteArray(hri);
    }

    /**
     * Create a {@link RegionInfo} from the serialized version on-disk.
     * 
     * @param fs        {@link FileSystem} that contains the Region Info file
     * @param regionDir {@link Path} to the Region Directory that contains the Info
     *                  file
     * @return An {@link RegionInfo} instance gotten from the Region Info file.
     * @throws IOException if an error occurred during file open/read operation.
     */
    public static RegionInfo loadRegionInfoFileContent(final FileSystem fs, final Path regionDir)
            throws IOException {
        FSDataInputStream in = fs.open(new Path(regionDir, REGION_INFO_FILE));
        try {
            return RegionInfo.parseFrom(in);
        } finally {
            in.close();
        }
    }

    /**
     * Write out an info file under the stored region directory. Useful recovering
     * mangled regions. If
     * the regionInfo already exists on-disk, then we fast exit.
     */
    void checkRegionInfoOnFilesystem() throws IOException {
        // Compose the content of the file so we can compare to length in filesystem. If
        // not same,
        // rewrite it (it may have been written in the old format using Writables
        // instead of pb). The
        // pb version is much shorter -- we write now w/o the toString version -- so
        // checking length
        // only should be sufficient. I don't want to read the file every time to check
        // if it pb
        // serialized.
        byte[] content = getRegionInfoFileContent(regionInfoForFs);

        // Verify if the region directory exists before opening a region. We need to do
        // this since if
        // the region directory doesn't exist we will re-create the region directory and
        // a new HRI
        // when HRegion.openHRegion() is called.
        try {
            fs.getFileStatus(getRegionDir());
        } catch (FileNotFoundException e) {
            LOG.warn(getRegionDir() + " doesn't exist for region: " + regionInfoForFs.getEncodedName()
                    + " on table " + regionInfo.getTable());
        }

        try {
            Path regionInfoFile = new Path(getRegionDir(), REGION_INFO_FILE);
            FileStatus status = fs.getFileStatus(regionInfoFile);
            if (status != null && status.getLen() == content.length) {
                // Then assume the content good and move on.
                // NOTE: that the length is not sufficient to define the the content matches.
                return;
            }

            LOG.info("Rewriting .regioninfo file at: " + regionInfoFile);
            if (!fs.delete(regionInfoFile, false)) {
                throw new IOException("Unable to remove existing " + regionInfoFile);
            }
        } catch (FileNotFoundException e) {
            LOG.warn(REGION_INFO_FILE + " file not found for region: " + regionInfoForFs.getEncodedName()
                    + " on table " + regionInfo.getTable());
        }

        // Write HRI to a file in case we need to recover hbase:meta
        writeRegionInfoOnFilesystem(content, true);
    }

    /**
     * Write the .regioninfo file on-disk.
     * <p/>
     * Overwrites if exists already.
     */
    private static void writeRegionInfoFileContent(final Configuration conf, final FileSystem fs,
            final Path regionInfoFile, final byte[] content) throws IOException {
        // First check to get the permissions
        FsPermission perms = CommonFSUtils.getFilePermissions(fs, conf, HConstants.DATA_FILE_UMASK_KEY);
        // Write the RegionInfo file content
        try (FSDataOutputStream out = FSUtils.create(conf, fs, regionInfoFile, perms, null)) {
            out.write(content);
        }
    }

    /**
     * Deletes a directory. Assumes the user has already checked for this directory
     * existence.
     * 
     * @return true if the directory is deleted.
     */
    boolean deleteDir(Path dir) throws IOException {
        IOException lastIOE = null;
        int i = 0;
        do {
            try {
                return fs.delete(dir, true);
            } catch (IOException ioe) {
                lastIOE = ioe;
                if (!fs.exists(dir))
                    return true;
                // dir is there, retry deleting after some time.
                try {
                    sleepBeforeRetry("Delete Directory", i + 1);
                } catch (InterruptedException e) {
                    throw (InterruptedIOException) new InterruptedIOException().initCause(e);
                }
            }
        } while (++i <= hdfsClientRetriesNumber);

        throw new IOException("Exception in DeleteDir", lastIOE);
    }

    /**
     * Renames a directory. Assumes the user has already checked for this directory
     * existence.
     * 
     * @return true if rename is successful.
     */
    boolean rename(Path srcpath, Path dstPath) throws IOException {
        IOException lastIOE = null;
        int i = 0;
        do {
            try {
                return fs.rename(srcpath, dstPath);
            } catch (IOException ioe) {
                lastIOE = ioe;
                if (!fs.exists(srcpath) && fs.exists(dstPath))
                    return true; // successful move
                // dir is not there, retry after some time.
                try {
                    sleepBeforeRetry("Rename Directory", i + 1);
                } catch (InterruptedException e) {
                    throw (InterruptedIOException) new InterruptedIOException().initCause(e);
                }
            }
        } while (++i <= hdfsClientRetriesNumber);

        throw new IOException("Exception in rename", lastIOE);
    }

    /**
     * sleeping logic; handles the interrupt exception.
     */
    private void sleepBeforeRetry(String msg, int sleepMultiplier) throws InterruptedException {
        sleepBeforeRetry(msg, sleepMultiplier, baseSleepBeforeRetries, hdfsClientRetriesNumber);
    }

    /**
     * sleeping logic for static methods; handles the interrupt exception. Keeping a
     * static version
     * for this to avoid re-looking for the integer values.
     */
    private static void sleepBeforeRetry(String msg, int sleepMultiplier, int baseSleepBeforeRetries,
            int hdfsClientRetriesNumber) throws InterruptedException {
        if (sleepMultiplier > hdfsClientRetriesNumber) {
            if (LOG.isDebugEnabled()) {
                LOG.debug(msg + ", retries exhausted");
            }
            return;
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug(msg + ", sleeping " + baseSleepBeforeRetries + " times " + sleepMultiplier);
        }
        Thread.sleep((long) baseSleepBeforeRetries * sleepMultiplier);
    }
}
