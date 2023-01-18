package org.waterme7on.hbase.util;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.ClusterId;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.FSProtos;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.primitives.Ints;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.FileSystemVersionException;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.StringUtils;

public class FSUtils {
    private static final Logger LOG = LoggerFactory.getLogger(FSUtils.class);

    /**
     * Returns the value of the unique cluster ID stored for this HBase instance.
     * 
     * @param fs      the root directory FileSystem
     * @param rootdir the path to the HBase root directory
     * @return the unique cluster identifier
     * @throws IOException if reading the cluster ID file fails
     */
    public static ClusterId getClusterId(FileSystem fs, Path rootdir) throws IOException {
        Path idPath = new Path(rootdir, HConstants.CLUSTER_ID_FILE_NAME);
        ClusterId clusterId = null;
        FileStatus status = fs.exists(idPath) ? fs.getFileStatus(idPath) : null;
        if (status != null) {
            int len = Ints.checkedCast(status.getLen());
            byte[] content = new byte[len];
            FSDataInputStream in = fs.open(idPath);
            try {
                in.readFully(content);
            } catch (EOFException eof) {
                LOG.warn("Cluster ID file {} is empty", idPath);
            } finally {
                in.close();
            }
            try {
                clusterId = ClusterId.parseFrom(content);
            } catch (DeserializationException e) {
                throw new IOException("content=" + Bytes.toString(content), e);
            }
            // If not pb'd, make it so.
            if (!ProtobufUtil.isPBMagicPrefix(content)) {
                String cid = null;
                in = fs.open(idPath);
                try {
                    cid = in.readUTF();
                    clusterId = new ClusterId(cid);
                } catch (EOFException eof) {
                    LOG.warn("Cluster ID file {} is empty", idPath);
                } finally {
                    in.close();
                }
                rewriteAsPb(fs, rootdir, idPath, clusterId);
            }
            return clusterId;
        } else {
            LOG.warn("Cluster ID file does not exist at {}", idPath);
        }
        return clusterId;
    }

    /**
     *   */
    private static void rewriteAsPb(final FileSystem fs, final Path rootdir, final Path p,
            final ClusterId cid) throws IOException {
        // Rewrite the file as pb. Move aside the old one first, write new
        // then delete the moved-aside file.
        Path movedAsideName = new Path(p + "." + EnvironmentEdgeManager.currentTime());
        if (!fs.rename(p, movedAsideName))
            throw new IOException("Failed rename of " + p);
        setClusterId(fs, rootdir, cid, 100);
        if (!fs.delete(movedAsideName, false)) {
            throw new IOException("Failed delete of " + movedAsideName);
        }
        LOG.debug("Rewrote the hbase.id file as pb");
    }

    /**
     * Writes a new unique identifier for this cluster to the "hbase.id" file in the
     * HBase root
     * directory
     * 
     * @param fs        the root directory FileSystem
     * @param rootdir   the path to the HBase root directory
     * @param clusterId the unique identifier to store
     * @param wait      how long (in milliseconds) to wait between retries
     * @throws IOException if writing to the FileSystem fails and no wait value
     */
    public static void setClusterId(FileSystem fs, Path rootdir, ClusterId clusterId, int wait)
            throws IOException {
        while (true) {
            try {
                Path idFile = new Path(rootdir, HConstants.CLUSTER_ID_FILE_NAME);
                Path tempIdFile = new Path(rootdir,
                        HConstants.HBASE_TEMP_DIRECTORY + Path.SEPARATOR + HConstants.CLUSTER_ID_FILE_NAME);
                // Write the id file to a temporary location
                FSDataOutputStream s = fs.create(tempIdFile);
                try {
                    s.write(clusterId.toByteArray());
                    s.close();
                    s = null;
                    // Move the temporary file to its normal location. Throw an IOE if
                    // the rename failed
                    if (!fs.rename(tempIdFile, idFile)) {
                        throw new IOException("Unable to move temp version file to " + idFile);
                    }
                } finally {
                    // Attempt to close the stream if still open on the way out
                    try {
                        if (s != null)
                            s.close();
                    } catch (IOException ignore) {
                    }
                }
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Created cluster ID file at " + idFile.toString() + " with ID: " + clusterId);
                }
                return;
            } catch (IOException ioe) {
                if (wait > 0) {
                    LOG.warn("Unable to create cluster ID file in " + rootdir.toString() + ", retrying in "
                            + wait + "msec: " + StringUtils.stringifyException(ioe));
                    try {
                        Thread.sleep(wait);
                    } catch (InterruptedException e) {
                        throw (InterruptedIOException) new InterruptedIOException().initCause(e);
                    }
                } else {
                    throw ioe;
                }
            }
        }
    }

    /**
     * If DFS, check safe mode and if so, wait until we clear it.
     * 
     * @param conf configuration
     * @param wait Sleep between retries
     * @throws IOException e
     */
    public static void waitOnSafeMode(final Configuration conf, final long wait) throws IOException {
        return;
    }

    /**
     * Sets version of file system
     * 
     * @param fs      filesystem object
     * @param rootdir hbase root
     * @param wait    time to wait for retry
     * @param retries number of times to retry before failing
     * @throws IOException e
     */
    public static void setVersion(FileSystem fs, Path rootdir, int wait, int retries)
            throws IOException {
        setVersion(fs, rootdir, HConstants.FILE_SYSTEM_VERSION, wait, retries);
    }

    /**
     * Sets version of file system
     * 
     * @param fs      filesystem object
     * @param rootdir hbase root directory
     * @param version version to set
     * @param wait    time to wait for retry
     * @param retries number of times to retry before throwing an IOException
     * @throws IOException e
     */
    public static void setVersion(FileSystem fs, Path rootdir, String version, int wait, int retries)
            throws IOException {
        Path versionFile = new Path(rootdir, HConstants.VERSION_FILE_NAME);
        Path tempVersionFile = new Path(rootdir,
                HConstants.HBASE_TEMP_DIRECTORY + Path.SEPARATOR + HConstants.VERSION_FILE_NAME);
        while (true) {
            try {
                // Write the version to a temporary file
                FSDataOutputStream s = fs.create(tempVersionFile);
                try {
                    s.write(toVersionByteArray(version));
                    s.close();
                    s = null;
                    // Move the temp version file to its normal location. Returns false
                    // if the rename failed. Throw an IOE in that case.
                    if (!fs.rename(tempVersionFile, versionFile)) {
                        throw new IOException("Unable to move temp version file to " + versionFile);
                    }
                } finally {
                    // Cleaning up the temporary if the rename failed would be trying
                    // too hard. We'll unconditionally create it again the next time
                    // through anyway, files are overwritten by default by create().

                    // Attempt to close the stream on the way out if it is still open.
                    try {
                        if (s != null)
                            s.close();
                    } catch (IOException ignore) {
                    }
                }
                LOG.info("Created version file at " + rootdir.toString() + " with version=" + version);
                return;
            } catch (IOException e) {
                if (retries > 0) {
                    LOG.debug("Unable to create version file at " + rootdir.toString() + ", retrying", e);
                    fs.delete(versionFile, false);
                    try {
                        if (wait > 0) {
                            Thread.sleep(wait);
                        }
                    } catch (InterruptedException ie) {
                        throw (InterruptedIOException) new InterruptedIOException().initCause(ie);
                    }
                    retries--;
                } else {
                    throw e;
                }
            }
        }
    }

    public static List<Path> getTableDirs(final FileSystem fs, final Path rootdir)
            throws IOException {
        List<Path> tableDirs = new ArrayList<>();
        Path baseNamespaceDir = new Path(rootdir, HConstants.BASE_NAMESPACE_DIR);
        if (fs.exists(baseNamespaceDir)) {
            for (FileStatus status : fs.globStatus(new Path(baseNamespaceDir, "*"))) {
                tableDirs.addAll(FSUtils.getLocalTableDirs(fs, status.getPath()));
            }
        }
        return tableDirs;
    }

    /**
     * @return All the table directories under <code>rootdir</code>. Ignore non
     *         table hbase folders
     *         such as .logs, .oldlogs, .corrupt folders.
     */
    public static List<Path> getLocalTableDirs(final FileSystem fs, final Path rootdir)
            throws IOException {
        // presumes any directory under hbase.rootdir is a table
        FileStatus[] dirs = fs.listStatus(rootdir, new UserTableDirFilter(fs));
        List<Path> tabledirs = new ArrayList<>(dirs.length);
        for (FileStatus dir : dirs) {
            tabledirs.add(dir.getPath());
        }
        return tabledirs;
    }

    /**
     * Directory filter that doesn't include any of the directories in the specified
     * blacklist
     */
    public static class BlackListDirFilter extends AbstractFileStatusFilter {
        private final FileSystem fs;
        private List<String> blacklist;

        /**
         * Create a filter on the givem filesystem with the specified blacklist
         * 
         * @param fs                     filesystem to filter
         * @param directoryNameBlackList list of the names of the directories to filter.
         *                               If
         *                               <tt>null</tt>, all directories are returned
         */
        @SuppressWarnings("unchecked")
        public BlackListDirFilter(final FileSystem fs, final List<String> directoryNameBlackList) {
            this.fs = fs;
            blacklist = (List<String>) (directoryNameBlackList == null
                    ? Collections.emptyList()
                    : directoryNameBlackList);
        }

        @Override
        protected boolean accept(Path p, Boolean isDir) {
            if (!isValidName(p.getName())) {
                return false;
            }

            try {
                return isDirectory(fs, isDir, p);
            } catch (IOException e) {
                LOG.warn("An error occurred while verifying if [{}] is a valid directory."
                        + " Returning 'not valid' and continuing.", p, e);
                return false;
            }
        }

        protected boolean isValidName(final String name) {
            return !blacklist.contains(name);
        }
    }

    /**
     * A {@link PathFilter} that only allows directories.
     */
    public static class DirFilter extends BlackListDirFilter {

        public DirFilter(FileSystem fs) {
            super(fs, null);
        }
    }

    /**
     * A {@link PathFilter} that returns usertable directories. To get all
     * directories use the
     * {@link BlackListDirFilter} with a <tt>null</tt> blacklist
     */
    public static class UserTableDirFilter extends BlackListDirFilter {
        public UserTableDirFilter(FileSystem fs) {
            super(fs, HConstants.HBASE_NON_TABLE_DIRS);
        }

        @Override
        protected boolean isValidName(final String name) {
            if (!super.isValidName(name))
                return false;

            try {
                TableName.isLegalTableQualifierName(Bytes.toBytes(name));
            } catch (IllegalArgumentException e) {
                LOG.info("Invalid table name: {}", name);
                return false;
            }
            return true;
        }
    }

    /**
     * Create the content to write into the ${HBASE_ROOTDIR}/hbase.version file.
     * 
     * @param version Version to persist
     * @return Serialized protobuf with <code>version</code> content and a bit of pb
     *         magic for a
     *         prefix.
     */
    static byte[] toVersionByteArray(final String version) {
        FSProtos.HBaseVersionFileContent.Builder builder = FSProtos.HBaseVersionFileContent.newBuilder();
        return ProtobufUtil.prependPBMagic(builder.setVersion(version).build().toByteArray());
    }

    /**
     * Verifies current version of file system
     * 
     * @param fs      file system
     * @param rootdir root directory of HBase installation
     * @param message if true, issues a message on System.out
     * @throws IOException              if the version file cannot be opened
     * @throws DeserializationException if the contents of the version file cannot
     *                                  be parsed
     */
    public static void checkVersion(FileSystem fs, Path rootdir, boolean message)
            throws IOException, DeserializationException {
        checkVersion(fs, rootdir, message, 0, HConstants.DEFAULT_VERSION_FILE_WRITE_ATTEMPTS);
    }

    /**
     * Verifies current version of file system
     * 
     * @param fs      file system
     * @param rootdir root directory of HBase installation
     * @param message if true, issues a message on System.out
     * @param wait    wait interval
     * @param retries number of times to retry
     * @throws IOException              if the version file cannot be opened
     * @throws DeserializationException if the contents of the version file cannot
     *                                  be parsed
     */
    public static void checkVersion(FileSystem fs, Path rootdir, boolean message, int wait,
            int retries) throws IOException, DeserializationException {
        String version = getVersion(fs, rootdir);
        String msg;
        if (version == null) {
            if (!metaRegionExists(fs, rootdir)) {
                // rootDir is empty (no version file and no root region)
                // just create new version file (HBASE-1195)
                setVersion(fs, rootdir, wait, retries);
                return;
            } else {
                msg = "hbase.version file is missing. Is your hbase.rootdir valid? "
                        + "You can restore hbase.version file by running 'HBCK2 filesystem -fix'. "
                        + "See https://github.com/apache/hbase-operator-tools/tree/master/hbase-hbck2";
            }
        } else if (version.compareTo(HConstants.FILE_SYSTEM_VERSION) == 0) {
            return;
        } else {
            msg = "HBase file layout needs to be upgraded. Current filesystem version is " + version
                    + " but software requires version " + HConstants.FILE_SYSTEM_VERSION
                    + ". Consult http://hbase.apache.org/book.html for further information about "
                    + "upgrading HBase.";
        }

        // version is deprecated require migration
        // Output on stdout so user sees it in terminal.
        if (message) {
            System.out.println("WARNING! " + msg);
        }
        throw new FileSystemVersionException(msg);
    }

    /**
     * Verifies current version of file system
     * 
     * @param fs      filesystem object
     * @param rootdir root hbase directory
     * @return null if no version file exists, version string otherwise
     * @throws IOException              if the version file fails to open
     * @throws DeserializationException if the version data cannot be translated
     *                                  into a version
     */
    public static String getVersion(FileSystem fs, Path rootdir)
            throws IOException, DeserializationException {
        final Path versionFile = new Path(rootdir, HConstants.VERSION_FILE_NAME);
        FileStatus[] status = null;
        try {
            // hadoop 2.0 throws FNFE if directory does not exist.
            // hadoop 1.0 returns null if directory does not exist.
            status = fs.listStatus(versionFile);
        } catch (FileNotFoundException fnfe) {
            return null;
        }
        if (ArrayUtils.getLength(status) == 0) {
            return null;
        }
        String version = null;
        byte[] content = new byte[(int) status[0].getLen()];
        FSDataInputStream s = fs.open(versionFile);
        try {
            IOUtils.readFully(s, content, 0, content.length);
            if (ProtobufUtil.isPBMagicPrefix(content)) {
                version = parseVersionFrom(content);
            } else {
                // Presume it pre-pb format.
                try (DataInputStream dis = new DataInputStream(new ByteArrayInputStream(content))) {
                    version = dis.readUTF();
                }
            }
        } catch (EOFException eof) {
            LOG.warn("Version file was empty, odd, will try to set it.");
        } finally {
            s.close();
        }
        return version;
    }

    /**
     * Parse the content of the ${HBASE_ROOTDIR}/hbase.version file.
     * 
     * @param bytes The byte content of the hbase.version file
     * @return The version found in the file as a String
     * @throws DeserializationException if the version data cannot be translated
     *                                  into a version
     */
    static String parseVersionFrom(final byte[] bytes) throws DeserializationException {
        ProtobufUtil.expectPBMagicPrefix(bytes);
        int pblen = ProtobufUtil.lengthOfPBMagic();
        FSProtos.HBaseVersionFileContent.Builder builder = FSProtos.HBaseVersionFileContent.newBuilder();
        try {
            ProtobufUtil.mergeFrom(builder, bytes, pblen, bytes.length - pblen);
            return builder.getVersion();
        } catch (IOException e) {
            // Convert
            throw new DeserializationException(e);
        }
    }

    /**
     * Checks if meta region exists
     * 
     * @param fs      file system
     * @param rootDir root directory of HBase installation
     * @return true if exists
     */
    public static boolean metaRegionExists(FileSystem fs, Path rootDir) throws IOException {
        Path metaRegionDir = getRegionDirFromRootDir(rootDir, RegionInfoBuilder.FIRST_META_REGIONINFO);
        return fs.exists(metaRegionDir);
    }

    public static Path getRegionDirFromRootDir(Path rootDir, RegionInfo region) {
        return getRegionDirFromTableDir(CommonFSUtils.getTableDir(rootDir, region.getTable()), region);
    }

    public static Path getRegionDirFromTableDir(Path tableDir, RegionInfo region) {
        return getRegionDirFromTableDir(tableDir,
                ServerRegionReplicaUtil.getRegionInfoForFs(region).getEncodedName());
    }

    public static Path getRegionDirFromTableDir(Path tableDir, String encodedRegionName) {
        return new Path(tableDir, encodedRegionName);
    }

    /**
     * Checks that a cluster ID file exists in the HBase root directory
     * 
     * @param fs      the root directory FileSystem
     * @param rootdir the HBase root directory in HDFS
     * @param wait    how long to wait between retries
     * @return <code>true</code> if the file exists, otherwise <code>false</code>
     * @throws IOException if checking the FileSystem fails
     */
    public static boolean checkClusterIdExists(FileSystem fs, Path rootdir, long wait)
            throws IOException {
        while (true) {
            try {
                Path filePath = new Path(rootdir, HConstants.CLUSTER_ID_FILE_NAME);
                return fs.exists(filePath);
            } catch (IOException ioe) {
                if (wait > 0L) {
                    LOG.warn("Unable to check cluster ID file in {}, retrying in {}ms", rootdir, wait, ioe);
                    try {
                        Thread.sleep(wait);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw (InterruptedIOException) new InterruptedIOException().initCause(e);
                    }
                } else {
                    throw ioe;
                }
            }
        }
    }
}