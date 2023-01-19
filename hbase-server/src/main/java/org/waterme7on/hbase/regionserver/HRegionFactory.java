package org.waterme7on.hbase.regionserver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.waterme7on.hbase.Server;
import org.waterme7on.hbase.wal.WALFactory;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;

import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;

/*
* The factory class for creating a region
 * */
public final class HRegionFactory {
    private static final String FLUSH_SIZE_KEY = "hbase.master.store.region.flush.size";
    private static final long DEFAULT_FLUSH_SIZE = 1024 * 1024 * 128L;
    private static final String FLUSH_INTERVAL_MS_KEY = "hbase.master.store.region.flush.interval.ms";
    public static final String TRACKER_IMPL = "hbase.master.store.region.file-tracker.impl";
    public static final TableName TABLE_NAME = TableName.valueOf("master:store");
    public static final byte[] PROC_FAMILY = Bytes.toBytes("proc");
    public static final byte[] REGION_SERVER_FAMILY = Bytes.toBytes("rs");
    public static final String MASTER_STORE_DIR = "MasterData";

    private static final TableDescriptor TABLE_DESC = TableDescriptorBuilder.newBuilder(TABLE_NAME)
            .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(HConstants.CATALOG_FAMILY)
                    .setMaxVersions(HConstants.DEFAULT_HBASE_META_VERSIONS).setInMemory(true)
                    .setBlocksize(HConstants.DEFAULT_HBASE_META_BLOCK_SIZE).setBloomFilterType(BloomType.ROWCOL)
                    .setDataBlockEncoding(DataBlockEncoding.ROW_INDEX_V1).build())
            .setColumnFamily(ColumnFamilyDescriptorBuilder.of(PROC_FAMILY))
            .setColumnFamily(ColumnFamilyDescriptorBuilder.of(REGION_SERVER_FAMILY)).build();

    // default to flush every 15 minutes, for safety
    private static final long DEFAULT_FLUSH_INTERVAL_MS = TimeUnit.MINUTES.toMillis(15);

    public static MasterRegion create(Server server) throws IOException {
        // Configuration conf = server.getConfiguration();

        return new MasterRegion(server);
    }

    public static class MasterRegion {
        private HRegion region;

        public MasterRegion(Server server) throws IOException {
            Configuration baseConf = server.getConfiguration();
            FileSystem fs = CommonFSUtils.getRootDirFileSystem(baseConf);
            FileSystem walFs = CommonFSUtils.getWALFileSystem(baseConf);
            Path globalRootDir = CommonFSUtils.getRootDir(baseConf);
            Path globalWALRootDir = CommonFSUtils.getWALRootDir(baseConf);
            Path rootDir = new Path(globalRootDir, MASTER_STORE_DIR);
            Path walRootDir = new Path(globalWALRootDir, MASTER_STORE_DIR);
            WALFactory wal = null;
            this.region = bootstrap(baseConf, TABLE_DESC, fs, rootDir, walFs, walRootDir, wal, server.getServerName(),
                    false);
        }

        private static HRegion bootstrap(Configuration conf, TableDescriptor td, FileSystem fs,
                Path rootDir, FileSystem walFs, Path walRootDir, WALFactory walFactory,
                ServerName serverName, boolean touchInitializingFlag)
                throws IOException {
            // TODO
            return null;
        }
    }
}
