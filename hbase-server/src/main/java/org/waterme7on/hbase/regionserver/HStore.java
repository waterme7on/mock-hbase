package org.waterme7on.hbase.regionserver;

import org.waterme7on.hbase.regionserver.store.DefaultMemStore;
import org.waterme7on.hbase.regionserver.store.StoreEngine;
import org.waterme7on.hbase.regionserver.store.MemStore;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MemoryCompactionPolicy;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.regionserver.HStoreFile;
import org.apache.hadoop.hbase.regionserver.ScanInfo;
import org.apache.hadoop.hbase.regionserver.StoreConfigInformation;
import org.apache.hadoop.hbase.regionserver.StoreFileManager;
import org.apache.hadoop.hbase.regionserver.StoreFileScanner;
import org.apache.hadoop.hbase.regionserver.querymatcher.ScanQueryMatcher;
import org.apache.hadoop.hbase.util.ReflectionUtils;
import org.apache.hbase.thirdparty.com.google.common.collect.ImmutableCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.NavigableSet;
import java.util.OptionalDouble;
import java.util.OptionalLong;

public class HStore implements Store, StoreConfigInformation {

    final StoreEngine storeEngine;
    private static final Logger LOG = LoggerFactory.getLogger(HStore.class);

    public static final String BLOCKING_STOREFILES_KEY = "hbase.hstore.blockingStoreFiles";
    public static final int DEFAULT_BLOCKING_STOREFILE_COUNT = 16;
    public static final String COMPACTCHECKER_INTERVAL_MULTIPLIER_KEY = "hbase.server.compactchecker.interval.multiplier";
    public static final int DEFAULT_COMPACTCHECKER_INTERVAL_MULTIPLIER = 1000;
    public static final String MEMSTORE_CLASS_NAME = "hbase.regionserver.memstore.class";
    private ScanInfo scanInfo;
    private final Configuration conf;
    private final HRegion region;
    private final StoreContext storeContext;
    private final MemStore memstore;
    private long blockingFileCount;
    private int compactionCheckMultiplier;

    protected HStore(final HRegion region, final ColumnFamilyDescriptor family,
            final Configuration confParam, boolean warmup) throws IOException {
        this.conf = StoreUtils.createStoreConfiguration(confParam, region.getTableDescriptor(), family);

        this.region = region;
        this.storeContext = initializeStoreContext(family);
        this.memstore = getMemstore();
        // Assemble the store's home directory and Ensure it exists.
        region.getRegionFileSystem().createStoreDir(family.getNameAsString());
        this.storeEngine = new StoreEngine(this.conf, this, region.getCellComparator());
        this.blockingFileCount = conf.getInt(BLOCKING_STOREFILES_KEY, DEFAULT_BLOCKING_STOREFILE_COUNT);
        this.compactionCheckMultiplier = conf.getInt(COMPACTCHECKER_INTERVAL_MULTIPLIER_KEY,
                DEFAULT_COMPACTCHECKER_INTERVAL_MULTIPLIER);

        long timeToPurgeDeletes = Math.max(conf.getLong("hbase.hstore.time.to.purge.deletes", 0), 0);
        // Get TTL
        long ttl = determineTTLFromFamily(family);
        this.scanInfo = new ScanInfo(conf, family, ttl, timeToPurgeDeletes, region.getCellComparator());
    }

    private MemStore getMemstore() {

        MemStore ms = null;
        Class<? extends MemStore> memStoreClass = conf.getClass(MEMSTORE_CLASS_NAME, DefaultMemStore.class,
                MemStore.class);
        ms = ReflectionUtils.newInstance(memStoreClass,
                new Object[] { conf, getComparator() });
        return ms;
    }

    private StoreContext initializeStoreContext(ColumnFamilyDescriptor family) throws IOException {
        return new StoreContext.Builder().withBlockSize(family.getBlocksize())
                .withBloomType(family.getBloomFilterType())
                // .withCacheConfig(createCacheConf(family))
                .withCellComparator(region.getCellComparator()).withColumnFamilyDescriptor(family)
                .withRegionFileSystem(region.getRegionFileSystem())
                .withFamilyStoreDirectoryPath(
                        region.getRegionFileSystem().getStoreDir(family.getNameAsString()))
                .build();
    }

    /**
     * Close all the readers We don't need to worry about subsequent requests
     * because the Region holds
     * a write lock that will prevent any more reads or writes.
     * 
     * @return the {@link StoreFile StoreFiles} that were previously being used.
     * @throws IOException on failure
     */
    public ImmutableCollection<HStoreFile> close() throws IOException {
        return null;
    }

    public CellComparator getComparator() {
        return storeContext.getComparator();
    }

    public Collection<? extends StoreFile> getStorefiles() {
        return null;
    }

    public Collection<? extends StoreFile> getCompactedFiles() {
        return null;
    }

    public long timeOfOldestEdit() {
        return 0;
    }

    public FileSystem getFileSystem() {
        return null;
    }

    public boolean shouldPerformMajorCompaction() throws IOException {
        return false;
    }

    public boolean needsCompaction() {
        return false;
    }

    public int getCompactPriority() {
        return 0;
    }

    public boolean canSplit() {
        return false;
    }

    public boolean hasReferences() {
        return false;
    }

    public MemStoreSize getMemStoreSize() {
        return null;
    }

    public MemStoreSize getFlushableSize() {
        return null;
    }

    public MemStoreSize getSnapshotSize() {
        return null;
    }

    public ColumnFamilyDescriptor getColumnFamilyDescriptor() {
        return this.storeContext.getFamily();
    }

    public OptionalLong getMaxSequenceId() {
        return null;
    }

    public OptionalLong getMaxMemStoreTS() {
        return null;
    }

    public long getLastCompactSize() {
        return 0;
    }

    public long getSize() {
        return 0;
    }

    public int getStorefilesCount() {
        return 0;
    }

    public int getCompactedFilesCount() {
        return 0;
    }

    public OptionalLong getMaxStoreFileAge() {
        return null;
    }

    public OptionalLong getMinStoreFileAge() {
        return null;
    }

    public OptionalDouble getAvgStoreFileAge() {
        return null;
    }

    public long getNumReferenceFiles() {
        return 0;
    }

    public long getNumHFiles() {
        return 0;
    }

    public long getStoreSizeUncompressed() {
        return 0;
    }

    public long getStorefilesSize() {
        return 0;
    }

    public long getHFilesSize() {
        return 0;
    }

    public long getStorefilesRootLevelIndexSize() {
        return 0;
    }

    public long getTotalStaticIndexSize() {
        return 0;
    }

    public long getTotalStaticBloomSize() {
        return 0;
    }

    public RegionInfo getRegionInfo() {
        return region.getRegionInfo();
    }

    public boolean areWritesEnabled() {
        return false;
    }

    public long getSmallestReadPoint() {
        return 0;
    }

    public String getColumnFamilyName() {
        return this.storeContext.getFamily().getNameAsString();

    }

    public TableName getTableName() {
        return this.getRegionInfo().getTable();
    }

    public long getFlushedCellsCount() {
        return 0;
    }

    public long getFlushedCellsSize() {
        return 0;
    }

    public long getFlushedOutputFileSize() {
        return 0;
    }

    public long getCompactedCellsCount() {
        return 0;
    }

    public long getCompactedCellsSize() {
        return 0;
    }

    public long getMajorCompactedCellsCount() {
        return 0;
    }

    public long getMajorCompactedCellsSize() {
        return 0;
    }

    public boolean hasTooManyStoreFiles() {
        return false;
    }

    public void refreshStoreFiles() throws IOException {

    }

    public double getCompactionPressure() {
        return 0;
    }

    public boolean isPrimaryReplicaStore() {
        return false;
    }

    public boolean isSloppyMemStore() {
        return false;
    }

    public int getCurrentParallelPutCount() {
        return 0;
    }

    public long getMemstoreOnlyRowReadsCount() {
        return 0;
    }

    public long getMixedRowReadsCount() {
        return 0;
    }

    public Configuration getReadOnlyConfiguration() {
        return null;
    }

    public long getBloomFilterRequestsCount() {
        return 0;
    }

    public long getBloomFilterNegativeResultsCount() {
        return 0;
    }

    public long getBloomFilterEligibleRequestsCount() {
        return 0;
    }

    public HRegion getHRegion() {
        return this.region;
    }

    public StoreContext getStoreContext() {
        return storeContext;
    }

    /**
     * Return a scanner for both the memstore and the HStore files. Assumes we are
     * not in a
     * compaction.
     * 
     * @param scan       Scan to apply when scanning the stores
     * @param targetCols columns to scan
     * @return a scanner over the current key values
     * @throws IOException on failure
     */
    public KeyValueScanner getScanner(Scan scan, final NavigableSet<byte[]> targetCols, long readPt)
            throws IOException {
        storeEngine.readLock();
        try {
            ScanInfo scanInfo = getScanInfo();
            return createScanner(scan, scanInfo, targetCols, readPt);
        } finally {
            storeEngine.readUnlock();
        }
    }

    protected KeyValueScanner createScanner(Scan scan, ScanInfo scanInfo,
            NavigableSet<byte[]> targetCols, long readPt) throws IOException {
        // return scan.isReversed()
        // ? new ReversedStoreScanner(this, scanInfo, scan, targetCols, readPt)
        return (KeyValueScanner) new StoreScanner(this, scanInfo, scan, targetCols, readPt);
    }

    // protected KeyValueScanner createFileStoreScanner(Scan scan, ScanInfo
    // scanInfo,
    // NavigableSet<byte[]> targetCols, long readPt) throws IOException {
    // // return scan.isReversed()
    // // ? new ReversedStoreScanner(this, scanInfo, scan, targetCols, readPt)
    // return (KeyValueScanner) new StoreScanner(this, scanInfo, scan, targetCols,
    // readPt, true);
    // }

    public ScanInfo getScanInfo() {
        return scanInfo;
    }

    /**
     * Adds or replaces the specified KeyValues.
     * <p>
     * For each KeyValue specified, if a cell with the same row, family, and
     * qualifier exists in
     * MemStore, it will be replaced. Otherwise, it will just be inserted to
     * MemStore.
     * <p>
     * This operation is atomic on each KeyValue (row/family/qualifier) but not
     * necessarily atomic
     * across all of them.
     * 
     * @param readpoint readpoint below which we can safely remove duplicate KVs
     */
    public void upsert(Iterable<Cell> cells, long readpoint, MemStoreSizing memstoreSizing)
            throws IOException {
        LOG.debug("upsert: {}", cells.toString());
        this.storeEngine.readLock();
        try {
            this.memstore.upsert(cells, readpoint, memstoreSizing);
        } finally {
            this.storeEngine.readUnlock();
        }
    }

    public static long determineTTLFromFamily(final ColumnFamilyDescriptor family) {
        // HCD.getTimeToLive returns ttl in seconds. Convert to milliseconds.
        long ttl = family.getTimeToLive();
        if (ttl == HConstants.FOREVER) {
            // Default is unlimited ttl.
            ttl = Long.MAX_VALUE;
        } else if (ttl == -1) {
            ttl = Long.MAX_VALUE;
        } else {
            // Second -> ms adjust for user data
            ttl *= 1000;
        }
        return ttl;
    }

    /**
     * Adds the specified value to the memstore
     */
    public void add(final Iterable<Cell> cells, MemStoreSizing memstoreSizing) {
        LOG.debug("add: {}", cells.toString());
        storeEngine.readLock();
        try {
            // if (this.currentParallelPutCount.getAndIncrement() >
            // this.parallelPutCountPrintThreshold) {
            // LOG.trace("tableName={}, encodedName={}, columnFamilyName={} is too busy!",
            // this.getTableName(), this.getRegionInfo().getEncodedName(),
            // this.getColumnFamilyName());
            // }
            LOG.debug("HStore.add before {}", this.memstore.toString());
            this.memstore.add(cells, memstoreSizing);
            LOG.debug("HStore.add after  {}", this.memstore.toString());
        } finally {
            storeEngine.readUnlock();
            // currentParallelPutCount.decrementAndGet();
        }
    }

    public static org.apache.hadoop.hbase.regionserver.MemStoreSizing getOriginMemStoreSizing(
            MemStoreSizing memstoreSizing) {
        org.apache.hadoop.hbase.regionserver.MemStoreSizing memstoreSizing1;
        memstoreSizing1 = org.apache.hadoop.hbase.regionserver.MemStoreSizing.DUD;
        memstoreSizing1.incMemStoreSize(memstoreSizing.getDataSize() - memstoreSizing1.getDataSize(),
                memstoreSizing.getHeapSize() - memstoreSizing1
                        .getHeapSize(),
                memstoreSizing.getOffHeapSize() - memstoreSizing1.getOffHeapSize(),
                memstoreSizing.getCellsCount() - memstoreSizing1.getCellsCount());
        return memstoreSizing1;
    }

    public List<org.apache.hadoop.hbase.regionserver.KeyValueScanner> getFileStoreSacnners(boolean cacheBlocks,
            boolean usePread,
            boolean isCompaction, ScanQueryMatcher matcher, byte[] startRow, boolean includeStartRow,
            byte[] stopRow, boolean includeStopRow, long readPt) throws IOException {
        Collection<HStoreFile> storeFilesToScan;
        this.storeEngine.readLock();
        try {
            storeFilesToScan = this.storeEngine.getStoreFileManager().getFilesForScan(startRow,
                    includeStartRow, stopRow, includeStopRow);
        } finally {
            this.storeEngine.readUnlock();
        }
        List<StoreFileScanner> sfScanners = null;
        // new <org.apache.hadoop.hbase.regionserver.KeyValueScanner>();
        try {
            // First the store file scanners

            // TODO this used to get the store files in descending order,
            // but now we get them in ascending order, which I think is
            // actually more correct, since memstore get put at the end.
            sfScanners = StoreFileScanner.getScannersForStoreFiles(
                    storeFilesToScan, cacheBlocks, usePread, isCompaction, false, matcher,
                    readPt);
            List<org.apache.hadoop.hbase.regionserver.KeyValueScanner> memStoreScanners = new ArrayList();
            memStoreScanners.addAll(sfScanners);
            return memStoreScanners;
        } catch (Throwable t) {
            clearAndCloseStoreFileScanner(sfScanners);
            throw t instanceof IOException ? (IOException) t : new IOException(t);
        }
    }

    public List<KeyValueScanner> getScanners(boolean cacheBlocks, boolean usePread,
            boolean isCompaction, ScanQueryMatcher matcher, byte[] startRow, boolean includeStartRow,
            byte[] stopRow, boolean includeStopRow, long readPt) throws IOException {
        // Collection<HStoreFile> storeFilesToScan;
        List<KeyValueScanner> memStoreScanners;
        this.storeEngine.readLock();
        try {
            // storeFilesToScan =
            // this.storeEngine.getStoreFileManager().getFilesForScan(startRow,
            // includeStartRow, stopRow, includeStopRow);
            memStoreScanners = this.memstore.getScanners(readPt);
        } finally {
            this.storeEngine.readUnlock();
        }

        try {
            // First the store file scanners

            // TODO this used to get the store files in descending order,
            // but now we get them in ascending order, which I think is
            // actually more correct, since memstore get put at the end.
            // List<StoreFileScanner> sfScanners =
            // StoreFileScanner.getScannersForStoreFiles(
            // storeFilesToScan, cacheBlocks, usePread, isCompaction, false, matcher,
            // readPt);
            List<KeyValueScanner> scanners = new ArrayList<>();
            // scanners.addAll(sfScanners);
            // Then the memstore scanners
            scanners.addAll(memStoreScanners);
            return scanners;
        } catch (Throwable t) {
            clearAndClose(memStoreScanners);
            throw t instanceof IOException ? (IOException) t : new IOException(t);
        }
    }

    private static void clearAndCloseStoreFileScanner(
            List<StoreFileScanner> sfScanners) {
        if (sfScanners == null) {
            return;
        }
        for (org.apache.hadoop.hbase.regionserver.KeyValueScanner s : sfScanners) {
            s.close();
        }
        sfScanners.clear();
    }

    private static void clearAndClose(List<KeyValueScanner> scanners) {
        if (scanners == null) {
            return;
        }
        for (KeyValueScanner s : scanners) {
            s.close();
        }
        scanners.clear();
    }

    public List<KeyValueScanner> recreateScanners(List<KeyValueScanner> scannersToClose, boolean cacheBlocks, boolean b,
            boolean c, ScanQueryMatcher matcher, byte[] startRow, boolean includeStartRow, byte[] stopRow,
            boolean includeStopRow, long readPt, boolean d) {
        // TODO
        return null;
    }

    @Override
    public long getMemStoreFlushSize() {
        return this.region.memstoreFlushSize;
    }

    @Override
    public long getStoreFileTtl() {
        // TTL only applies if there's no MIN_VERSIONs setting on the column.
        return (this.scanInfo.getMinVersions() == 0) ? this.scanInfo.getTtl() : Long.MAX_VALUE;

    }

    @Override
    public long getCompactionCheckMultiplier() {
        return this.compactionCheckMultiplier;
    }

    @Override
    public long getBlockingFileCount() {
        return blockingFileCount;
    }
}
