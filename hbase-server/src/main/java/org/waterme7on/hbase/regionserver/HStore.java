package org.waterme7on.hbase.regionserver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.MemoryCompactionPolicy;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.regionserver.CompactingMemStore;
import org.apache.hadoop.hbase.regionserver.DefaultMemStore;
import org.apache.hadoop.hbase.regionserver.HStoreFile;
import org.apache.hadoop.hbase.regionserver.MemStore;
import org.apache.hadoop.hbase.util.ReflectionUtils;
import org.apache.hbase.thirdparty.com.google.common.collect.ImmutableCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.waterme7on.hbase.regionserver.store.StoreEngine;
import java.io.IOException;
import java.util.Collection;
import java.util.OptionalDouble;
import java.util.OptionalLong;

public class HStore implements Store {

    final StoreEngine storeEngine;
    private static final Logger LOG = LoggerFactory.getLogger(HStore.class);

    public static final String MEMSTORE_CLASS_NAME = "hbase.regionserver.memstore.class";

    private final Configuration conf;
    private final HRegion region;
    private final StoreContext storeContext;
    private final MemStore memstore;

    protected HStore(final HRegion region, final ColumnFamilyDescriptor family,
            final Configuration confParam, boolean warmup) throws IOException {
        this.conf = StoreUtils.createStoreConfiguration(confParam, region.getTableDescriptor(), family);

        this.region = region;
        this.storeContext = initializeStoreContext(family);
        this.memstore = getMemstore();
        // Assemble the store's home directory and Ensure it exists.
        region.getRegionFileSystem().createStoreDir(family.getNameAsString());
        this.storeEngine = new StoreEngine(this.conf, this, region.getCellComparator());

    }

    private MemStore getMemstore() {

        MemStore ms = null;
        // Check if in-memory-compaction configured. Note MemoryCompactionPolicy is an
        // enum!
        MemoryCompactionPolicy inMemoryCompaction = null;
        if (this.getTableName().isSystemTable()) {
            inMemoryCompaction = MemoryCompactionPolicy
                    .valueOf(conf.get("hbase.systemtables.compacting.memstore.type", "NONE").toUpperCase());
        } else {
            inMemoryCompaction = getColumnFamilyDescriptor().getInMemoryCompaction();
        }
        if (inMemoryCompaction == null) {
            inMemoryCompaction = MemoryCompactionPolicy
                    .valueOf(conf.get(CompactingMemStore.COMPACTING_MEMSTORE_TYPE_KEY,
                            CompactingMemStore.COMPACTING_MEMSTORE_TYPE_DEFAULT).toUpperCase());
        }

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
            this.memstore.upsert(cells, readpoint, getOriginMemStoreSizing(memstoreSizing));
        } finally {
            this.storeEngine.readUnlock();
        }
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
            this.memstore.add(cells, getOriginMemStoreSizing(memstoreSizing));
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
}
