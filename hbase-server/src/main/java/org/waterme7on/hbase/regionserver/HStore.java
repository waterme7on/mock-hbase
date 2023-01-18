package org.waterme7on.hbase.regionserver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.RegionInfo;

import java.io.IOException;
import java.util.Collection;
import java.util.OptionalDouble;
import java.util.OptionalLong;

public class HStore  implements Store {
    public CellComparator getComparator() {
        return null;
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
        return null;
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
        return null;
    }

    public boolean areWritesEnabled() {
        return false;
    }

    public long getSmallestReadPoint() {
        return 0;
    }

    public String getColumnFamilyName() {
        return null;
    }

    public TableName getTableName() {
        return null;
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
}
