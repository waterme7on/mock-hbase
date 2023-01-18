package org.waterme7on.hbase.regionserver;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.CheckAndMutate;
import org.apache.hadoop.hbase.client.CheckAndMutateResult;
import org.apache.hadoop.hbase.client.CompactionState;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.filter.ByteArrayComparable;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.io.TimeRange;
import org.apache.hadoop.hbase.util.Bytes;

/*
*  * <pre>
* hbase
*   |
*   --region dir
*       |
*       --data
*       |  |
*       |  --ns/table/encoded-region-name <---- The region data
*       |      |
*       |      --replay <---- The edits to replay
*       |
*       --WALs
*          |
*          --server-name <---- The WAL dir
* */
public class HRegion implements Region {
    private long flushSize;
    private long flushIntervalMs;
    private final HRegionFileSystem fs;

    // TODO
    public HRegion(Configuration conf) {
        // this.flushSize = flushSize;
        // this.flushIntervalMs = flushIntervalMs;
        this.fs = new HRegionFileSystem(conf); // TODO
    }

    @Override
    public RegionInfo getRegionInfo() {
        return this.fs.getRegionInfo();
    }

    @Override
    public void onConfigurationChange(Configuration conf) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'onConfigurationChange'");
    }

    @Override
    public TableDescriptor getTableDescriptor() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getTableDescriptor'");
    }

    @Override
    public boolean isAvailable() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'isAvailable'");
    }

    @Override
    public boolean isClosed() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'isClosed'");
    }

    @Override
    public boolean isClosing() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'isClosing'");
    }

    @Override
    public boolean isReadOnly() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'isReadOnly'");
    }

    @Override
    public boolean isSplittable() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'isSplittable'");
    }

    @Override
    public boolean isMergeable() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'isMergeable'");
    }

    @Override
    public List<? extends Store> getStores() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getStores'");
    }

    @Override
    public Store getStore(byte[] family) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getStore'");
    }

    @Override
    public List<String> getStoreFileList(byte[][] columns) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getStoreFileList'");
    }

    @Override
    public boolean refreshStoreFiles() throws IOException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'refreshStoreFiles'");
    }

    @Override
    public long getMaxFlushedSeqId() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getMaxFlushedSeqId'");
    }

    @Override
    public long getOldestHfileTs(boolean majorCompactionOnly) throws IOException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getOldestHfileTs'");
    }

    @Override
    public Map<byte[], Long> getMaxStoreSeqId() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getMaxStoreSeqId'");
    }

    @Override
    public long getEarliestFlushTimeForAllStores() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getEarliestFlushTimeForAllStores'");
    }

    @Override
    public long getReadRequestsCount() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getReadRequestsCount'");
    }

    @Override
    public long getFilteredReadRequestsCount() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getFilteredReadRequestsCount'");
    }

    @Override
    public long getWriteRequestsCount() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getWriteRequestsCount'");
    }

    @Override
    public long getMemStoreDataSize() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getMemStoreDataSize'");
    }

    @Override
    public long getMemStoreHeapSize() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getMemStoreHeapSize'");
    }

    @Override
    public long getMemStoreOffHeapSize() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getMemStoreOffHeapSize'");
    }

    @Override
    public long getNumMutationsWithoutWAL() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getNumMutationsWithoutWAL'");
    }

    @Override
    public long getDataInMemoryWithoutWAL() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getDataInMemoryWithoutWAL'");
    }

    @Override
    public long getBlockedRequestsCount() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getBlockedRequestsCount'");
    }

    @Override
    public long getCheckAndMutateChecksPassed() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getCheckAndMutateChecksPassed'");
    }

    @Override
    public long getCheckAndMutateChecksFailed() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getCheckAndMutateChecksFailed'");
    }

    @Override
    public void startRegionOperation() throws IOException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'startRegionOperation'");
    }

    @Override
    public void startRegionOperation(Operation op) throws IOException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'startRegionOperation'");
    }

    @Override
    public void closeRegionOperation() throws IOException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'closeRegionOperation'");
    }

    @Override
    public void closeRegionOperation(Operation op) throws IOException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'closeRegionOperation'");
    }

    @Override
    public RowLock getRowLock(byte[] row, boolean readLock) throws IOException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getRowLock'");
    }

    @Override
    public Result append(Append append) throws IOException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'append'");
    }

    @Override
    public boolean checkAndMutate(byte[] row, byte[] family, byte[] qualifier, CompareOperator op,
            ByteArrayComparable comparator, TimeRange timeRange, Mutation mutation) throws IOException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'checkAndMutate'");
    }

    @Override
    public boolean checkAndMutate(byte[] row, Filter filter, TimeRange timeRange, Mutation mutation)
            throws IOException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'checkAndMutate'");
    }

    @Override
    public boolean checkAndRowMutate(byte[] row, byte[] family, byte[] qualifier, CompareOperator op,
            ByteArrayComparable comparator, TimeRange timeRange, RowMutations mutations) throws IOException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'checkAndRowMutate'");
    }

    @Override
    public boolean checkAndRowMutate(byte[] row, Filter filter, TimeRange timeRange, RowMutations mutations)
            throws IOException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'checkAndRowMutate'");
    }

    @Override
    public CheckAndMutateResult checkAndMutate(CheckAndMutate checkAndMutate) throws IOException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'checkAndMutate'");
    }

    @Override
    public void delete(Delete delete) throws IOException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'delete'");
    }

    @Override
    public Result get(Get get) throws IOException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'get'");
    }

    @Override
    public List<Cell> get(Get get, boolean withCoprocessor) throws IOException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'get'");
    }

    @Override
    public RegionScanner getScanner(Scan scan) throws IOException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getScanner'");
    }

    @Override
    public CellComparator getCellComparator() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getCellComparator'");
    }

    @Override
    public Result increment(Increment increment) throws IOException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'increment'");
    }

    @Override
    public Result mutateRow(RowMutations mutations) throws IOException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'mutateRow'");
    }

    @Override
    public void mutateRowsWithLocks(Collection<Mutation> mutations, Collection<byte[]> rowsToLock, long nonceGroup,
            long nonce) throws IOException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'mutateRowsWithLocks'");
    }

    @Override
    public void processRowsWithLocks(RowProcessor<?, ?> processor) throws IOException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'processRowsWithLocks'");
    }

    @Override
    public void processRowsWithLocks(RowProcessor<?, ?> processor, long nonceGroup, long nonce) throws IOException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'processRowsWithLocks'");
    }

    @Override
    public void processRowsWithLocks(RowProcessor<?, ?> processor, long timeout, long nonceGroup, long nonce)
            throws IOException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'processRowsWithLocks'");
    }

    @Override
    public void put(Put put) throws IOException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'put'");
    }

    @Override
    public CompactionState getCompactionState() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getCompactionState'");
    }

    @Override
    public boolean waitForFlushes(long timeout) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'waitForFlushes'");
    }

    @Override
    public Configuration getReadOnlyConfiguration() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getReadOnlyConfiguration'");
    }

    /**
     * Determines if the specified row is within the row range specified by the
     * specified RegionInfo
     * 
     * @param info RegionInfo that specifies the row range
     * @param row  row to be checked
     * @return true if the row is within the range specified by the RegionInfo
     */
    public static boolean rowIsInRange(RegionInfo info, final byte[] row) {
        return ((info.getStartKey().length == 0) || (Bytes.compareTo(info.getStartKey(), row) <= 0))
                && ((info.getEndKey().length == 0) || (Bytes.compareTo(info.getEndKey(), row) > 0));
    }
}
