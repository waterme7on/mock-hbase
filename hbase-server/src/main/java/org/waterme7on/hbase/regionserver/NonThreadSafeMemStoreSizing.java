package org.waterme7on.hbase.regionserver;

/**
 * Accounting of current heap and data sizes. <em>NOT THREAD SAFE</em>. Use in a
 * 'local' context
 * only where just a single-thread is updating. No concurrency! Used, for
 * example, when summing all
 * Cells in a single batch where result is then applied to the Store.
 * 
 * @see ThreadSafeMemStoreSizing
 */
public class NonThreadSafeMemStoreSizing implements MemStoreSizing {
    private long dataSize = 0;
    private long heapSize = 0;
    private long offHeapSize = 0;
    private int cellsCount = 0;

    public NonThreadSafeMemStoreSizing() {
        this(0, 0, 0, 0);
    }

    NonThreadSafeMemStoreSizing(MemStoreSize mss) {
        this(mss.getDataSize(), mss.getHeapSize(), mss.getOffHeapSize(), mss.getCellsCount());
    }

    NonThreadSafeMemStoreSizing(long dataSize, long heapSize, long offHeapSize, int cellsCount) {
        incMemStoreSize(dataSize, heapSize, offHeapSize, cellsCount);
    }

    @Override
    public MemStoreSize getMemStoreSize() {
        return new MemStoreSize(this.dataSize, this.heapSize, this.offHeapSize, this.cellsCount);
    }

    @Override
    public long incMemStoreSize(long dataSizeDelta, long heapSizeDelta, long offHeapSizeDelta,
            int cellsCountDelta) {
        this.offHeapSize += offHeapSizeDelta;
        this.heapSize += heapSizeDelta;
        this.dataSize += dataSizeDelta;
        this.cellsCount += cellsCountDelta;
        return this.dataSize;
    }

    @Override
    public boolean compareAndSetDataSize(long expected, long updated) {
        if (dataSize == expected) {
            dataSize = updated;
            return true;
        }
        return false;
    }

    @Override
    public long getDataSize() {
        return dataSize;
    }

    @Override
    public long getHeapSize() {
        return heapSize;
    }

    @Override
    public long getOffHeapSize() {
        return offHeapSize;
    }

    @Override
    public int getCellsCount() {
        return cellsCount;
    }

    @Override
    public String toString() {
        return getMemStoreSize().toString();
    }
}