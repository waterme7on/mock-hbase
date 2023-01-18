package org.waterme7on.hbase.regionserver;

/**
 * Data structure of three longs. Convenient package in which to carry current
 * state of three
 * counters.
 * <p>
 * Immutable!
 * </p>
 * 
 * @see MemStoreSizing
 */
public class MemStoreSize {
    /**
     * 'dataSize' tracks the Cell's data bytes size alone (Key bytes, value bytes).
     * A cell's data can
     * be in on heap or off heap area depending on the MSLAB and its configuration
     * to be using on heap
     * or off heap LABs
     */
    private final long dataSize;

    /**
     * 'getHeapSize' tracks all Cell's heap size occupancy. This will include Cell
     * POJO heap overhead.
     * When Cells in on heap area, this will include the cells data size as well.
     */
    private final long heapSize;

    /**
     * off-heap size: the aggregated size of all data that is allocated off-heap
     * including all
     * key-values that reside off-heap and the metadata that resides off-heap
     */
    private final long offHeapSize;

    private final int cellsCount;

    /**
     * Package private constructor.
     */
    MemStoreSize() {
        this(0L, 0L, 0L, 0);
    }

    /**
     * Package private constructor.
     */
    MemStoreSize(long dataSize, long heapSize, long offHeapSize, int cellsCount) {
        this.dataSize = dataSize;
        this.heapSize = heapSize;
        this.offHeapSize = offHeapSize;
        this.cellsCount = cellsCount;
    }

    /**
     * Package private constructor.
     */
    MemStoreSize(MemStoreSize memStoreSize) {
        this.dataSize = memStoreSize.getDataSize();
        this.heapSize = memStoreSize.getHeapSize();
        this.offHeapSize = memStoreSize.getOffHeapSize();
        this.cellsCount = memStoreSize.getCellsCount();
    }

    public boolean isEmpty() {
        return this.dataSize == 0 && this.heapSize == 0 && this.offHeapSize == 0
                && this.cellsCount == 0;
    }

    public long getDataSize() {
        return this.dataSize;
    }

    public long getHeapSize() {
        return this.heapSize;
    }

    public long getOffHeapSize() {
        return this.offHeapSize;
    }

    public int getCellsCount() {
        return this.cellsCount;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (!(obj instanceof MemStoreSize)) {
            return false;
        }
        MemStoreSize other = (MemStoreSize) obj;
        return this.dataSize == other.dataSize && this.heapSize == other.heapSize
                && this.offHeapSize == other.offHeapSize && this.cellsCount == other.cellsCount;
    }

    @Override
    public int hashCode() {
        long h = this.dataSize;
        h = h * 31 + this.heapSize;
        h = h * 31 + this.offHeapSize;
        h = h * 31 + this.cellsCount;
        return (int) h;
    }

    @Override
    public String toString() {
        return "dataSize=" + this.dataSize + ", getHeapSize=" + this.heapSize + ", getOffHeapSize="
                + this.offHeapSize + ", getCellsCount=" + this.cellsCount;
    }
}
