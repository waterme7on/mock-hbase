package org.waterme7on.hbase.regionserver.store;

import java.util.List;

import org.waterme7on.hbase.regionserver.KeyValueScanner;
import org.waterme7on.hbase.regionserver.MemStoreSize;
import org.waterme7on.hbase.regionserver.TimeRangeTracker;

/**
 * {@link MemStoreSnapshot} is a Context Object to hold details of the snapshot
 * taken on a MemStore.
 * Details include the snapshot's identifier, count of cells in it and total
 * memory size occupied by
 * all the cells, timestamp information of all the cells and the snapshot
 * immutableSegment.
 * <p>
 * NOTE:Every time when {@link MemStoreSnapshot#getScanners} is called, we
 * create new
 * {@link SnapshotSegmentScanner}s on the
 * {@link MemStoreSnapshot#snapshotImmutableSegment},and
 * {@link Segment#incScannerCount} is invoked in the
 * {@link SnapshotSegmentScanner} ctor to increase
 * the reference count of {@link MemStoreLAB} which used by
 * {@link MemStoreSnapshot#snapshotImmutableSegment}, so after we finish using
 * these scanners, we
 * must call their close method to invoke {@link Segment#decScannerCount}.
 */
public class MemStoreSnapshot {
    private final long id;
    private final int cellsCount;
    private final MemStoreSize memStoreSize;
    private final TimeRangeTracker timeRangeTracker;
    private final boolean tagsPresent;
    private final ImmutableSegment snapshotImmutableSegment;

    public MemStoreSnapshot(long id, ImmutableSegment snapshot) {
        this.id = id;
        this.cellsCount = snapshot.getCellsCount();
        this.memStoreSize = snapshot.getMemStoreSize();
        this.timeRangeTracker = snapshot.getTimeRangeTracker();
        this.tagsPresent = snapshot.isTagsPresent();
        this.snapshotImmutableSegment = snapshot;
    }

    /** Returns snapshot's identifier. */
    public long getId() {
        return id;
    }

    /** Returns Number of Cells in this snapshot. */
    public int getCellsCount() {
        return cellsCount;
    }

    public long getDataSize() {
        return memStoreSize.getDataSize();
    }

    public MemStoreSize getMemStoreSize() {
        return memStoreSize;
    }

    /** Returns {@link TimeRangeTracker} for all the Cells in the snapshot. */
    public TimeRangeTracker getTimeRangeTracker() {
        return timeRangeTracker;
    }

    /**
     * Create new {@link SnapshotSegmentScanner}s for iterating over the snapshot.
     * <br/>
     * NOTE:Here when create new {@link SnapshotSegmentScanner}s,
     * {@link Segment#incScannerCount} is
     * invoked in the {@link SnapshotSegmentScanner} ctor,so after we use these
     * {@link SnapshotSegmentScanner}s, we must call
     * {@link SnapshotSegmentScanner#close} to invoke
     * {@link Segment#decScannerCount}.
     * 
     * @return {@link KeyValueScanner}s(Which type is
     *         {@link SnapshotSegmentScanner}) for iterating
     *         over the snapshot.
     */
    public List<KeyValueScanner> getScanners() {
        return snapshotImmutableSegment.getSnapshotScanners();
    }

    /** Returns true if tags are present in this snapshot */
    public boolean isTagsPresent() {
        return this.tagsPresent;
    }
}
