package org.waterme7on.hbase.regionserver.store;

import java.io.IOException;
import java.util.List;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.exceptions.UnexpectedStateException;
import org.waterme7on.hbase.regionserver.KeyValueScanner;
import org.apache.yetus.audience.InterfaceAudience;
import org.waterme7on.hbase.regionserver.MemStoreSize;
import org.waterme7on.hbase.regionserver.MemStoreSizing;

/**
 * The MemStore holds in-memory modifications to the Store. Modifications are
 * {@link Cell}s.
 * <p>
 * The MemStore functions should not be called in parallel. Callers should hold
 * write and read
 * locks. This is done in {@link HStore}.
 * </p>
 */
@InterfaceAudience.Private
public interface MemStore {

    /**
     * Creates a snapshot of the current memstore. Snapshot must be cleared by call
     * to
     * {@link #clearSnapshot(long)}.
     * 
     * @return {@link MemStoreSnapshot}
     */
    MemStoreSnapshot snapshot();

    /**
     * Clears the current snapshot of the Memstore.
     * 
     * @see #snapshot()
     */
    void clearSnapshot(long id) throws UnexpectedStateException;

    /**
     * Flush will first clear out the data in snapshot if any (It will take a second
     * flush invocation
     * to clear the current Cell set). If snapshot is empty, current Cell set will
     * be flushed.
     * 
     * @return On flush, how much memory we will clear.
     */
    MemStoreSize getFlushableSize();

    /**
     * Return the size of the snapshot(s) if any
     * 
     * @return size of the memstore snapshot
     */
    MemStoreSize getSnapshotSize();

    /**
     * Write an update
     * 
     * @param memstoreSizing The delta in memstore size will be passed back via
     *                       this. This will
     *                       include both data size and heap overhead delta.
     */
    void add(final Cell cell, MemStoreSizing memstoreSizing);

    /**
     * Write the updates
     * 
     * @param memstoreSizing The delta in memstore size will be passed back via
     *                       this. This will
     *                       include both data size and heap overhead delta.
     */
    void add(Iterable<Cell> cells, MemStoreSizing memstoreSizing);

    /** Returns Oldest timestamp of all the Cells in the MemStore */
    long timeOfOldestEdit();

    /**
     * Update or insert the specified cells.
     * <p>
     * For each Cell, insert into MemStore. This will atomically upsert the value
     * for that
     * row/family/qualifier. If a Cell did already exist, it will then be removed.
     * <p>
     * Currently the memstoreTS is kept at 0 so as each insert happens, it will be
     * immediately
     * visible. May want to change this so it is atomic across all KeyValues.
     * <p>
     * This is called under row lock, so Get operations will still see updates
     * atomically. Scans will
     * only see each KeyValue update as atomic.
     * 
     * @param readpoint      readpoint below which we can safely remove duplicate
     *                       Cells.
     * @param memstoreSizing The delta in memstore size will be passed back via
     *                       this. This will
     *                       include both data size and heap overhead delta.
     */
    void upsert(Iterable<Cell> cells, long readpoint, MemStoreSizing memstoreSizing);

    /**
     * @return scanner over the memstore. This might include scanner over the
     *         snapshot when one is
     *         present.
     */
    List<KeyValueScanner> getScanners(long readPt) throws IOException;

    /**
     * @return Total memory occupied by this MemStore. This won't include any size
     *         occupied by the
     *         snapshot. We assume the snapshot will get cleared soon. This is not
     *         thread safe and the
     *         memstore may be changed while computing its size. It is the
     *         responsibility of the
     *         caller to make sure this doesn't happen.
     */
    MemStoreSize size();

    /**
     * This method is called before the flush is executed.
     * 
     * @return an estimation (lower bound) of the unflushed sequence id in memstore
     *         after the flush is
     *         executed. if memstore will be cleared returns
     *         {@code HConstants.NO_SEQNUM}.
     */
    long preFlushSeqIDEstimation();

    /* Return true if the memstore may use some extra memory space */
    boolean isSloppy();

    /**
     * This message intends to inform the MemStore that next coming updates are
     * going to be part of
     * the replaying edits from WAL
     */
    default void startReplayingFromWAL() {
        return;
    }

    /**
     * This message intends to inform the MemStore that the replaying edits from WAL
     * are done
     */
    default void stopReplayingFromWAL() {
        return;
    }
}
