package org.waterme7on.hbase.regionserver;

import java.io.IOException;
import java.util.Optional;
import java.util.OptionalLong;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;

/**
 * An interface to describe a store data file.
 * <p>
 * <strong>NOTICE: </strong>this interface is mainly designed for coprocessor,
 * so it will not expose
 * all the internal APIs for a 'store file'. If you are implementing something
 * inside HBase, i.e,
 * not a coprocessor hook, usually you should use {@link HStoreFile} directly as
 * it is the only
 * implementation of this interface.
 */
public interface StoreFile {

    /**
     * Get the first key in this store file.
     */
    Optional<Cell> getFirstKey();

    /**
     * Get the last key in this store file.
     */
    Optional<Cell> getLastKey();

    /**
     * Get the comparator for comparing two cells.
     */
    CellComparator getComparator();

    /**
     * Get max of the MemstoreTS in the KV's in this store file.
     */
    long getMaxMemStoreTS();

    /** Returns Path or null if this StoreFile was made with a Stream. */
    Path getPath();

    /** Returns Encoded Path if this StoreFile was made with a Stream. */
    Path getEncodedPath();

    /** Returns Returns the qualified path of this StoreFile */
    Path getQualifiedPath();

    /** Returns True if this is a StoreFile Reference. */
    boolean isReference();

    /** Returns True if this is HFile. */
    boolean isHFile();

    /** Returns True if this file was made by a major compaction. */
    boolean isMajorCompactionResult();

    /** Returns True if this file should not be part of a minor compaction. */
    boolean excludeFromMinorCompaction();

    /** Returns This files maximum edit sequence id. */
    long getMaxSequenceId();

    /**
     * Get the modification time of this store file. Usually will access the file
     * system so throws
     * IOException.
     * 
     * @deprecated Since 2.0.0. Will be removed in 3.0.0.
     * @see #getModificationTimestamp()
     */
    @Deprecated
    long getModificationTimeStamp() throws IOException;

    /**
     * Get the modification time of this store file. Usually will access the file
     * system so throws
     * IOException.
     */
    long getModificationTimestamp() throws IOException;

    /**
     * Check if this storefile was created by bulk load. When a hfile is bulk loaded
     * into HBase, we
     * append {@code '_SeqId_<id-when-loaded>'} to the hfile name, unless
     * "hbase.mapreduce.bulkload.assign.sequenceNumbers" is explicitly turned off.
     * If
     * "hbase.mapreduce.bulkload.assign.sequenceNumbers" is turned off, fall back to
     * BULKLOAD_TIME_KEY.
     * 
     * @return true if this storefile was created by bulk load.
     */
    boolean isBulkLoadResult();

    /**
     * Return the timestamp at which this bulk load file was generated.
     */
    OptionalLong getBulkLoadTimestamp();

    /** Returns a length description of this StoreFile, suitable for debug output */
    String toStringDetailed();

    /**
     * Get the min timestamp of all the cells in the store file.
     */
    OptionalLong getMinimumTimestamp();

    /**
     * Get the max timestamp of all the cells in the store file.
     */
    OptionalLong getMaximumTimestamp();
}
