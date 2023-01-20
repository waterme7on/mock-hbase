package org.waterme7on.hbase.wal;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.PrivateCellUtil;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.waterme7on.hbase.ipc.ServerCall;
import org.apache.hadoop.hbase.regionserver.MultiVersionConcurrencyControl;
import org.apache.hadoop.hbase.util.Bytes;
import org.waterme7on.hbase.wal.WAL.Entry;
import org.apache.hadoop.hbase.wal.WALKeyImpl;

import org.apache.hbase.thirdparty.org.apache.commons.collections4.CollectionUtils;

/**
 * A WAL Entry for {@link AbstractFSWAL} implementation. Immutable. A subclass
 * of {@link Entry} that
 * carries extra info across the ring buffer such as region sequenceid (we want
 * to use this later,
 * just before we write the WAL to ensure region edits maintain order). The
 * extra info added here is
 * not 'serialized' as part of the WALEdit hence marked 'transient' to underline
 * this fact. It also
 * adds mechanism so we can wait on the assign of the region sequence id. See
 * #stampRegionSequenceId().
 */

class FSWALEntry extends Entry {
    // The below data members are denoted 'transient' just to highlight these are
    // not persisted;
    // they are only in memory and held here while passing over the ring buffer.
    private final transient long txid;

    /**
     * If false, means this is a meta edit written by the hbase system itself. It
     * was not in memstore.
     * HBase uses these edit types to note in the log operational transitions such
     * as compactions,
     * flushes, or region open/closes.
     */
    private final transient boolean inMemstore;

    /**
     * Set if this is a meta edit and it is of close region type.
     */
    private final transient boolean closeRegion;

    private final transient RegionInfo regionInfo;
    private final transient Set<byte[]> familyNames;
    private final transient ServerCall<?> rpcCall;

    /**
     * @param inMemstore If true, then this is a data edit, one that came from
     *                   client. If false, it is
     *                   a meta edit made by the hbase system itself and is for the
     *                   WAL only.
     */
    FSWALEntry(final long txid, final WALKeyImpl key, final WALEdit edit, final RegionInfo regionInfo,
            final boolean inMemstore, org.waterme7on.hbase.ipc.ServerCall<?> rpcCall2) {
        super(key, edit);
        this.inMemstore = inMemstore;
        this.closeRegion = !inMemstore && edit.isRegionCloseMarker();
        this.regionInfo = regionInfo;
        this.txid = txid;
        if (inMemstore) {
            // construct familyNames here to reduce the work of log sinker.
            Set<byte[]> families = edit.getFamilies();
            this.familyNames = families != null ? families : collectFamilies(edit.getCells());
        } else {
            this.familyNames = Collections.emptySet();
        }
        this.rpcCall = rpcCall2;
        if (rpcCall2 != null) {
            rpcCall2.retainByWAL();
        }
    }

    static Set<byte[]> collectFamilies(List<Cell> cells) {
        if (CollectionUtils.isEmpty(cells)) {
            return Collections.emptySet();
        } else {
            Set<byte[]> set = new TreeSet<>(Bytes.BYTES_COMPARATOR);
            for (Cell cell : cells) {
                if (!WALEdit.isMetaEditFamily(cell)) {
                    set.add(CellUtil.cloneFamily(cell));
                }
            }
            return set;
        }
    }

    @Override
    public String toString() {
        return "sequence=" + this.txid + ", " + super.toString();
    }

    boolean isInMemStore() {
        return this.inMemstore;
    }

    boolean isCloseRegion() {
        return closeRegion;
    }

    RegionInfo getRegionInfo() {
        return this.regionInfo;
    }

    /** Returns The transaction id of this edit. */
    long getTxid() {
        return this.txid;
    }

    /**
     * Here is where a WAL edit gets its sequenceid. SIDE-EFFECT is our stamping the
     * sequenceid into
     * every Cell AND setting the sequenceid into the MVCC WriteEntry!!!!
     * 
     * @return The sequenceid we stamped on this edit.
     */
    long stampRegionSequenceId(MultiVersionConcurrencyControl.WriteEntry we) throws IOException {
        long regionSequenceId = we.getWriteNumber();
        if (!this.getEdit().isReplay() && inMemstore) {
            for (Cell c : getEdit().getCells()) {
                PrivateCellUtil.setSequenceId(c, regionSequenceId);
            }
        }

        getKey().setWriteEntry(we);
        return regionSequenceId;
    }

    /** Returns the family names which are effected by this edit. */
    Set<byte[]> getFamilyNames() {
        return familyNames;
    }

    void release() {
        if (rpcCall != null) {
            rpcCall.releaseByWAL();
        }
    }
}
