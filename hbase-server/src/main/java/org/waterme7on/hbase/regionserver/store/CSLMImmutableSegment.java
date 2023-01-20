package org.waterme7on.hbase.regionserver.store;

import org.apache.hadoop.hbase.util.ClassSize;
import org.waterme7on.hbase.regionserver.MemStoreSizing;
import org.waterme7on.hbase.regionserver.Segment;

/**
 * CSLMImmutableSegment is an abstract class that extends the API supported by a
 * {@link Segment},
 * and {@link ImmutableSegment}. This immutable segment is working with CellSet
 * with
 * ConcurrentSkipListMap (CSLM) delegatee.
 */
public class CSLMImmutableSegment extends ImmutableSegment {
    public static final long DEEP_OVERHEAD_CSLM = ImmutableSegment.DEEP_OVERHEAD + ClassSize.CONCURRENT_SKIPLISTMAP;

    /**
     * ------------------------------------------------------------------------ Copy
     * C-tor to be used
     * when new CSLMImmutableSegment is being built from a Mutable one. This C-tor
     * should be used when
     * active MutableSegment is pushed into the compaction pipeline and becomes an
     * ImmutableSegment.
     */
    protected CSLMImmutableSegment(Segment segment, MemStoreSizing memstoreSizing) {
        super(segment);
        // update the segment metadata heap size
        long indexOverhead = -MutableSegment.DEEP_OVERHEAD + DEEP_OVERHEAD_CSLM;
        incMemStoreSize(0, indexOverhead, 0, 0); // CSLM is always on-heap
        if (memstoreSizing != null) {
            memstoreSizing.incMemStoreSize(0, indexOverhead, 0, 0);
        }
    }

    @Override
    protected long indexEntrySize() {
        return ClassSize.CONCURRENT_SKIPLISTMAP_ENTRY;
    }

    @Override
    protected boolean canBeFlattened() {
        return true;
    }
}
