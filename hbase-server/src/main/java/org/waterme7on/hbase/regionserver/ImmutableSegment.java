package org.waterme7on.hbase.regionserver;

import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hbase.regionserver.CellSet;
import org.apache.hadoop.hbase.regionserver.KeyValueScanner;
import org.apache.hadoop.hbase.CellComparator;
import org.waterme7on.hbase.util.ClassSize;

/**
 * ImmutableSegment is an abstract class that extends the API supported by a
 * {@link Segment}, and is
 * not needed for a {@link MutableSegment}.
 */
public abstract class ImmutableSegment extends Segment {

    public static final long DEEP_OVERHEAD = Segment.DEEP_OVERHEAD + ClassSize.NON_SYNC_TIMERANGE_TRACKER;

    // each sub-type of immutable segment knows whether it is flat or not
    protected abstract boolean canBeFlattened();

    public int getNumUniqueKeys() {
        return getCellSet().getNumUniqueKeys();
    }

    ///////////////////// CONSTRUCTORS /////////////////////
    /**
     * ------------------------------------------------------------------------
     * Empty C-tor to be used
     * only for CompositeImmutableSegment
     */
    protected ImmutableSegment(CellComparator comparator) {
        super(comparator, TimeRangeTracker.create(TimeRangeTracker.Type.NON_SYNC));
    }

    protected ImmutableSegment(CellComparator comparator, List<ImmutableSegment> segments) {
        super(comparator, TimeRangeTracker.create(TimeRangeTracker.Type.NON_SYNC));
    }

    /**
     * ------------------------------------------------------------------------
     * C-tor to be used to
     * build the derived classes
     */
    protected ImmutableSegment(CellSet cs, CellComparator comparator, MemStoreLAB memStoreLAB) {
        super(cs, comparator, memStoreLAB, TimeRangeTracker.create(TimeRangeTracker.Type.NON_SYNC));
    }

    /**
     * ------------------------------------------------------------------------ Copy
     * C-tor to be used
     * when new CSLMImmutableSegment (derived) is being built from a Mutable one.
     * This C-tor should be
     * used when active MutableSegment is pushed into the compaction pipeline and
     * becomes an
     * ImmutableSegment.
     */
    protected ImmutableSegment(Segment segment) {
        super(segment);
    }

    ///////////////////// PUBLIC METHODS /////////////////////

    public int getNumOfSegments() {
        return 1;
    }

    public List<Segment> getAllSegments() {
        return Collections.singletonList(this);
    }

    @Override
    public String toString() {
        String res = super.toString();
        res += "Num uniques " + getNumUniqueKeys() + "; ";
        return res;
    }

    /**
     * We create a new {@link SnapshotSegmentScanner} to increase the reference
     * count of
     * {@link MemStoreLABImpl} used by this segment.
     */
    List<KeyValueScanner> getSnapshotScanners() {
        throw new UnsupportedOperationException("Not supported yet.");
        // return Collections.singletonList(new SnapshotSegmentScanner(this));
    }
}
