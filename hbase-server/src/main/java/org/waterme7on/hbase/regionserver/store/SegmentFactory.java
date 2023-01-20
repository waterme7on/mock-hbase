package org.waterme7on.hbase.regionserver.store;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.regionserver.CellSet;
import org.waterme7on.hbase.regionserver.MemStoreLAB;
import org.waterme7on.hbase.regionserver.MemStoreSizing;

/**
 * A singleton store segment factory. Generate concrete store segments.
 */
public final class SegmentFactory {

    private SegmentFactory() {
    }

    private static SegmentFactory instance = new SegmentFactory();

    public static SegmentFactory instance() {
        return instance;
    }

    /**
     * create empty immutable segment for initializations This ImmutableSegment is
     * used as a place
     * holder for snapshot in Memstore. It won't flush later, So it is not necessary
     * to record the
     * initial size for it.
     * 
     * @param comparator comparator
     */
    public ImmutableSegment createImmutableSegment(CellComparator comparator) {
        MutableSegment segment = generateMutableSegment(null, comparator, null, null);
        return createImmutableSegment(segment, null);
    }

    // create not-flat immutable segment from mutable segment
    public ImmutableSegment createImmutableSegment(MutableSegment segment,
            MemStoreSizing memstoreSizing) {
        return new CSLMImmutableSegment(segment, memstoreSizing);
    }

    // create mutable segment
    public MutableSegment createMutableSegment(final Configuration conf, CellComparator comparator,
            MemStoreSizing memstoreSizing) {
        MemStoreLAB memStoreLAB = MemStoreLAB.newInstance(conf);
        return generateMutableSegment(conf, comparator, memStoreLAB, memstoreSizing);
    }

    private MutableSegment generateMutableSegment(final Configuration conf, CellComparator comparator,
            MemStoreLAB memStoreLAB, MemStoreSizing memstoreSizing) {
        // TBD use configuration to set type of segment
        CellSet set = new CellSet(comparator);
        return new MutableSegment(set, comparator, memStoreLAB, memstoreSizing);
    }
}
