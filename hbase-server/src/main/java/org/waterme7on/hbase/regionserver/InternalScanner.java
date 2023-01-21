package org.waterme7on.hbase.regionserver;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import org.apache.hadoop.hbase.Cell;
import org.waterme7on.hbase.NoLimitScannerContext;
import org.waterme7on.hbase.ScannerContext;

/**
 * Internal scanners differ from client-side scanners in that they operate on
 * HStoreKeys and byte[]
 * instead of RowResults. This is because they are actually close to how the
 * data is physically
 * stored, and therefore it is more convenient to interact with them that way.
 * It is also much
 * easier to merge the results across SortedMaps than RowResults.
 * <p>
 * Additionally, we need to be able to determine if the scanner is doing
 * wildcard column matches
 * (when only a column family is specified or if a column regex is specified) or
 * if multiple members
 * of the same column family were specified. If so, we need to ignore the
 * timestamp to ensure that
 * we get all the family members, as they may have been last updated at
 * different times.
 */
public interface InternalScanner extends Closeable {
    /**
     * Grab the next row's worth of values.
     * 
     * @param result return output array
     * @return true if more rows exist after this one, false if scanner is done
     * @throws IOException e
     */
    default boolean next(List<Cell> result) throws IOException {
        return next(result, NoLimitScannerContext.getInstance());
    }

    /**
     * Grab the next row's worth of values.
     * 
     * @param result return output array
     * @return true if more rows exist after this one, false if scanner is done
     * @throws IOException e
     */
    boolean next(List<Cell> result, ScannerContext scannerContext) throws IOException;

    /**
     * Closes the scanner and releases any resources it has allocated
     */
    @Override
    void close() throws IOException;
}
