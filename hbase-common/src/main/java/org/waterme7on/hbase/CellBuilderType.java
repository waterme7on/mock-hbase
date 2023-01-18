package org.waterme7on.hbase;

import org.apache.yetus.audience.InterfaceAudience;

/**
 * Used by {@link CellBuilderFactory} and {@link ExtendedCellBuilderFactory}.
 * Indicates which memory
 * copy is used in building cell.
 */
@InterfaceAudience.Public
public enum CellBuilderType {
    /**
     * The cell builder will copy all passed bytes for building cell.
     */
    DEEP_COPY,
    /**
     * DON'T modify the byte array passed to cell builder because all fields in new
     * cell are reference
     * to input arguments
     */
    SHALLOW_COPY
}
