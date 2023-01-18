package org.waterme7on.hbase;

import java.io.IOException;

/**
 * An interface for iterating through a sequence of cells. Similar to Java's
 * Iterator, but without
 * the hasNext() or remove() methods. The hasNext() method is problematic
 * because it may require
 * actually loading the next object, which in turn requires storing the previous
 * object somewhere.
 * <p>
 * The core data block decoder should be as fast as possible, so we push the
 * complexity and
 * performance expense of concurrently tracking multiple cells to layers above
 * the CellScanner.
 * <p>
 * The {@link #current()} method will return a reference to a Cell
 * implementation. This reference
 * may or may not point to a reusable cell implementation, so users of the
 * CellScanner should not,
 * for example, accumulate a List of Cells. All of the references may point to
 * the same object,
 * which would be the latest state of the underlying Cell. In short, the Cell is
 * mutable.
 * </p>
 * Typical usage:
 *
 * <pre>
 * while (scanner.advance()) {
 *     Cell cell = scanner.current();
 *     // do something
 * }
 * </pre>
 * <p>
 * Often used reading {@link org.apache.hadoop.hbase.Cell}s written by
 * {@link org.apache.hadoop.hbase.io.CellOutputStream}.
 */
public interface CellScanner {
    /** Returns the current Cell which may be mutable */
    Cell current();

    /**
     * Advance the scanner 1 cell.
     * 
     * @return true if the next cell is found and {@link #current()} will return a
     *         valid Cell
     * @throws IOException if advancing the scanner fails
     */
    boolean advance() throws IOException;
}
