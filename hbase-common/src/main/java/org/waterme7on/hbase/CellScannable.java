package org.waterme7on.hbase;

/**
 * Implementer can return a CellScanner over its Cell content. Class name is
 * ugly but mimicing
 * java.util.Iterable only we are about the dumber CellScanner rather than say
 * Iterator&lt;Cell&gt;.
 * See CellScanner class comment for why we go dumber than java.util.Iterator.
 */
public interface CellScannable {
    /** Returns A CellScanner over the contained {@link Cell}s */
    CellScanner cellScanner();
}
