package org.waterme7on.hbase.regionserver;

import java.io.IOException;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.hadoop.hbase.Cell;

/**
 * A "non-reversed &amp; non-lazy" scanner which does not support backward
 * scanning and always does
 * a real seek operation. Most scanners are inherited from this class.
 */
public abstract class NonReversedNonLazyKeyValueScanner extends NonLazyKeyValueScanner {

    @Override
    public boolean backwardSeek(Cell key) throws IOException {
        throw new NotImplementedException(
                "backwardSeek must not be called on a " + "non-reversed scanner");
    }

    @Override
    public boolean seekToPreviousRow(Cell key) throws IOException {
        throw new NotImplementedException(
                "seekToPreviousRow must not be called on a " + "non-reversed scanner");
    }

    @Override
    public boolean seekToLastRow() throws IOException {
        throw new NotImplementedException(
                "seekToLastRow must not be called on a " + "non-reversed scanner");
    }

}
