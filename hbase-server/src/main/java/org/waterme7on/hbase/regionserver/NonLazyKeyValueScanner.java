package org.waterme7on.hbase.regionserver;

import java.io.IOException;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.Cell;

/**
 * A "non-lazy" scanner which always does a real seek operation. Most scanners
 * are inherited from
 * this class.
 */
public abstract class NonLazyKeyValueScanner implements KeyValueScanner {

    @Override
    public boolean requestSeek(Cell kv, boolean forward, boolean useBloom) throws IOException {
        return doRealSeek(this, kv, forward);
    }

    @Override
    public boolean realSeekDone() {
        return true;
    }

    @Override
    public void enforceSeek() throws IOException {
        throw new NotImplementedException("enforceSeek must not be called on a " + "non-lazy scanner");
    }

    public static boolean doRealSeek(KeyValueScanner scanner, Cell kv, boolean forward)
            throws IOException {
        return forward ? scanner.reseek(kv) : scanner.seek(kv);
    }

    @Override
    public boolean shouldUseScanner(Scan scan, HStore store, long oldestUnexpiredTS) {
        // No optimizations implemented by default.
        return true;
    }

    @Override
    public boolean isFileScanner() {
        // Not a file by default.
        return false;
    }

    @Override
    public Path getFilePath() {
        // Not a file by default.
        return null;
    }

    @Override
    public Cell getNextIndexedKey() {
        return null;
    }

    @Override
    public void shipped() throws IOException {
        // do nothing
    }
}
