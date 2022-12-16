package org.waterme7on.hbase.wal;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;

public interface WALProvider {
    // writer interface
    interface WriterBase extends Closeable {
        long getLength();
        long getSyncedLength();
    }
    interface Writer extends WriterBase {
        void sync(boolean forceSync) throws IOException;

        void append(WAL.Entry entry) throws IOException;
    }

    interface AsyncWriter extends WriterBase {
        CompletableFuture<Long> sync(boolean forceSync);

        void append(WAL.Entry entry);
    }
}
