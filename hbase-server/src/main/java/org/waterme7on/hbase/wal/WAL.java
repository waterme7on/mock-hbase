package org.waterme7on.hbase.wal;

import java.io.Closeable;

public interface WAL extends Closeable  {
    class Entry {
        private final String edit;
        private final String key;
        public Entry() {
            this(new String(), new String());
        }
        public Entry(String edit, String key) {
            this.edit = edit;
            this.key = key;
        }
        @Override
        public java.lang.String toString() {
            return this.key + "=" + this.edit;
        }
    }
}
