package org.waterme7on.hbase;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public interface Writable {
    void write(DataOutput var1) throws IOException;

    void readFields(DataInput var1) throws IOException;
}
