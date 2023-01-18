package org.waterme7on.hbase;

import java.io.IOException;
import java.io.OutputStream;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * An extension of the KeyValue where the tags length is always 0
 */
public class NoTagsKeyValue extends KeyValue {
    public NoTagsKeyValue(byte[] bytes, int offset, int length) {
        super(bytes, offset, length);
    }

    @Override
    public int getTagsLength() {
        return 0;
    }

    @Override
    public int write(OutputStream out, boolean withTags) throws IOException {
        out.write(this.bytes, this.offset, this.length);
        return this.length;
    }

    @Override
    public int getSerializedSize(boolean withTags) {
        return this.length;
    }

    @Override
    public ExtendedCell deepClone() {
        byte[] copy = Bytes.copy(this.bytes, this.offset, this.length);
        KeyValue kv = new NoTagsKeyValue(copy, 0, copy.length);
        kv.setSequenceId(this.getSequenceId());
        return kv;
    }
}
