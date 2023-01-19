package org.waterme7on.hbase.regionserver;

import java.nio.ByteBuffer;
import org.apache.hadoop.hbase.NoTagsByteBufferKeyValue;
import org.waterme7on.hbase.util.ByteBufferUtils;

public class NoTagByteBufferChunkKeyValue extends NoTagsByteBufferKeyValue {

    public NoTagByteBufferChunkKeyValue(ByteBuffer buf, int offset, int length) {
        super(buf, offset, length);
    }

    public NoTagByteBufferChunkKeyValue(ByteBuffer buf, int offset, int length, long seqId) {
        super(buf, offset, length, seqId);
    }

    @Override
    public int getChunkId() {
        // The chunkId is embedded at the 0th offset of the bytebuffer
        return ByteBufferUtils.toInt(buf, 0);
    }

}
