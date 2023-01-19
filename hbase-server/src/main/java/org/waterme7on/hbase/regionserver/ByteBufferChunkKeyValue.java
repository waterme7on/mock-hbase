package org.waterme7on.hbase.regionserver;

import java.nio.ByteBuffer;
import org.apache.hadoop.hbase.ByteBufferKeyValue;
import org.waterme7on.hbase.util.ByteBufferUtils;

/**
 * ByteBuffer based cell which has the chunkid at the 0th offset
 * 
 * @see MemStoreLAB
 */
// TODO : When moving this cell to CellChunkMap we will have the following
// things
// to be serialized
// chunkId (Integer) + offset (Integer) + length (Integer) + seqId (Long) = 20
// bytes
public class ByteBufferChunkKeyValue extends ByteBufferKeyValue {
    public ByteBufferChunkKeyValue(ByteBuffer buf, int offset, int length) {
        super(buf, offset, length);
    }

    public ByteBufferChunkKeyValue(ByteBuffer buf, int offset, int length, long seqId) {
        super(buf, offset, length, seqId);
    }

    @Override
    public int getChunkId() {
        // The chunkId is embedded at the 0th offset of the bytebuffer
        return ByteBufferUtils.toInt(buf, 0);
    }
}
