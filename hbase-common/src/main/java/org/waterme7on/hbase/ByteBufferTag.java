package org.waterme7on.hbase;

import java.nio.ByteBuffer;
import org.waterme7on.hbase.util.ByteBufferUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;


/**
 * This is a {@link Tag} implementation in which value is backed by
 * {@link java.nio.ByteBuffer}
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class ByteBufferTag implements Tag {

    private ByteBuffer buffer;
    private int offset, length;
    private byte type;

    public ByteBufferTag(ByteBuffer buffer, int offset, int length) {
        this.buffer = buffer;
        this.offset = offset;
        this.length = length;
        this.type = ByteBufferUtils.toByte(buffer, offset + TAG_LENGTH_SIZE);
    }

    @Override
    public byte getType() {
        return this.type;
    }

    @Override
    public int getValueOffset() {
        return this.offset + INFRASTRUCTURE_SIZE;
    }

    @Override
    public int getValueLength() {
        return this.length - INFRASTRUCTURE_SIZE;
    }

    @Override
    public boolean hasArray() {
        return false;
    }

    @Override
    public byte[] getValueArray() {
        throw new UnsupportedOperationException(
                "Tag is backed by an off heap buffer. Use getValueByteBuffer()");
    }

    @Override
    public ByteBuffer getValueByteBuffer() {
        return this.buffer;
    }

    @Override
    public String toString() {
        return "[Tag type : " + this.type + ", value : "
                + ByteBufferUtils.toStringBinary(buffer, getValueOffset(), getValueLength()) + "]";
    }
}
