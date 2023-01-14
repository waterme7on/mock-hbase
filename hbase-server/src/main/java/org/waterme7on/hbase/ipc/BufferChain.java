package org.waterme7on.hbase.ipc;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.GatheringByteChannel;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Chain of ByteBuffers. Used writing out an array of byte buffers.
 */
class BufferChain {
    private final ByteBuffer[] buffers;
    private int remaining = 0;
    private int size;

    BufferChain(ByteBuffer... buffers) {
        for (ByteBuffer b : buffers) {
            this.remaining += b.remaining();
        }
        this.size = remaining;
        this.buffers = buffers;
    }

    /**
     * Expensive. Makes a new buffer to hold a copy of what is in contained
     * ByteBuffers. This call
     * drains this instance; it cannot be used subsequent to the call.
     * 
     * @return A new byte buffer with the content of all contained ByteBuffers.
     */
    byte[] getBytes() {
        if (!hasRemaining())
            throw new IllegalAccessError();
        byte[] bytes = new byte[this.remaining];
        int offset = 0;
        for (ByteBuffer bb : this.buffers) {
            int length = bb.remaining();
            bb.get(bytes, offset, length);
            offset += length;
        }
        return bytes;
    }

    boolean hasRemaining() {
        return remaining > 0;
    }

    long write(GatheringByteChannel channel) throws IOException {
        if (!hasRemaining()) {
            return 0;
        }
        long written = 0;
        for (ByteBuffer bb : this.buffers) {
            if (bb.hasRemaining()) {
                final int pos = bb.position();
                final int result = channel.write(bb);
                if (result <= 0) {
                    // Write error. Return how much we were able to write until now.
                    return written;
                }
                // Adjust the position of buffers already written so we don't write out
                // duplicate data upon retry of incomplete write with the same buffer chain.
                bb.position(pos + result);
                remaining -= result;
                written += result;
            }
        }
        return written;
    }

    int size() {
        return size;
    }

    ByteBuffer[] getBuffers() {
        return this.buffers;
    }
}
