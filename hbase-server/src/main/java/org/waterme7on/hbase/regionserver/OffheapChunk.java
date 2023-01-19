package org.waterme7on.hbase.regionserver;

import java.nio.ByteBuffer;

import org.waterme7on.hbase.regionserver.ChunkCreator.ChunkType;

/**
 * An off heap chunk implementation.
 */
public class OffheapChunk extends Chunk {

    OffheapChunk(int size, int id, ChunkType chunkType) {
        // better if this is always created fromPool. This should not be called
        super(size, id, chunkType);
    }

    OffheapChunk(int size, int id, ChunkType chunkType, boolean fromPool) {
        super(size, id, chunkType, fromPool);
        assert fromPool == true;
    }

    @Override
    void allocateDataBuffer() {
        if (data == null) {
            data = ByteBuffer.allocateDirect(this.size);
            data.putInt(0, this.getId());
        }
    }
}
