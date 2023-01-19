package org.waterme7on.hbase.regionserver;

import java.nio.ByteBuffer;
import org.waterme7on.hbase.regionserver.ChunkCreator.ChunkType;

/**
 * An on heap chunk implementation.
 */
public class OnheapChunk extends Chunk {

    OnheapChunk(int size, int id, ChunkType chunkType) {
        super(size, id, chunkType);
    }

    OnheapChunk(int size, int id, ChunkType chunkType, boolean fromPool) {
        super(size, id, chunkType, fromPool);
    }

    @Override
    void allocateDataBuffer() {
        if (data == null) {
            data = ByteBuffer.allocate(this.size);
            data.putInt(0, this.getId());
        }
    }
}
