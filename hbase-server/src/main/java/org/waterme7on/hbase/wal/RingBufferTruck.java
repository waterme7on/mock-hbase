package org.waterme7on.hbase.wal;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A 'truck' to carry a payload across the ring buffer from Handler to WAL. Has
 * EITHER a
 * {@link FSWALEntry} for making an append OR it has a {@link SyncFuture} to
 * represent a 'sync'
 * invocation. Truck instances are reused by the disruptor when it gets around
 * to it so their
 * payload references must be discarded on consumption to release them to GC.
 */
final class RingBufferTruck {

    private static final Logger LOG = LoggerFactory.getLogger(RingBufferTruck.class);

    public enum Type {
        APPEND,
        SYNC,
        EMPTY
    }

    private Type type = Type.EMPTY;

    /**
     * Either this syncFuture is set or entry is set, but not both.
     */
    private SyncFuture sync;
    private FSWALEntry entry;

    /**
     * Load the truck with a {@link FSWALEntry}.
     */
    void load(FSWALEntry entry) {
        LOG.debug("loadEntry: {}", entry.toString());
        this.entry = entry;
        this.type = Type.APPEND;
    }

    /**
     * Load the truck with a {@link SyncFuture}.
     */
    void load(final SyncFuture syncFuture) {
        LOG.debug("loadSync: {}", syncFuture.toString());
        this.sync = syncFuture;
        this.type = Type.SYNC;
    }

    /** Returns the type of this truck's payload. */
    Type type() {
        return type;
    }

    /**
     * Unload the truck of its {@link FSWALEntry} payload. The internal reference is
     * released.
     */
    FSWALEntry unloadAppend() {
        FSWALEntry entry = this.entry;
        LOG.debug("unloadAppend: {}", entry.toString());
        this.entry = null;
        this.type = Type.EMPTY;
        return entry;
    }

    /**
     * Unload the truck of its {@link SyncFuture} payload. The internal reference is
     * released.
     */
    SyncFuture unloadSync() {
        SyncFuture sync = this.sync;
        this.sync = null;
        this.type = Type.EMPTY;
        LOG.debug("unloadSync: {}", sync.toString());
        return sync;
    }
}