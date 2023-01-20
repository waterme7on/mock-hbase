package org.waterme7on.hbase.regionserver.store;

import org.apache.hadoop.hbase.regionserver.MemStoreFlusher;

import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.waterme7on.hbase.regionserver.HStore;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CellComparator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StoreEngine {

    private static final Logger LOG = LoggerFactory.getLogger(StoreEngine.class);
    private final ReadWriteLock storeLock = new ReentrantReadWriteLock();

    // private DefaultStoreFlusher flusher;
    // private DefaultStoreFileManager storeFileManager;

    public StoreEngine(Configuration conf, HStore store,
            CellComparator cellComparator) {

    }

    public void readLock() {
        storeLock.readLock().lock();
    }

    /**
     * Release read lock of this store.
     */
    public void readUnlock() {
        storeLock.readLock().unlock();
    }

    /**
     * Acquire write lock of this store.
     */
    public void writeLock() {
        storeLock.writeLock().lock();
    }

    /**
     * Release write lock of this store.
     */
    public void writeUnlock() {
        storeLock.writeLock().unlock();
    }
}
