package org.waterme7on.hbase.regionserver.store;

import org.apache.hadoop.hbase.regionserver.MemStoreFlusher;
import org.apache.hadoop.hbase.regionserver.StoreConfigInformation;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionPolicy;
import org.apache.hadoop.hbase.regionserver.compactions.ExploringCompactionPolicy;

import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.waterme7on.hbase.regionserver.HStore;
import org.waterme7on.hbase.regionserver.StoreContext;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StoreEngine {

    private static final Logger LOG = LoggerFactory.getLogger(StoreEngine.class);
    private final ReadWriteLock storeLock = new ReentrantReadWriteLock();
    protected StoreFileManager storeFileManager;
    protected CompactionPolicy compactionPolicy;
    private Configuration conf;
    private StoreContext ctx;

    public StoreEngine(Configuration conf, HStore store,
            CellComparator cellComparator) {
        this.conf = conf;
        this.ctx = store.getStoreContext();
        this.compactionPolicy = new ExploringCompactionPolicy(conf, store);
        storeFileManager = new DefaultStoreFileManager(
                cellComparator, StoreFileComparators.SEQ_ID, conf,
                this.compactionPolicy.getConf());
    }

    /** Returns Store file manager to use. */
    public StoreFileManager getStoreFileManager() {
        return this.storeFileManager;
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
