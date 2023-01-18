package org.waterme7on.hbase.util;

import java.lang.ref.Reference;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.hadoop.hbase.util.Threads;

/**
 * Allows multiple concurrent clients to lock on a numeric id with
 * ReentrantReadWriteLock. The
 * intended usage for read lock is as follows:
 *
 * <pre>
 * ReentrantReadWriteLock lock = idReadWriteLock.getLock(id);
 * try {
 *     lock.readLock().lock();
 *     // User code.
 * } finally {
 *     lock.readLock().unlock();
 * }
 * </pre>
 *
 * For write lock, use lock.writeLock()
 */
public class IdReadWriteLock<T> {
    // The number of lock we want to easily support. It's not a maximum.
    private static final int NB_CONCURRENT_LOCKS = 1000;
    /**
     * The pool to get entry from, entries are mapped by {@link Reference} and will
     * be automatically
     * garbage-collected by JVM
     */
    private final ObjectPool<T, ReentrantReadWriteLock> lockPool;
    private final ReferenceType refType;

    public IdReadWriteLock() {
        this(ReferenceType.WEAK);
    }

    /**
     * Constructor of IdReadWriteLock
     * 
     * @param referenceType type of the reference used in lock pool,
     *                      {@link ReferenceType#WEAK} by
     *                      default. Use {@link ReferenceType#SOFT} if the key set
     *                      is limited and the
     *                      locks will be reused with a high frequency
     */
    public IdReadWriteLock(ReferenceType referenceType) {
        this.refType = referenceType;
        switch (referenceType) {
            case SOFT:
                lockPool = new SoftObjectPool<>(new ObjectPool.ObjectFactory<T, ReentrantReadWriteLock>() {
                    @Override
                    public ReentrantReadWriteLock createObject(T id) {
                        return new ReentrantReadWriteLock();
                    }
                }, NB_CONCURRENT_LOCKS);
                break;
            case WEAK:
            default:
                lockPool = new WeakObjectPool<>(new ObjectPool.ObjectFactory<T, ReentrantReadWriteLock>() {
                    @Override
                    public ReentrantReadWriteLock createObject(T id) {
                        return new ReentrantReadWriteLock();
                    }
                }, NB_CONCURRENT_LOCKS);
        }
    }

    public static enum ReferenceType {
        WEAK,
        SOFT
    }

    /**
     * Get the ReentrantReadWriteLock corresponding to the given id
     * 
     * @param id an arbitrary number to identify the lock
     */
    public ReentrantReadWriteLock getLock(T id) {
        lockPool.purge();
        ReentrantReadWriteLock readWriteLock = lockPool.get(id);
        return readWriteLock;
    }

    /** For testing */
    int purgeAndGetEntryPoolSize() {
        gc();
        Threads.sleep(200);
        lockPool.purge();
        return lockPool.size();
    }

    private void gc() {
        System.gc();
    }

    public void waitForWaiters(T id, int numWaiters) throws InterruptedException {
        for (ReentrantReadWriteLock readWriteLock;;) {
            readWriteLock = lockPool.get(id);
            if (readWriteLock != null) {
                synchronized (readWriteLock) {
                    if (readWriteLock.getQueueLength() >= numWaiters) {
                        return;
                    }
                }
            }
            Thread.sleep(50);
        }
    }

    public ReferenceType getReferenceType() {
        return this.refType;
    }
}
