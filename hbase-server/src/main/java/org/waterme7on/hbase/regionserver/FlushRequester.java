package org.waterme7on.hbase.regionserver;

import java.util.List;

/**
 * Request a flush.
 */
public interface FlushRequester {
    /**
     * Tell the listener the cache needs to be flushed.
     * 
     * @param region the Region requesting the cache flush
     * @return true if our region is added into the queue, false otherwise
     */
    boolean requestFlush(HRegion region, FlushLifeCycleTracker tracker);

    /**
     * Tell the listener the cache needs to be flushed.
     * 
     * @param region   the Region requesting the cache flush
     * @param families stores of region to flush, if null then use flush policy
     * @return true if our region is added into the queue, false otherwise
     */
    boolean requestFlush(HRegion region, List<byte[]> families, FlushLifeCycleTracker tracker);

    /**
     * Tell the listener the cache needs to be flushed after a delay
     * 
     * @param region the Region requesting the cache flush
     * @param delay  after how much time should the flush happen
     * @return true if our region is added into the queue, false otherwise
     */
    boolean requestDelayedFlush(HRegion region, long delay);

    /**
     * Register a FlushRequestListener
     */
    void registerFlushRequestListener(final FlushRequestListener listener);

    /**
     * Unregister the given FlushRequestListener
     * 
     * @return true when passed listener is unregistered successfully.
     */
    public boolean unregisterFlushRequestListener(final FlushRequestListener listener);

    /**
     * Sets the global memstore limit to a new size.
     */
    public void setGlobalMemStoreLimit(long globalMemStoreSize);
}
