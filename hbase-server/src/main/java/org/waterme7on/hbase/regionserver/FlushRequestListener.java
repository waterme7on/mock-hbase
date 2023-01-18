package org.waterme7on.hbase.regionserver;

/**
 * Listener which will get notified regarding flush requests of regions.
 */
public interface FlushRequestListener {

    /**
     * Callback which will get called when a flush request is made for a region.
     * 
     * @param type   The type of flush. (ie. Whether a normal flush or flush because
     *               of global heap
     *               preassure)
     * @param region The region for which flush is requested
     */
    void flushRequested(FlushType type, Region region);
}
