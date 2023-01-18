package org.waterme7on.hbase.regionserver;

/**
 * Reasons we flush.
 * 
 * @see MemStoreFlusher
 * @see FlushRequester
 */
enum FlushType {
    NORMAL,
    ABOVE_ONHEAP_LOWER_MARK, /*
                              * happens due to lower mark breach of onheap memstore settings An
                              * offheap memstore can even breach the onheap_lower_mark
                              */
    ABOVE_ONHEAP_HIGHER_MARK, /*
                               * happens due to higher mark breach of onheap memstore settings An
                               * offheap memstore can even breach the onheap_higher_mark
                               */
    ABOVE_OFFHEAP_LOWER_MARK, /* happens due to lower mark breach of offheap memstore settings */
    ABOVE_OFFHEAP_HIGHER_MARK;/* /* happens due to higer mark breach of offheap memstore settings */
}
