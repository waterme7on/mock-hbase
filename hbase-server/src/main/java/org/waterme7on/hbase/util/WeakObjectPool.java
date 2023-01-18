package org.waterme7on.hbase.util;

import java.lang.ref.Reference;
import java.lang.ref.WeakReference;

/**
 * A {@code WeakReference} based shared object pool. The objects are kept in
 * weak references and
 * associated with keys which are identified by the {@code equals} method. The
 * objects are created
 * by {@link org.apache.hadoop.hbase.util.ObjectPool.ObjectFactory} on demand.
 * The object creation
 * is expected to be lightweight, and the objects may be excessively created and
 * discarded. Thread
 * safe.
 */
public class WeakObjectPool<K, V> extends ObjectPool<K, V> {

    public WeakObjectPool(ObjectFactory<K, V> objectFactory) {
        super(objectFactory);
    }

    public WeakObjectPool(ObjectFactory<K, V> objectFactory, int initialCapacity) {
        super(objectFactory, initialCapacity);
    }

    public WeakObjectPool(ObjectFactory<K, V> objectFactory, int initialCapacity,
            int concurrencyLevel) {
        super(objectFactory, initialCapacity, concurrencyLevel);
    }

    @Override
    public Reference<V> createReference(K key, V obj) {
        return new WeakObjectReference(key, obj);
    }

    private class WeakObjectReference extends WeakReference<V> {
        final K key;

        WeakObjectReference(K key, V obj) {
            super(obj, staleRefQueue);
            this.key = key;
        }
    }

    @Override
    public K getReferenceKey(Reference<V> ref) {
        return ((WeakObjectReference) ref).key;
    }

}
