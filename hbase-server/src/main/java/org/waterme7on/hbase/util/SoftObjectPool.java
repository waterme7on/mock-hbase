package org.waterme7on.hbase.util;

import java.lang.ref.Reference;
import java.lang.ref.SoftReference;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * A {@code SoftReference} based shared object pool. The objects are kept in
 * soft references and
 * associated with keys which are identified by the {@code equals} method. The
 * objects are created
 * by ObjectFactory on demand. The object creation is expected to be
 * lightweight, and the objects
 * may be excessively created and discarded. Thread safe.
 */
public class SoftObjectPool<K, V> extends ObjectPool<K, V> {

    public SoftObjectPool(ObjectFactory<K, V> objectFactory) {
        super(objectFactory);
    }

    public SoftObjectPool(ObjectFactory<K, V> objectFactory, int initialCapacity) {
        super(objectFactory, initialCapacity);
    }

    public SoftObjectPool(ObjectFactory<K, V> objectFactory, int initialCapacity,
            int concurrencyLevel) {
        super(objectFactory, initialCapacity, concurrencyLevel);
    }

    @Override
    public Reference<V> createReference(K key, V obj) {
        return new SoftObjectReference(key, obj);
    }

    private class SoftObjectReference extends SoftReference<V> {
        final K key;

        SoftObjectReference(K key, V obj) {
            super(obj, staleRefQueue);
            this.key = key;
        }
    }

    @Override
    public K getReferenceKey(Reference<V> ref) {
        return ((SoftObjectReference) ref).key;
    }

}
