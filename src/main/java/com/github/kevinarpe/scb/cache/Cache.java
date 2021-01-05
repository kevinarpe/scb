package com.github.kevinarpe.scb.cache;

import javax.annotation.concurrent.ThreadSafe;

/**
 * ThreadSafe?  Implementations of this interface must be thread-safe.
 *
 * @author Kevin Connor ARPE (kevinarpe@gmail.com)
 */
@ThreadSafe
public interface Cache<TKey, TValue> {

    /**
     * Retrieves a value mapped by key from a cache.  If the key is unmapped, then a mapping function is called and the
     * key-value pair is stored in the cache.  The mapping function is guaranteed to only be called <b>once</b> for each
     * key -- Multiple threads are safe.
     *
     * @param key
     *        must not be {@code null}
     *
     * @return never {@code null}
     *
     * @throws NullPointerException
     *         if {@code key} is {@code null}
     * @throws RuntimeException
     *         throws by mapping function when key is unmapped
     */
    TValue get(TKey key);
}
