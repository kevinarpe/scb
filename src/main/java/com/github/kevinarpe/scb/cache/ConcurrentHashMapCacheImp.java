package com.github.kevinarpe.scb.cache;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * This is a simple implementation that uses {@link ConcurrentHashMap} and calls
 * {@link ConcurrentHashMap#computeIfAbsent(Object, Function)}.
 * <p>
 * For null keys or values, please use {@link Optional}.
 *
 * @author Kevin Connor ARPE (kevinarpe@gmail.com)
 *
 * @see CacheImp
 * @see ProbablySlowerCacheImp
 * @see LinkedHashMapCacheImp
 * @see LinkedHashMapCacheImp2
 */
@ThreadSafe
public final class ConcurrentHashMapCacheImp<TKey, TValue>
implements Cache<TKey, TValue> {

    private final Function<TKey, TValue> getFunc;
    private final ConcurrentHashMap<TKey, TValue> cacheConcurrentHashMap;

    public ConcurrentHashMapCacheImp(Function<TKey, TValue> getFunc) {

        Objects.requireNonNull(getFunc);
        this.getFunc = (TKey key) -> {
            @Nullable
            final TValue value = getFunc.apply(key);
            if (null == value) {
                // Intentional: Add key to exception message for easier debugging.  :)
                throw new NullPointerException("Key [" + key + "] maps to a null value");
            }
            return value;
        };
        this.cacheConcurrentHashMap = new ConcurrentHashMap<>();
    }

    @Override
    public TValue get(TKey key) {

        Objects.requireNonNull(key);

        final TValue value = cacheConcurrentHashMap.computeIfAbsent(key, getFunc);
        return value;
    }
}
