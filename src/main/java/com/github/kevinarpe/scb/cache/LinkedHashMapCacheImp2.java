package com.github.kevinarpe.scb.cache;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import java.util.LinkedHashMap;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

/**
 * This is a simple implementation that uses {@link LinkedHashMap} and does <b>not</b> call
 * {@link LinkedHashMap#computeIfAbsent(Object, Function)}.
 * <p>
 * For null keys or values, please use {@link Optional}.
 *
 * @author Kevin Connor ARPE (kevinarpe@gmail.com)
 *
 * @see CacheImp
 * @see ProbablySlowerCacheImp
 * @see ConcurrentHashMapCacheImp
 * @see LinkedHashMapCacheImp
 */
@ThreadSafe
public final class LinkedHashMapCacheImp2<TKey, TValue>
implements Cache<TKey, TValue> {

    private final Function<TKey, TValue> getFunc;
    private final LinkedHashMap<TKey, TValue> cacheLinkedHashMap;

    public LinkedHashMapCacheImp2(Function<TKey, TValue> getFunc) {

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
        this.cacheLinkedHashMap = new LinkedHashMap<>();
    }

    @Override
    public TValue get(TKey key) {

        Objects.requireNonNull(key);
        synchronized (cacheLinkedHashMap) {
            // This is a simple replacement for Map.computeIfAbsent(), but is less efficient because we need to do TWO
            // key lookups which is slow.  :)
            @Nullable
            final TValue nullableValue = cacheLinkedHashMap.get(key);
            if (null != nullableValue) {
                return nullableValue;
            }
            else {
                final TValue value = getFunc.apply(key);
                cacheLinkedHashMap.put(key, value);
                return value;
            }
        }
    }
}
