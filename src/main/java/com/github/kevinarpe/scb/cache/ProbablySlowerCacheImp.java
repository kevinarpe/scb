package com.github.kevinarpe.scb.cache;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import java.util.LinkedHashMap;
import java.util.Objects;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;

/**
 * This is an alternative implementation.  Compared to {@link CacheImp}, it uses {@link ReentrantReadWriteLock}, which
 * usually has worse performance, and does not allow read locks to be upgraded to write locks.
 *
 * @author Kevin Connor ARPE (kevinarpe@gmail.com)
 *
 * @see CacheImp
 * @see ConcurrentHashMapCacheImp
 * @see LinkedHashMapCacheImp
 * @see LinkedHashMapCacheImp2
 */
@ThreadSafe
public final class ProbablySlowerCacheImp<TKey, TValue>
implements Cache<TKey, TValue> {

    private final IsFairLock isFairLock;
    private final Function<TKey, TValue> getFunc;
    private final ReentrantReadWriteLock readWriteLock;
    private final LinkedHashMap<TKey, TValue> cacheLinkedHashMap;

    public ProbablySlowerCacheImp(Function<TKey, TValue> getFunc) {
        this(IsFairLock.JAVA_DEFAULT, getFunc);
    }

    public ProbablySlowerCacheImp(IsFairLock isFairLock, Function<TKey, TValue> getFunc) {

        this.isFairLock = Objects.requireNonNull(isFairLock);
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
        this.readWriteLock = new ReentrantReadWriteLock(isFairLock.booleanValue);
        this.cacheLinkedHashMap = new LinkedHashMap<>();
    }

    @Override
    public TValue get(TKey key) {

        Objects.requireNonNull(key);

        boolean isReadLocked = true;
        readWriteLock.readLock().lock();
        try {
            // Small scope to capture local var 'value'
            {
                @Nullable
                final TValue value = cacheLinkedHashMap.get(key);
                if (null != value) {
                    return value;
                }
            }
            // This R/W lock does not support upgrade from read to write.
            readWriteLock.readLock().unlock();
            isReadLocked = false;
            // WARNING: Between the read unlock and write lock, another thread may capture this write lock first.
            // Thus, below, we need to check *again* if the key is already mapped.
            readWriteLock.writeLock().lock();
            try {
                // See "WARNING" above to understand why we need this duplicate check.
                {
                    @Nullable
                    final TValue value = cacheLinkedHashMap.get(key);
                    if (null != value) {
                        return value;
                    }
                }
                final TValue value = getFunc.apply(key);
                cacheLinkedHashMap.put(key, value);
                return value;
            }
            finally {
                readWriteLock.writeLock().unlock();
            }
        }
        finally {
            if (isReadLocked) {
                readWriteLock.readLock().unlock();
            }
        }
    }
}
