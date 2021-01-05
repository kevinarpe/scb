package com.github.kevinarpe.scb.cache;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.locks.StampedLock;
import java.util.function.Function;

/**
 * This is the fastest, most efficient implementation I could think of that does not call
 * {@link Map#computeIfAbsent(Object, Function)}.
 *
 * @author Kevin Connor ARPE (kevinarpe@gmail.com)
 *
 * @see ProbablySlowerCacheImp
 * @see ConcurrentHashMapCacheImp
 * @see LinkedHashMapCacheImp
 * @see LinkedHashMapCacheImp2
 */
@ThreadSafe
public final class CacheImp<TKey, TValue>
implements Cache<TKey, TValue> {

    // package-private for testing
    interface _IStampedLock {

        long readLock();
        long tryConvertToWriteLock(long anyTypeOfLockStamp);
        void unlockRead(long readLockStamp);
        long writeLock();
        void unlock(long anyTypeOfLockStamp);
    }

    // package-private for testing
    static final class _StampedLockImp
    implements _IStampedLock {

        private final StampedLock stampedLock;

        // package-private for testing
        _StampedLockImp() {
            this.stampedLock = new StampedLock();
        }

        @Override
        public long readLock() {
            return stampedLock.readLock();
        }

        @Override
        public long tryConvertToWriteLock(final long anyTypeOfLockStamp) {
            return stampedLock.tryConvertToWriteLock(anyTypeOfLockStamp);
        }

        @Override
        public void unlockRead(final long readLockStamp) {
            stampedLock.unlockRead(readLockStamp);
        }

        @Override
        public long writeLock() {
            return stampedLock.writeLock();
        }

        @Override
        public void unlock(final long anyTypeOfLockStamp) {
            stampedLock.unlock(anyTypeOfLockStamp);
        }
    }

    private final Function<TKey, TValue> getFunc;
    private final _IStampedLock stampedLock;
    private final LinkedHashMap<TKey, TValue> cacheLinkedHashMap;

    public CacheImp(Function<TKey, TValue> getFunc) {

        this(new _StampedLockImp(), getFunc);
    }

    // package-private for testing
    CacheImp(_IStampedLock stampedLock, Function<TKey, TValue> getFunc) {

        this.stampedLock = Objects.requireNonNull(stampedLock);
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

        final long readLockStamp = stampedLock.readLock();
        long unlockStamp = readLockStamp;
        try {
            // Intentional: Do not use a 'while (true)' loop here.  Why?  It provides no indication about the expected
            // number of iterations. Instead, use a loop pattern that clearly indicates there are *AT A MAXIMUM* two iterations.
            for (int i = 0; ; ++i) {
                if (2 == i) {
                    throw new IllegalStateException();
                }
                @Nullable
                final TValue nullableValue = cacheLinkedHashMap.get(key);
                if (null != nullableValue) {
                    return nullableValue;
                }
                final long writeLockStamp = stampedLock.tryConvertToWriteLock(unlockStamp);
                // Is write lock valid?
                if (0L != writeLockStamp) {
                    unlockStamp = writeLockStamp;
                    final TValue value = getFunc.apply(key);
                    cacheLinkedHashMap.put(key, value);
                    return value;
                }
                // Write lock is not valid
                stampedLock.unlockRead(unlockStamp);
                unlockStamp = stampedLock.writeLock();
            }
        }
        finally {
            stampedLock.unlock(unlockStamp);
        }
    }
}
