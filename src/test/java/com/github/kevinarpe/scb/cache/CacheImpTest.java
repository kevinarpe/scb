package com.github.kevinarpe.scb.cache;

import org.testng.Assert;
import org.testng.annotations.Test;

import javax.annotation.Nullable;
import java.util.LinkedHashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author Kevin Connor ARPE (kevinarpe@gmail.com)
 */
public class CacheImpTest {

    // Order is important.  Please do not change!  This matches the implementation, so it is easier to read test code. :)
    public enum _Action {

        _1_REQUIRED_BEFORE_readLock,
        _1_REQUIRED_AFTER_readLock,
        _2_OPTIONAL_BEFORE_tryConvertToWriteLock,
        _2_OPTIONAL_AFTER_tryConvertToWriteLock,
        _3_OPTIONAL_BEFORE_unlockRead,
        _3_OPTIONAL_AFTER_unlockRead,
        _4_OPTIONAL_BEFORE_writeLock,
        _4_OPTIONAL_AFTER_writeLock,
        _5_REQUIRED_BEFORE_unlock,
        _5_REQUIRED_AFTER_unlock,
    }

    private static final class _ThreadAction {

        public final String threadName;
        public final _Action action;

        private _ThreadAction(String threadName, _Action action) {

            this.threadName = threadName;
            this.action = action;
        }

        @Override
        public String toString() {
            final String x = String.format("%s: %s", threadName, action.name());
            return x;
        }
    }

    private static final class _StampedLockImp
    implements CacheImp._IStampedLock {

        private static final class _ThreadData {

            public final String threadName;
            public AtomicReference<_ThreadAction> atomicNullableThreadAction;
            @Nullable
            public volatile _Action nullableVolatileWaitExpectedAction;
            public volatile boolean volatileSyncNotifyFlag;
            public final Object syncNotifyLock;

            private _ThreadData(String threadName) {

                this.threadName = threadName;
                this.atomicNullableThreadAction = new AtomicReference<>();
                this.nullableVolatileWaitExpectedAction = null;
                this.volatileSyncNotifyFlag = false;
                this.syncNotifyLock = new Object();
            }
        }

        // Intentional: Use as a delegate to get better test coverage.
        private final CacheImp._StampedLockImp stampedLock;
        private final ConcurrentHashMap<String, _ThreadData> threadName_To_Data_ConcurrentMap;

        private _StampedLockImp() {

            this.stampedLock = new CacheImp._StampedLockImp();
            this.threadName_To_Data_ConcurrentMap = new ConcurrentHashMap<>();
        }

        // @Blocking
        public _Action awaitWaitExpectedAction(String threadName) {

            final _ThreadData threadData = _getThreadData(threadName);
            int totalSleepMillis = 0;
            _Action expectedAction = null;
            while (null == (expectedAction = threadData.nullableVolatileWaitExpectedAction)) {
                final int sleepMillis = 100;
                try {
                    Thread.sleep(sleepMillis);
                }
                catch (InterruptedException e) {
                    throw new IllegalStateException(e);
                }
                totalSleepMillis += sleepMillis;
                if (totalSleepMillis > 10_000) {
                    throw new IllegalStateException("Thread [" + threadName + "]: Slept too long!");
                }
            }
            return expectedAction;
        }

        // @Blocking
        public void awaitWaitExpectedAction_IS_NULL(String threadName) {

            final _ThreadData threadData = _getThreadData(threadName);
            int totalSleepMillis = 0;
            while (null != threadData.nullableVolatileWaitExpectedAction) {
                final int sleepMillis = 100;
                try {
                    Thread.sleep(sleepMillis);
                }
                catch (InterruptedException e) {
                    throw new IllegalStateException(e);
                }
                totalSleepMillis += sleepMillis;
                if (totalSleepMillis > 10_000) {
                    throw new IllegalStateException("Thread [" + threadName + "]: Slept too long!");
                }
            }
        }

        // @Blocking
        private void _await(_Action expectedAction) {

            final _ThreadData threadData = _getCurrentThreadData();
            System.out.printf("%s: Begin waiting on %s%n", threadData.threadName, expectedAction.name());
            _setWaitExpectedAction(threadData, expectedAction);

            synchronized (threadData) {
                // Remember: We must use a condition variable with a loop to protect against spurious wake-ups from wait().
                while (false == _isAwaitConditionMatch(expectedAction, threadData)) {

                    _wait(threadData, threadData.threadName + ".threadData");
                }
                // Before or after sync notify?  Not sure!
                threadData.atomicNullableThreadAction.set(null);
                threadData.volatileSyncNotifyFlag = true;

                synchronized (threadData.syncNotifyLock) {

                    threadData.syncNotifyLock.notify();
                }
                threadData.nullableVolatileWaitExpectedAction = null;
            }
            System.out.printf("%s: End   waiting on %s%n", threadData.threadName, expectedAction.name());
        }

        // @NonBlocking
        private _ThreadData _getCurrentThreadData() {

            final _ThreadData x = _getThreadData(Thread.currentThread().getName());
            return x;
        }

        // @NonBlocking
        private _ThreadData _getThreadData(String threadName) {

            final _ThreadData x =
                threadName_To_Data_ConcurrentMap.computeIfAbsent(threadName,
                    any -> new _ThreadData(threadName));
            return x;
        }

        // @NonBlocking
        private void _setWaitExpectedAction(_ThreadData threadData, _Action expectedAction) {

            if (null != threadData.nullableVolatileWaitExpectedAction) {

                throw new IllegalStateException("Thread [" + threadData.threadName + "]: "
                    + "null != " + threadData.nullableVolatileWaitExpectedAction);
            }
            threadData.nullableVolatileWaitExpectedAction = expectedAction;
        }

        // @NonBlocking
        private boolean _isAwaitConditionMatch(_Action expectedAction, _ThreadData threadData) {

            @Nullable
            final _ThreadAction nullableThreadAction = threadData.atomicNullableThreadAction.get();
            if (null != nullableThreadAction) {
                if (expectedAction.equals(nullableThreadAction.action)
                    &&
                    Thread.currentThread().getName().equals(nullableThreadAction.threadName))
                {
                    return true;
                }
                throw new IllegalStateException(String.format("Expected [%s.%s], but found [%s.%s]",
                    Thread.currentThread().getName(), expectedAction.name(),
                    nullableThreadAction.threadName, nullableThreadAction.action));
            }
            return false;
        }

        // @Blocking
        private void _wait(Object lock, String description) {

            System.out.printf("%s: %s.wait(): Begin%n", Thread.currentThread().getName(), description);
            try {
                lock.wait();
            }
            catch (InterruptedException e) {
                throw new IllegalStateException(e);
            }
            System.out.printf("%s: %s.wait(): End%n", Thread.currentThread().getName(), description);
        }

        // @NonBlocking
        public void asyncNotify(_ThreadAction threadAction) {

            final _ThreadData threadData = _getThreadData(threadAction.threadName);

            synchronized (threadData) {
                @Nullable
                final _ThreadAction nullablePrevThreadAction =
                    threadData.atomicNullableThreadAction.compareAndExchange(null, threadAction);
                if (null != nullablePrevThreadAction) {
                    throw new IllegalStateException("Thread [" + threadData.threadName + "]: "
                        + "null != " + nullablePrevThreadAction);
                }
                threadData.volatileSyncNotifyFlag = false;
                System.out.printf("%s: %s.notify()%n", Thread.currentThread().getName(), threadData.threadName);
                threadData.notify();
            }
        }

        // @Blocking
        public void waitNotifyDone(String threadName) {

            final _ThreadData threadData = _getThreadData(threadName);

            synchronized (threadData.syncNotifyLock) {

                while (false == threadData.volatileSyncNotifyFlag) {

                    _wait(threadData.syncNotifyLock, threadName + ".syncNotifyLock");
                }
            }
        }

        // @Blocking
        public void blockingNotify(_ThreadAction threadAction) {

            asyncNotify(threadAction);
            waitNotifyDone(threadAction.threadName);
        }

        @Override
        public long readLock() {

            _await(_Action._1_REQUIRED_BEFORE_readLock);
            try {
                return stampedLock.readLock();
            }
            finally {
                _await(_Action._1_REQUIRED_AFTER_readLock);
            }
        }

        @Override
        public long tryConvertToWriteLock(final long anyTypeOfLockStamp) {

            _await(_Action._2_OPTIONAL_BEFORE_tryConvertToWriteLock);
            try {
                return stampedLock.tryConvertToWriteLock(anyTypeOfLockStamp);
            }
            finally {
                _await(_Action._2_OPTIONAL_AFTER_tryConvertToWriteLock);
            }
        }

        @Override
        public void unlockRead(final long readLockStamp) {

            _await(_Action._3_OPTIONAL_BEFORE_unlockRead);
            try {
                stampedLock.unlockRead(readLockStamp);
            }
            finally {
                _await(_Action._3_OPTIONAL_AFTER_unlockRead);
            }
        }

        @Override
        public long writeLock() {

            _await(_Action._4_OPTIONAL_BEFORE_writeLock);
            try {
                return stampedLock.writeLock();
            }
            finally {
                _await(_Action._4_OPTIONAL_AFTER_writeLock);
            }
        }

        @Override
        public void unlock(final long anyTypeOfLockStamp) {

            _await(_Action._5_REQUIRED_BEFORE_unlock);
            try {
                stampedLock.unlock(anyTypeOfLockStamp);
            }
            finally {
                _await(_Action._5_REQUIRED_AFTER_unlock);
            }
        }
    }

    @Test
    public void passSimple() {

        final LinkedHashMap<Integer, String> keyValueMap = new LinkedHashMap<>();
        final Cache<Integer, String> classUnderTest =
            new CacheImp<>(
                (Integer key) -> {
                    final String value = key.toString();
                    @Nullable
                    final String nullableOldValue = keyValueMap.put(key, value);
                    if (null != nullableOldValue) {
                        throw new IllegalStateException(
                            String.format("Key [%d] mapped to value TWICE! [%s] & [%s]", key, nullableOldValue, value));
                    }
                    return value;
                });
        {
            final String _1 = classUnderTest.get(1);
            Assert.assertEquals(_1, "1");
            Assert.assertSame(_1, keyValueMap.get(1));
        }
        // Now demonstrate the key mapper function is *NOT* called a second time.
        {
            final String _1 = classUnderTest.get(1);
            Assert.assertEquals(_1, "1");
            Assert.assertSame(_1, keyValueMap.get(1));
        }
        {
            final String _2 = classUnderTest.get(2);
            Assert.assertEquals(_2, "2");
            Assert.assertSame(_2, keyValueMap.get(2));
        }
        Assert.assertEquals(keyValueMap.size(), 2);
    }

    private static final class _TestData {

        public final _StampedLockImp stampedLock = new _StampedLockImp();

        public final Cache<Integer, String> classUnderTest =
            new CacheImp<>(stampedLock,
                (Integer any) -> Thread.currentThread().getName());

        public final LinkedHashMap<Thread, Throwable> uncaughtExceptionMap = new LinkedHashMap<>();
        public final Thread.UncaughtExceptionHandler uncaughtExceptionHandler =
            (Thread t, Throwable e) -> {
                System.err.printf("%s: ", t.getName());
                e.printStackTrace();
                uncaughtExceptionMap.put(t, e);
            };
        public final int key = 1234;
        public final String[] readerResultRef = {null};
        public final Thread readerThread =
            new Thread(
                () -> {
                    readerResultRef[0] = classUnderTest.get(key);
                    // @DebugBreakpoint
                    int dummy = 1;
                });
        public final String readerThreadName = "reader";

        public final String[] writerResultRef = {null};
        public final Thread writerThread =
            new Thread(
                () -> {
                    writerResultRef[0] = classUnderTest.get(key);
                    // @DebugBreakpoint
                    int dummy = 1;
                });
        public final String writerThreadName = "writer";

        public _TestData() {

            readerThread.setName(readerThreadName);
            readerThread.setUncaughtExceptionHandler(uncaughtExceptionHandler);

            writerThread.setName(writerThreadName);
            writerThread.setUncaughtExceptionHandler(uncaughtExceptionHandler);
        }
    }

    @Test
    public void passWhenWriterThreadBlocksReaderThread()
    throws InterruptedException {

        _TestData td = new _TestData();

        td.readerThread.start();
        td.writerThread.start();

        Assert.assertEquals(td.stampedLock.awaitWaitExpectedAction(td.readerThreadName), _Action._1_REQUIRED_BEFORE_readLock);
        Assert.assertEquals(td.stampedLock.awaitWaitExpectedAction(td.writerThreadName), _Action._1_REQUIRED_BEFORE_readLock);

        td.stampedLock.blockingNotify(new _ThreadAction(td.writerThreadName, _Action._1_REQUIRED_BEFORE_readLock));
        // writerThread attempts to acquire shared readLock
        Assert.assertEquals(td.stampedLock.awaitWaitExpectedAction(td.writerThreadName), _Action._1_REQUIRED_AFTER_readLock);
        // writerThread has acquired shared readLock
        td.stampedLock.blockingNotify(new _ThreadAction(td.writerThreadName, _Action._1_REQUIRED_AFTER_readLock));

        td.stampedLock.blockingNotify(new _ThreadAction(td.writerThreadName, _Action._2_OPTIONAL_BEFORE_tryConvertToWriteLock));
        // writerThread attempts to acquire exclusive writeLock
        Assert.assertEquals(td.stampedLock.awaitWaitExpectedAction(td.writerThreadName), _Action._2_OPTIONAL_AFTER_tryConvertToWriteLock);
        // writerThread has acquired exclusive writeLock
        td.stampedLock.blockingNotify(new _ThreadAction(td.writerThreadName, _Action._2_OPTIONAL_AFTER_tryConvertToWriteLock));

        Assert.assertEquals(td.stampedLock.awaitWaitExpectedAction(td.writerThreadName), _Action._5_REQUIRED_BEFORE_unlock);
        // writerThread has updated cache

        td.stampedLock.blockingNotify(new _ThreadAction(td.readerThreadName, _Action._1_REQUIRED_BEFORE_readLock));
        // readerThread attempts to acquire shared readLock

        // Demonstrate readerThread is blocked when acquiring shared readLock
        Thread.sleep(1000);
        td.stampedLock.awaitWaitExpectedAction_IS_NULL(td.readerThreadName);

        // Demonstrate our threads are not complete and have not updated result refs.
        Assert.assertNull(td.readerResultRef[0]);
        Assert.assertNull(td.writerResultRef[0]);

        td.stampedLock.blockingNotify(new _ThreadAction(td.writerThreadName, _Action._5_REQUIRED_BEFORE_unlock));
        // writerThread begins to release exclusive writeLock
        Assert.assertEquals(td.stampedLock.awaitWaitExpectedAction(td.writerThreadName), _Action._5_REQUIRED_AFTER_unlock);
        // writerThread has released exclusive writeLock
        td.stampedLock.blockingNotify(new _ThreadAction(td.writerThreadName, _Action._5_REQUIRED_AFTER_unlock));

        Assert.assertEquals(td.stampedLock.awaitWaitExpectedAction(td.readerThreadName), _Action._1_REQUIRED_AFTER_readLock);
        // readerThread has acquired shared readLock
        td.stampedLock.blockingNotify(new _ThreadAction(td.readerThreadName, _Action._1_REQUIRED_AFTER_readLock));

        Assert.assertEquals(td.stampedLock.awaitWaitExpectedAction(td.readerThreadName), _Action._5_REQUIRED_BEFORE_unlock);
        // readerThread has read from cache

        td.stampedLock.blockingNotify(new _ThreadAction(td.readerThreadName, _Action._5_REQUIRED_BEFORE_unlock));
        // readerThread begins to release shared readLock
        Assert.assertEquals(td.stampedLock.awaitWaitExpectedAction(td.readerThreadName), _Action._5_REQUIRED_AFTER_unlock);
        // readerThread has released shared readLock
        td.stampedLock.blockingNotify(new _ThreadAction(td.readerThreadName, _Action._5_REQUIRED_AFTER_unlock));

        td.readerThread.join();
        td.writerThread.join();

        if (td.uncaughtExceptionMap.size() > 0) {
            throw new IllegalStateException(td.uncaughtExceptionMap.toString());
        }
        Assert.assertEquals(td.readerResultRef[0], td.writerThreadName);
        Assert.assertEquals(td.writerResultRef[0], td.writerThreadName);
    }

    @Test
    public void passWhenBothReaderAndWriterThreadsAcquireReadLock()
    throws InterruptedException {

        _TestData td = new _TestData();

        td.readerThread.start();
        td.writerThread.start();

        Assert.assertEquals(td.stampedLock.awaitWaitExpectedAction(td.readerThreadName), _Action._1_REQUIRED_BEFORE_readLock);
        Assert.assertEquals(td.stampedLock.awaitWaitExpectedAction(td.writerThreadName), _Action._1_REQUIRED_BEFORE_readLock);

        td.stampedLock.blockingNotify(new _ThreadAction(td.writerThreadName, _Action._1_REQUIRED_BEFORE_readLock));
        // writerThread attempts to acquire shared readLock
        td.stampedLock.blockingNotify(new _ThreadAction(td.readerThreadName, _Action._1_REQUIRED_BEFORE_readLock));
        // readerThread attempts to acquire shared readLock

        Assert.assertEquals(td.stampedLock.awaitWaitExpectedAction(td.writerThreadName), _Action._1_REQUIRED_AFTER_readLock);
        // writerThread has acquired shared readLock
        Assert.assertEquals(td.stampedLock.awaitWaitExpectedAction(td.readerThreadName), _Action._1_REQUIRED_AFTER_readLock);
        // readerThread has acquired shared readLock

        td.stampedLock.blockingNotify(new _ThreadAction(td.writerThreadName, _Action._1_REQUIRED_AFTER_readLock));
        td.stampedLock.blockingNotify(new _ThreadAction(td.readerThreadName, _Action._1_REQUIRED_AFTER_readLock));

        td.stampedLock.blockingNotify(new _ThreadAction(td.writerThreadName, _Action._2_OPTIONAL_BEFORE_tryConvertToWriteLock));
        // writerThread attempts to acquire exclusive writeLock
        Assert.assertEquals(td.stampedLock.awaitWaitExpectedAction(td.writerThreadName), _Action._2_OPTIONAL_AFTER_tryConvertToWriteLock);
        // writerThread has failed to acquire exclusive writeLock
        td.stampedLock.blockingNotify(new _ThreadAction(td.writerThreadName, _Action._2_OPTIONAL_AFTER_tryConvertToWriteLock));
        Assert.assertEquals(td.stampedLock.awaitWaitExpectedAction(td.writerThreadName), _Action._3_OPTIONAL_BEFORE_unlockRead);

        td.stampedLock.blockingNotify(new _ThreadAction(td.readerThreadName, _Action._2_OPTIONAL_BEFORE_tryConvertToWriteLock));
        // readerThread attempts to acquire exclusive writeLock
        Assert.assertEquals(td.stampedLock.awaitWaitExpectedAction(td.readerThreadName), _Action._2_OPTIONAL_AFTER_tryConvertToWriteLock);
        // readerThread has failed to acquire exclusive writeLock
        td.stampedLock.blockingNotify(new _ThreadAction(td.readerThreadName, _Action._2_OPTIONAL_AFTER_tryConvertToWriteLock));
        Assert.assertEquals(td.stampedLock.awaitWaitExpectedAction(td.readerThreadName), _Action._3_OPTIONAL_BEFORE_unlockRead);

        td.stampedLock.blockingNotify(new _ThreadAction(td.writerThreadName, _Action._3_OPTIONAL_BEFORE_unlockRead));
        // writerThread begins to release shared readLock
        Assert.assertEquals(td.stampedLock.awaitWaitExpectedAction(td.writerThreadName), _Action._3_OPTIONAL_AFTER_unlockRead);
        // writerThread has released shared readLock
        td.stampedLock.blockingNotify(new _ThreadAction(td.writerThreadName, _Action._3_OPTIONAL_AFTER_unlockRead));
        Assert.assertEquals(td.stampedLock.awaitWaitExpectedAction(td.writerThreadName), _Action._4_OPTIONAL_BEFORE_writeLock);

        td.stampedLock.blockingNotify(new _ThreadAction(td.writerThreadName, _Action._4_OPTIONAL_BEFORE_writeLock));
        // writerThread attempts to acquire exclusive writeLock

        // Demonstrate writerThread is blocked when acquiring exclusive writeLock
        Thread.sleep(1000);
        td.stampedLock.awaitWaitExpectedAction_IS_NULL(td.writerThreadName);

        td.stampedLock.blockingNotify(new _ThreadAction(td.readerThreadName, _Action._3_OPTIONAL_BEFORE_unlockRead));
        // readerThread begins to release shared readLock
        Assert.assertEquals(td.stampedLock.awaitWaitExpectedAction(td.readerThreadName), _Action._3_OPTIONAL_AFTER_unlockRead);
        // readerThread has released shared readLock
        td.stampedLock.blockingNotify(new _ThreadAction(td.readerThreadName, _Action._3_OPTIONAL_AFTER_unlockRead));

        Assert.assertEquals(td.stampedLock.awaitWaitExpectedAction(td.writerThreadName), _Action._4_OPTIONAL_AFTER_writeLock);
        // writerThread has acquired exclusive writeLock

        td.stampedLock.blockingNotify(new _ThreadAction(td.readerThreadName, _Action._4_OPTIONAL_BEFORE_writeLock));
        // readerThread attempts to acquire exclusive writeLock

        // Demonstrate readerThread is blocked when acquiring exclusive writeLock
        Thread.sleep(1000);
        td.stampedLock.awaitWaitExpectedAction_IS_NULL(td.readerThreadName);

        td.stampedLock.blockingNotify(new _ThreadAction(td.writerThreadName, _Action._4_OPTIONAL_AFTER_writeLock));
        Assert.assertEquals(td.stampedLock.awaitWaitExpectedAction(td.writerThreadName), _Action._2_OPTIONAL_BEFORE_tryConvertToWriteLock);

        td.stampedLock.blockingNotify(new _ThreadAction(td.writerThreadName, _Action._2_OPTIONAL_BEFORE_tryConvertToWriteLock));
        // writerThread attempts to acquire exclusive writeLock already held by writerThread
        Assert.assertEquals(td.stampedLock.awaitWaitExpectedAction(td.writerThreadName), _Action._2_OPTIONAL_AFTER_tryConvertToWriteLock);
        // writerThread has acquired exclusive writeLock already held by writerThread
        td.stampedLock.blockingNotify(new _ThreadAction(td.writerThreadName, _Action._2_OPTIONAL_AFTER_tryConvertToWriteLock));
        Assert.assertEquals(td.stampedLock.awaitWaitExpectedAction(td.writerThreadName), _Action._5_REQUIRED_BEFORE_unlock);
        // writerThread has updated cache

        td.stampedLock.blockingNotify(new _ThreadAction(td.writerThreadName, _Action._5_REQUIRED_BEFORE_unlock));
        // writerThread begins to release exclusive writeLock
        Assert.assertEquals(td.stampedLock.awaitWaitExpectedAction(td.writerThreadName), _Action._5_REQUIRED_AFTER_unlock);
        // writerThread has released exclusive writeLock

        Assert.assertEquals(td.stampedLock.awaitWaitExpectedAction(td.readerThreadName), _Action._4_OPTIONAL_AFTER_writeLock);
        // readerThread has acquired exclusive writeLock
        td.stampedLock.blockingNotify(new _ThreadAction(td.readerThreadName, _Action._4_OPTIONAL_AFTER_writeLock));

        Assert.assertEquals(td.stampedLock.awaitWaitExpectedAction(td.readerThreadName), _Action._5_REQUIRED_BEFORE_unlock);
        // readerThread has read from cache
        td.stampedLock.blockingNotify(new _ThreadAction(td.readerThreadName, _Action._5_REQUIRED_BEFORE_unlock));
        // readerThread begins to release exclusive writeLock
        Assert.assertEquals(td.stampedLock.awaitWaitExpectedAction(td.readerThreadName), _Action._5_REQUIRED_AFTER_unlock);
        // readerThread has released exclusive writeLock

        td.stampedLock.blockingNotify(new _ThreadAction(td.writerThreadName, _Action._5_REQUIRED_AFTER_unlock));
        td.stampedLock.blockingNotify(new _ThreadAction(td.readerThreadName, _Action._5_REQUIRED_AFTER_unlock));

        td.readerThread.join();
        td.writerThread.join();

        if (td.uncaughtExceptionMap.size() > 0) {
            throw new IllegalStateException(td.uncaughtExceptionMap.toString());
        }
        Assert.assertEquals(td.readerResultRef[0], td.writerThreadName);
        Assert.assertEquals(td.writerResultRef[0], td.writerThreadName);
    }
}
