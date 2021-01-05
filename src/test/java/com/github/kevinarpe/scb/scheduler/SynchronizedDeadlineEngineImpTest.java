package com.github.kevinarpe.scb.scheduler;

import com.carrotsearch.hppc.LongHashSet;
import com.carrotsearch.hppc.cursors.LongCursor;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * @author Kevin Connor ARPE (kevinarpe@gmail.com)
 */
public class SynchronizedDeadlineEngineImpTest {

    private SynchronizedDeadlineEngineImp classUnderTest;

    @BeforeMethod
    public void beforeEachTestMethod() {

        final DeadlineEngine delegate = new DeadlineEngineImp();
//        final DeadlineEngine delegate = new UsingExternalLibrariesDeadlineEngineImp();
        classUnderTest = new SynchronizedDeadlineEngineImp(delegate);
    }

    // Dear Reader: This isn't a /great/ test case because it is hard (impossible?) to reliably create a scenario where
    // the class can be used in an thread-UNsafe manner.  As a compromise, I try to "hammer" the class during schedule,
    // then poll to check class behaviour appears to be thread-safe.
    @Test
    public void passWhenTwoThreadsScheduleManyTasks()
    throws InterruptedException {

        final long deadlineEpochMillis = 1234;
        final long requestIdCount = 2_000_000;
        final LongHashSet remainingRequestIdSet = new LongHashSet();
        final LongHashSet duplicateRequestIdSet = new LongHashSet();
        final List<Throwable> exceptionList = Collections.synchronizedList(new ArrayList<>());
        {
            final Runnable runnable = () -> {
                for (int i = 0; i < requestIdCount / 2; ++i) {
                    if (0 == (i % 10_000)) {
                        System.out.printf("%s: %d%n", Thread.currentThread().getName(), i);
                    }
                    final long requestId = classUnderTest.schedule(deadlineEpochMillis);
                    synchronized (remainingRequestIdSet) {
                        synchronized (duplicateRequestIdSet) {
                            if (false == remainingRequestIdSet.add(requestId)) {
                                duplicateRequestIdSet.add(requestId);
                            }
                        }
                    }
                }
            };
            final Thread thread = new Thread(runnable);
            thread.setName("schedule");
            thread.setUncaughtExceptionHandler((Thread t, Throwable e) -> exceptionList.add(e));
            final Thread thread2 = new Thread(runnable);
            thread2.setName("schedule2");
            thread2.setUncaughtExceptionHandler((Thread t, Throwable e) -> exceptionList.add(e));

            thread.start();
            thread2.start();
            thread.join();
            thread2.join();

            Assert.assertEquals(exceptionList.size(), 0);
            Assert.assertEquals(classUnderTest.size(), requestIdCount);
            Assert.assertEquals(duplicateRequestIdSet.size(), 0);
            Assert.assertEquals(remainingRequestIdSet.size(), requestIdCount);
        }
        {
            final int maxPollCount = 1_500_000;
            final int maxPollCount2 = 500_000;
            final int countRef[] = {-1};
            final int countRef2[] = {-1};
            final LongHashSet requestIdSet = new LongHashSet();
            final LongHashSet requestIdSet2 = new LongHashSet();

            final Thread thread = new Thread(() -> {
                countRef[0] = classUnderTest.poll(deadlineEpochMillis, (Long id) -> requestIdSet.add(id), maxPollCount);
            });
            thread.setName("poll");

            final Thread thread2 = new Thread(() -> {
                countRef2[0] = classUnderTest.poll(deadlineEpochMillis, (Long id) -> requestIdSet2.add(id), maxPollCount2);
            });
            thread2.setName("poll2");

            thread.start();
            thread2.start();
            thread.join();
            thread2.join();

            Assert.assertEquals(countRef[0], maxPollCount);
            Assert.assertEquals(countRef2[0], maxPollCount2);

            Assert.assertFalse(_containsAny(requestIdSet, requestIdSet2));
            Assert.assertFalse(_containsAny(requestIdSet2, requestIdSet));

            remainingRequestIdSet.removeAll(requestIdSet);
            remainingRequestIdSet.removeAll(requestIdSet2);
            Assert.assertEquals(remainingRequestIdSet.size(), 0);
            Assert.assertEquals(classUnderTest.size(), 0);
        }
    }

    private boolean _containsAny(LongHashSet parent, LongHashSet child) {

        for (final Iterator<LongCursor> iter = child.iterator(); iter.hasNext(); ) {

            final LongCursor cursor = iter.next();
            if (parent.contains(cursor.value)) {
                return true;
            }
        }
        return false;
    }
}
