package com.github.kevinarpe.scb.scheduler;

import com.carrotsearch.hppc.LongHashSet;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * @author Kevin Connor ARPE (kevinarpe@gmail.com)
 */
public class DeadlineEngineImpTest {

    private DeadlineEngine classUnderTest;

    @BeforeMethod
    public void beforeEachTestMethod() {

        classUnderTest = new DeadlineEngineImp();
//        classUnderTest = new UsingExternalLibrariesDeadlineEngineImp();
    }

    @Test
    public void passWhenPollWhenZeroTasksScheduled() {

        final int count = classUnderTest.poll(1234, any -> {}, 999);
        Assert.assertEquals(count, 0);
    }

    @Test
    public void passWhenScheduleOneThenPoll_Before_Deadline() {

        final long deadlineEpochMillis = 1234;
        final long requestId = classUnderTest.schedule(deadlineEpochMillis);
        Assert.assertEquals(requestId, DeadlineEngineImp.MIN_REQUEST_ID);
        Assert.assertEquals(classUnderTest.size(), 1);

        final int count = classUnderTest.poll(deadlineEpochMillis - 1, any -> {}, 999);
        Assert.assertEquals(count, 0);
        Assert.assertEquals(classUnderTest.size(), 1);
    }

    @Test
    public void passWhenScheduleOneThenPoll_At_Deadline() {

        final long deadlineEpochMillis = 1234;
        final long requestId = classUnderTest.schedule(deadlineEpochMillis);
        Assert.assertEquals(requestId, DeadlineEngineImp.MIN_REQUEST_ID);
        Assert.assertEquals(classUnderTest.size(), 1);

        final LongHashSet requestIdSet = new LongHashSet();
        final int count = classUnderTest.poll(deadlineEpochMillis, (Long id) -> requestIdSet.add(id), 999);
        Assert.assertEquals(count, 1);
        Assert.assertEquals(requestIdSet.size(), 1);
        Assert.assertTrue(requestIdSet.contains(requestId));
        Assert.assertEquals(classUnderTest.size(), 0);
    }

    @Test
    public void passWhenScheduleOneThenPoll_After_Deadline() {

        final long deadlineEpochMillis = 1234;
        final long requestId = classUnderTest.schedule(deadlineEpochMillis);
        Assert.assertEquals(requestId, DeadlineEngineImp.MIN_REQUEST_ID);
        Assert.assertEquals(classUnderTest.size(), 1);

        final LongHashSet requestIdSet = new LongHashSet();
        final int count = classUnderTest.poll(deadlineEpochMillis + 1, (Long id) -> requestIdSet.add(id), 999);
        Assert.assertEquals(count, 1);
        Assert.assertEquals(requestIdSet.size(), 1);
        Assert.assertTrue(requestIdSet.contains(requestId));
        Assert.assertEquals(classUnderTest.size(), 0);
    }

    @Test
    public void passWhenScheduleSecond_Before_First() {

        final long deadlineEpochMillis = 1234;
        final long requestId = classUnderTest.schedule(deadlineEpochMillis);
        Assert.assertEquals(requestId, DeadlineEngineImp.MIN_REQUEST_ID);
        Assert.assertEquals(classUnderTest.size(), 1);

        final long deadlineEpochMillis2 = deadlineEpochMillis - 1;
        final long requestId2 = classUnderTest.schedule(deadlineEpochMillis2);
        Assert.assertEquals(requestId2, 1 + requestId);
        Assert.assertEquals(classUnderTest.size(), 2);

        // Poll for second
        {
            final LongHashSet requestIdSet = new LongHashSet();
            final int count = classUnderTest.poll(deadlineEpochMillis2, (Long id) -> requestIdSet.add(id), 999);
            Assert.assertEquals(count, 1);
            Assert.assertEquals(requestIdSet.size(), 1);
            Assert.assertTrue(requestIdSet.contains(requestId2));
        }
        Assert.assertEquals(classUnderTest.size(), 1);

        // Poll for first
        {
            final LongHashSet requestIdSet = new LongHashSet();
            final int count = classUnderTest.poll(deadlineEpochMillis, (Long id) -> requestIdSet.add(id), 999);
            Assert.assertEquals(count, 1);
            Assert.assertEquals(requestIdSet.size(), 1);
            Assert.assertTrue(requestIdSet.contains(requestId));
        }
        Assert.assertEquals(classUnderTest.size(), 0);
    }

    @Test
    public void passWhenScheduleSecond_At_First() {

        final long deadlineEpochMillis = 1234;
        final long requestId = classUnderTest.schedule(deadlineEpochMillis);
        Assert.assertEquals(requestId, DeadlineEngineImp.MIN_REQUEST_ID);
        Assert.assertEquals(classUnderTest.size(), 1);

        final long requestId2 = classUnderTest.schedule(deadlineEpochMillis);
        Assert.assertEquals(requestId2, 1 + requestId);
        Assert.assertEquals(classUnderTest.size(), 2);

        // Poll for both
        final LongHashSet requestIdSet = new LongHashSet();
        final int count = classUnderTest.poll(deadlineEpochMillis, (Long id) -> requestIdSet.add(id), 999);
        Assert.assertEquals(count, 2);
        Assert.assertEquals(requestIdSet.size(), 2);
        Assert.assertTrue(requestIdSet.contains(requestId));
        Assert.assertTrue(requestIdSet.contains(requestId2));
        Assert.assertEquals(classUnderTest.size(), 0);
    }

    @Test
    public void passWhenScheduleSecond_After_First() {

        final long deadlineEpochMillis = 1234;
        final long requestId = classUnderTest.schedule(deadlineEpochMillis);
        Assert.assertEquals(requestId, DeadlineEngineImp.MIN_REQUEST_ID);
        Assert.assertEquals(classUnderTest.size(), 1);

        final long deadlineEpochMillis2 = 1 + deadlineEpochMillis;
        final long requestId2 = classUnderTest.schedule(deadlineEpochMillis2);
        Assert.assertEquals(requestId2, 1 + requestId);
        Assert.assertEquals(classUnderTest.size(), 2);

        // Poll for first
        {
            final LongHashSet requestIdSet = new LongHashSet();
            final int count = classUnderTest.poll(deadlineEpochMillis, (Long id) -> requestIdSet.add(id), 999);
            Assert.assertEquals(count, 1);
            Assert.assertEquals(requestIdSet.size(), 1);
            Assert.assertTrue(requestIdSet.contains(requestId));
        }
        Assert.assertEquals(classUnderTest.size(), 1);

        // Poll for second
        {
            final LongHashSet requestIdSet = new LongHashSet();
            final int count = classUnderTest.poll(deadlineEpochMillis2, (Long id) -> requestIdSet.add(id), 999);
            Assert.assertEquals(count, 1);
            Assert.assertEquals(requestIdSet.size(), 1);
            Assert.assertTrue(requestIdSet.contains(requestId2));
        }
        Assert.assertEquals(classUnderTest.size(), 0);
    }

    @Test
    public void passWhenScheduleThreeThenPollWithMaxPollCount() {

        // Schedule three tasks with same deadline
        final LongHashSet remainingRequestIdSet = new LongHashSet();

        final long deadlineEpochMillis = 1234;
        final long requestId = classUnderTest.schedule(deadlineEpochMillis);
        Assert.assertEquals(requestId, DeadlineEngineImp.MIN_REQUEST_ID);
        Assert.assertEquals(classUnderTest.size(), 1);
        remainingRequestIdSet.add(requestId);

        final long requestId2 = classUnderTest.schedule(deadlineEpochMillis);
        Assert.assertEquals(requestId2, 1 + requestId);
        Assert.assertEquals(classUnderTest.size(), 2);
        remainingRequestIdSet.add(requestId2);

        final long requestId3 = classUnderTest.schedule(deadlineEpochMillis);
        Assert.assertEquals(requestId3, 1 + requestId2);
        Assert.assertEquals(classUnderTest.size(), 3);
        remainingRequestIdSet.add(requestId3);

        // Poll for two
        {
            final LongHashSet requestIdSet = new LongHashSet();
            final int count = classUnderTest.poll(deadlineEpochMillis, (Long id) -> requestIdSet.add(id), 2);
            Assert.assertEquals(count, 2);
            Assert.assertEquals(requestIdSet.size(), 2);
            remainingRequestIdSet.removeAll(requestIdSet);
            Assert.assertEquals(classUnderTest.size(), 1);
        }
        // Poll for last
        {
            final LongHashSet requestIdSet = new LongHashSet();
            final int count = classUnderTest.poll(deadlineEpochMillis, (Long id) -> requestIdSet.add(id), 999);
            Assert.assertEquals(count, 1);
            Assert.assertEquals(requestIdSet.size(), 1);
            remainingRequestIdSet.removeAll(requestIdSet);
            Assert.assertEquals(remainingRequestIdSet.size(), 0);
            Assert.assertEquals(classUnderTest.size(), 0);
        }
    }

    @Test
    public void passWhenCancelRequestIdNotExist() {

        final boolean isCancelled = classUnderTest.cancel(7);
        Assert.assertFalse(isCancelled);
    }

    @Test
    public void passWhenScheduledOneThenCancel() {

        final long deadlineEpochMillis = 1234;
        final long requestId = classUnderTest.schedule(deadlineEpochMillis);
        Assert.assertEquals(requestId, DeadlineEngineImp.MIN_REQUEST_ID);
        Assert.assertEquals(classUnderTest.size(), 1);

        final boolean isCancelled = classUnderTest.cancel(requestId);
        Assert.assertTrue(isCancelled);
        Assert.assertEquals(classUnderTest.size(), 0);

        final int count = classUnderTest.poll(deadlineEpochMillis, any -> {}, 999);
        Assert.assertEquals(count, 0);
    }

    @Test
    public void passWhenScheduledTwoWithSameDeadlineThenCancel() {

        final long deadlineEpochMillis = 1234;
        final long requestId = classUnderTest.schedule(deadlineEpochMillis);
        Assert.assertEquals(requestId, DeadlineEngineImp.MIN_REQUEST_ID);
        Assert.assertEquals(classUnderTest.size(), 1);

        final long requestId2 = classUnderTest.schedule(deadlineEpochMillis);
        Assert.assertEquals(requestId2, 1 + requestId);
        Assert.assertEquals(classUnderTest.size(), 2);

        final boolean isCancelled = classUnderTest.cancel(requestId);
        Assert.assertTrue(isCancelled);
        Assert.assertEquals(classUnderTest.size(), 1);

        // Poll for only
        final LongHashSet requestIdSet = new LongHashSet();
        final int count = classUnderTest.poll(deadlineEpochMillis, (Long id) -> requestIdSet.add(id), 999);
        Assert.assertEquals(count, 1);
        Assert.assertEquals(requestIdSet.size(), 1);
        Assert.assertTrue(requestIdSet.contains(requestId2));
        Assert.assertEquals(classUnderTest.size(), 0);
    }

    @Test
    public void passWhenScheduledTwoWithDifferentDeadlineThenCancel() {

        final long deadlineEpochMillis = 1234;
        final long requestId = classUnderTest.schedule(deadlineEpochMillis);
        Assert.assertEquals(requestId, DeadlineEngineImp.MIN_REQUEST_ID);
        Assert.assertEquals(classUnderTest.size(), 1);

        final long deadlineEpochMillis2 = 1 + deadlineEpochMillis;
        final long requestId2 = classUnderTest.schedule(deadlineEpochMillis2);
        Assert.assertEquals(requestId2, 1 + requestId);
        Assert.assertEquals(classUnderTest.size(), 2);

        final boolean isCancelled = classUnderTest.cancel(requestId);
        Assert.assertTrue(isCancelled);
        Assert.assertEquals(classUnderTest.size(), 1);

        {
            final int count = classUnderTest.poll(deadlineEpochMillis, any -> {}, 999);
            Assert.assertEquals(count, 0);
        }
        // Poll for only
        final LongHashSet requestIdSet = new LongHashSet();
        final int count = classUnderTest.poll(deadlineEpochMillis2, (Long id) -> requestIdSet.add(id), 999);
        Assert.assertEquals(count, 1);
        Assert.assertEquals(requestIdSet.size(), 1);
        Assert.assertTrue(requestIdSet.contains(requestId2));
        Assert.assertEquals(classUnderTest.size(), 0);
    }
}
