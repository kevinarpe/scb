# README

Author: Kevin Connor ARPE (<kevinarpe@gmail.com>)

# Question #1: Cache

* Implementation: [com.github.kevinarpe.scb.cache.CacheImp](https://github.com/kevinarpe/scb/blob/master/src/main/java/com/github/kevinarpe/scb/cache/CacheImp.java)
* Tests: [com.github.kevinarpe.scb.cache.CacheImpTest](https://github.com/kevinarpe/scb/blob/master/src/test/java/com/github/kevinarpe/scb/cache/CacheImpTest.java)

# Question #2: DeadlineEngine

* **Not** Thread-Safe
    * Implementation: [com.github.kevinarpe.scb.scheduler.DeadlineEngineImp](https://github.com/kevinarpe/scb/blob/master/src/main/java/com/github/kevinarpe/scb/scheduler/DeadlineEngineImp.java)
    * Tests: [com.github.kevinarpe.scb.scheduler.DeadlineEngineImpTest](https://github.com/kevinarpe/scb/blob/master/src/test/java/com/github/kevinarpe/scb/scheduler/DeadlineEngineImpTest.java)
* Thread-Safe
    * Implementation: [com.github.kevinarpe.scb.scheduler.SynchronizedDeadlineEngineImp](https://github.com/kevinarpe/scb/blob/master/src/main/java/com/github/kevinarpe/scb/scheduler/SynchronizedDeadlineEngineImp.java)
    * Tests: [com.github.kevinarpe.scb.scheduler.SynchronizedDeadlineEngineImpTest](https://github.com/kevinarpe/scb/blob/master/src/test/java/com/github/kevinarpe/scb/scheduler/SynchronizedDeadlineEngineImpTest.java)
