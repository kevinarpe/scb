package com.github.kevinarpe.scb.scheduler;

import javax.annotation.concurrent.ThreadSafe;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

/**
 * ThreadSafe?  This is a trivial thread-safe wrapper for {@link DeadlineEngine}.
 * <p>
 * Inspired by {@link java.util.Collections#synchronizedList(List)}.
 *
 * @author Kevin Connor ARPE (kevinarpe@gmail.com)
 *
 * @see UsingExternalLibrariesDeadlineEngineImp
 */
@ThreadSafe
public final class SynchronizedDeadlineEngineImp
implements DeadlineEngine {

    private final DeadlineEngine delegate;

    public SynchronizedDeadlineEngineImp(DeadlineEngine delegate) {
        this.delegate = Objects.requireNonNull(delegate);
    }

    @Override
    public long schedule(long deadlineMs) {
        synchronized (delegate) {
            return delegate.schedule(deadlineMs);
        }
    }

    @Override
    public boolean cancel(long requestId) {
        synchronized (delegate) {
            return delegate.cancel(requestId);
        }
    }

    @Override
    public int poll(long nowMs, Consumer<Long> handler, int maxPoll) {
        synchronized (delegate) {
            return delegate.poll(nowMs, handler, maxPoll);
        }
    }

    @Override
    public int size() {
        synchronized (delegate) {
            return delegate.size();
        }
    }
}
