package com.github.kevinarpe.scb.scheduler;

import javax.annotation.Nullable;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.Consumer;

/**
 * This implementation does not use any external Java libraries.
 *
 * @author Kevin Connor ARPE (kevinarpe@gmail.com)
 *
 * @see UsingExternalLibrariesDeadlineEngineImp
 */
public final class DeadlineEngineImp
implements DeadlineEngine {

    // package-private for testing
    static final long MIN_REQUEST_ID = 1;
    private long nextRequestId;
    // These two data structures are mirrors of one another.
    private final TreeMap<Long, TreeSet<Long>> deadlineEpochMillis_To_RequestIdSet_Map;
    private final HashMap<Long, Long> requestId_To_DeadlineEpochMillis_Map;

    public DeadlineEngineImp() {

        this.nextRequestId = MIN_REQUEST_ID;
        this.deadlineEpochMillis_To_RequestIdSet_Map = new TreeMap<>(Comparator.naturalOrder());
        this.requestId_To_DeadlineEpochMillis_Map = new HashMap<>();
    }

    // Time complexity: O(log n) + O(1) = O(log n) -> logarithmic
    @Override
    public long schedule(final long deadlineEpochMillis) {

        _checkEpochMillis(deadlineEpochMillis, "deadlineEpochMillis");

        final long requestId = nextRequestId;
        ++nextRequestId;

        // Time complexity: O(log n)
        final TreeSet<Long> requestIdSet =
            deadlineEpochMillis_To_RequestIdSet_Map.computeIfAbsent(deadlineEpochMillis,
                any -> new TreeSet<>(Comparator.naturalOrder()));

        requestIdSet.add(requestId);

        // Time complexity: O(1)
        requestId_To_DeadlineEpochMillis_Map.put(requestId, deadlineEpochMillis);
        return requestId;
    }

    private void _checkEpochMillis(final long deadlineEpochMillis, String argName) {

        if (deadlineEpochMillis < 0) {
            throw new IllegalArgumentException("Argument '" + argName + "' must be positive: " + deadlineEpochMillis);
        }
    }

    // Time complexity: O(1) + O(log n) = O(log n) -> logarithmic
    @Override
    public boolean cancel(final long requestId) {

        // Intentional: Do not check argument.  Allow any random 'requestId' to be passed.

        // Time complexity: O(1)
        @Nullable
        final Long deadlineEpochMillis = requestId_To_DeadlineEpochMillis_Map.remove(requestId);
        if (null == deadlineEpochMillis) {
            return false;
        }
        // Time complexity: O(log n)
        final TreeSet<Long> requestIdSet =
            deadlineEpochMillis_To_RequestIdSet_Map.computeIfAbsent(deadlineEpochMillis,
                any -> new TreeSet<>(Comparator.naturalOrder()));

        requestIdSet.remove(requestId);
        return true;
    }

    // Time complexity: O(log n) + O(1) + O(1) = O(log n) -> logarithmic
    @Override
    public int poll(final long nowEpochMillis,
                    Consumer<Long> handler,
                    final int maxPollCount) {

        _checkEpochMillis(nowEpochMillis, "nowEpochMillis");
        Objects.requireNonNull(handler);
        if (maxPollCount <= 0) {
            throw new IllegalArgumentException("Argument 'maxPollCount' must be positive: " + maxPollCount);
        }

        // Intentional: 'inclusive' here will *include* deadlines that match 'nowEpochMillis'.  This is important, as
        // the interface clearly states: "When the deadline is met or exceeded (>=)...".  Thus, less-than-or-equal is
        // required, not just less-than.
        final boolean inclusive = true;
        // Time complexity: O(log n)
        final NavigableMap<Long, TreeSet<Long>> lessEqualDeadlineMap =
            deadlineEpochMillis_To_RequestIdSet_Map.headMap(nowEpochMillis, inclusive);

        int remainPollCount = maxPollCount;
        int count = 0;
BREAK_LABEL:
        for (final Map.Entry<Long, TreeSet<Long>> entry : lessEqualDeadlineMap.entrySet()) {

            final TreeSet<Long> requestIdSet = entry.getValue();
            for (final Iterator<Long> requestIdIter = requestIdSet.iterator(); requestIdIter.hasNext() ; ) {

                final long requestId = requestIdIter.next();
                handler.accept(requestId);
                // Time complexity: O(1)
                requestIdIter.remove();
                // Time complexity: O(1)
                requestId_To_DeadlineEpochMillis_Map.remove(requestId);
                --remainPollCount;
                ++count;
                if (0 == remainPollCount) {
                    break BREAK_LABEL;
                }
            }
        }
        return count;
    }

    @Override
    public int size() {
        final int x = requestId_To_DeadlineEpochMillis_Map.size();
        return x;
    }
}
