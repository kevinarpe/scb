package com.github.kevinarpe.scb.scheduler;

import com.carrotsearch.hppc.LongLongHashMap;
import com.google.common.collect.TreeMultimap;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.function.Consumer;

/**
 * This is an alternative implementation.  Compared to {@link DeadlineEngineImp}, it uses external Java libraries.
 *
 * @author Kevin Connor ARPE (kevinarpe@gmail.com)
 *
 * @see DeadlineEngineImp
 */
public final class UsingExternalLibrariesDeadlineEngineImp
implements DeadlineEngine {

    // package-private for testing
    static final long MIN_REQUEST_ID = 1;
    private long nextRequestId;
    // These two data structures are mirrors of one another.
    // This type is borrowed from Google Guava library.  It is effectively a TreeMap<Long, TreeSet<Long>>.
    private final TreeMultimap<Long, Long> deadlineEpochMillis_To_RequestId_Multimap;
    // This type is borrowed from Carrot Search Labs HPPC library.  It is a more memory efficient HashMap<Long, Long>
    // that avoids boxing.
    private final LongLongHashMap requestId_To_DeadlineEpochMillis_Map;

    public UsingExternalLibrariesDeadlineEngineImp() {

        this.nextRequestId = MIN_REQUEST_ID;
        this.deadlineEpochMillis_To_RequestId_Multimap = TreeMultimap.create();
        this.requestId_To_DeadlineEpochMillis_Map = new LongLongHashMap();
    }

    // Time complexity: O(log n) + O(1) = O(log n) -> logarithmic
    @Override
    public long schedule(final long deadlineEpochMillis) {

        _checkEpochMillis(deadlineEpochMillis, "deadlineEpochMillis");

        final long requestId = nextRequestId;
        ++nextRequestId;
        // Time complexity: O(log n)
        deadlineEpochMillis_To_RequestId_Multimap.put(deadlineEpochMillis, requestId);
        // Time complexity: O(1)
        requestId_To_DeadlineEpochMillis_Map.put(requestId, deadlineEpochMillis);
        _assertSizesMatch();
        return requestId;
    }

    private void _checkEpochMillis(final long deadlineEpochMillis, String argName) {

        // Intentional: Zero is reserved as a special sentinel value.  When we call
        // requestId_To_DeadlineEpochMillis_Map.remove(requestId), it will return zero if requestId does not exist.
        // We do this to avoid first calling containsKey(requestId), then remove(requestId).
        if (deadlineEpochMillis <= 0) {
            throw new IllegalArgumentException("Argument '" + argName + "' must be positive: " + deadlineEpochMillis);
        }
    }

    private void _assertSizesMatch() {
        if (deadlineEpochMillis_To_RequestId_Multimap.size() != requestId_To_DeadlineEpochMillis_Map.size()) {
            throw new IllegalStateException(deadlineEpochMillis_To_RequestId_Multimap.size() + " != " + requestId_To_DeadlineEpochMillis_Map.size());
        }
    }

    // Time complexity: O(1) + O(log n) = O(log n) -> logarithmic
    @Override
    public boolean cancel(final long requestId) {

        // Intentional: Do not check argument.  Allow any random 'requestId' to be passed.

        // Time complexity: O(1)
        final long deadlineEpochMillis = requestId_To_DeadlineEpochMillis_Map.remove(requestId);
        if (0 == deadlineEpochMillis) {
            return false;
        }
        // Time complexity: O(log n)
        deadlineEpochMillis_To_RequestId_Multimap.remove(deadlineEpochMillis, requestId);
        _assertSizesMatch();
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
        final NavigableMap<Long, Collection<Long>> lessEqualDeadlineMap =
            // Time complexity: O(log n)
            deadlineEpochMillis_To_RequestId_Multimap.asMap().headMap(nowEpochMillis, inclusive);

        int remainPollCount = maxPollCount;
        int count = 0;
BREAK_LABEL:
        for (final Map.Entry<Long, Collection<Long>> entry : lessEqualDeadlineMap.entrySet()) {

            final Collection<Long> requestIdColl = entry.getValue();
            for (final Iterator<Long> requestIdIter = requestIdColl.iterator(); requestIdIter.hasNext() ; ) {

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
        _assertSizesMatch();
        return count;
    }

    @Override
    public int size() {
        final int x = deadlineEpochMillis_To_RequestId_Multimap.size();
        return x;
    }
}
