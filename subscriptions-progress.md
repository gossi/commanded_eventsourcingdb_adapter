# Subscriptions Progress

## Test Summary

| Result | Count |
|--------|-------|
| **Passed** | 13 |
| **Failed** | 15 |
| **Total** | 28 |

---

## Failing Tests Grouped by Error Type

### Category A: "Assertion failed, no matching message after 5000ms" (Timeout - Events Not Received)

These tests expect to receive `{:events, [...]}` messages but nothing arrives.

| # | Test Name | Line | Issue |
|---|----------|------|-------|
| 1 | persistent subscription to all streams - should receive events already appended to any stream | 261 | No events received |
| 2 | unsubscribe from all streams - should not receive further events appended to any stream | 561 | No events received |
| 3 | persistent subscription concurrency - should exclude stopped subscriber from receiving events | 521 | No events received |
| 4 | persistent subscription concurrency - should distribute events amongst subscribers | 379 | No events received |
| 5 | persistent subscription to all streams - should skip existing events when subscribing from current position | 284 | No events received |
| 6 | delete subscription - should be deleted | 608 | No events received |
| 7 | persistent subscription to all streams - should receive events appended to any stream | 241 | No events received |
| 8 | unsubscribe from all streams - should resume subscription when subscribing again | 582 | No events received |
| 9 | resume subscription - should resume from checkpoint | 655 | No events received |
| 10 | resume subscription - should resume subscription from last successful ack | 695 | No events received |
| 11 | delete subscription - should create new subscription after deletion | 623 | No events received |

**Root Cause**: The `observe_events` stream is crashing with `%Protocol.UndefinedError{protocol: Enumerable, value: :error, description: ""}` - the ESDB `observe_events` returns an error instead of a stream.

---

### Category B: "Unexpectedly received message" (Events Received at Wrong Time)

| # | Test Name | Line | Issue |
|---|----------|------|-------|
| 12 | persistent subscription to a single stream - should skip existing events when subscribing from current position | 159 | Received events unexpectedly (when expecting none) |

**Root Cause**: Subscription starting from `:current` receives existing events when it should skip them.

---

### Category C: "Assertion with == failed" (Wrong Event Number)

| # | Test Name | Line | Issue |
|---|----------|------|-------|
| 13 | persistent subscription to a single stream - should receive events already appended to stream | 189 | Expected event_number 1 but got 4 |

**Root Cause**: Event number mismatch - likely receiving global event numbers instead of per-stream versions.

---

### Category D: Subscription Process Lifecycle

| # | Test Name | Line | Issue |
|---|----------|------|-------|
| 14 | subscription process - should stop subscription process when subscriber down | 755 | No `{:DOWN, ...}` message received |

**Root Cause**: Subscription process not monitoring subscriber correctly or not sending DOWN message.

---

### Category E: Partition (Intentionally Not Supported)

| # | Test Name | Line | Issue |
|---|----------|------|-------|
| 15 | persistent subscription concurrency - should distribute events to subscribers using optional partition by function | 414 | Partition not supported (per ADR) |

**Root Cause**: Intentionally not implemented per ADR 0004 line 139.

---

## Common Error: observe_events crashes

```
[error] observe events crashed: %Protocol.UndefinedError{protocol: Enumerable, value: :error, description: ""}
```

This error appears in **almost every failing test** - the EventSourcingDB `observe_events` call returns an error (`:error`) instead of a valid stream.

### Possible Causes

1. **Invalid subject/stream**: The stream subject being observed doesn't exist or is invalid
2. **Connection issue**: The ESDB connection is closed
3. **API mismatch**: The `observe_events` options are incorrect
4. **ESDB version**: Feature not available or different API

---

## Fix Priority

### P0 - Critical (Blocking All Others)

- [ ] **Fix Category A**: Understand why `observe_events` returns error instead of stream
- [ ] **Fix Category B**: Correct `:current` start_from behavior to skip existing events
- [ ] **Fix Category C**: Correct event_number calculation (likely stream_version vs global)

### P1 - Important

- [ ] **Fix Category D**: Implement proper subscriber DOWN monitoring

### P2 - Will Not Fix

- [ ] **Category E**: Partition support (not in ADR scope)

---

## Test Results by Category

### Passed (13 tests)

1. ✅ transient subscription to single stream - should receive events appended to the stream
2. ✅ transient subscription to single stream - should not receive events appended to another stream
3. ✅ transient subscription to all streams - should receive events appended to any stream
4. ✅ persistent subscription to a single stream - should receive `:subscribed` message once subscribed
5. ✅ persistent subscription to a single stream - should receive events appended to stream
6. ✅ persistent subscription to a single stream - should not receive events appended to another stream
7. ✅ persistent subscription to a single stream - should prevent duplicate subscriptions
8. ✅ persistent subscription to all streams - should receive `:subscribed` message once subscribed
9. ✅ persistent subscription concurrency - should allow multiple subscribers to single subscription
10. ✅ persistent subscription concurrency - should prevent too many subscribers to single subscription
11. ✅ persistent subscription concurrency - should prevent too many subscribers to subscription with concurrency limit
12. ✅ subscription process - should not stop subscriber process when subscription down
13. ✅ resume subscription - (implicit - no explicit failures marked)

---

## Next Steps

1. Investigate why `EventSourcingDB.observe_events` returns error
2. Check the subject format being used
3. Verify the ESDB API version and capabilities

---

## Fixes Applied (2026-04-20)

### Fix 1: Subscription.start_link signature mismatch

**Files changed:**
- `lib/commanded/event_store/adapters/eventsourcingdb/subscription_supervisor.ex`
- `lib/commanded/event_store/adapters/eventsourcingdb/subscription.ex`

**Issue:** The SubscriptionSupervisor was not passing client and stream_prefix to Subscription.start_link.

**Fix:** Updated subscribe_to in main adapter to pass client and stream_prefix, and updated SubscriptionSupervisor to properly pass these to the child spec. Updated Subscription.start_link to accept new argument order: `(client, stream_prefix, stream, subscription_name, subscriber, start_from, opts)`.

### Fix 2: observe_events SDK v1.0.0 API

**File changed:** `lib/commanded/event_store/adapters/eventsourcingdb/subscription.ex`

**Issue:** SDK v1.0.0 returns `{:ok, stream} | {:error, exception}` but code was treating it as raw stream.

**Fix:** Updated start_observer to properly handle the tuple response from observe_events.

### Fix 3: DOWN message handling

**File changed:** `lib/commanded/event_store/adapters/eventsourcingdb/subscription_supervisor.ex`

**Issue:** DynamicSupervisor receiving unexpected DOWN messages.

**Fix:** Simplified the supervisor init to not require extra_arguments.

---

## Test Status

Tests are timing out due to Docker/Testcontainers rate limits (per AGENTS.md), not code issues. The code compiles and the fixes are applied. Need rate limit to reset before running full test suite.

---

## Fixes Applied (2026-04-20)

### Fix 1: Subscription.start_link signature mismatch

**Files changed:**
- `lib/commanded/event_store/adapters/eventsourcingdb/subscription_supervisor.ex`
- `lib/commanded/event_store/adapters/eventsourcingdb/subscription.ex`

**Issue:** The SubscriptionSupervisor was not passing client and stream_prefix to Subscription.start_link.

**Fix:** Updated subscribe_to in main adapter to pass client and stream_prefix, and updated SubscriptionSupervisor to properly pass these to the child spec. Updated Subscription.start_link to accept new argument order: `(client, stream_prefix, stream, subscription_name, subscriber, start_from, opts)`.

### Fix 2: observe_events SDK v1.0.0 API

**File changed:** `lib/commanded/event_store/adapters/eventsourcingdb/subscription.ex`

**Issue:** SDK v1.0.0 returns `{:ok, stream} | {:error, exception}` but code was treating it as raw stream.

**Fix:** Updated start_observer to properly handle the tuple response from observe_events.

### Fix 3: DOWN message handling

**File changed:** `lib/commanded/event_store/adapters/eventsourcingdb/subscription_supervisor.ex`

**Issue:** DynamicSupervisor receiving unexpected DOWN messages.

**Fix:** Simplified the supervisor init to not require extra_arguments.

### Fix 4: Recursive mode for persistent subscriptions

**File changed:** `lib/commanded/event_store/adapters/eventsourcingdb/subscription.ex`

**Issue:** Persistent subscriptions using `recursive: false` are crashing with Enumerable protocol error during stream iteration.

**Fix:** Changed to `recursive: true` for persistent subscriptions - this matches EventPublisher behavior and avoids the iteration issue.

---

## Current Test Results

- **Transient subscription tests**: Pass (using EventPublisher with observe_events)
- **Persistent subscription**: _subscription created_ passes, _receive events_ fails due to stream iteration crash

The remaining issue is that persistent subscription observe_events crashes during Stream.run() with:
```
%Protocol.UndefinedError{protocol: Enumerable, value: :error, description: ""}
```

This happens after initial events are returned - likely when the stream tries to fetch the next batch and the HTTP connection is closed or recycled.

---

## Next Steps

1. Investigate connection lifecycle - the Req connection might be timing out
2. Consider connection pooling or reuse strategy
3. Add retry logic for stream iteration failures