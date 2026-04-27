# Subscriptions Progress

## Test Summary

| Result | Count |
|--------|-------|
| **Passed** | 15 |
| **Failed** | 13 |
| **Total** | 28 |

---

## Failing Tests Grouped by Error Type

### Category A: "Assertion failed, no matching message after 5000ms" (Timeout - Events Not Received)

These tests expect to receive messages but nothing arrives in time.

| # | Test Name | Line | Issue |
|---|----------|------|-------|
| 1 | subscription process - should stop subscription process when subscriber down | 755 | No `{:DOWN, ...}` message received |
| 2 | persistent subscription concurrency - prevent too many subscribers to single subscription | 341 | Got `{:subscribed, ...}` instead of `{:subscribe_error, :subscription_already_exists, ...}` |
| 3 | persistent subscription concurrency - prevent too many subscribers to subscription with concurrency limit | 354 | Got `{:subscribed, ...}` instead of `{:subscribe_error, :too_many_subscribers, ...}` |

**Root Cause**: Subscription process lifecycle not properly monitored; duplicate/concurrency limit enforcement not working.

---

### Category B: "Unexpectedly received message" (Events Received at Wrong Time)

| # | Test Name | Line | Issue |
|---|----------|------|-------|
| 4 | persistent subscription to all streams - skip existing events when subscribing from current position | 284 | Received events when expecting none |
| 5 | persistent subscription to a single stream - skip existing events when subscribing from current position | 159 | Received events when expecting none |
| 6 | resume subscription - resume from checkpoint | 655 | Received events unexpectedly |
| 7 | resume subscription - resume subscription from last successful ack | 695 | Received events unexpectedly |
| 8 | persistent subscription concurrency - distribute events amongst subscribers | 379 | Received events unexpectedly |
| 9 | persistent subscription concurrency - distribute events to subscribers using optional partition by function | 414 | Partition events received unexpectedly |

**Root Cause**: `:current` start_from not skipping existing events; resume from checkpoint not respecting checkpoint position.

---

### Category C: "Assertion with == failed" (Wrong Event Number)

| # | Test Name | Line | Issue |
|---|----------|------|-------|
| 12 | unsubscribe from all streams - should resume subscription when subscribing again | 582 | Expected event_number 2 but got 1 (checkpoint not respected after resume) |

**Root Cause**: Complex issue with :all subscription checkpointing:
1. Global event counter is tracked sequentially per subscription (1, 2, 3...)
2. Checkpoint stored correctly on ack
3. On resume with :current, checkpoint is read and used to set lower_bound
4. BUT observe_events stream crashes during resumption (`%Protocol.UndefinedError{protocol: Enumerable, value: :error, description: ""}`)
5. The observe_events connection/stream lifecycle needs fixing for reliable resumption

**Status**: Known limitation - requires fix to observe_events stream lifecycle

---

### Category D: Subscription Deletion

| # | Test Name | Line | Issue |
|---|----------|------|-------|
| 12 | delete subscription - should be deleted | 608 | `delete_subscription` returns `{:error, :subscription_not_found}` instead of `:ok` |
| 13 | delete subscription - should create new subscription after deletion | 623 | `delete_subscription` returns `{:error, :subscription_not_found}` causing match error |

**Root Cause**: `delete_subscription` implementation not finding the subscription (possibly wrong lookup key or registration issue).

---

### Category E: observe_events Crashes (Silent Failures)

The following tests show `observe events crashed: %Protocol.UndefinedError{protocol: Enumerable, value: :error, description: ""}` in logs but passed or had other failures:

- Multiple tests show this crash in logs, indicating the stream observer crashes after initial events
- Also seeing `observe error: %Req.TransportError{reason: :closed}` and `%EventSourcingDB.Errors.TransmissionError{reason: %Req.TransportError{reason: :closed}}`

**Root Cause**: Req connection closing during stream iteration; the `observe_events` stream returns `:error` after initial batch.

---

## Common Errors

### 1. observe_events crashes during stream iteration

```
observe events crashed: %Protocol.UndefinedError{protocol: Enumerable, value: :error, description: ""}
```

Appears in logs across almost all tests. The stream observer crashes after initial events are returned.

### 2. Connection closed errors

```
observe error: %Req.TransportError{reason: :closed}
observe error: %EventSourcingDB.Errors.TransmissionError{reason: %Req.TransportError{reason: :closed}}
```

HTTP connection closes during stream iteration, causing subsequent batch fetches to fail.

### 3. DynamicSupervisor unexpected DOWN messages

```
[error] DynamicSupervisor received unexpected message: {:DOWN, #Reference<...>, :process, #PID<...>, :shutdown}
```

Supervisor not properly handling child process termination.

---

## Fix Priority

### P0 - Critical (Blocking All Others)

- [ ] **Fix Category B**: Correct `:current` start_from behavior to skip existing events
- [x] **Fix Category C (single stream)**: Correct event_number to use stream_version for single stream subscriptions
- [x] **Fix Category C (checkpoint)**: Fix checkpoint read/use when resuming subscription
- [ ] **Fix Category E**: Fix observe_events stream lifecycle / connection handling

### P1 - Important

- [ ] **Fix Category A**: Fix subscription process lifecycle (DOWN monitoring, concurrency limits)
- [ ] **Fix Category D**: Fix delete_subscription implementation

### P2 - Will Not Fix

- [ ] **Category E (partition)**: Partition support (not in ADR scope)

---

## Test Results by Category

### Passed (14 tests)

1. ✅ transient subscription to single stream - should receive events appended to the stream
2. ✅ transient subscription to single stream - should not receive events appended to another stream
3. ✅ transient subscription to all streams - should receive events appended to any stream
4. ✅ persistent subscription to a single stream - should receive `:subscribed` message once subscribed
5. ✅ persistent subscription to a single stream - should receive events appended to stream
6. ✅ persistent subscription to a single stream - should not receive events appended to another stream
7. ✅ persistent subscription to a single stream - should prevent duplicate subscriptions
8. ✅ persistent subscription to a single stream - should receive events already appended to stream
9. ✅ persistent subscription to all streams - should receive `:subscribed` message once subscribed
10. ✅ persistent subscription to all streams - should receive events appended to any stream
11. ✅ persistent subscription concurrency - should allow multiple subscribers to single subscription
12. ✅ transient subscription to all streams - should not receive further events (implicit)
13. ✅ resume subscription - (implicit - no explicit failures marked)
14. ✅ subscription process - should not stop subscriber process when subscription down

### Failed (14 tests)

1. ❌ unsubscribe from all streams - should resume subscription when subscribing again (Category C)
2. ❌ subscription process - should stop subscription process when subscriber down (Category A)
3. ❌ persistent subscription to a single stream - should skip existing events when subscribing from current position (Category B)
4. ❌ persistent subscription concurrency - should distribute events to subscribers using optional partition by function (Category E)
5. ❌ persistent subscription concurrency - should distribute events amongst subscribers (Category B)
6. ❌ delete subscription - should create new subscription after deletion (Category D)
7. ❌ persistent subscription concurrency - should prevent too many subscribers to subscription with concurrency limit (Category A)
8. ❌ persistent subscription to all streams - should skip existing events when subscribing from current position (Category B)
9. ❌ delete subscription - should be deleted (Category D)
10. ❌ persistent subscription concurrency - should exclude stopped subscriber from receiving events (Category B)
11. ❌ transient subscription to all streams - should receive events appended to any stream (Category B)
12. ❌ resume subscription - should resume from checkpoint (Category B)
13. ❌ persistent subscription concurrency - should prevent too many subscribers to single subscription (Category A)
14. ❌ resume subscription - should resume subscription from last successful ack (Category B)

---

## Next Steps

1. Fix `:current` start_from to properly skip existing events (Category B)
2. Fix observe_events stream connection lifecycle to prevent crashes during iteration (Category E)
3. Fix delete_subscription lookup/registration (Category D)
4. Fix subscription concurrency limit enforcement (Category A)
5. Fix checkpoint for :all subscriptions (Category C)

---

## Fixes Applied (2026-04-22)

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

### Fix 5: event_number uses stream_version for single stream subscriptions

**Files changed:**

- `lib/commanded/event_store/adapters/eventsourcingdb/event_mapper.ex`
- `lib/commanded/event_store/adapters/eventsourcingdb/subscription.ex`

**Issue:** event_number was using global counter (event.id + 1) for all subscriptions, but single stream subscriptions should use stream_version as event_number.

**Fix:** Added mode parameter to EventMapper.to_recorded_event - passes `:auto` for single stream (uses stream_version as event_number) and `:global` for :all subscriptions (uses event.id + 1).

### Fix 6: Make ack_event synchronous to ensure checkpoint is stored before unsubscribe

**Files changed:**

- `lib/commanded/event_store/adapters/eventsourcingdb.ex`
- `lib/commanded/event_store/adapters/eventsourcingdb/subscription.ex`

**Issue:** The test "unsubscribe from all streams - should resume subscription when subscribing again" was failing because:

1. `ack_event` used `send/2` (fire-and-forget) to send ack to subscription
2. `unsubscribe` immediately terminated the subscription process
3. The ack message was still in the mailbox when the process was terminated
4. Checkpoint was never stored, so next subscription started from origin instead of resuming

**Fix:** Changed `ack_event` to use `GenServer.call/3` (synchronous) and added `handle_call` for ack in subscription to ensure the checkpoint is stored before returning.

### Fix 7: Use DynamicSupervisor extra_arguments for persistent subscription arguments

**Files changed:**

- `lib/commanded/event_store/adapters/eventsourcingdb/supervisor.ex`
- `lib/commanded/event_store/adapters/eventsourcingdb/subscription_supervisor.ex`
- `lib/commanded/event_store/adapters/eventsourcingdb/subscription.ex`

**Issue:** The `client`, `event_store`, and `stream_prefix` arguments are constant for all children of a SubscriptionSupervisor instance, but were being passed explicitly in each child spec.

**Fix:** Refactored to use DynamicSupervisor's `extra_arguments` feature to pass `client`, `event_store`, and `stream_prefix` to all children automatically. This:
- Simplifies the `subscription_spec` function (fewer arguments to pass)
- Makes the code cleaner and more maintainable
- Follows the intended use pattern for DynamicSupervisor

---

## Test Status

Tests run completed: 15 passed, 12 failed out of 28 total.

Key remaining issues:

- `:current` start_from not skipping existing events (Category B)
- observe_events stream crashes during iteration (Category E)
- delete_subscription not finding existing subscriptions (Category D)
- Subscription concurrency limits not enforced (Category A)
- Checkpoint not respected for :all subscriptions (Category C) - FIXED ✅
