# tiny-dead-letter-queue-manager

Accumulates failed events with retry metadata and exposes inspection and manual replay APIs, with no runtime dependencies.

## Usage

```js
import { DeadLetterQueue, RetryPolicy } from 'tiny-dead-letter-queue-manager';

const policy = new RetryPolicy({ maxAttempts: 3, baseDelayMs: 100, maxDelayMs: 1000 });
const dlq = new DeadLetterQueue({ policy, clock: () => Date.now() });

const id = dlq.add({ type: 'order.created', orderId: 42 }, new Error('downstream timeout'), { source: 'worker-1' });

// A worker tried and failed again.
dlq.recordAttempt(id);

// Inspect what is waiting.
for (const entry of dlq.list({ status: 'pending' })) {
  console.log(entry.id, entry.attempts, entry.nextAttemptAt);
}

// Manually replay once the downstream is healthy.
dlq.replay(id, (entry) => {
  console.log('replaying', entry.event);
});
```

## Why this exists

When an event handler fails, you usually want to keep the event around, count the attempts, and decide later whether to retry it or give up. Doing that with an ad-hoc array tends to lose the retry budget: attempts are tracked in one place, the next-attempt timestamp in another, and the two drift apart under concurrent workers.

This library keeps the retry decision in one object. `RetryPolicy` owns the exponential backoff and the attempt ceiling; `DeadLetterQueue` owns the entries and asks the policy every time an attempt is recorded. The trade-off is that the queue is in-memory and synchronous. It does not persist across restarts, and `replay` will not await an async handler. That is deliberate: a dead-letter store that silently buffers a promise is harder to reason about than one that refuses the call. If you need durability, drain `list()` into your own storage.

## The awkward edge

Time is injected, not read from the clock. `DeadLetterQueue` takes a `clock` function in its constructor and defaults to `Date.now`. Tests pass a fake clock so backoff schedules are deterministic. If you construct a queue without a clock and then freeze time in a test, the `nextAttemptAt` values will still move; pass a clock instead.

`recordAttempt` is the only method that mutates retry state. Calling it past `maxAttempts` marks the entry `exhausted` and clears `nextAttemptAt`, and `replay` will then throw rather than let you retry a budget you have already spent.
