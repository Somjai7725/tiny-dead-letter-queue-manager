import test from 'node:test';
import assert from 'node:assert/strict';
import { DeadLetterQueue, RetryPolicy } from '../src/index.js';

test('RetryPolicy validates constructor arguments', () => {
  assert.throws(() => new RetryPolicy({ maxAttempts: 0 }), TypeError);
  assert.throws(() => new RetryPolicy({ baseDelayMs: -1 }), TypeError);
  assert.throws(() => new RetryPolicy({ maxDelayMs: Number.NaN }), TypeError);
});

test('RetryPolicy computes capped exponential backoff', () => {
  const policy = new RetryPolicy({ maxAttempts: 4, baseDelayMs: 100, maxDelayMs: 250 });
  assert.equal(policy.delayForAttempt(1), 100);
  assert.equal(policy.delayForAttempt(2), 200);
  assert.equal(policy.delayForAttempt(3), 250);
  assert.equal(policy.delayForAttempt(4), 250);
  assert.equal(policy.shouldRetry(3), true);
  assert.equal(policy.shouldRetry(4), false);
});

test('RetryPolicy rejects invalid attempt numbers', () => {
  const policy = new RetryPolicy();
  assert.throws(() => policy.delayForAttempt(0), TypeError);
  assert.throws(() => policy.delayForAttempt(1.5), TypeError);
});

test('add returns an id and stores the entry', () => {
  const dlq = new DeadLetterQueue({ clock: () => 1000 });
  const id = dlq.add({ type: 'order.created' }, new Error('boom'), { source: 'test' });
  assert.equal(id, '1');
  assert.equal(dlq.size(), 1);
  const entry = dlq.get(id);
  assert.equal(entry.id, '1');
  assert.equal(entry.status, 'pending');
  assert.equal(entry.attempts, 0);
  assert.equal(entry.nextAttemptAt, 1000);
  assert.deepEqual(entry.context, { source: 'test' });
});

test('add rejects undefined event or error', () => {
  const dlq = new DeadLetterQueue();
  assert.throws(() => dlq.add(undefined, new Error('x')), TypeError);
  assert.throws(() => dlq.add({}, undefined), TypeError);
});

test('list filters by status and returns copies', () => {
  const dlq = new DeadLetterQueue({ clock: () => 0 });
  const a = dlq.add('a', new Error('a'));
  const b = dlq.add('b', new Error('b'));
  dlq.recordAttempt(a);
  assert.equal(dlq.list().length, 2);
  assert.equal(dlq.list({ status: 'pending' }).length, 2);
  assert.equal(dlq.list({ status: 'exhausted' }).length, 0);
  const copy = dlq.get(a);
  copy.status = 'mutated';
  assert.equal(dlq.get(a).status, 'pending');
  assert.ok(b);
});

test('recordAttempt increments attempts and schedules next attempt', () => {
  const clock = (() => {
    let now = 500;
    return () => {
      const value = now;
      now += 1000;
      return value;
    };
  })();
  const policy = new RetryPolicy({ maxAttempts: 3, baseDelayMs: 100, maxDelayMs: 1000 });
  const dlq = new DeadLetterQueue({ policy, clock });
  const id = dlq.add('event', new Error('fail'));
  const first = dlq.recordAttempt(id);
  assert.equal(first.attempts, 1);
  assert.equal(first.status, 'pending');
  assert.equal(first.nextAttemptAt, 1600);
  const second = dlq.recordAttempt(id);
  assert.equal(second.attempts, 2);
  assert.equal(second.nextAttemptAt, 2700);
  const third = dlq.recordAttempt(id);
  assert.equal(third.attempts, 3);
  assert.equal(third.status, 'exhausted');
  assert.equal(third.nextAttemptAt, null);
});

test('recordAttempt throws for unknown id', () => {
  const dlq = new DeadLetterQueue();
  assert.throws(() => dlq.recordAttempt('nope'), /unknown entry/);
});

test('replay invokes handler and marks entry replayed', () => {
  const dlq = new DeadLetterQueue({ clock: () => 0 });
  const id = dlq.add('payload', new Error('bad'));
  let seen = null;
  const entry = dlq.replay(id, (e) => {
    seen = e;
  });
  assert.equal(entry.status, 'replayed');
  assert.equal(entry.nextAttemptAt, null);
  assert.equal(seen.id, id);
  assert.equal(seen.event, 'payload');
});

test('replay refuses exhausted entries and async handlers', () => {
  const policy = new RetryPolicy({ maxAttempts: 1 });
  const dlq = new DeadLetterQueue({ policy, clock: () => 0 });
  const id = dlq.add('x', new Error('x'));
  dlq.recordAttempt(id);
  assert.throws(() => dlq.replay(id, () => {}), /exhausted/);

  const other = dlq.add('y', new Error('y'));
  assert.throws(() => dlq.replay(other, async () => {}), TypeError);
});

test('replay refuses a second replay of the same entry', () => {
  const dlq = new DeadLetterQueue({ clock: () => 0 });
  const id = dlq.add('z', new Error('z'));
  dlq.replay(id, () => {});
  assert.throws(() => dlq.replay(id, () => {}), /already been replayed/);
});

test('remove and clear manage storage', () => {
  const dlq = new DeadLetterQueue({ clock: () => 0 });
  const a = dlq.add('a', new Error('a'));
  dlq.add('b', new Error('b'));
  assert.equal(dlq.remove(a), true);
  assert.equal(dlq.remove(a), false);
  assert.equal(dlq.size(), 1);
  dlq.clear();
  assert.equal(dlq.size(), 0);
});
