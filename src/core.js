const DEFAULT_MAX_ATTEMPTS = 5;

export class RetryPolicy {
  constructor({ maxAttempts = DEFAULT_MAX_ATTEMPTS, baseDelayMs = 1000, maxDelayMs = 60000 } = {}) {
    if (!Number.isInteger(maxAttempts) || maxAttempts < 1) {
      throw new TypeError('maxAttempts must be a positive integer');
    }
    if (!Number.isFinite(baseDelayMs) || baseDelayMs < 0) {
      throw new TypeError('baseDelayMs must be a non-negative number');
    }
    if (!Number.isFinite(maxDelayMs) || maxDelayMs < 0) {
      throw new TypeError('maxDelayMs must be a non-negative number');
    }
    this.maxAttempts = maxAttempts;
    this.baseDelayMs = baseDelayMs;
    this.maxDelayMs = maxDelayMs;
  }

  delayForAttempt(attempt) {
    if (!Number.isInteger(attempt) || attempt < 1) {
      throw new TypeError('attempt must be a positive integer');
    }
    const exponential = this.baseDelayMs * 2 ** (attempt - 1);
    return Math.min(exponential, this.maxDelayMs);
  }

  shouldRetry(attempt) {
    return attempt < this.maxAttempts;
  }
}

export class DeadLetterQueue {
  #entries;
  #nextId;
  #policy;
  #clock;

  constructor({ policy, clock } = {}) {
    this.#entries = new Map();
    this.#nextId = 1;
    this.#policy = policy ?? new RetryPolicy();
    this.#clock = clock ?? (() => Date.now());
  }

  add(event, error, context = {}) {
    if (event === undefined) {
      throw new TypeError('event must be defined');
    }
    if (error === undefined) {
      throw new TypeError('error must be defined');
    }
    const id = String(this.#nextId++);
    const entry = {
      id,
      event,
      error,
      context: { ...context },
      attempts: 0,
      lastAttemptAt: null,
      nextAttemptAt: this.#clock(),
      status: 'pending',
    };
    this.#entries.set(id, entry);
    return id;
  }

  get(id) {
    const entry = this.#entries.get(id);
    return entry ? { ...entry, context: { ...entry.context } } : undefined;
  }

  list({ status } = {}) {
    const all = [...this.#entries.values()];
    const filtered = status ? all.filter((entry) => entry.status === status) : all;
    return filtered.map((entry) => ({ ...entry, context: { ...entry.context } }));
  }

  size() {
    return this.#entries.size;
  }

  recordAttempt(id) {
    const entry = this.#entries.get(id);
    if (!entry) {
      throw new Error(`unknown entry: ${id}`);
    }
    entry.attempts += 1;
    entry.lastAttemptAt = this.#clock();
    if (!this.#policy.shouldRetry(entry.attempts)) {
      entry.status = 'exhausted';
      entry.nextAttemptAt = null;
    } else {
      entry.status = 'pending';
      entry.nextAttemptAt = entry.lastAttemptAt + this.#policy.delayForAttempt(entry.attempts);
    }
    return { ...entry, context: { ...entry.context } };
  }

  replay(id, handler) {
    const entry = this.#entries.get(id);
    if (!entry) {
      throw new Error(`unknown entry: ${id}`);
    }
    if (entry.status === 'exhausted') {
      throw new Error(`entry ${id} has exhausted its retry budget`);
    }
    if (entry.status === 'replayed') {
      throw new Error(`entry ${id} has already been replayed`);
    }
    if (typeof handler !== 'function') {
      throw new TypeError('handler must be a function');
    }
    const result = handler({ ...entry, context: { ...entry.context } });
    if (result && typeof result.then === 'function') {
      throw new TypeError('handler must be synchronous; async handlers are not supported');
    }
    entry.status = 'replayed';
    entry.nextAttemptAt = null;
    return { ...entry, context: { ...entry.context } };
  }

  remove(id) {
    return this.#entries.delete(id);
  }

  clear() {
    this.#entries.clear();
  }
}
