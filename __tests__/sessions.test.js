/**
 * Session support tests for duroxide Node.js SDK.
 * Tests 1–9 run against PostgreSQL. Test 10 is a SQLite smoketest.
 */
const { describe, it, before } = require('node:test');
const assert = require('node:assert');
const path = require('node:path');
const { SqliteProvider, PostgresProvider, Client, Runtime } = require('../lib/duroxide.js');

// Load .env from project root
require('dotenv').config({ path: path.join(__dirname, '..', '.env') });

const SCHEMA = 'duroxide_node_sessions';
const RUN_ID = `sess${Date.now().toString(36)}`;
function uid(name) {
  return `${RUN_ID}-${name}`;
}

// ─── Helpers ─────────────────────────────────────────────────────

let provider;

before(async () => {
  const dbUrl = process.env.DATABASE_URL;
  if (!dbUrl) {
    throw new Error('DATABASE_URL not set. Create a .env file or export DATABASE_URL.');
  }
  provider = await PostgresProvider.connectWithSchema(dbUrl, SCHEMA);
});

async function runSessionTest(name, input, setupFn, options = {}, timeoutMs = 10_000) {
  const client = new Client(provider);
  const runtime = new Runtime(provider, {
    dispatcherPollIntervalMs: 50,
    ...options,
  });

  setupFn(runtime);
  await runtime.start();

  try {
    const instanceId = uid(name);
    await client.startOrchestration(instanceId, name, input);
    return await client.waitForOrchestration(instanceId, timeoutMs);
  } finally {
    await runtime.shutdown(100);
  }
}

// ─── 1. Basic session activity ───────────────────────────────────

describe('session: basic', () => {
  it('schedules activities with sessionId and completes', async () => {
    const result = await runSessionTest('SessionBasic', null, (rt) => {
      rt.registerActivity('Echo', async (ctx, input) => {
        return `echo:${input}`;
      });
      rt.registerOrchestration('SessionBasic', function* (ctx) {
        const r1 = yield ctx.scheduleActivityOnSession('Echo', 'hello', 'my-session');
        const r2 = yield ctx.scheduleActivityOnSession('Echo', 'world', 'my-session');
        return `${r1}|${r2}`;
      });
    });
    assert.strictEqual(result.status, 'Completed');
    assert.strictEqual(result.output, 'echo:hello|echo:world');
  });
});

// ─── 2. Session ID visible in activity context ──────────────────

describe('session: context visibility', () => {
  it('activity can read sessionId from context', async () => {
    const result = await runSessionTest('SessionCtx', null, (rt) => {
      rt.registerActivity('CheckSession', async (ctx, input) => {
        return ctx.sessionId || 'no-session';
      });
      rt.registerOrchestration('SessionCtx', function* (ctx) {
        const sid = yield ctx.scheduleActivityOnSession('CheckSession', null, 'ctx-test-session');
        return sid;
      });
    });
    assert.strictEqual(result.status, 'Completed');
    assert.strictEqual(result.output, 'ctx-test-session');
  });
});

// ─── 3. Multiple different sessions ─────────────────────────────

describe('session: multiple sessions', () => {
  it('different sessionIds can run in parallel', async () => {
    const result = await runSessionTest('SessionMulti', null, (rt) => {
      rt.registerActivity('Task', async (ctx, input) => input);
      rt.registerOrchestration('SessionMulti', function* (ctx) {
        const r1 = yield ctx.scheduleActivityOnSession('Task', 'a', 'session-A');
        const r2 = yield ctx.scheduleActivityOnSession('Task', 'b', 'session-B');
        const r3 = yield ctx.scheduleActivityOnSession('Task', 'c', 'session-A');
        return `${r1}|${r2}|${r3}`;
      });
    });
    assert.strictEqual(result.status, 'Completed');
    assert.strictEqual(result.output, 'a|b|c');
  });
});

// ─── 4. Mixed session and regular activities ─────────────────────

describe('session: mixed with regular', () => {
  it('session and non-session activities coexist', async () => {
    const result = await runSessionTest('SessionMixed', null, (rt) => {
      rt.registerActivity('SessionTask', async (ctx, input) => `session:${input}`);
      rt.registerActivity('RegularTask', async (ctx, input) => `regular:${input}`);
      rt.registerOrchestration('SessionMixed', function* (ctx) {
        const r1 = yield ctx.scheduleActivity('RegularTask', 'a');
        const r2 = yield ctx.scheduleActivityOnSession('SessionTask', 'b', 'sess-1');
        const r3 = yield ctx.scheduleActivity('RegularTask', 'c');
        return `${r1}|${r2}|${r3}`;
      });
    });
    assert.strictEqual(result.status, 'Completed');
    assert.strictEqual(result.output, 'regular:a|session:b|regular:c');
  });
});

// ─── 5. Regular activity has null sessionId ──────────────────────

describe('session: null for regular activities', () => {
  it('regular activities have null sessionId in context', async () => {
    const result = await runSessionTest('SessionNull', null, (rt) => {
      rt.registerActivity('Check', async (ctx) => {
        return ctx.sessionId === null ? 'null' : 'not-null';
      });
      rt.registerOrchestration('SessionNull', function* (ctx) {
        return yield ctx.scheduleActivity('Check', null);
      });
    });
    assert.strictEqual(result.status, 'Completed');
    assert.strictEqual(result.output, 'null');
  });
});

// ─── 6. Fan-out (same session) ────────────────────────────────────

describe('session: fan-out same session', () => {
  it('multiple activities with same sessionId all complete', async () => {
    const result = await runSessionTest('SessionFanOut', null, (rt) => {
      rt.registerActivity('Append', async (ctx, input) => {
        return `${input}!`;
      });
      rt.registerOrchestration('SessionFanOut', function* (ctx) {
        const results = [];
        for (let i = 0; i < 3; i++) {
          const r = yield ctx.scheduleActivityOnSession('Append', `item${i}`, 'fan-session');
          results.push(r);
        }
        return results;
      });
    });
    assert.strictEqual(result.status, 'Completed');
    assert.deepStrictEqual(result.output, ['item0!', 'item1!', 'item2!']);
  });
});

// ─── 7. Options arg sugar ────────────────────────────────────────

describe('session: options arg sugar', () => {
  it('scheduleActivity with { sessionId } option works like scheduleActivityOnSession', async () => {
    const result = await runSessionTest('SessionOptions', null, (rt) => {
      rt.registerActivity('Echo', async (ctx, input) => {
        return { value: input, session: ctx.sessionId };
      });
      rt.registerOrchestration('SessionOptions', function* (ctx) {
        const r = yield ctx.scheduleActivity('Echo', 'opt-test', { sessionId: 'opt-session' });
        return r;
      });
    });
    assert.strictEqual(result.status, 'Completed');
    assert.strictEqual(result.output.session, 'opt-session');
    assert.strictEqual(result.output.value, 'opt-test');
  });
});

// ─── 8. Runtime options config ───────────────────────────────────

describe('session: runtime options config', () => {
  it('maxSessionsPerRuntime and sessionIdleTimeoutMs are accepted', async () => {
    const result = await runSessionTest('SessionRtOpts', null, (rt) => {
      rt.registerActivity('Echo', async (ctx, input) => input);
      rt.registerOrchestration('SessionRtOpts', function* (ctx) {
        const r = yield ctx.scheduleActivityOnSession('Echo', 'opts-test', 'opts-session');
        return r;
      });
    }, {
      maxSessionsPerRuntime: 5,
      sessionIdleTimeoutMs: 60000,
    });
    assert.strictEqual(result.status, 'Completed');
    assert.strictEqual(result.output, 'opts-test');
  });
});

// ─── 9. Session with worker_node_id ──────────────────────────────

describe('session: worker node id', () => {
  it('completes with workerNodeId option set', async () => {
    const result = await runSessionTest('SessionNodeId', null, (rt) => {
      rt.registerActivity('Work', async (ctx, input) => `done:${input}`);
      rt.registerOrchestration('SessionNodeId', function* (ctx) {
        const r1 = yield ctx.scheduleActivityOnSession('Work', 'a', 'stable-sess');
        const r2 = yield ctx.scheduleActivityOnSession('Work', 'b', 'stable-sess');
        return `${r1}|${r2}`;
      });
    }, { workerNodeId: 'test-pod-1' });
    assert.strictEqual(result.status, 'Completed');
    assert.strictEqual(result.output, 'done:a|done:b');
  });
});

// ─── 10. SQLite session smoketest ─────────────────────────────────

describe('session: sqlite smoketest', () => {
  it('session activities work with SQLite provider', async () => {
    const sqliteProvider = await SqliteProvider.inMemory();
    const client = new Client(sqliteProvider);
    const runtime = new Runtime(sqliteProvider, { dispatcherPollIntervalMs: 50 });

    runtime.registerActivity('Echo', async (ctx, input) => {
      return { value: input, session: ctx.sessionId };
    });
    runtime.registerOrchestration('SqliteSessionTest', function* (ctx) {
      const r = yield ctx.scheduleActivityOnSession('Echo', 'hello', 'sqlite-sess');
      return r;
    });

    await runtime.start();
    try {
      const instanceId = uid('sqlite-session');
      await client.startOrchestration(instanceId, 'SqliteSessionTest', 'hello');
      const result = await client.waitForOrchestration(instanceId, 5_000);
      assert.strictEqual(result.status, 'Completed');
      assert.strictEqual(result.output.value, 'hello');
      assert.strictEqual(result.output.session, 'sqlite-sess');
    } finally {
      await runtime.shutdown(100);
    }
  });
});
