/**
 * Tests for ctx.all() (join) and ctx.race() (select) with mixed task types,
 * including activity cooperative cancellation via isCancelled().
 */
const { describe, it, before } = require('node:test');
const assert = require('node:assert');
const path = require('node:path');
const { PostgresProvider, Client, Runtime } = require('../lib/duroxide.js');

// Load .env from project root
require('dotenv').config({ path: path.join(__dirname, '..', '.env') });

const SCHEMA = 'duroxide_node_races';
const RUN_ID = `rc${Date.now().toString(36)}`;
function uid(name) {
  return `${RUN_ID}-${name}`;
}

let provider;

before(async () => {
  const dbUrl = process.env.DATABASE_URL;
  if (!dbUrl) {
    throw new Error('DATABASE_URL not set. Create a .env file or export DATABASE_URL.');
  }
  provider = await PostgresProvider.connectWithSchema(dbUrl, SCHEMA);
});

// ─── Helper ──────────────────────────────────────────────────────

async function runOrchestration(name, input, registerFn) {
  const client = new Client(provider);
  const runtime = new Runtime(provider, { dispatcherPollIntervalMs: 50 });
  registerFn(runtime);
  await runtime.start();
  try {
    const instanceId = uid(name);
    await client.startOrchestration(instanceId, name, input);
    return await client.waitForOrchestration(instanceId, 10000);
  } finally {
    await runtime.shutdown(100);
  }
}

// ─── ctx.all() with mixed task types ─────────────────────────────

describe('all() with mixed task types', () => {
  it('joins activity + timer', async () => {
    const result = await runOrchestration('AllActivityTimer', null, (rt) => {
      rt.registerActivity('Slow', async (ctx, input) => `done-${input}`);
      rt.registerOrchestration('AllActivityTimer', function* (ctx) {
        const results = yield ctx.all([
          ctx.scheduleActivity('Slow', 'work'),
          ctx.scheduleTimer(50),
        ]);
        return results;
      });
    });
    assert.strictEqual(result.status, 'Completed');
    assert.strictEqual(result.output.length, 2);
    assert.strictEqual(JSON.parse(result.output[0].ok), 'done-work');
    assert.strictEqual(result.output[1].ok, null);
  });

  it('joins activity + waitEvent', async () => {
    const instanceId = uid('all-wait');
    const client = new Client(provider);
    const runtime = new Runtime(provider, { dispatcherPollIntervalMs: 50 });

    runtime.registerActivity('Quick', async (ctx, input) => `quick-${input}`);
    runtime.registerOrchestration('AllActivityWait', function* (ctx) {
      const results = yield ctx.all([
        ctx.scheduleActivity('Quick', 'go'),
        ctx.waitForEvent('signal'),
      ]);
      return results;
    });

    await runtime.start();
    try {
      await client.startOrchestration(instanceId, 'AllActivityWait', null);
      await new Promise((r) => setTimeout(r, 500));
      await client.raiseEvent(instanceId, 'signal', { msg: 'hi' });
      const result = await client.waitForOrchestration(instanceId, 10000);

      assert.strictEqual(result.status, 'Completed');
      assert.strictEqual(result.output.length, 2);
      assert.strictEqual(JSON.parse(result.output[0].ok), 'quick-go');
      assert.deepStrictEqual(JSON.parse(result.output[1].ok), { msg: 'hi' });
    } finally {
      await runtime.shutdown(100);
    }
  });

  it('joins multiple timers', async () => {
    const result = await runOrchestration('AllTimers', null, (rt) => {
      rt.registerOrchestration('AllTimers', function* (ctx) {
        const results = yield ctx.all([
          ctx.scheduleTimer(50),
          ctx.scheduleTimer(100),
        ]);
        return results;
      });
    });
    assert.strictEqual(result.status, 'Completed');
    assert.strictEqual(result.output.length, 2);
    assert.strictEqual(result.output[0].ok, null);
    assert.strictEqual(result.output[1].ok, null);
  });
});

// ─── ctx.race() with mixed task types ────────────────────────────

describe('race() with mixed task types', () => {
  it('races activity vs timer (activity wins)', async () => {
    const result = await runOrchestration('RaceActTimer', null, (rt) => {
      rt.registerActivity('Fast', async (ctx, input) => `fast-${input}`);
      rt.registerOrchestration('RaceActTimer', function* (ctx) {
        const winner = yield ctx.race(
          ctx.scheduleActivity('Fast', 'go'),
          ctx.scheduleTimer(60000),
        );
        return winner;
      });
    });
    assert.strictEqual(result.status, 'Completed');
    assert.strictEqual(result.output.index, 0);
    assert.strictEqual(JSON.parse(result.output.value), 'fast-go');
  });

  it('races timer vs activity (timer wins, activity cooperatively cancels)', async () => {
    const instanceId = uid('race-timer-act');
    const client = new Client(provider);
    // Short lock timeout so cancellation is detected quickly
    const runtime = new Runtime(provider, {
      dispatcherPollIntervalMs: 50,
      workerLockTimeoutMs: 2000,
    });
    let activityCancelled = false;

    runtime.registerActivity('Glacial', async (ctx, input) => {
      // Cooperative cancellation: poll isCancelled() instead of sleeping forever
      while (!ctx.isCancelled()) {
        await new Promise((r) => setTimeout(r, 50));
      }
      activityCancelled = true;
      return 'cancelled';
    });
    runtime.registerOrchestration('RaceTimerAct', function* (ctx) {
      const winner = yield ctx.race(
        ctx.scheduleTimer(50),
        ctx.scheduleActivity('Glacial', 'x'),
      );
      return winner;
    });

    await runtime.start();
    try {
      await client.startOrchestration(instanceId, 'RaceTimerAct', null);
      const result = await client.waitForOrchestration(instanceId, 15000);

      assert.strictEqual(result.status, 'Completed');
      assert.strictEqual(result.output.index, 0);
      assert.strictEqual(result.output.value, 'null');

      // Wait for the cancellation signal to propagate to the activity
      for (let i = 0; i < 60 && !activityCancelled; i++) {
        await new Promise((r) => setTimeout(r, 100));
      }
      assert.ok(activityCancelled, 'activity should have seen isCancelled()');
    } finally {
      await runtime.shutdown(2000);
    }
  });

  it('races waitEvent vs timer (event wins)', async () => {
    const instanceId = uid('race-wait');
    const client = new Client(provider);
    const runtime = new Runtime(provider, { dispatcherPollIntervalMs: 50 });

    runtime.registerOrchestration('RaceWaitTimer', function* (ctx) {
      const winner = yield ctx.race(
        ctx.waitForEvent('approval'),
        ctx.scheduleTimer(60000),
      );
      return winner;
    });

    await runtime.start();
    try {
      await client.startOrchestration(instanceId, 'RaceWaitTimer', null);
      await new Promise((r) => setTimeout(r, 300));
      await client.raiseEvent(instanceId, 'approval', { ok: true });
      const result = await client.waitForOrchestration(instanceId, 10000);

      assert.strictEqual(result.status, 'Completed');
      assert.strictEqual(result.output.index, 0);
      assert.deepStrictEqual(JSON.parse(result.output.value), { ok: true });
    } finally {
      await runtime.shutdown(100);
    }
  });

  it('races two timers (shorter wins)', async () => {
    const result = await runOrchestration('RaceTwoTimers', null, (rt) => {
      rt.registerOrchestration('RaceTwoTimers', function* (ctx) {
        const winner = yield ctx.race(
          ctx.scheduleTimer(50),
          ctx.scheduleTimer(60000),
        );
        return winner;
      });
    });
    assert.strictEqual(result.status, 'Completed');
    assert.strictEqual(result.output.index, 0);
  });
});
