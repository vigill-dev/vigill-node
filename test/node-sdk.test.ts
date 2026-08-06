/**
 * The Node SDK's contract with a server process.
 *
 * Like the browser SDK, the load-bearing property is "it cannot make things worse." On a
 * server that means: it must not swallow the crash the app expected, it must not report its
 * own traffic into a loop, and it must post exactly the envelope the ingest endpoint
 * validates. A fake fetch stands in for the endpoint so these run without a server.
 */
import { test, beforeEach, afterEach, describe } from 'node:test';
import assert from 'node:assert/strict';

interface Sent {
  url: string;
  body: {
    sdk: { name: string; runtime: string };
    project_key: string;
    events: {
      type: string;
      message: string;
      exception?: { type?: string };
      http?: { status?: number };
      context: { tags?: Record<string, string> };
    }[];
  };
}

let sent: Sent[] = [];
let realFetch: typeof fetch;
let sdk: typeof import('../src/index.ts');
let instance = 0;

beforeEach(async () => {
  sent = [];
  realFetch = globalThis.fetch;
  globalThis.fetch = (async (input: RequestInfo | URL, init?: RequestInit) => {
    const url = String(typeof input === 'string' ? input : (input as { url?: string }).url ?? input);
    if (url.includes('/api/ingest')) {
      sent.push({ url, body: JSON.parse(String(init?.body)) });
      return new Response('{}', { status: 202 });
    }
    if (url.includes('upstream-500')) return new Response('err', { status: 500 });
    if (url.includes('upstream-boom')) throw new TypeError('fetch failed');
    return new Response('{}', { status: 200 });
  }) as typeof fetch;
  sdk = (await import(`../src/index.ts?i=${instance++}`)) as typeof import('../src/index.ts');
});

afterEach(() => {
  sdk.close();
  globalThis.fetch = realFetch;
});

const flushWait = () => new Promise((r) => setTimeout(r, 20));

describe('@vigil/node', () => {
  test('captureError posts the envelope the ingest endpoint expects', async () => {
    sdk.init({ key: 'vg_pub_test', captureHttp: false });
    sdk.captureError(new Error('database pool exhausted'));
    await sdk.flush();

    assert.equal(sent.length, 1);
    const env = sent[0]!.body;
    assert.equal(env.sdk.name, 'vigil-node');
    assert.equal(env.project_key, 'vg_pub_test');
    const ev = env.events[0]!;
    assert.equal(ev.type, 'error');
    assert.equal(ev.exception?.type, 'Error');
    assert.match(ev.message, /database pool exhausted/);
    // Server identity rides in tags because the envelope's context shape is fixed.
    assert.match(ev.context.tags?.runtime ?? '', /^node-/);
    assert.ok(ev.context.tags?.server_name);
  });

  test('a failing outbound request is captured as an http_failure', async () => {
    sdk.init({ key: 'vg_pub_test', captureHttp: true });
    await fetch('https://api.stripe.test/upstream-500').catch(() => {});
    await sdk.flush();
    const failure = sent.flatMap((s) => s.body.events).find((e) => e.type === 'http_failure');
    assert.ok(failure, 'the 500 should be captured');
    assert.equal(failure.http?.status, 500);
  });

  test('a network error on an outbound request is captured and rethrown untouched', async () => {
    sdk.init({ key: 'vg_pub_test', captureHttp: true });
    // The host's own error handling must see exactly the error it would have without us.
    await assert.rejects(() => fetch('https://api.stripe.test/upstream-boom'), TypeError);
    await sdk.flush();
    const failure = sent.flatMap((s) => s.body.events).find((e) => e.http?.status === 0);
    assert.ok(failure);
  });

  test('the SDK never reports its own ingest traffic', async () => {
    sdk.init({ key: 'vg_pub_test', captureHttp: true });
    sdk.captureError(new Error('one'));
    await sdk.flush();
    await flushWait();
    const selfReports = sent.flatMap((s) => s.body.events).filter((e) => e.message.includes('/api/ingest'));
    assert.equal(selfReports.length, 0);
  });

  test('captureMessage sends an info-level message', async () => {
    sdk.init({ key: 'vg_pub_test', captureHttp: false });
    sdk.captureMessage('worker booted', 'info');
    await sdk.flush();
    const ev = sent[0]!.body.events[0]!;
    assert.equal(ev.type, 'message');
  });

  test('the public API is inert before init', () => {
    assert.doesNotThrow(() => sdk.captureError(new Error('before init')));
    assert.doesNotThrow(() => sdk.captureMessage('before init'));
  });

  test('close restores the original fetch, leaving the host unpatched', async () => {
    const before = globalThis.fetch;
    sdk.init({ key: 'vg_pub_test', captureHttp: true });
    assert.notEqual(globalThis.fetch, before, 'fetch is patched while active');
    sdk.close();
    assert.equal(globalThis.fetch, before, 'fetch is restored on close');
  });
});
