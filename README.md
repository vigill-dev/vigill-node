# @vigill.dev/node

Plain-English production error monitoring for Node.js servers — the Vigil Node SDK (Core B).

Vigil turns crashes into **what broke / who's affected / is it costing money / how urgent**, plus a copy-paste **Fix Prompt** for your AI coding tool. Zero runtime dependencies, Node 18+ (global `fetch`).

**Prime directive:** never take the host process down. On `uncaughtException`, after capturing, the SDK restores Node's default crash so the process behaves exactly as it would have without Vigil.

## Install

```bash
npm install @vigill.dev/node
```

```js
import { init, captureError } from '@vigill.dev/node';

init({ key: 'vg_pub_your_public_key' });

// uncaughtException and unhandledRejection are captured automatically.
try {
  await charge(order);
} catch (err) {
  captureError(err, { tags: { area: 'billing' } });
  throw err;
}
```

> Your `key` is public and write-only — it can only send events. You can also supply it via the `VIGIL_KEY` env var (an explicit `key` wins).

## API

- `init(config)` — `{ key?, endpoint?, environment?, release?, tags?, captureHttp?, debug? }`
- `captureError(error, { message?, tags? })`
- `captureMessage(message, level)` — level is `'error' | 'warning' | 'info'`
- `flush()` / `close()`

### Endpoint

You never set `endpoint`. It defaults to the hosted `https://vigill.dev/api/ingest`. Resolution order if you do: explicit `endpoint` → `VIGIL_ENDPOINT` env var → hosted default.

### HTTP dependency failures

With `captureHttp` (default on) it wraps global `fetch` and reports 4xx/5xx and network failures to third parties — the raw material for "this isn't your bug, Stripe is down." Its own ingest traffic is never reported.

### Crash detection

`uncaughtException`, `unhandledRejection` and `SIGTERM`/`SIGINT` are reported live. Out-of-memory aborts and `SIGKILL` cannot be caught in-process, so the SDK keeps a small liveness marker in the temp dir, refreshed on a timer and cleared on every clean or handled exit. A hard kill leaves the marker behind; the next `init()` finds it and reports that the previous run died unexpectedly, tagged with the last-known memory usage so OOM can be told apart from an external kill.

## License

MIT © Rajeev Chourey
