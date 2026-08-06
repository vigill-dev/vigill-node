/**
 * @vigil/node — Core B (engineering design §3.1).
 *
 * The server counterpart to the browser SDK. Same prime directive: it must never take the
 * host process down. The one place that is genuinely hard on a server is uncaughtException —
 * registering a handler suppresses Node's default crash, so after capturing we restore the
 * crash ourselves rather than leaving a process running in an unknown state.
 *
 * Zero runtime dependencies. Node 18+ (global fetch).
 */
import { hostname, tmpdir } from 'node:os';
import { readFileSync, writeFileSync, unlinkSync } from 'node:fs';
import { join } from 'node:path';
import { createHash } from 'node:crypto';

type EventType = 'error' | 'http_failure' | 'console' | 'message';
type Level = 'error' | 'warning' | 'info';

interface StackFrame {
  file?: string;
  function?: string;
  line?: number;
  col?: number;
}

interface VigilEvent {
  event_id: string;
  timestamp: string;
  type: EventType;
  level: Level;
  message: string;
  exception?: { type?: string; value?: string; stacktrace?: StackFrame[] };
  http?: { method?: string; url?: string; status?: number };
  context: {
    environment: string;
    release?: string;
    tags?: Record<string, string>;
  };
}

export interface VigilNodeConfig {
  /**
   * The SDK key. Optional here: when omitted, `init()` reads `VIGIL_KEY` from the
   * environment. An explicit value always wins over the env var.
   */
  key?: string;
  /**
   * Ingest endpoint. Defaults to the hosted `https://vigill.dev/api/ingest`, so you never
   * need to set this. Resolution order if you do: this explicit value → `VIGIL_ENDPOINT`
   * env var → the hosted default. (Vigil's own dev/CI point it at a local instance.)
   */
  endpoint?: string;
  environment?: string;
  release?: string;
  tags?: Record<string, string>;
  /** Watch outbound fetch for 4xx/5xx to third parties (feeds dependency-failure detection). */
  captureHttp?: boolean;
  debug?: boolean;
}

const SDK_NAME = 'vigil-node';
const SDK_VERSION = '0.1.0';
const FLUSH_INTERVAL_MS = 5000;
// How often the liveness marker is refreshed. A hard kill (OOM, SIGKILL) stops the process
// mid-interval; the leftover marker is how the next start knows the previous run died.
const HEARTBEAT_MS = 15_000;
const mb = (bytes?: number): string => (bytes ? `${Math.round(bytes / 1048576)} MB` : 'unknown');
const BATCH_MAX = 20;
const SHUTDOWN_FLUSH_MS = 2000;

function uuid(): string {
  const c = globalThis.crypto as Crypto | undefined;
  if (c && typeof c.randomUUID === 'function') return c.randomUUID();
  return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, (ch) => {
    const r = (Math.random() * 16) | 0;
    return (ch === 'x' ? r : (r & 0x3) | 0x8).toString(16);
  });
}

/** V8 stack lines → structured frames, best effort. Minified isn't a concern server-side. */
function parseStack(stack: string | undefined): StackFrame[] {
  if (!stack) return [];
  const frames: StackFrame[] = [];
  for (const raw of stack.split('\n').slice(0, 40)) {
    const m = /at\s+(?:(.+?)\s+\()?(?:(.+?):(\d+):(\d+))\)?$/.exec(raw.trim());
    if (m) {
      frames.push({
        function: (m[1] || '').trim() || undefined,
        file: m[2],
        line: m[3] ? parseInt(m[3], 10) : undefined,
        col: m[4] ? parseInt(m[4], 10) : undefined,
      });
    }
  }
  return frames;
}

class VigilNode {
  private cfg: Required<Pick<VigilNodeConfig, 'key' | 'endpoint' | 'environment'>> & VigilNodeConfig;
  private queue: VigilEvent[] = [];
  private timer: ReturnType<typeof setInterval> | undefined;
  private dead = false;
  private failures = 0;
  private originalFetch: typeof fetch | undefined;
  private onUncaught?: (err: Error) => void;
  private onRejection?: (reason: unknown) => void;
  private onExit?: () => void;
  private onSignal?: (sig: NodeJS.Signals) => void;
  private onProcExit?: () => void;
  private heartbeat?: ReturnType<typeof setInterval>;
  private markerFile?: string;
  private startedAt = new Date().toISOString();

  constructor(cfg: VigilNodeConfig) {
    this.cfg = {
      endpoint: 'https://vigill.dev/api/ingest',
      environment: process.env.NODE_ENV ?? 'production',
      captureHttp: true,
      ...cfg,
    } as VigilNode['cfg'];
  }

  start(): void {
    if (this.dead) return;
    try {
      this.timer = setInterval(() => void this.flush(), FLUSH_INTERVAL_MS);
      this.timer.unref?.(); // never keep the process alive just for our flush loop

      this.onUncaught = (err: Error) => {
        this.capture({ type: 'error', message: err?.message ?? 'Uncaught exception', error: err });
        /*
         * Registering this listener suppressed Node's default crash. If we are the only
         * listener, the app expected to die here — so flush and re-exit rather than leave a
         * process limping. If the host registered its own handler too, it owns the outcome.
         */
        if (process.listenerCount('uncaughtException') <= 1) {
          void this.flush(true).finally(() => process.exit(1));
        }
      };
      this.onRejection = (reason: unknown) => {
        const err = reason instanceof Error ? reason : undefined;
        this.capture({
          type: 'error',
          message: err ? err.message : `Unhandled rejection: ${String(reason)}`,
          error: err,
          tags: { unhandled_rejection: 'true' },
        });
      };
      this.onExit = () => void this.flush(true);

      // SIGTERM/SIGINT are an *expected* shutdown (a deploy, an orchestrator, Ctrl-C). Note it
      // so an unexpected restart is visible, clear the crash marker so it is not mistaken for a
      // hard kill, drain, then let the process exit as it normally would.
      this.onSignal = (sig: NodeJS.Signals) => {
        this.capture({
          type: 'message',
          level: 'warning',
          message: `Process received ${sig} and is shutting down.`,
          tags: { signal: sig },
        });
        this.clearMarker();
        void this.flush(true).finally(() => {
          if (process.listenerCount(sig) <= 1) process.exit(0);
        });
      };

      process.on('uncaughtException', this.onUncaught);
      process.on('unhandledRejection', this.onRejection);
      process.on('beforeExit', this.onExit);
      process.on('SIGTERM', this.onSignal);
      process.on('SIGINT', this.onSignal);

      this.installCrashDetection();

      if (this.cfg.captureHttp) this.installFetchHook();
    } catch (err) {
      this.log('start failed', err);
      this.dead = true;
    }
  }

  capture(input: {
    type: EventType;
    level?: Level;
    message: string;
    error?: unknown;
    http?: { method?: string; url?: string; status?: number };
    tags?: Record<string, string>;
  }): void {
    if (this.dead) return;
    try {
      const err = input.error as Error | undefined;
      const event: VigilEvent = {
        event_id: uuid(),
        timestamp: new Date().toISOString(),
        type: input.type,
        level: input.level ?? 'error',
        message: String(input.message ?? 'Unknown error').slice(0, 4096),
        context: {
          environment: this.cfg.environment,
          release: this.cfg.release,
          // The envelope's context schema is fixed (browser-shaped), and unknown top-level
          // keys are stripped at ingestion — so server identity rides in tags, which persist.
          tags: {
            runtime: `node-${process.version}`,
            server_name: hostname(),
            ...(this.cfg.tags ?? {}),
            ...(input.tags ?? {}),
          },
        },
      };
      if (err && (err.stack || err.name)) {
        event.exception = {
          type: err.name || 'Error',
          value: String(err.message ?? '').slice(0, 4096),
          stacktrace: parseStack(err.stack),
        };
      }
      if (input.http) event.http = input.http;

      this.queue.push(event);
      if (this.queue.length >= BATCH_MAX) void this.flush();
    } catch (e) {
      this.log('capture failed', e);
    }
  }

  async flush(final = false): Promise<void> {
    if (this.dead || this.queue.length === 0) return;
    const batch = this.queue.splice(0, BATCH_MAX);
    const body = JSON.stringify({
      sdk: { name: SDK_NAME, version: SDK_VERSION, runtime: 'node' },
      project_key: this.cfg.key,
      events: batch,
    });

    const send = this.originalFetch ?? fetch;
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), final ? SHUTDOWN_FLUSH_MS : 10_000);
    try {
      const res = await send(this.cfg.endpoint, {
        method: 'POST',
        body,
        headers: { 'Content-Type': 'application/json' },
        signal: controller.signal,
      });
      // 429 is the endpoint asking us to slow down — healthy, not a reason to give up.
      if (res.ok || res.status === 429) this.failures = 0;
      else this.trip();
    } catch (e) {
      this.log('flush failed', e);
      this.trip();
    } finally {
      clearTimeout(timeout);
    }
  }

  /** Circuit breaker: an unhealthy endpoint must not become unbounded retries. */
  private trip(): void {
    if (++this.failures >= 3) {
      this.log('circuit breaker tripped, shutting down');
      this.close();
    }
  }

  private installFetchHook(): void {
    if (typeof fetch !== 'function') return;
    const original = fetch;
    this.originalFetch = original;
    const self = this;
    const patched = async (input: RequestInfo | URL, init?: RequestInit): Promise<Response> => {
      const url = typeof input === 'string' ? input : input instanceof URL ? input.href : input.url;
      const method = init?.method ?? 'GET';
      // Never report our own ingest traffic — that is how you build an infinite loop.
      if (url.includes(self.cfg.endpoint) || url.includes('/api/ingest')) {
        return original.call(globalThis, input as RequestInfo, init);
      }
      try {
        const res = await original.call(globalThis, input as RequestInfo, init);
        if (res.status >= 400) {
          self.capture({
            type: 'http_failure',
            message: `${method} ${url} failed with ${res.status}`,
            http: { method, url, status: res.status },
          });
        }
        return res;
      } catch (err) {
        self.capture({
          type: 'http_failure',
          message: `${method} ${url} could not be reached`,
          error: err,
          http: { method, url, status: 0 },
        });
        throw err; // the host's own error handling must see exactly what it expected
      }
    };
    globalThis.fetch = patched as typeof fetch;
  }

  /**
   * OOM and SIGKILL cannot be caught in-process — the OS or V8 tears the process down before
   * any handler runs. So we detect them *after the fact*: a liveness marker is refreshed on a
   * timer, cleared on every clean or handled exit, and left behind only by a hard kill. If the
   * next start finds a leftover marker, the previous run died unexpectedly; the last recorded
   * memory usage lets the plain-English layer tell OOM apart from an external kill.
   */
  private installCrashDetection(): void {
    try {
      const id = createHash('sha1')
        .update(`${process.cwd()}|${this.cfg.endpoint}`)
        .digest('hex')
        .slice(0, 16);
      this.markerFile = join(tmpdir(), `vigil-run-${id}.json`);

      try {
        const prev = JSON.parse(readFileSync(this.markerFile, 'utf8')) as {
          lastSeen?: string;
          rss?: number;
          heapUsed?: number;
          heapTotal?: number;
        };
        if (prev?.lastSeen) {
          const near = prev.heapTotal && prev.heapUsed ? prev.heapUsed / prev.heapTotal >= 0.9 : false;
          const cause = near ? 'ran out of memory' : 'was killed unexpectedly (out of memory or a forced kill)';
          this.capture({
            type: 'error',
            message:
              `The previous run of this process ${cause}. It stopped without a clean shutdown ` +
              `around ${prev.lastSeen}. Last memory: rss ${mb(prev.rss)}, heap ${mb(prev.heapUsed)} of ${mb(prev.heapTotal)}.`,
            tags: {
              crash: 'unexpected_exit',
              likely_oom: String(near),
              ...(prev.rss ? { last_rss_bytes: String(prev.rss) } : {}),
              ...(prev.heapUsed ? { last_heap_used_bytes: String(prev.heapUsed) } : {}),
            },
          });
        }
      } catch {
        /* no prior marker (clean last run) or unreadable — nothing to report */
      }

      const beat = () => {
        try {
          const m = process.memoryUsage();
          writeFileSync(
            this.markerFile!,
            JSON.stringify({
              startedAt: this.startedAt,
              lastSeen: new Date().toISOString(),
              pid: process.pid,
              rss: m.rss,
              heapUsed: m.heapUsed,
              heapTotal: m.heapTotal,
            }),
          );
        } catch {
          /* marker write is best-effort; never let it break the host */
        }
      };
      beat();
      this.heartbeat = setInterval(beat, HEARTBEAT_MS);
      this.heartbeat.unref?.();

      // 'exit' fires on normal exits, process.exit(), and after our signal/uncaught handlers,
      // but NOT on a hard kill — so clearing here means only hard kills leave the marker.
      this.onProcExit = () => this.clearMarker();
      process.on('exit', this.onProcExit);
    } catch (err) {
      this.log('crash detection setup failed', err);
    }
  }

  private clearMarker(): void {
    if (!this.markerFile) return;
    try {
      unlinkSync(this.markerFile);
    } catch {
      /* already gone */
    }
  }

  close(): void {
    if (this.dead) return;
    this.dead = true;
    if (this.timer) clearInterval(this.timer);
    if (this.heartbeat) clearInterval(this.heartbeat);
    this.clearMarker();
    if (this.originalFetch) globalThis.fetch = this.originalFetch;
    if (this.onUncaught) process.off('uncaughtException', this.onUncaught);
    if (this.onRejection) process.off('unhandledRejection', this.onRejection);
    if (this.onExit) process.off('beforeExit', this.onExit);
    if (this.onSignal) {
      process.off('SIGTERM', this.onSignal);
      process.off('SIGINT', this.onSignal);
    }
    if (this.onProcExit) process.off('exit', this.onProcExit);
  }

  private log(...args: unknown[]): void {
    if (this.cfg.debug) {
      try {
        console.error('[vigil]', ...args);
      } catch {
        /* logging must never throw */
      }
    }
  }
}

let client: VigilNode | undefined;

export function init(config: VigilNodeConfig = {}): void {
  if (client) return;
  // Explicit config wins; otherwise read the key/endpoint from the environment. A missing
  // key disables the SDK silently rather than throwing — an SDK must never break the host.
  const key = config.key ?? process.env.VIGIL_KEY;
  if (!key) {
    if (config.debug) console.warn('[vigil] no key supplied and VIGIL_KEY is unset — disabled');
    return;
  }
  const endpoint = config.endpoint ?? process.env.VIGIL_ENDPOINT;
  client = new VigilNode({ ...config, key, ...(endpoint ? { endpoint } : {}) });
  client.start();
}

export function captureError(
  error: unknown,
  context?: { message?: string; tags?: Record<string, string> },
): void {
  client?.capture({
    type: 'error',
    message: context?.message ?? (error instanceof Error ? error.message : String(error)),
    error,
    tags: context?.tags,
  });
}

export function captureMessage(message: string, level: Level = 'info'): void {
  client?.capture({ type: 'message', level, message });
}

export function flush(): Promise<void> {
  return client?.flush(true) ?? Promise.resolve();
}

export function close(): void {
  client?.close();
  client = undefined;
}
