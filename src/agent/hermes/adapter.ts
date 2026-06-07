/**
 * Hermes Agent adapter (ACP mode).
 *
 * Spawns one persistent `hermes acp` subprocess and speaks JSON-RPC 2.0 over
 * stdio. Each turn:
 *
 *   1. ensure-session: if no ACP sessionId is known for this chat, send
 *      `session/new` and remember the returned id. Otherwise reuse the
 *      cached one (or `session/load` a stored bridge sessionId).
 *   2. send `session/prompt` with the user text.
 *   3. drain stdout NDJSON, emitting `thinking` / `text` / `tool_use` /
 *      `tool_result` / `usage` events as `session/update` notifications
 *      arrive, then `done` when the prompt response resolves.
 *
 * Streamed cards: unlike `-Q` CLI mode, ACP delivers `agent_message_chunk`
 * deltas in real time. Bridge's `run-renderer.ts` consumes these directly
 * to patch the Feishu card mid-run, giving true streaming UX.
 *
 * Session continuity: `AgentRunOptions.sessionId` is treated as the
 * bridge-catalog id and reused as the ACP `sessionId` (Hermes persists
 * state under that id). First-turn in a new chat sends `session/new` and
 * the returned id is yielded in the `system` event for the catalog to
 * remember.
 *
 * No `--ignore-rules`: ACP is the user's editor-facing surface, so we let
 * SOUL/AGENTS/memory flow through normally. The "context between subjects
 * doesn't pollute" guarantee comes from the per-chat `sessionId` — each
 * chat gets its own ACP session, no cross-talk.
 */
import { spawnProcess, type SpawnedProcessByStdio } from '../../platform/spawn';
import { buildLarkChannelEnv, type LarkChannelEnvContext } from '../lark-channel-env';
import { checkAgentAvailability, type AgentAvailability } from '../preflight';
import type {
  AgentAdapter,
  AgentBotIdentity,
  AgentEvent,
  AgentRun,
  AgentRunOptions,
} from '../types';
import type { Readable, Writable } from 'node:stream';
import { buildHermesAcpArgs } from './argv';
import {
  extractSessionUpdateDeltas,
  parseAcpFrames,
  type AcpFrame,
} from './jsonl';

export interface HermesAdapterOptions {
  binary?: string;
  /** Default model id (e.g. 'custom:minimax_coding'). */
  defaultModel?: string;
  larkChannel?: LarkChannelEnvContext;
}

type HermesChild = SpawnedProcessByStdio<Writable, Readable, Readable>;

interface PendingRequest {
  resolve: (result: unknown) => void;
  reject: (err: Error) => void;
  method: string;
}

/** Notification routed to a specific run. */
interface RunSubscription {
  sessionId: string;
  onNotification: (params: unknown) => void;
  onPromptResponse: (result: unknown) => void;
  onPromptError: (err: Error) => void;
}

const PROTOCOL_VERSION = 1;
/** Hard cap on a single prompt's wall-clock time. */
const PROMPT_TIMEOUT_MS = 5 * 60 * 1000;
/** Soft cap on the post-response drain window. */
const POST_RESPONSE_DRAIN_ROUNDS = 5;

export class HermesAdapter implements AgentAdapter {
  readonly id = 'hermes';
  readonly displayName = 'Hermes Agent (Python gateway)';

  private readonly binary: string;
  private readonly defaultModel: string | undefined;
  private readonly larkChannel: LarkChannelEnvContext | undefined;
  private botIdentity: AgentBotIdentity | undefined;

  /** Lazily-spawned ACP process; shared across all turns. */
  private child: HermesChild | null = null;
  private nextId = 1;
  private pending = new Map<number, PendingRequest>();
  /** Maps an in-flight prompt request id to the run waiting for it. */
  private promptWaiters = new Map<number, RunSubscription>();
  private stdoutBuf = '';
  private stderrChunks: Buffer[] = [];
  private initPromise: Promise<void> | null = null;
  /** chatKey → acpSessionId. chatKey is derived from cwd by the caller. */
  private sessions = new Map<string, string>();
  private runtimeError: Error | null = null;

  constructor(opts: HermesAdapterOptions = {}) {
    this.binary = opts.binary ?? 'hermes';
    this.defaultModel = opts.defaultModel;
    this.larkChannel = opts.larkChannel;
  }

  setBotIdentity(identity: AgentBotIdentity): void {
    this.botIdentity = identity;
  }

  async isAvailable(): Promise<boolean> {
    return (await this.checkAvailability()).ok;
  }

  async checkAvailability(): Promise<AgentAvailability> {
    return checkAgentAvailability({
      agentId: 'hermes',
      agentName: 'Hermes Agent',
      command: this.binary,
      binaryPath: this.binary,
    });
  }

  private chatKey(opts: AgentRunOptions): string {
    return opts.cwd ?? '<no-cwd>';
  }

  private ensureProcess(): HermesChild {
    if (this.child) return this.child;
    const envOverrides = buildLarkChannelEnv(this.larkChannel);
    const child = spawnProcess(this.binary, buildHermesAcpArgs(), {
      env: { ...process.env, ...envOverrides },
      stdio: ['pipe', 'pipe', 'pipe'],
    }) as HermesChild;

    this.child = child;
    this.runtimeError = null;
    this.stderrChunks = [];
    this.stdoutBuf = '';
    this.pending.clear();
    this.promptWaiters.clear();

    child.stdout.on('data', (chunk: Buffer) => {
      this.stdoutBuf += chunk.toString('utf8');
      this.drainStdout();
    });
    child.stderr.on('data', (chunk: Buffer) => {
      this.stderrChunks.push(chunk);
    });
    child.on('error', (err: Error) => {
      this.runtimeError = err;
      for (const [id, p] of this.pending) {
        p.reject(err);
        this.pending.delete(id);
      }
      for (const [, w] of this.promptWaiters) {
        w.onPromptError(err);
      }
      this.promptWaiters.clear();
    });
    child.on('exit', (code) => {
      const err = new Error(`hermes acp exited unexpectedly (code=${code})`);
      for (const [id, p] of this.pending) {
        p.reject(err);
        this.pending.delete(id);
      }
      for (const [, w] of this.promptWaiters) {
        w.onPromptError(err);
      }
      this.promptWaiters.clear();
      this.child = null;
      this.initPromise = null;
    });

    return child;
  }

  private drainStdout(): void {
    let nl = this.stdoutBuf.indexOf('\n');
    while (nl !== -1) {
      const line = this.stdoutBuf.slice(0, nl);
      this.stdoutBuf = this.stdoutBuf.slice(nl + 1);
      this.handleLine(line);
      nl = this.stdoutBuf.indexOf('\n');
    }
  }

  private handleLine(line: string): void {
    if (!line.trim()) return;
    const frames = parseAcpFrames(line + '\n');
    for (const frame of frames) this.dispatchFrame(frame);
  }

  private dispatchFrame(frame: AcpFrame): void {
    if (frame.kind === 'response' || frame.kind === 'error') {
      const pending = this.pending.get(frame.id);
      if (pending) {
        this.pending.delete(frame.id);
        if (frame.kind === 'response') pending.resolve(frame.result);
        else pending.reject(new Error(`${pending.method}: ${frame.error.message}`));
      }
      // Also check if this is a prompt completion for an active run.
      const waiter = this.promptWaiters.get(frame.id);
      if (waiter) {
        this.promptWaiters.delete(frame.id);
        if (frame.kind === 'response') waiter.onPromptResponse(frame.result);
        else waiter.onPromptError(new Error(frame.error.message));
      }
      return;
    }
    // Server-initiated notification. Route to the run whose sessionId matches.
    if (frame.method === 'session/update') {
      const params = frame.params as { sessionId?: string } | undefined;
      const sid = params?.sessionId;
      if (typeof sid === 'string') {
        for (const [, sub] of this.promptWaiters) {
          if (sub.sessionId === sid) sub.onNotification(params);
        }
      }
    }
  }

  private send<T = unknown>(method: string, params: Record<string, unknown>): Promise<T> {
    const child = this.ensureProcess();
    const id = this.nextId++;
    const payload = JSON.stringify({ jsonrpc: '2.0', id, method, params }) + '\n';
    return new Promise<T>((resolve, reject) => {
      this.pending.set(id, { resolve: resolve as (r: unknown) => void, reject, method });
      const stdin = child.stdin;
      if (!stdin) {
        this.pending.delete(id);
        reject(new Error('hermes acp stdin not available'));
        return;
      }
      stdin.write(payload, 'utf8', (err: Error | null | undefined) => {
        if (err) {
          this.pending.delete(id);
          reject(err);
        }
      });
      stdin.on('error', (err: Error) => {
        this.pending.delete(id);
        reject(err);
      });
    });
  }

  private async ensureInitialized(): Promise<void> {
    if (this.initPromise) return this.initPromise;
    this.initPromise = (async () => {
      this.ensureProcess();
      await this.send('initialize', {
        protocolVersion: PROTOCOL_VERSION,
        clientInfo: { name: 'lark-channel-bridge', version: '0.2.2' },
        capabilities: {},
      });
    })();
    return this.initPromise;
  }

  private async getOrCreateSession(chatKey: string, cwd: string): Promise<string> {
    const existing = this.sessions.get(chatKey);
    if (existing) return existing;
    await this.ensureInitialized();
    const result = (await this.send<{ sessionId: string }>('session/new', {
      cwd,
      mcpServers: [],
    })) as { sessionId: string };
    this.sessions.set(chatKey, result.sessionId);
    return result.sessionId;
  }

  run(opts: AgentRunOptions): AgentRun {
    if (!opts.cwd) {
      throw new Error('cwd is required for HermesAdapter.run');
    }
    const model = opts.model ?? this.defaultModel;
    const cwd = opts.cwd;
    const key = this.chatKey(opts);

    return {
      runId: opts.runId,
      events: this.runStream(opts, key, cwd, model),
      async stop() {
        // No-op: the shared ACP process keeps running. Per-run cancellation
        // would require session/cancel; for now, callers can SIGTERM the
        // whole bridge to stop everything.
      },
      waitForExit(_timeoutMs: number): Promise<boolean> {
        // Shared long-lived process; per-run completion is signalled by
        // the `done` event.
        return Promise.resolve(true);
      },
    };
  }

  private async *runStream(
    opts: AgentRunOptions,
    key: string,
    cwd: string,
    model: string | undefined,
  ): AsyncGenerator<AgentEvent> {
    const runtimeError = this.runtimeError;
    if (runtimeError) {
      yield { type: 'error', message: `hermes not running: ${runtimeError.message}`, terminationReason: 'failed' };
      return;
    }

    let sessionId: string;
    try {
      sessionId = opts.sessionId ?? (await this.getOrCreateSession(key, cwd));
    } catch (err) {
      yield {
        type: 'error',
        message: `hermes session setup failed: ${(err as Error).message}`,
        terminationReason: 'failed',
      };
      return;
    }

    yield { type: 'system', sessionId };

    // Channel that delivers notifications to this run's generator.
    const queue: unknown[] = [];
    let resolveTick: (() => void) | null = null;
    const push = (params: unknown): void => {
      queue.push(params);
      if (resolveTick) {
        const r = resolveTick;
        resolveTick = null;
        r();
      }
    };
    let promptError: Error | null = null;
    let promptResult: unknown = null;

    const sub: RunSubscription = {
      sessionId,
      onNotification: (params: unknown) => push(params),
      onPromptResponse: (result: unknown) => {
        promptResult = result;
        if (resolveTick) {
          const r = resolveTick;
          resolveTick = null;
          r();
        }
      },
      onPromptError: (err: Error): void => {
        promptError = err;
        if (resolveTick) {
          const r = resolveTick;
          resolveTick = null;
          r();
        }
      },
    };

    // Send the prompt. Track the request id so we can register the waiter.
    const id = this.nextId++;
    const params: Record<string, unknown> = {
      sessionId,
      prompt: [{ type: 'text', text: opts.prompt }],
    };
    if (model) params['model'] = model;
    const payload = JSON.stringify({ jsonrpc: '2.0', id, method: 'session/prompt', params }) + '\n';
    this.promptWaiters.set(id, sub);
    try {
      await new Promise<void>((resolve, reject) => {
        const child = this.child;
        const stdin = child?.stdin;
        if (!stdin) { reject(new Error('hermes acp stdin not available')); return; }
        stdin.write(payload, 'utf8', (err: Error | null | undefined) => (err ? reject(err) : resolve()));
      });
    } catch (err) {
      this.promptWaiters.delete(id);
      const msg = err instanceof Error ? err.message : String(err);
      yield { type: 'error', message: `failed to send prompt: ${msg}`, terminationReason: 'failed' };
      return;
    }

    const deadline = Date.now() + PROMPT_TIMEOUT_MS;
    let termination: 'normal' | 'interrupted' | 'timeout' = 'normal';
    let postDrainRounds = 0;

    while (true) {
      if (promptError !== null) {
        const errMsg = (promptError as Error).message;
        yield { type: 'error', message: `hermes prompt failed: ${errMsg}`, terminationReason: 'failed' };
        return;
      }
      if (Date.now() > deadline) {
        termination = 'timeout';
        break;
      }

      // Drain any queued notifications.
      while (queue.length > 0) {
        const params = queue.shift();
        const deltas = extractSessionUpdateDeltas(params);
        if (!deltas) continue;
        switch (deltas.kind) {
          case 'thinking':
            if (deltas.delta) yield { type: 'thinking', delta: deltas.delta };
            break;
          case 'text':
            if (deltas.delta) yield { type: 'text', delta: deltas.delta };
            break;
          case 'tool_use':
            if (deltas.toolCallId) {
              yield {
                type: 'tool_use',
                id: deltas.toolCallId,
                name: deltas.name ?? 'tool',
                input: deltas.input,
              };
            }
            break;
          case 'tool_result':
            if (deltas.toolCallId) {
              yield {
                type: 'tool_result',
                id: deltas.toolCallId,
                output: deltas.output ?? '',
                isError: deltas.isError ?? false,
              };
            }
            break;
          case 'usage':
            if (deltas.inputTokens !== undefined) {
              yield { type: 'usage', inputTokens: deltas.inputTokens };
            }
            break;
          default:
            break;
        }
      }

      // After we see the response, give notifications a few more ticks
      // to drain before declaring done.
      if (promptResult !== null) {
        if (postDrainRounds >= POST_RESPONSE_DRAIN_ROUNDS) break;
        postDrainRounds++;
        await new Promise<void>((r) => setTimeout(r, 50));
        continue;
      }

      // Wait for next notification or prompt completion.
      await new Promise<void>((resolve) => {
        if (queue.length > 0 || promptResult !== null || promptError) {
          resolve();
          return;
        }
        resolveTick = resolve;
        setTimeout(() => {
          if (resolveTick === resolve) {
            resolveTick = null;
            resolve();
          }
        }, 200);
      });
    }

    this.promptWaiters.delete(id);
    // Reference promptResult to keep tsc happy; we don't currently inspect it.
    void promptResult;
    yield { type: 'done', sessionId, terminationReason: termination };
  }
}
