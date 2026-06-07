/**
 * Parse JSON-RPC 2.0 frames from `hermes acp` stdout (newline-delimited).
 *
 * The ACP server emits two kinds of frames:
 *
 *   1. Responses to client requests (have an `id` that matches a request we
 *      sent): `{jsonrpc, id, result}` or `{jsonrpc, id, error}`.
 *
 *   2. Server-initiated notifications (no `id`): `{jsonrpc, method, params}`.
 *      For our use case, the only relevant one is `session/update` carrying
 *      a `sessionUpdate` discriminator:
 *        - `agent_thought_chunk`     → thinking delta
 *        - `agent_message_chunk`     → final text delta
 *        - `agent_tool_call`         → tool use
 *        - `tool_call_update`        → tool result
 *        - `usage_update`            → token usage
 *        - `available_commands_update` → ignore
 *        - `current_mode_update`     → ignore
 *
 * Each chunk in the text/thinking stream has shape:
 *   { sessionId, update: { sessionUpdate: 'agent_message_chunk',
 *                           content: { type: 'text', text: 'delta' } } }
 */

export type AcpFrame =
  | { kind: 'response'; id: number; result: unknown }
  | { kind: 'error'; id: number; error: { code: number; message: string; data?: unknown } }
  | { kind: 'notification'; method: string; params: unknown };

export type AcpContentBlock =
  | { type: 'text'; text: string }
  | { type: 'image'; data: string; mimeType: string }
  | { type: 'resource'; resource: unknown };

export interface AcpSessionUpdate {
  sessionId: string;
  update: {
    sessionUpdate: string;
    content?: AcpContentBlock;
    // tool_call
    toolCallId?: string;
    title?: string;
    kind?: string;
    status?: string;
    rawInput?: unknown;
    // tool_call_update
    toolCallUpdates?: Array<{ toolCallId: string; status?: string; content?: AcpContentBlock[]; rawOutput?: unknown }>;
    // usage_update
    size?: number;
    used?: number;
  };
}

/** Parse a stdout chunk into zero or more complete NDJSON frames. */
export function parseAcpFrames(chunk: string): AcpFrame[] {
  const frames: AcpFrame[] = [];
  for (const line of chunk.split('\n')) {
    const trimmed = line.trim();
    if (!trimmed) continue;
    let parsed: unknown;
    try {
      parsed = JSON.parse(trimmed);
    } catch {
      // Non-JSON line — log to stderr and skip. ACP should not produce these.
      continue;
    }
    if (!parsed || typeof parsed !== 'object') continue;
    const obj = parsed as Record<string, unknown>;
    if (obj.jsonrpc !== '2.0') continue;

    if (typeof obj.id === 'number') {
      if ('result' in obj) {
        frames.push({ kind: 'response', id: obj.id, result: obj.result });
      } else if ('error' in obj) {
        const err = obj.error as { code: number; message: string; data?: unknown };
        frames.push({ kind: 'error', id: obj.id, error: err });
      }
    } else if (typeof obj.method === 'string') {
      frames.push({ kind: 'notification', method: obj.method, params: obj.params });
    }
  }
  return frames;
}

/** Extract the deltas from a session/update notification. */
export function extractSessionUpdateDeltas(params: unknown): {
  sessionId: string;
  kind: 'thinking' | 'text' | 'tool_use' | 'tool_result' | 'usage' | 'unknown';
  delta?: string;
  toolCallId?: string;
  name?: string;
  input?: unknown;
  output?: string;
  isError?: boolean;
  inputTokens?: number;
  outputTokens?: number;
  cachedInputTokens?: number;
} | null {
  if (!params || typeof params !== 'object') return null;
  const p = params as { sessionId?: unknown; update?: unknown };
  if (typeof p.sessionId !== 'string' || !p.update || typeof p.update !== 'object') {
    return null;
  }
  const u = p.update as Record<string, unknown>;
  const sessionId = p.sessionId;
  const sessionUpdate = typeof u.sessionUpdate === 'string' ? u.sessionUpdate : '';

  if (sessionUpdate === 'agent_thought_chunk') {
    const text = (u.content as { text?: unknown } | undefined)?.text;
    if (typeof text !== 'string') return null;
    return { sessionId, kind: 'thinking', delta: text };
  }
  if (sessionUpdate === 'agent_message_chunk') {
    const text = (u.content as { text?: unknown } | undefined)?.text;
    if (typeof text !== 'string') return null;
    return { sessionId, kind: 'text', delta: text };
  }
  if (sessionUpdate === 'agent_tool_call') {
    return {
      sessionId,
      kind: 'tool_use',
      toolCallId: typeof u.toolCallId === 'string' ? u.toolCallId : undefined,
      name: typeof u.title === 'string' ? u.title : undefined,
      input: u.rawInput,
    };
  }
  if (sessionUpdate === 'tool_call_update') {
    const updates = Array.isArray(u.toolCallUpdates) ? u.toolCallUpdates : [];
    const first = updates[0] as
      | { toolCallId?: unknown; status?: unknown; content?: Array<{ type?: string; text?: string }>; rawOutput?: unknown }
      | undefined;
    if (!first) return null;
    const output = first.content?.[0]?.text ?? (typeof first.rawOutput === 'string' ? first.rawOutput : undefined);
    return {
      sessionId,
      kind: 'tool_result',
      toolCallId: typeof first.toolCallId === 'string' ? first.toolCallId : undefined,
      output: typeof output === 'string' ? output : undefined,
      isError: first.status === 'failed',
    };
  }
  if (sessionUpdate === 'usage_update') {
    return {
      sessionId,
      kind: 'usage',
      inputTokens: typeof u.used === 'number' ? u.used : undefined,
    };
  }
  return { sessionId, kind: 'unknown' };
}
