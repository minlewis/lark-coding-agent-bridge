import { describe, expect, it } from 'vitest';
import {
  extractSessionUpdateDeltas,
  parseAcpFrames,
} from '../../../src/agent/hermes/jsonl.js';

describe('ACP NDJSON parser', () => {
  it('parses a response frame', () => {
    const frames = parseAcpFrames(
      '{"jsonrpc":"2.0","id":1,"result":{"sessionId":"abc-123"}}\n',
    );
    expect(frames).toEqual([
      { kind: 'response', id: 1, result: { sessionId: 'abc-123' } },
    ]);
  });

  it('parses an error frame', () => {
    const frames = parseAcpFrames(
      '{"jsonrpc":"2.0","id":2,"error":{"code":-32600,"message":"bad request"}}\n',
    );
    expect(frames).toEqual([
      {
        kind: 'error',
        id: 2,
        error: { code: -32600, message: 'bad request' },
      },
    ]);
  });

  it('parses a session/update notification', () => {
    const frames = parseAcpFrames(
      '{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"s1","update":{"sessionUpdate":"agent_message_chunk","content":{"type":"text","text":"hello"}}}}\n',
    );
    expect(frames).toEqual([
      { kind: 'notification', method: 'session/update', params: {
        sessionId: 's1',
        update: { sessionUpdate: 'agent_message_chunk', content: { type: 'text', text: 'hello' } },
      } },
    ]);
  });

  it('skips blank lines and non-JSON garbage', () => {
    const frames = parseAcpFrames('\n[acp] ready\n{"jsonrpc":"2.0","id":1,"result":{}}\n');
    expect(frames).toEqual([{ kind: 'response', id: 1, result: {} }]);
  });

  it('extracts agent_message_chunk as a text delta', () => {
    const deltas = extractSessionUpdateDeltas({
      sessionId: 's1',
      update: { sessionUpdate: 'agent_message_chunk', content: { type: 'text', text: 'hi' } },
    });
    expect(deltas).toEqual({ sessionId: 's1', kind: 'text', delta: 'hi' });
  });

  it('extracts agent_thought_chunk as a thinking delta', () => {
    const deltas = extractSessionUpdateDeltas({
      sessionId: 's1',
      update: { sessionUpdate: 'agent_thought_chunk', content: { type: 'text', text: 'pondering' } },
    });
    expect(deltas).toEqual({ sessionId: 's1', kind: 'thinking', delta: 'pondering' });
  });

  it('extracts tool_use and tool_result deltas', () => {
    const use = extractSessionUpdateDeltas({
      sessionId: 's1',
      update: { sessionUpdate: 'agent_tool_call', toolCallId: 't1', title: 'Bash', rawInput: { cmd: 'ls' } },
    });
    expect(use?.kind).toBe('tool_use');
    expect(use?.toolCallId).toBe('t1');
    expect(use?.name).toBe('Bash');

    const result = extractSessionUpdateDeltas({
      sessionId: 's1',
      update: {
        sessionUpdate: 'tool_call_update',
        toolCallUpdates: [{ toolCallId: 't1', status: 'completed', content: [{ type: 'text', text: 'file.txt' }] }],
      },
    });
    expect(result?.kind).toBe('tool_result');
    expect(result?.output).toBe('file.txt');
    expect(result?.isError).toBe(false);
  });

  it('returns null for an unknown update type', () => {
    const deltas = extractSessionUpdateDeltas({
      sessionId: 's1',
      update: { sessionUpdate: 'available_commands_update' },
    });
    expect(deltas).toEqual({ sessionId: 's1', kind: 'unknown' });
  });
});
