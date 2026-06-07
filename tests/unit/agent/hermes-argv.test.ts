import { describe, expect, it } from 'vitest';
import { buildHermesAcpArgs } from '../../../src/agent/hermes/argv.js';

describe('Hermes ACP argv contract', () => {
  it('spawns the hermes acp server with --accept-hooks', () => {
    expect(buildHermesAcpArgs()).toEqual(['acp', '--accept-hooks']);
  });

  it('always includes the acp subcommand (never spawns hermes chat or hermes run)', () => {
    const args = buildHermesAcpArgs();
    expect(args[0]).toBe('acp');
    expect(args).not.toContain('chat');
    expect(args).not.toContain('run');
  });
});
