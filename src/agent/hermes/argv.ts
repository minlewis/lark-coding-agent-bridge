/**
 * Build the argv for spawning `hermes acp`.
 *
 * The hermes ACP (Agent Client Protocol) server speaks JSON-RPC 2.0 over
 * stdio. We use `--accept-hooks` so any shell-hook approvals in the user's
 * config don't block in the absence of a TTY — this matches upstream
 * bridge behaviour for non-interactive agent spawning.
 */
export function buildHermesAcpArgs(): string[] {
  return ['acp', '--accept-hooks'];
}
