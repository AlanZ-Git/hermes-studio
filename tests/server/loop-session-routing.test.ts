import { describe, expect, it } from 'vitest'
import { isHermesLoopSession } from '../../packages/server/src/modules/studio/services/chat-run/loop-driver'

describe('native loop routing', () => {
  it('accepts api_server Hermes sessions but never explicit coding engines', () => {
    expect(isHermesLoopSession({ source: 'api_server', agent: '' })).toBe(true)
    expect(isHermesLoopSession({ source: 'cli', agent: 'hermes' })).toBe(true)
    for (const agent of ['codex', 'ekko-agent', 'claude-code']) {
      expect(isHermesLoopSession({ source: 'api_server', agent })).toBe(false)
    }
    expect(isHermesLoopSession({ source: 'api_server', agent_session_id: 'external' })).toBe(false)
    expect(isHermesLoopSession(null)).toBe(false)
  })
})
