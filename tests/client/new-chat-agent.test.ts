// @vitest-environment jsdom
import { beforeEach, describe, expect, it, vi } from 'vitest'
import { AGENT_OPTIONS } from '../../packages/client/src/utils/agent-options'
import { DEFAULT_NEW_CHAT_AGENT, loadNewChatAgent, NEW_CHAT_AGENT_STORAGE_KEY, resolveNewChatAgent } from '../../packages/client/src/utils/new-chat-agent'

beforeEach(() => { vi.restoreAllMocks(); localStorage.clear() })

describe('new chat default (independent of shared picker order)', () => {
  it.each([undefined, null, '', 'unknown', 7])('falls back to Hermes for %s', saved => {
    expect(resolveNewChatAgent(saved)).toBe('hermes')
  })

  it('uses Hermes when storage is empty or unavailable', () => {
    expect(loadNewChatAgent()).toBe(DEFAULT_NEW_CHAT_AGENT)
    vi.mocked(localStorage.getItem).mockImplementationOnce(() => { throw new Error('Unavailable') })
    expect(loadNewChatAgent()).toBe('hermes')
  })

  it.each(AGENT_OPTIONS)('preserves explicit $value choices, including Ekko', ({ value }) => {
    localStorage.setItem(NEW_CHAT_AGENT_STORAGE_KEY, value)
    expect(loadNewChatAgent()).toBe(value)
  })

  it('does not change workflow/group picker ordering to change the new-chat default', () => {
    expect(AGENT_OPTIONS[0].value).toBe('ekko-agent')
    expect(DEFAULT_NEW_CHAT_AGENT).toBe('hermes')
  })
})
