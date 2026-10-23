// @vitest-environment jsdom
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { io } from 'socket.io-client'
import { connectChatRun, disconnectChatRun } from '@/api/studio/chat'
import { setApiKey } from '@/api/client'

vi.mock('socket.io-client', () => ({
  io: vi.fn(() => {
    const socket: any = { connected: true, on: vi.fn(() => socket), off: vi.fn(() => socket),
      emit: vi.fn(), disconnect: vi.fn(), removeAllListeners: vi.fn(() => socket) }
    return socket
  }),
}))

beforeEach(() => {
  disconnectChatRun()
  vi.mocked(io).mockClear()
  setApiKey('synthetic-profile-isolation')
})
afterEach(() => disconnectChatRun())

describe('chat profile transport isolation', () => {
  it('uses independent Managers across profiles and namespaces, while reusing the same route', () => {
    const first = connectChatRun('alpha')
    expect(connectChatRun('alpha')).toBe(first)
    connectChatRun('beta')
    connectChatRun('beta', 'global-agent')
    expect(io).toHaveBeenCalledTimes(3)
    for (const [, options] of vi.mocked(io).mock.calls) {
      expect(options).toMatchObject({ forceNew: true })
    }
    expect(vi.mocked(io).mock.calls.map(([, options]) => options?.query?.profile)).toEqual(['alpha', 'beta', 'beta'])
  })
})
