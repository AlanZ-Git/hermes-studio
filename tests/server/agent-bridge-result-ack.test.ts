import { describe, expect, it, vi } from 'vitest'
import { AgentBridgeClient } from '../../packages/server/src/modules/hermes/services/bridge/client'

describe('result consumption acknowledgement', () => {
  it('does not ACK a read or a rejected consumer, retries only after application', async () => {
    const client = new AgentBridgeClient({ endpoint: 'tcp://127.0.0.1:1', timeoutMs: 1 })
    const chunk = { ok: true as const, run_id: 'r', session_id: 's', status: 'complete' as const, delta: 'answer', output: 'answer', done: true, cursor: 3, event_cursor: 7, events: [] }
    const request = vi.spyOn(client, 'request').mockImplementation(async (payload: any) => {
      if (payload.action === 'get_output') return chunk
      return { ok: true, acknowledged: true } as any
    })
    await client.getOutput('r')
    expect(request.mock.calls.map(c => c[0].action)).toEqual(['get_output'])
    await expect(client.chatStream('s', 'x', async () => { throw new Error('persist failed') })).rejects.toThrow('persist failed')
    expect(request.mock.calls.some(c => c[0].action === 'result_ack')).toBe(false)
    request.mockClear()
    let attempts = 0
    request.mockImplementation(async (payload: any) => {
      if (payload.action === 'chat') return { ok: true, run_id: 'r' } as any
      if (payload.action === 'get_output') return chunk
      attempts++
      if (attempts === 1) throw new Error('ACK response lost')
      return { ok: true, acknowledged: true } as any
    })
    let applied = false
    await client.chatStream('s', 'x', async () => { applied = true })
    expect(applied).toBe(true)
    expect(request.mock.calls.filter(c => c[0].action === 'result_ack').map(c => c[0])).toEqual([
      { action: 'result_ack', run_id: 'r', cursor: 3, event_cursor: 7 },
      { action: 'result_ack', run_id: 'r', cursor: 3, event_cursor: 7 },
    ])
  })
})
