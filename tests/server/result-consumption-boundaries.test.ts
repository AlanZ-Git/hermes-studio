import { describe, expect, it, vi } from 'vitest'
import { AgentBridgeClient } from '../../packages/server/src/modules/hermes/services/bridge/client'

describe('terminal consumption boundaries', () => {
  const terminal = { ok: true as const, run_id: 'r', session_id: 's', status: 'complete' as const, delta: '', output: '', done: true, cursor: 4, event_cursor: 8, events: [] }
  it('retains a failed ACK and resumes it without acknowledging a running finalizer', async () => {
    const client = new AgentBridgeClient()
    const request = vi.spyOn(client, 'request').mockResolvedValue({ ok: true, acknowledged: false } as any)
    expect(await client.ackResult('r', 4, 8)).toBe(false)
    expect(request).toHaveBeenCalledTimes(3)
    request.mockResolvedValue({ ok: true, acknowledged: true } as any)
    await client.flushResultAcks()
    expect(request).toHaveBeenCalledTimes(4)
    await client.flushResultAcks()
    expect(request).toHaveBeenCalledTimes(4)
  })
  it('applies an empty terminal callback before ACK and never ACKs a thrown persistence callback', async () => {
    const client = new AgentBridgeClient()
    const actions: string[] = []
    vi.spyOn(client, 'request').mockImplementation(async (p: any) => {
      actions.push(p.action)
      if (p.action === 'chat') return { ok:true,run_id:'r' } as any
      if (p.action === 'get_output') return terminal
      return { ok:true,acknowledged:true } as any
    })
    await client.chatStream('s','synthetic',async () => { actions.push('persist') })
    expect(actions).toEqual(['chat','get_output','persist','result_ack'])
    actions.length=0
    await expect(client.chatStream('s','synthetic',async () => { throw new Error('storage unavailable') })).rejects.toThrow('storage unavailable')
    expect(actions).toEqual(['chat','get_output'])
  })
})
