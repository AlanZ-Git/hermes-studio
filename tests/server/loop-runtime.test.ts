import { beforeEach, describe, expect, it, vi } from 'vitest'

const rows = vi.hoisted(() => new Map<string, any>())
const details = vi.hoisted(() => new Map<string, any>())
vi.mock('../../packages/server/src/modules/studio/repositories/session-store', () => ({
  getSession: (id: string) => rows.get(id),
  getSessionDetail: (id: string) => details.get(id),
}))
vi.mock('../../packages/server/src/modules/studio/services/chat-run/compression', () => ({
  getOrCreateSession: (map: Map<string, any>, id: string) => {
    if (!map.has(id)) map.set(id, { messages: [], queue: [], events: [], isWorking: false })
    return map.get(id)
  },
}))
import { LoopRuntime } from '../../packages/server/src/modules/studio/services/chat-run/loop-runtime'

function fixture(recovery = false) {
  const owner = { id: 1, username: 'fixture', role: 'super_admin' }
  rows.set('session', { id: 'session', source: 'api_server', profile: 'alpha', user_id: '1' })
  const item = { session_id: 'session', route: { profile: 'alpha' }, generation: 2, next_due_at: 0,
    ...(recovery ? { recovery: { id: 'recovery', status: 'pending' } } : {}) }
  const host: any = {
    sessionMap: new Map(), profiles: () => ['alpha'], owner: vi.fn(() => owner),
    socket: () => ({ data: { user: owner } }), closing: () => false, emit: vi.fn(),
    request: vi.fn(async (data: any) => data.action === 'loop_list' ? { loops: [item] }
      : data.op === 'claim' ? { claimed: true, generation: 2, prompt: 'inspect' }
        : { authorized: true }),
    run: vi.fn(async (_socket: any, data: any) => {
      await data.loopBeforeDispatch()
      data.loopDispatchConfirmed()
      data.onEvent('run.started', { run_id: 'run' })
      data.loopProof({ done: true, run_id: 'run', status: 'complete', result: { completed: true } })
      details.set('session', { messages: [{ id: 5, role: 'assistant', content: 'done' }] })
      data.onEvent('run.completed', { run_id: 'run', message_id: 5, output: 'done' })
    }),
    command: vi.fn(async () => ({ response: '' })),
  }
  const runtime = new LoopRuntime(host)
  return { host, runtime, item }
}
async function settle(runtime: LoopRuntime) {
  await runtime.poll()
  await vi.waitFor(() => expect((runtime as any).runs.size).toBe(0))
}

describe('native loop runtime dispatch evidence', () => {
  beforeEach(() => { rows.clear(); details.clear() })
  it('completes only after matching terminal proof and persisted assistant readback', async () => {
    const { host, runtime } = fixture()
    await settle(runtime)
    expect(host.request).toHaveBeenCalledWith(expect.objectContaining({ op: 'complete', run_id: 'run', generation: 2 }))
    expect(host.sessionMap.get('session').isWorking).toBe(false)
  })
  it('does not count a successful-looking callback without native completion proof', async () => {
    const { host, runtime } = fixture()
    host.run.mockImplementationOnce(async (_: any, data: any) => {
      await data.loopBeforeDispatch(); data.loopDispatchConfirmed()
      data.onEvent('run.started', { run_id: 'run' })
      data.onEvent('run.completed', { run_id: 'run', message_id: 5 })
    })
    await settle(runtime)
    expect(host.request).not.toHaveBeenCalledWith(expect.objectContaining({ op: 'complete' }))
    expect(host.request).toHaveBeenCalledWith(expect.objectContaining({ op: 'unknown' }))
  })
  it('routes scheduled commands through the command dispatcher, never through model fallback', async () => {
    const { host, runtime } = fixture()
    host.request.mockImplementation(async (data: any) => data.action === 'loop_list'
      ? { loops: [{ session_id: 'session', route: { profile: 'alpha' }, next_due_at: 0 }] }
      : data.op === 'claim' ? { claimed: true, generation: 2, prompt: '/status' } : { authorized: true })
    await settle(runtime)
    expect(host.command).toHaveBeenCalledOnce()
    expect(host.run).not.toHaveBeenCalled()
    expect(host.request).toHaveBeenCalledWith(expect.objectContaining({ op: 'complete' }))
  })
  it('abandons only when the local path positively proves no dispatch', async () => {
    const { host, runtime } = fixture()
    host.run.mockImplementationOnce(async () => { throw new Error('preflight rejected before bridge call') })
    await settle(runtime)
    expect(host.request).toHaveBeenCalledWith(expect.objectContaining({ op: 'abandon', not_started: true }))
    expect(host.request).not.toHaveBeenCalledWith(expect.objectContaining({ op: 'unknown' }))
  })
  it('retains an uncertain dispatch when the bridge accepted work but its reply was lost', async () => {
    const { host, runtime } = fixture()
    host.run.mockImplementationOnce(async (_: any, data: any) => {
      await data.loopBeforeDispatch()
      data.loopDispatchConfirmed()
      throw new Error('transport reply lost after send')
    })
    await settle(runtime)
    expect(host.request).toHaveBeenCalledWith(expect.objectContaining({ op: 'unknown', not_started: false }))
    expect(host.request).not.toHaveBeenCalledWith(expect.objectContaining({ op: 'abandon' }))
    expect(host.run).toHaveBeenCalledOnce()
  })
  it('does not refund when completion persistence acknowledgement is lost', async () => {
    const { host, runtime } = fixture()
    const normalRequest = host.request.getMockImplementation()!
    host.request.mockImplementation(async (data: any) => {
      if (data.op === 'complete') throw new Error('completion receipt unavailable')
      return normalRequest(data)
    })
    await settle(runtime)
    expect(host.request).toHaveBeenCalledWith(expect.objectContaining({ op: 'unknown', not_started: false, run_id: 'run' }))
    expect(host.request).not.toHaveBeenCalledWith(expect.objectContaining({ op: 'abandon' }))
    expect(host.run).toHaveBeenCalledOnce()
  })
  it('rejects invalidated owner before dispatch and closes capabilities at shutdown', async () => {
    const { host, runtime } = fixture()
    host.run.mockImplementationOnce(async (_: any, data: any) => {
      rows.get('session').user_id = 'other'
      await data.loopBeforeDispatch()
    })
    await settle(runtime)
    expect(host.request).not.toHaveBeenCalledWith(expect.objectContaining({ op: 'inject' }))
    expect(host.request).toHaveBeenCalledWith(expect.objectContaining({ op: 'abandon' }))
    runtime.close()
    await expect(runtime.recoveryOperation('missing', { id: 1 } as any, 'session', 'alpha', {})).rejects.toThrow()
  })
})
