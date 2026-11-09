import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

const log = vi.hoisted(() => ({ warn: vi.fn(), info: vi.fn(), error: vi.fn(), debug: vi.fn() }))
vi.mock('../../packages/server/src/modules/studio/public/logging', () => ({ logger: log, bridgeLogger: log }))
const rows = vi.hoisted(() => new Map<string, any>())
const details = vi.hoisted(() => new Map<string, any>())
vi.mock('../../packages/server/src/modules/studio/repositories/session-store', () => ({
  getSession: (id: string) => rows.get(id),
  getSessionDetail: (id: string) => details.get(id),
  addMessage: (message: any) => {
    const saved = details.get(message.session_id) || { messages: [] }
    const id = saved.messages.length + 1
    saved.messages.push({ ...message, id })
    details.set(message.session_id, saved)
    return id
  },
  addMessages: () => { throw new Error('unexpected atomic write') },
}))
vi.mock('../../packages/server/src/modules/studio/services/chat-run/compression', () => ({
  getOrCreateSession: (map: Map<string, any>, id: string) => {
    if (!map.has(id)) map.set(id, { messages: [], queue: [], events: [], isWorking: false })
    return map.get(id)
  },
}))
import { LoopRuntime } from '../../packages/server/src/modules/studio/services/chat-run/loop-runtime'
import { flushBridgePendingToDb } from '../../packages/server/src/modules/studio/services/chat-run/bridge-message'

function fixture(recovery = false) {
  const owner = { id: 1, username: 'fixture', role: 'super_admin' }
  rows.set('session', { id: 'session', source: 'api_server', profile: 'alpha', user_id: '1' })
  const item = { session_id: 'session', route: { profile: 'alpha' }, generation: 2, next_due_at: 0,
    ...(recovery ? { recovery: { id: 'recovery', status: 'pending' } } : {}) }
  const host: any = {
    sessionMap: new Map(), profiles: () => ['alpha'], owner: vi.fn(() => owner),
    socket: () => ({ data: { user: owner } }), closing: () => false, emit: vi.fn(),
    abortRecovery: vi.fn(async () => ({ outcome: 'unknown', reason: 'synthetic unproven' })),
    observeRecoveryExit: vi.fn(async () => ({ outcome: 'unknown', reason: 'synthetic unproven' })),
    request: vi.fn(async (data: any) => data.action === 'recovery_resources' ? { resources: [] }
      : data.action === 'loop_list' ? { loops: [item] }
      : data.op === 'recovery_claim' ? { authorized: true, generation: 2, reservation_id: 'reservation' }
      : data.op === 'claim' ? { claimed: true, generation: 2, prompt: 'inspect' }
        : { authorized: true }),
    run: vi.fn(async (_socket: any, data: any) => {
      await data.recoveryRegister?.({ version: 1, owner_id: '1', profile: 'alpha', run_marker: 'recovery_v1_fixture', token_path: '/synthetic/token-reference' })
      await data.loopBeforeDispatch()
      data.loopDispatchConfirmed()
      data.onEvent('run.started', { run_id: 'run' })
      data.loopProof({ done: true, run_id: 'run', session_id: 'session', status: 'complete', result: { completed: true } })
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
  beforeEach(() => { rows.clear(); details.clear(); vi.clearAllMocks() })
  afterEach(() => { vi.restoreAllMocks(); vi.useRealTimers() })
  it('restores only the bound session/profile recovery diagnosis without exposing cleanup credentials', async () => {
    const { host, runtime } = fixture()
    const resources = [{ session_id: 'session', profile: 'alpha', recovery_id: 'old-recovery',
      expected_generation: 1, cancel_requested: true, outcome: 'unknown', last_observed_at: 100,
      evidence: { reason: 'original_worker_unavailable' }, model_run_auth: { token_path: 'PRIVATE' } },
    { session_id: 'other', profile: 'alpha', cancel_requested: true },
    { session_id: 'session', profile: 'beta', cancel_requested: true }]
    host.request.mockResolvedValue({ resources })
    const issues = await runtime.recoveryIssues('session', 'alpha')
    expect(issues).toHaveLength(1)
    expect(issues[0]).toMatchObject({ event: 'agent.event', recovery_id: 'old-recovery', generation: 1, last_observed_at: 100000 })
    expect(issues[0].text).toContain('Recovery stop result pending verification')
    expect(issues[0].text).toContain('original_worker_unavailable')
    expect(JSON.stringify(issues)).not.toContain('PRIVATE')
    expect(host.run).not.toHaveBeenCalled()
    host.request.mockResolvedValue({ resources: [] })
    expect(await runtime.recoveryIssues('session', 'alpha')).toEqual([])
  })

  it('owns one timer and stops polling on close', async () => {
    vi.useFakeTimers()
    const { host, runtime } = fixture()
    host.request.mockImplementation(async (data: any) => data.action === 'loop_list' ? { loops: [] } : {})
    runtime.start(); runtime.start()
    await vi.advanceTimersByTimeAsync(1100)
    expect(host.request.mock.calls.filter(([v]: any[]) => v.action === 'loop_list')).toHaveLength(3)
    await runtime.close()
    const count = host.request.mock.calls.length
    await vi.advanceTimersByTimeAsync(2000)
    runtime.start()
    expect(host.request).toHaveBeenCalledTimes(count)
  })
  it('backs off uncertain reconciliation and logs only safe correlation and error classification', async () => {
    const { host, runtime } = fixture()
    let now = Date.now()
    vi.spyOn(Date, 'now').mockImplementation(() => now)
    const normal = host.request.getMockImplementation()!
    host.request.mockImplementation(async (data: any) => {
      if (['claim', 'reconcile_claim'].includes(data.op)) throw new Error('timed out SECRET bearer token prompt')
      return normal(data)
    })
    await settle(runtime)
    await runtime.poll()
    const first = host.request.mock.calls.length
    now += 1999; await runtime.poll()
    expect(host.request.mock.calls.slice(first).filter(([v]: any[]) => v.op === 'reconcile_claim')).toHaveLength(0)
    now += 2; await runtime.poll()
    const second = host.request.mock.calls.length
    now += 2100; await runtime.poll()
    expect(host.request.mock.calls.slice(second).filter(([v]: any[]) => v.op === 'reconcile_claim')).toHaveLength(0)
    expect(host.request.mock.calls.filter(([v]: any[]) => v.op === 'claim')).toHaveLength(1)
    expect(log.warn).toHaveBeenCalledWith(expect.objectContaining({ event: 'loop.claim', session_id: 'session', generation: 2, error_class: 'timeout' }), expect.any(String))
    expect(JSON.stringify(log.warn.mock.calls)).not.toContain('SECRET')
    runtime.close()
  })
  it('retries a transient read failure after backoff without a process-wide permanent fault', async () => {
    const { host, runtime } = fixture()
    let now = Date.now()
    vi.spyOn(Date, 'now').mockImplementation(() => now)
    const normal = host.request.getMockImplementation()!
    let failed = false
    host.request.mockImplementation(async (data: any) => {
      if (data.action === 'loop_list' && !failed) {
        failed = true
        throw new Error('synthetic list failure')
      }
      return normal(data)
    })
    await runtime.poll()
    expect(host.run).not.toHaveBeenCalled()
    const calls = host.request.mock.calls.length
    await runtime.poll()
    expect(host.request).toHaveBeenCalledTimes(calls)
    now += 2100
    await settle(runtime)
    expect(host.run).toHaveBeenCalledOnce()
    expect(host.request.mock.calls.filter(([v]: any[]) => v.action === 'loop_recover').map(([v]: any[]) => v))
      .toEqual([{ action: 'loop_recover', profiles: ['alpha'], recovery_only: true }])
  })
  it('retries recovery-only startup after a lost acknowledgement without blocking unrelated business sessions', async () => {
    const { host, runtime, item } = fixture(true)
    const other = { ...item, session_id: 'other', recovery: undefined }
    rows.set('other', { ...rows.get('session'), id: 'other' })
    let now = Date.now(), attempts = 0
    vi.spyOn(Date, 'now').mockImplementation(() => now)
    const normal = host.request.getMockImplementation()!
    host.request.mockImplementation(async (data: any) => {
      if (data.action === 'loop_recover') {
        expect(data).toEqual({ action: 'loop_recover', profiles: ['alpha'], recovery_only: true })
        if (++attempts === 1) throw new Error('synthetic startup recovery acknowledgement loss')
        return { loops: [] }
      }
      if (data.action === 'loop_list') return { loops: [item, other] }
      if (data.op === 'claim') return { claimed: true, generation: 2, prompt: '/status' }
      return normal(data)
    })
    await settle(runtime)
    expect(attempts).toBe(1)
    expect(host.command).toHaveBeenCalledOnce()
    expect(host.run).not.toHaveBeenCalled()
    expect(host.request).not.toHaveBeenCalledWith(expect.objectContaining({ op: 'recovery_claim' }))
    await settle(runtime)
    expect(attempts).toBe(1)
    now += 2100
    await settle(runtime)
    expect(attempts).toBe(2)
    expect(host.request).toHaveBeenCalledWith(expect.objectContaining({ op: 'recovery_commit' }))
    await settle(runtime)
    expect(attempts).toBe(2)
    runtime.close()
  })
  it('rechecks owner before reconciliation and never substitutes a newer listed generation', async () => {
    const { host, runtime, item } = fixture()
    let now = Date.now()
    vi.spyOn(Date, 'now').mockImplementation(() => now)
    const normal = host.request.getMockImplementation()!
    host.request.mockImplementation(async (data: any) => {
      if (data.op === 'claim') throw new Error('synthetic uncertain claim')
      if (data.op === 'reconcile_claim') return { claim_resolution: 'claim-only-retired' }
      return normal(data)
    })
    await settle(runtime)
    const claim = host.request.mock.calls.find(([v]: any[]) => v.op === 'claim')![0]
    expect(claim.generation).toBe(2)
    item.generation = 3
    rows.get('session').user_id = 'other'
    await runtime.poll()
    expect(host.request).not.toHaveBeenCalledWith(expect.objectContaining({ op: 'reconcile_claim' }))
    expect(host.run).not.toHaveBeenCalled()
    rows.get('session').user_id = '1'
    now += 2100
    await runtime.poll()
    expect(host.request).toHaveBeenCalledWith(expect.objectContaining({ op: 'reconcile_claim', tick_id: claim.tick_id, generation: 2, profile: 'alpha' }))
    expect(host.request.mock.calls.filter(([v]: any[]) => v.op === 'claim')).toHaveLength(1)
    expect(host.run).not.toHaveBeenCalled()
  })
  it('keeps uncertain work blocked while allowing a different authorized session to proceed', async () => {
    const { host, runtime, item } = fixture()
    const other = { ...item, session_id: 'other' }
    rows.set('other', { ...rows.get('session'), id: 'other' })
    host.request.mockImplementation(async (data: any) => {
      if (data.action === 'loop_list') return { loops: [item, other] }
      if (data.session_id === 'session' && ['claim', 'reconcile_claim'].includes(data.op)) throw new Error('synthetic unavailable receipt')
      return data.op === 'claim' ? { claimed: true, generation: 2, prompt: '/status' } : { authorized: true }
    })
    await settle(runtime)
    expect(host.command).toHaveBeenCalledOnce()
    expect(host.request.mock.calls.filter(([v]: any[]) => v.op === 'claim' && v.session_id === 'session')).toHaveLength(1)
    await runtime.poll()
    expect(host.request).toHaveBeenCalledWith(expect.objectContaining({ op: 'reconcile_claim', session_id: 'session' }))
    expect(host.emit).toHaveBeenCalledWith('session', 'loop.updated', expect.objectContaining({ claim_resolution: 'receipt-uncertain' }))
    expect(host.run).not.toHaveBeenCalled()
  })
  it('logs bounded loop correlation without prompt response or error bodies', async () => {
    const { AgentBridgeClient } = await import('../../packages/server/src/modules/hermes/services/bridge/client')
    const client = new AgentBridgeClient()
    const summary = (client as any).summarizePayload({ action: 'loop_operation', op: 'reconcile_claim',
      session_id: 'session', profile: 'alpha', tick_id: 'x'.repeat(300), generation: 2, expected_generation: 3,
      prompt: 'SECRET', response: 'SECRET', reason: 'SECRET', error: 'SECRET', command: 'SECRET', body: 'SECRET' })
    expect(summary).toEqual({ action: 'loop_operation', op: 'reconcile_claim', session_id: 'session', profile: 'alpha',
      tick_id: 'x'.repeat(128), generation: 2, expected_generation: 3 })
    expect(JSON.stringify(summary)).not.toContain('SECRET')
  })
  it('accepts the string message ID emitted by real bridge persistence with native proof', async () => {
    const { host, runtime } = fixture()
    host.run.mockImplementationOnce(async (_: any, data: any) => {
      await data.loopBeforeDispatch(); data.loopDispatchConfirmed()
      data.onEvent('run.started', { run_id: 'run' })
      data.loopProof({ done: true, run_id: 'run', session_id: 'session', status: 'complete', result: { completed: true } })
      const state = host.sessionMap.get('session')
      state.bridgePendingAssistantContent = 'verified final response'
      const messageId = flushBridgePendingToDb(state, 'session', 'native-marker')
      expect(typeof messageId).toBe('string')
      expect(typeof details.get('session').messages[0].id).toBe('number')
      data.onEvent('run.completed', { run_id: 'run', message_id: messageId, output: 'verified final response' })
    })
    await settle(runtime)
    expect(host.request).toHaveBeenCalledWith(expect.objectContaining({ op: 'complete', run_id: 'run' }))
    expect(host.request).not.toHaveBeenCalledWith(expect.objectContaining({ op: 'unknown' }))
  })
  it('completes only after matching terminal proof and persisted assistant readback', async () => {
    const { host, runtime } = fixture()
    await settle(runtime)
    expect(host.request).toHaveBeenCalledWith(expect.objectContaining({ op: 'complete', run_id: 'run', generation: 2 }))
    expect(host.sessionMap.get('session').isWorking).toBe(false)
  })
  for (const messageId of ['01', '1 ', null, undefined]) {
    it(`rejects non-exact persistence ID ${String(messageId)} without replay`, async () => {
      const { host, runtime } = fixture()
      host.run.mockImplementationOnce(async (_: any, data: any) => {
        await data.loopBeforeDispatch(); data.loopDispatchConfirmed()
        data.onEvent('run.started', { run_id: 'run' })
        data.loopProof({ done: true, run_id: 'run', session_id: 'session', status: 'complete' })
        details.set('session', { messages: [{ id: 1, role: 'assistant', content: 'done' }] })
        data.onEvent('run.completed', { run_id: 'run', message_id: messageId })
      })
      await settle(runtime)
      expect(host.request).toHaveBeenCalledWith(expect.objectContaining({ op: 'unknown' }))
      expect(host.run).toHaveBeenCalledOnce()
    })
  }
  it('rejects native proof belonging to another session even when run IDs match', async () => {
    const { host, runtime } = fixture()
    host.run.mockImplementationOnce(async (_: any, data: any) => {
      await data.loopBeforeDispatch(); data.loopDispatchConfirmed()
      data.onEvent('run.started', { run_id: 'run' })
      data.loopProof({ done: true, run_id: 'run', session_id: 'other-session', status: 'complete' })
      details.set('session', { messages: [{ id: 1, role: 'assistant', content: 'done' }] })
      data.onEvent('run.completed', { run_id: 'run', message_id: '1' })
    })
    await settle(runtime)
    expect(host.request).not.toHaveBeenCalledWith(expect.objectContaining({ op: 'complete' }))
    expect(host.request).toHaveBeenCalledWith(expect.objectContaining({ op: 'unknown' }))
  })
  it.each([
    ['partial', { result: { partial: true } }, 'run.completed', 'unknown'],
    ['interrupted', { result: { interrupted: true } }, 'abort.completed', 'interrupted'],
    ['failed', { error: 'synthetic failure', status: 'error' }, 'run.failed', 'failed'],
    ['stale run', { run_id: 'old-run' }, 'run.completed', 'unknown'],
  ])('retains count and does not replay %s terminal evidence', async (_label, change, event, outcome) => {
    const { host, runtime } = fixture()
    host.run.mockImplementationOnce(async (_: any, data: any) => {
      await data.loopBeforeDispatch(); data.loopDispatchConfirmed()
      data.onEvent('run.started', { run_id: 'run' })
      data.loopProof({ done: true, run_id: 'run', session_id: 'session', status: 'complete', ...change })
      data.onEvent(event, { run_id: 'run', message_id: '1' })
    })
    await settle(runtime)
    expect(host.run).toHaveBeenCalledOnce()
    expect(host.request).toHaveBeenCalledWith(expect.objectContaining({ op: outcome, not_started: false }))
    expect(host.request).not.toHaveBeenCalledWith(expect.objectContaining({ op: 'abandon' }))
    // Fixture list remains due; real-store no-replay is exercised separately.
  })
  it('rejects cross-profile rebinding before native dispatch', async () => {
    const { host, runtime } = fixture()
    host.run.mockImplementationOnce(async (_: any, data: any) => {
      rows.get('session').profile = 'beta'
      await data.loopBeforeDispatch()
    })
    await settle(runtime)
    expect(host.request).not.toHaveBeenCalledWith(expect.objectContaining({ op: 'inject' }))
    expect(host.request).toHaveBeenCalledWith(expect.objectContaining({ op: 'abandon' }))
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
      ? { loops: [{ session_id: 'session', route: { profile: 'alpha' }, generation: 2, next_due_at: 0 }] }
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
  it('cancels only the persisted original binding after 301 seconds, retaining unknown capacity', async () => {
    const { host, runtime } = fixture()
    const resource = { session_id: 'session', profile: 'old-profile', recovery_id: 'old-recovery', expected_generation: 1,
      reservation_id: 'old-reservation', run_id: 'old-run', run_marker: 'recovery_v1_old', deadline_at: Date.now() / 1000 - 301,
      model_run_auth: { version: 1, profile: 'old-profile', owner_id: 'old-owner', run_marker: 'recovery_v1_old', token_path: '/synthetic/PRIVATE' } }
    host.request.mockImplementation(async (value: any) => value.action === 'recovery_resources' ? { resources: [resource] } : { loops: [] })
    await runtime.poll()
    expect(host.abortRecovery).toHaveBeenCalledWith(resource, 'Recovery decision deadline expired')
    expect((runtime as any).resources.size).toBe(1)
    expect(host.run).not.toHaveBeenCalled()
    expect(JSON.stringify(host.emit.mock.calls)).not.toContain('PRIVATE')
    host.abortRecovery.mockResolvedValue({ outcome: 'exited_proven' })
    await runtime.poll()
    expect((runtime as any).resources.size).toBe(0)
  })
  it('awaits original exit during close and closes admission before taking the final inventory', async () => {
    const { host, runtime } = fixture()
    const resource = { session_id: 'session', profile: 'alpha', recovery_id: 'original', expected_generation: 1, reservation_id: 'reservation' }
    let saved = [resource]
    let release!: () => void
    const exit = new Promise<void>(resolve => { release = resolve })
    host.request.mockImplementation(async (value: any) => value.action === 'recovery_resources' ? { resources: saved } : {})
    host.abortRecovery.mockImplementation(async () => { await exit; saved = []; return { outcome: 'exited_proven' } })
    let closed = false
    const closing = runtime.close(Date.now() + 3000).then(result => { closed = true; return result })
    await vi.waitFor(() => expect(host.abortRecovery).toHaveBeenCalledOnce())
    expect(closed).toBe(false)
    expect(host.request.mock.calls[0][0]).toEqual({ action: 'recovery_quiesce' })
    release()
    await expect(closing).resolves.toEqual({ status: 'drained', unresolved: 0 })
    expect(host.run).not.toHaveBeenCalled()
  })
  it('cancels recovery promptly but retains ordinary in-flight responsibility at the shutdown deadline', async () => {
    const { host, runtime } = fixture()
    let release!: () => void
    const ordinary = new Promise<void>(resolve => { release = resolve })
    host.run.mockImplementation(async () => { await ordinary })
    await runtime.poll()
    await vi.waitFor(() => expect(host.run).toHaveBeenCalledOnce())
    let quiesced = false, cancelled = false
    const resource = { session_id: 'other', profile: 'beta', recovery_id: 'original', expected_generation: 1, reservation_id: 'original' }
    host.request.mockImplementation(async (data: any) => {
      if (data.action === 'recovery_quiesce') quiesced = true
      return data.action === 'recovery_resources' ? { resources: quiesced && !cancelled ? [resource] : [] } : {}
    })
    host.abortRecovery.mockImplementation(async () => { cancelled = true; return { outcome: 'exited_proven' } })
    const closing = runtime.close(Date.now() + 400)
    await vi.waitFor(() => expect(host.abortRecovery).toHaveBeenCalledWith(resource, 'Studio shutdown; recovery decisions revoked'))
    await expect(closing).resolves.toEqual({ status: 'unresolved', unresolved: 1 })
    expect((runtime as any).runs.size).toBe(1)
    release()
    await vi.waitFor(() => expect((runtime as any).runs.size).toBe(0))
    await expect(runtime.close(Date.now() + 400)).resolves.toEqual({ status: 'drained', unresolved: 0 })
  })
  it('does not call a racing poll drained or admit its returned ordinary loop after quiesce', async () => {
    const { host, runtime, item } = fixture()
    let release!: (value: any) => void
    const listing = new Promise(resolve => { release = resolve })
    const normal = host.request.getMockImplementation()!
    host.request.mockImplementation((data: any) => data.action === 'loop_list' ? listing : normal(data))
    const poll = runtime.poll()
    await vi.waitFor(() => expect(host.request).toHaveBeenCalledWith(expect.objectContaining({ action: 'loop_list' })))
    await expect(runtime.close(Date.now() + 300)).resolves.toEqual({ status: 'unresolved', unresolved: 1 })
    expect(host.run).not.toHaveBeenCalled()
    release({ loops: [item] })
    await poll
    expect(host.run).not.toHaveBeenCalled()
    await expect(runtime.close(Date.now() + 300)).resolves.toEqual({ status: 'drained', unresolved: 0 })
  })
  it('reports unresolved rather than draining on a short remaining shutdown budget', async () => {
    vi.useFakeTimers()
    const { host, runtime } = fixture()
    const resource = { session_id: 'session', profile: 'alpha', recovery_id: 'original', expected_generation: 1, reservation_id: 'reservation' }
    host.request.mockImplementation(async (value: any) => value.action === 'recovery_resources' ? { resources: [resource] } : {})
    host.abortRecovery.mockImplementation(() => new Promise(() => {}))
    const closing = runtime.close(Date.now() + 250)
    await vi.advanceTimersByTimeAsync(151)
    await expect(closing).resolves.toEqual({ status: 'unresolved', unresolved: 1 })
    expect((runtime as any).resources.size).toBe(1)
  })
  it('protected rollback does not admit recovery or ordinary scheduling', async () => {
    const { host, runtime } = fixture(true)
    vi.stubEnv('HERMES_STUDIO_COMPATIBILITY_ROLLBACK', '1')
    try {
      await runtime.poll()
      expect(host.request).not.toHaveBeenCalled()
      expect(host.run).not.toHaveBeenCalled()
    } finally { vi.unstubAllEnvs() }
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
