import { createServer as httpServer } from 'node:http'
import { createServer as socketServer } from 'node:net'
import { spawn, type ChildProcess } from 'node:child_process'
import { appendFileSync, mkdirSync, mkdtempSync, writeFileSync } from 'node:fs'
import { join, resolve } from 'node:path'
import { tmpdir } from 'node:os'
import { DatabaseSync } from 'node:sqlite'
import { once } from 'node:events'
import { expect, it, vi } from 'vitest'

vi.mock('../../packages/server/src/modules/studio/public/runs/prompt', () => ({ getSystemPrompt: () => 'Synthetic local acceptance. Reply briefly. No tools.' }))
vi.mock('../../packages/server/src/modules/studio/repositories/compression-snapshot', () => ({ getCompressionSnapshot: () => null, deleteCompressionSnapshot: vi.fn() }))
vi.mock('../../packages/server/src/modules/studio/public/provider-runtime', () => ({ getModelContextLength: () => 256_000 }))
vi.mock('../../packages/server/src/modules/studio/services/chat-run/model-config', () => ({ resolveBridgeRunModelConfig: async () => ({ model: 'synthetic-local', provider: 'custom' }) }))
vi.mock('../../packages/server/src/modules/studio/public/authorized-provider-runtime', () => ({ resolveAuthorizedProviderRuntimeCredentials: vi.fn() }))
vi.mock('../../packages/server/src/modules/studio/services/chat-run/workspace-diff-tracker', () => ({ startWorkspaceRunCheckpoint: vi.fn(), completeWorkspaceRunCheckpoint: () => null }))
vi.mock('../../packages/server/src/modules/studio/public/profile-config', () => ({
  getProfileDir: () => process.env.LOOP_ACCEPTANCE_HOME!, saveEnvValueForProfile: vi.fn(),
  readConfigYamlForProfile: async () => ({ compression: { enabled: false } }),
}))
vi.mock('../../packages/server/src/modules/studio/public/auth', () => ({
  issueModelRunJwt: async () => 'synthetic-local-only',
  authenticateUserToken: async () => ({ id: 1, username: 'synthetic', role: 'super_admin' }),
}))

it.skipIf(process.env.NATIVE_LOOP_WALL !== '1')('real timer, broker, worker, SQLite and local-provider recurring acceptance', async () => {
  const evidence = process.env.LOOP_ACCEPTANCE_DIR
  const core = process.env.LOOP_ACCEPTANCE_CORE
  expect(evidence).toBeTruthy(); expect(core).toBeTruthy()
  const home = join(evidence!, 'home'); mkdirSync(home, { recursive: true })
  process.env.LOOP_ACCEPTANCE_HOME = home
  const workspace = join(home, 'workspace'); mkdirSync(workspace, { recursive: true })
  const socketDir = mkdtempSync(join(process.env.LOOP_ACCEPTANCE_SOCKET_ROOT || tmpdir(), 'loop-wall-'))
  const endpoint = `ipc://${join(socketDir, 'broker.sock')}`
  const proxyEndpoint = `ipc://${join(socketDir, 'fault.sock')}`
  const events: any[] = []
  const record = (origin: string, value: any = {}) => {
    const event = { at: new Date().toISOString(), epoch_ms: Date.now(), origin, ...value }
    events.push(event); appendFileSync(join(evidence!, 'events.jsonl'), JSON.stringify(event) + '\n')
    console.log(JSON.stringify(event))
  }
  const db = new DatabaseSync(join(home, 'studio.db'))
  vi.doMock('../../packages/server/src/modules/studio/infrastructure/database/index', () => ({ getDb: () => db, isSqliteAvailable: () => true, getStoragePath: () => home }))
  const { initAllHermesTables } = await import('../../packages/server/src/modules/studio/infrastructure/database/schemas')
  initAllHermesTables()
  const { createSession, getSessionDetail } = await import('../../packages/server/src/modules/studio/repositories/session-store')
  const { handleBridgeRun } = await import('../../packages/server/src/modules/studio/services/chat-run/handle-bridge-run')
  const { AgentBridgeClient } = await import('../../packages/server/src/modules/hermes/services/bridge/client')
  const { LoopRuntime } = await import('../../packages/server/src/modules/studio/services/chat-run/loop-runtime')
  let modelCalls = 0
  const model = httpServer(async (req, res) => {
    let raw = ''; for await (const chunk of req) raw += chunk
    const body = JSON.parse(raw || '{}')
    const text = `Synthetic local response ${++modelCalls}`
    record('synthetic-provider', { path: req.url, call: modelCalls, stream: !!body.stream })
    if (body.stream) {
      res.writeHead(200, { 'Content-Type': 'text/event-stream' })
      for (const chunk of [
        { choices: [{ index: 0, delta: { role: 'assistant', content: text }, finish_reason: null }] },
        { choices: [{ index: 0, delta: {}, finish_reason: 'stop' }], usage: { prompt_tokens: 12, completion_tokens: 5, total_tokens: 17 } },
      ]) res.write(`data: ${JSON.stringify({ id: `synthetic-${modelCalls}`, object: 'chat.completion.chunk', created: Math.floor(Date.now() / 1000), model: 'synthetic-local', ...chunk })}\n\n`)
      res.end('data: [DONE]\n\n')
    } else {
      res.writeHead(200, { 'Content-Type': 'application/json' })
      res.end(JSON.stringify({ id: `synthetic-${modelCalls}`, object: 'chat.completion', model: 'synthetic-local', choices: [{ index: 0, message: { role: 'assistant', content: text }, finish_reason: 'stop' }], usage: { prompt_tokens: 12, completion_tokens: 5, total_tokens: 17 } }))
    }
  })
  model.listen(0, '127.0.0.1'); await once(model, 'listening')
  const port = (model.address() as any).port
  writeFileSync(join(home, 'config.yaml'), `model:\n  default: synthetic-local\n  provider: custom\n  base_url: http://127.0.0.1:${port}/v1\n  api_key: synthetic-local-only\n  api_mode: chat_completions\nagent:\n  max_turns: 2\n  enabled_toolsets: []\ncompression:\n  enabled: false\nloops:\n  min_interval_seconds: 30\nmcp_servers: {}\n`)
  const cleanEnv: Record<string, string> = {
    PATH: process.env.PATH!, HOME: home, HERMES_HOME: home, HERMES_AGENT_BRIDGE_BASE_HOME: home,
    HERMES_AGENT_ROOT: core!, PYTHONPATH: core!, PYTHONDONTWRITEBYTECODE: '1',
    HERMES_SKIP_VENV_CHECK: '1', OPENAI_API_KEY: 'synthetic-local-only',
    OPENAI_BASE_URL: `http://127.0.0.1:${port}/v1`, NO_PROXY: '127.0.0.1,localhost',
  }
  const broker: ChildProcess = spawn(process.env.NATIVE_PYTHON || 'python3', ['-B', resolve('packages/server/src/modules/hermes/services/bridge/python/hermes_bridge.py'), '--endpoint', endpoint, '--agent-root', core!, '--hermes-home', home], { env: cleanEnv, stdio: ['ignore', 'pipe', 'pipe'] })
  broker.stdout!.on('data', data => appendFileSync(join(evidence!, 'broker.stdout.log'), data))
  broker.stderr!.on('data', data => appendFileSync(join(evidence!, 'broker.stderr.log'), data))
  const backend = new AgentBridgeClient({ endpoint, timeoutMs: 20_000 })
  const client = new AgentBridgeClient({ endpoint: proxyEndpoint, timeoutMs: 20_000 })
  let claims = 0, fault = false, rejectRemaining = 0, loseCommitted = false
  let failedRequest: any
  const proxy = socketServer(conn => {
    let raw = ''
    conn.on('data', async chunk => {
      raw += chunk.toString(); if (!raw.includes('\n')) return
      const request = JSON.parse(raw.split('\n')[0]); raw = ''
      if (request.op === 'claim') {
        claims++; record('claim-transport', { tick_id: request.tick_id, generation: request.generation, attempt: claims })
        if (claims === 2 || rejectRemaining > 0) {
          if (rejectRemaining > 0) rejectRemaining--
          failedRequest = request
          fault = true; record('fault-precommit', { tick_id: request.tick_id })
          conn.end(JSON.stringify({ ok: false, error: 'Synthetic precommit startup failure', error_type: 'SyntheticStartupError' }) + '\n'); return
        }
      }
      try {
        const result = await backend.request(request)
        if (request.op === 'claim' && loseCommitted) {
          loseCommitted = false
          record('fault-committed-receipt-loss', { tick_id: request.tick_id, claimed: result.claimed })
          conn.destroy(); return
        }
        conn.end(JSON.stringify(result) + '\n')
      }
      catch { conn.end(JSON.stringify({ ok: false, error: 'Synthetic proxy backend request failed' }) + '\n') }
    })
  })
  proxy.listen(join(socketDir, 'fault.sock')); await once(proxy, 'listening')
  const session = 'synthetic-recurring'
  createSession({ id: session, profile: 'default', source: 'cli', user_id: '1', model: 'synthetic-local', provider: 'custom', workspace })
  const owner = { id: 1, username: 'synthetic', role: 'super_admin' }
  const emitter = { emit: (name: string, value: any) => { if (['run.started', 'run.completed', 'run.failed'].includes(name)) record('execution-event', { name, run_id: value.run_id, error: value.error }) }, except: () => emitter }
  const socket: any = { connected: true, emit: emitter.emit, join: vi.fn(), to: () => emitter, data: { user: owner } }
  const nsp: any = { adapter: { rooms: new Map() }, to: () => emitter }
  const sessionMap = new Map()
  const host: any = {
    sessionMap, profiles: () => ['default'], owner: () => owner, socket: () => socket, closing: () => false,
    request: (value: any) => client.request(value), command: () => { throw new Error('Not a command acceptance') },
    emit: (_id: string, _event: string, value: any) => record('loop.updated', value),
    run: async (_socket: any, data: any, profile: string) => {
      record('loop-run', { count: events.filter(e => e.origin === 'loop-run').length + 1 })
      await handleBridgeRun(nsp, socket, { ...data, instructions: 'Synthetic local acceptance' }, profile, sessionMap, client, false, vi.fn(), vi.fn())
    },
  }
  const runtime = new LoopRuntime(host)
  const waitFor = async (predicate: () => boolean | Promise<boolean>, label: string) => {
    await vi.waitFor(async () => expect(await predicate(), label).toBe(true), { timeout: 150_000, interval: 200 })
  }
  const status = () => client.command(session, '/loop status', 'default') as Promise<any>
  try {
    await waitFor(async () => { try { return !!(await backend.request({ action: 'ping' })).ok } catch { return false } }, 'broker ready')
    record('broker-ready', { pid: broker.pid })
    const configured: any = await client.command(session, '/loop 30s synthetic recurring acceptance --times 3', 'default')
    record('loop-created', configured); expect(configured.created).toBe(true)
    runtime.start()
    await waitFor(() => events.some(e => e.origin === 'loop.updated' && e.loop_state?.ticks_fired === 1 && !e.current_tick), 'first immediate completion')
    const before: any = await status()
    for (const origin of ['normal-message', 'completion-notification']) {
      record(origin)
      await handleBridgeRun(nsp, socket, { session_id: session, source: 'cli', input: `Synthetic ${origin}`, model: 'synthetic-local', provider: 'custom', ...(origin === 'completion-notification' ? { background_notification_kind: 'kanban' } : {}) }, 'default', sessionMap, client, false, vi.fn(), vi.fn())
      const after: any = await status()
      expect(after.loop_state).toEqual(before.loop_state)
    }
    await waitFor(() => events.some(e => e.origin === 'loop.updated' && e.loop_state?.status === 'done'), 'two subsequent wall-clock wakeups')
    const final: any = await status(); record('final-status', final)
    expect(fault).toBe(true); expect(final.loop_state.ticks_fired).toBe(3)
    const duplicate: any = await backend.request({ ...failedRequest, op: 'reconcile_claim' })
    expect(duplicate.claim_resolution).toBe('not-committed-fenced')
    expect(duplicate.loop_state).toEqual(final.loop_state)
    expect((await backend.request(failedRequest)).claimed).toBe(false)
    record('late-and-duplicate-fenced', { tick_id: failedRequest.tick_id, ticks_fired: duplicate.loop_state.ticks_fired })
    expect(events.filter(e => e.origin === 'loop-run')).toHaveLength(3)
    expect(events.some(e => e.claim_resolution === 'not-committed-fenced' && e.loop_state.ticks_fired === 1)).toBe(true)
    const ticks = events.filter(e => e.origin === 'loop-run')
    expect(ticks[1].epoch_ms - ticks[0].epoch_ms).toBeGreaterThanOrEqual(30_000)
    expect(ticks[2].epoch_ms - ticks[1].epoch_ms).toBeGreaterThanOrEqual(30_000)
    expect(getSessionDetail(session)!.messages.filter(m => m.role === 'assistant' && m.content.startsWith('Synthetic local response'))).toHaveLength(5)
    await client.command(session, '/loop 30s synthetic control acceptance --times 3', 'default')
    await client.command(session, '/loop pause', 'default')
    const paused: any = await status(); record('pause-status', paused)
    expect(paused.loop_state.status).toBe('paused')
    const count = events.filter(e => e.origin === 'loop-run').length
    await new Promise(r => setTimeout(r, 31_000))
    expect(events.filter(e => e.origin === 'loop-run')).toHaveLength(count)
    await client.command(session, '/loop stop', 'default')
    const stopped: any = await status(); record('stop-status', stopped)
    expect(stopped.loop_state.status).toBe('cleared')
    await new Promise(r => setTimeout(r, 31_000))
    expect(events.filter(e => e.origin === 'loop-run')).toHaveLength(count)
    record('stop-held-past-cadence', { loop_runs: count })
    loseCommitted = true
    await client.command(session, '/loop 30s synthetic committed receipt loss --times 1', 'default')
    await waitFor(async () => {
      const state: any = await status()
      return state.loop_state.status === 'paused' && state.loop_state.paused_reason.includes('exhausted')
    }, 'committed receipt loss retains exhausted count without injection')
    const committed: any = await status(); record('committed-loss-status', committed)
    expect(committed.loop_state.ticks_fired).toBe(1)
    expect(events.filter(e => e.origin === 'loop-run')).toHaveLength(count)
    rejectRemaining = 4
    await client.command(session, '/loop 30s synthetic persistent startup failure --times 1', 'default')
    await waitFor(async () => {
      const state: any = await status()
      return state.loop_state.status === 'paused' && state.loop_state.paused_reason.includes('startup-failed')
    }, 'persistent startup failures pause durably')
    const failed: any = await status(); record('persistent-failure-status', failed)
    expect(failed.startup_failure.attempts).toBe(4)
    expect(failed.loop_state.ticks_fired).toBe(0)
    expect(events.filter(e => e.origin === 'loop-run')).toHaveLength(count)
    await client.command(session, '/loop stop', 'default')
    record('acceptance-passed', { provider_substitution: 'loopback deterministic OpenAI-compatible endpoint; actual Core AIAgent execution', loop_runs: 3, endpoint_requests: modelCalls })
  } finally {
    runtime.close()
    try { await backend.shutdown() } catch { broker.kill('SIGTERM') }
    if (broker.exitCode === null && broker.signalCode === null) await once(broker, 'exit')
    proxy.close(); model.close(); db.close()
    vi.doUnmock('../../packages/server/src/modules/studio/infrastructure/database/index')
  }
}, 300_000)
