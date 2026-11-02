/**
 * Agent Bridge runs: Studio owns compression. A pre-run "context window too small" error must
 * settle the run, and the debug force-compress request keeps working below the threshold.
 */
import type { Namespace, Socket } from 'socket.io'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { handleBridgeRun } from '../../packages/server/src/modules/studio/services/chat-run/handle-bridge-run'
import { readConfigYamlForProfile } from '../../packages/server/src/modules/studio/public/profile-config'
import { estimateUsageTokensFromMessages } from '../../packages/server/src/modules/studio/services/chat-run/usage'
import type { PrimaryAgentBridgeClient } from '../../packages/server/src/modules/studio/public/chat-agent-runtime'
import type { HermesMessageRow } from '../../packages/server/src/modules/studio/repositories/session-store'
import type { ChatMessage } from '../../packages/server/src/modules/studio/services/context-compressor'
import type { SessionState } from '../../packages/server/src/modules/studio/services/chat-run/types'

const mocks = vi.hoisted(() => ({
  rows: [] as Partial<HermesMessageRow>[],
  compress: vi.fn(async (history: unknown[]) => ({
    messages: history,
    meta: { compressed: false, llmCompressed: false, verbatimCount: history.length, compressedStartIndex: -1 },
  })),
}))

vi.mock('../../packages/server/src/modules/studio/public/runs/prompt', () => ({
  getSystemPrompt: vi.fn(() => 'system prompt'),
}))

vi.mock('../../packages/server/src/modules/studio/repositories/compression-snapshot', () => ({
  getCompressionSnapshot: vi.fn(() => null),
  deleteCompressionSnapshot: vi.fn(),
}))

vi.mock('../../packages/server/src/modules/studio/services/context-compressor', () => ({
  SUMMARY_PREFIX: '[Previous context summary]',
  ChatContextCompressor: class {
    compress = mocks.compress
  },
}))

vi.mock('../../packages/server/src/modules/studio/repositories/usage-store', () => ({
  updateUsage: vi.fn(),
  getUsage: vi.fn(),
}))

vi.mock('../../packages/server/src/modules/studio/public/logging', () => ({
  logger: { info: vi.fn(), warn: vi.fn(), error: vi.fn(), debug: vi.fn() },
  bridgeLogger: { info: vi.fn(), warn: vi.fn(), error: vi.fn(), debug: vi.fn() },
}))

vi.mock('../../packages/server/src/modules/studio/public/provider-runtime', () => ({
  getModelContextLength: vi.fn(() => 256_000),
}))

vi.mock('../../packages/server/src/modules/studio/services/chat-run/usage', () => ({
  calcAndUpdateUsage: vi.fn(async () => ({ inputTokens: 1, outputTokens: 1 })),
  estimateUsageTokensFromMessages: vi.fn(() => ({ inputTokens: 1, outputTokens: 1 })),
  getCachedBridgeContextOverhead: vi.fn(() => undefined),
  contextTokensWithCachedOverhead: vi.fn((_state: unknown, tokens: number) => tokens),
  updateContextTokenUsage: vi.fn(),
  updateMessageContextTokenUsage: vi.fn(),
}))

vi.mock('../../packages/server/src/modules/studio/services/chat-run/model-config', () => ({
  resolveBridgeRunModelConfig: vi.fn(async () => ({ model: 'gpt-test', provider: 'openai' })),
}))

vi.mock('../../packages/server/src/modules/studio/public/authorized-provider-runtime', () => ({
  resolveAuthorizedProviderRuntimeCredentials: vi.fn(),
}))

vi.mock('../../packages/server/src/modules/studio/services/chat-run/workspace-diff-tracker', () => ({
  startWorkspaceRunCheckpoint: vi.fn(),
  completeWorkspaceRunCheckpoint: vi.fn(() => null),
}))

vi.mock('../../packages/server/src/modules/studio/public/profile-config', () => ({
  getProfileDir: (profile: string) => `/tmp/hermes-bridge-compression/${profile || 'default'}`,
  saveEnvValueForProfile: vi.fn(),
  readConfigYamlForProfile: vi.fn(async () => ({ compression: { enabled: false } })),
}))

vi.mock('../../packages/server/src/modules/studio/public/auth', () => ({
  issueModelRunJwt: vi.fn(async () => 'model-run-token'),
  authenticateUserToken: vi.fn(async () => ({ id: 1, username: 'fixture', role: 'super_admin' })),
}))


import { LoopRuntime } from '../../packages/server/src/modules/studio/services/chat-run/loop-runtime'
import { DatabaseSync } from 'node:sqlite'
import { execFileSync } from 'node:child_process'
import { mkdtempSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'

describe('loop native proof -> real bridge handler -> SQLite assistant readback', () => {
  let db: any
  beforeEach(async () => {
    vi.resetModules()
    db = new DatabaseSync(':memory:')
    vi.doMock('../../packages/server/src/modules/studio/infrastructure/database/index', () => ({
      getDb: () => db, isSqliteAvailable: () => true, getStoragePath: () => ':memory:',
    }))
    const { initAllHermesTables } = await import('../../packages/server/src/modules/studio/infrastructure/database/schemas')
    initAllHermesTables()
  })
  afterEach(() => { vi.restoreAllMocks(); db.close(); vi.doUnmock('../../packages/server/src/modules/studio/infrastructure/database/index'); vi.resetModules() })

  it('retires a committed claim after lost receipt without poisoning subsequent polls', async () => {
    expect(process.env.NATIVE_PYTHON).toBeTruthy()
    const { LoopRuntime } = await import('../../packages/server/src/modules/studio/services/chat-run/loop-runtime')
    const { createSession } = await import('../../packages/server/src/modules/studio/repositories/session-store')
    createSession({ id: 'session-1', profile: 'default', source: 'cli', user_id: '1' })
    const home = mkdtempSync(join(tmpdir(), 'lost-claim-'))
    const native = (value: any) => JSON.parse(execFileSync(process.env.NATIVE_PYTHON!, ['-B', 'tests/server/native_loop_fixture.py', JSON.stringify({ ...value, _home: home })], { encoding: 'utf8' }))
    native({ action: 'fixture_command', command: '30s inspect --times 3' })
    native({ action: 'fixture_due' })
    let lost = false
    const host: any = {
      sessionMap: new Map(), profiles: () => ['default'], owner: () => ({ id: 1 }), closing: () => false,
      socket: vi.fn(), run: vi.fn(), command: vi.fn(), emit: vi.fn(),
      request: vi.fn(async (value: any) => {
        const result = native(value)
        if (value.op === 'claim' && !lost) { lost = true; throw new Error('synthetic committed receipt loss') }
        return result
      }),
    }
    const runtime = new LoopRuntime(host)
    await runtime.poll()
    await vi.waitFor(() => expect((runtime as any).runs.size).toBe(0))
    expect(native({ action: 'fixture_status' }).current_tick.phase).toBe('claimed')
    await runtime.poll()
    const saved = native({ action: 'fixture_status' })
    expect(saved.current_tick).toBeNull()
    expect(saved.loop_state.ticks_fired).toBe(1)
    expect(saved.loop_state.next_due_at * 1000).toBeGreaterThan(Date.now())
    expect(host.run).not.toHaveBeenCalled()
    expect(host.request.mock.calls.filter(([v]: any[]) => v.op === 'claim')).toHaveLength(1)
    runtime.close()
  })

  it.each(['normal', 'committed-lost', 'precommit-late', 'reconcile-lost', 'new-set', 'precommit-new-set', 'legacy-new-set'])('finishes two successive native-proof turns after %s without replay or manual resume', async (mode) => {
    const { createSession, getSessionDetail } = await import('../../packages/server/src/modules/studio/repositories/session-store')
    const { handleBridgeRun } = await import('../../packages/server/src/modules/studio/services/chat-run/handle-bridge-run')
    const { LoopRuntime } = await import('../../packages/server/src/modules/studio/services/chat-run/loop-runtime')
    createSession({ id: 'session-1', profile: 'default', source: 'cli', user_id: '1', model: 'gpt-test', provider: 'openai', workspace: '/audit/isolation/tmp/project' })
    const owner = { id: 1, username: 'fixture', role: 'super_admin' }
    const emitter = { emit: vi.fn(), except: vi.fn(() => ({ emit: vi.fn() })) }
    const nsp: any = { adapter: { rooms: new Map() }, to: () => emitter }
    const socket: any = { connected: true, emit: vi.fn(), join: vi.fn(), to: () => emitter, data: { user: owner } }
    let turn = 0
    const bridge: any = {
      chat: vi.fn(async () => ({ run_id: `native-${++turn}`, status: 'running' })),
      contextEstimate: vi.fn(async () => ({ token_count: 10, fixed_context_tokens: 5, message_count: 0, tool_count: 0, system_prompt_chars: 1 })),
      streamOutput: vi.fn(async function* (runId: string) {
        yield { run_id: runId, session_id: 'session-1', done: false, status: 'running', delta: 'verified response', output: 'verified response', events: [], cursor: 1, event_cursor: 0 }
        yield nativeHome ? native({ action: 'fixture_proof', run_id: runId }) : { run_id: runId, session_id: 'session-1', done: true, status: 'complete', delta: '', output: 'verified response', result: { completed: true }, events: [], cursor: 1, event_cursor: 0 }
      }),
      ackResult: vi.fn(async () => true),
      request: vi.fn(async () => ({ running: false, model_run_background_active: false })),
      getSessionTitle: vi.fn(async () => ({ title: 'fixture' })),
    }
    const sessionMap = new Map()
    expect(process.env.NATIVE_PYTHON).toBeTruthy()
    const nativeHome = mkdtempSync(join(tmpdir(), 'native-loop-store-'))
    let now = Date.now()
    vi.spyOn(Date, 'now').mockImplementation(() => now)
    const native = (value: any) => JSON.parse(execFileSync(process.env.NATIVE_PYTHON!, ['-B', 'tests/server/native_loop_fixture.py', JSON.stringify({ ...value, _home: nativeHome, _now: now / 1000 })], { encoding: 'utf8' }))
    const retained = ['committed-lost', 'reconcile-lost'].includes(mode) ? 1 : 0
    const legacy = mode === 'legacy-new-set'
    if (legacy) {
      const old = native({ action: 'fixture_legacy_claim' })
      expect(old.generation).toBe(4)
      expect(old.current_tick).toEqual({ id: 'legacy-gen4', generation: 4, phase: 'claimed' })
      expect(old.loop_state.ticks_fired).toBe(1)
      expect(native({ action: 'fixture_snapshot' }).meta).toEqual({ generation: 4, tick: old.current_tick })
    } else native({ action: 'fixture_command', command: `30s inspect --times ${2 + retained}` })
    const legacyNewer = legacy ? native({ action: 'fixture_legacy_set', command: '30s newer --times 2' }) : undefined
    const legacyBytes = legacy ? native({ action: 'fixture_snapshot' }).native_bytes : undefined
    let attempted: any = legacy ? { action: 'loop_operation', op: 'claim', session_id: 'session-1', profile: 'default', tick_id: 'legacy-gen4', generation: 4 } : undefined
    let reconciliationLost = false
    const request = async (data: any) => {
      if (mode !== 'normal' && data.op === 'claim' && !attempted) {
        attempted = { ...data }
        if (!mode.startsWith('precommit')) expect(native(data).claimed).toBe(true)
        throw new Error('synthetic claim receipt loss')
      }
      const result = native(data)
      if (mode === 'reconcile-lost' && data.op === 'reconcile_claim' && !reconciliationLost) {
        reconciliationLost = true
        throw new Error('synthetic reconciliation receipt loss after commit')
      }
      return result
    }
    const host: any = {
      sessionMap, profiles: () => ['default'], owner: () => owner, socket: () => socket, closing: () => false,
      emit: vi.fn(), command: vi.fn(),
      request: vi.fn(request),
      run: (_socket: any, data: any, profile: string) => handleBridgeRun(nsp, socket, { ...data, instructions: 'synthetic integration' }, profile, sessionMap, bridge, false, vi.fn(), vi.fn()),
    }
    const runtime = new LoopRuntime(host)
    if (mode !== 'normal') {
      if (!legacy) {
        await runtime.poll()
        await vi.waitFor(() => expect((runtime as any).runs.size).toBe(0))
        expect(bridge.chat).not.toHaveBeenCalled()
      }
      const newer = legacyNewer ?? (mode.endsWith('new-set') ? native({ action: 'fixture_command', command: '30s newer --times 2' }) : undefined)
      await runtime.poll()
      if (mode === 'reconcile-lost') {
        const calls = host.request.mock.calls.length
        await runtime.poll()
        expect(host.request.mock.calls.slice(calls).some(([v]: any[]) => v.op === 'reconcile_claim')).toBe(false)
        now += 2100
        await runtime.poll()
      }
      const saved = native({ action: 'fixture_status' })
      expect(saved.current_tick).toBeNull()
      expect(saved.loop_state.ticks_fired).toBe(retained)
      if (newer) expect(saved.loop_state).toEqual(newer.loop_state)
      else expect(saved.loop_state.next_due_at * 1000).toBeGreaterThan(now)
      if (legacy) {
        const snapshot = native({ action: 'fixture_snapshot' })
        expect(saved.generation).toBe(5)
        expect(snapshot.native_bytes).toBe(legacyBytes)
        expect(snapshot.meta.retired_ticks).toEqual([{ id: 'legacy-gen4', generation: 4, phase: 'claimed', outcome: 'claim-only-retired' }])
        // Deployed old set already overwrote the old count: do not fabricate it.
        expect(snapshot.meta.retired_ticks[0]).not.toHaveProperty('ticks_fired')
        expect(snapshot.meta.claim_fences['legacy-gen4']).toEqual({ ...snapshot.meta.retired_ticks[0], profile: 'default' })
        expect(host.request.mock.calls.map(([v]: any[]) => v.action === 'loop_operation' ? v.op : v.action))
          .toEqual(['loop_recover', 'loop_list', 'reconcile_claim'])
      }
      expect(native(attempted).claimed).toBe(false)
      expect(bridge.chat).not.toHaveBeenCalled()
      now = Math.max(now, saved.loop_state.next_due_at * 1000 + 1)
    }
    for (let tick = 0; tick < 2; tick++) {
      await runtime.poll()
      await vi.waitFor(() => expect((runtime as any).runs.size).toBe(0), { timeout: 5000 })
      if (nativeHome) {
        const saved = native({ action: 'fixture_status' })
        expect(saved.loop_state.status).toBe(tick === 0 ? 'active' : 'done')
        expect(saved.loop_state.ticks_fired).toBe(tick + 1 + retained)
        if (tick === 0) {
          await runtime.poll()
          expect(bridge.chat).toHaveBeenCalledTimes(1)
          now = saved.loop_state.next_due_at * 1000 + 1
        }
      }
    }
    const completions = host.request.mock.calls.map(([value]: any[]) => value).filter((value: any) => value.op === 'complete')
    expect(completions.map((value: any) => value.run_id)).toEqual(['native-1', 'native-2'])
    expect(host.request.mock.calls.some(([value]: any[]) => value.op === 'unknown')).toBe(false)
    expect(bridge.ackResult).toHaveBeenCalledTimes(2)
    expect(getSessionDetail('session-1')!.messages.filter(m => m.role === 'assistant').map(m => m.content)).toEqual(['verified response', 'verified response'])
    expect(sessionMap.get('session-1').isWorking).toBe(false)
    runtime.close()
  })
})
