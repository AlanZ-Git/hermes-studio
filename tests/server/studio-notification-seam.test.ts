import { afterAll, beforeAll, expect, it, vi } from 'vitest'
import { spawn, spawnSync, type ChildProcess } from 'node:child_process'
import { once } from 'node:events'
import { createInterface } from 'node:readline'
import { realpathSync, statSync } from 'node:fs'
import { createStudioTestRoot, studioTestEnv } from './studio-receipt-fixture'
import { resolve } from 'node:path'
import { DatabaseSync } from 'node:sqlite'
import { AgentBridgeClient } from '../../packages/server/src/modules/hermes/services/bridge/client'

vi.mock('../../packages/server/src/modules/studio/public/chat-agent-runtime', () => ({}))
vi.mock('../../packages/server/src/modules/studio/services/chat-run/handle-ekko-agent-run', () => ({}))
vi.mock('../../packages/server/src/modules/studio/services/chat-run/handle-coding-agent-run', () => ({}))
vi.mock('../../packages/server/src/modules/studio/services/chat-run/ekko-session-command', () => ({}))
vi.mock('../../packages/server/src/modules/studio/public/profile-config', () => ({
  readConfigYamlForProfile: async () => ({}), getProfileDir: () => process.env.STUDIO_TEST_ROOT + '/core-home', saveEnvValueForProfile: async () => {},
}))
vi.mock('../../packages/server/src/modules/studio/public/runs/prompt', () => ({ getSystemPrompt: () => 'synthetic prompt' }))
vi.mock('../../packages/server/src/modules/studio/services/chat-run/model-config', () => ({
  resolveBridgeRunModelConfig: async () => ({ model: 'synthetic-model', provider: 'openai' }),
}))
vi.mock('../../packages/server/src/modules/studio/services/chat-run/model-run-source', () => ({
  createModelRunSource: async () => ({ token: 'SYNTHETIC-NOT-A-CREDENTIAL' }), closeModelRunSource: async () => {}, readModelRunSource: async () => ({}),
}))
vi.mock('../../packages/server/src/modules/studio/services/chat-run/workspace', () => ({ ensureHermesRunWorkspace: async () => process.env.STUDIO_TEST_ROOT + '/workspace' }))
vi.mock('../../packages/server/src/modules/studio/services/chat-run/workspace-diff-tracker', () => ({ startWorkspaceRunCheckpoint: () => {}, completeWorkspaceRunCheckpoint: () => null }))
vi.mock('../../packages/server/src/modules/studio/services/chat-run/compression', () => ({
  getOrCreateSession: (m: any, id: string) => m.get(id), buildCompressedHistory: async () => [], buildDbSnapshotAwareHistory: async () => [],
  pushState: () => {}, replaceState: () => {}, forceCompressBridgeHistory: async () => [],
}))
vi.mock('../../packages/server/src/modules/studio/services/chat-run/usage', () => ({
  calcAndUpdateUsage: async () => ({ inputTokens: 1, outputTokens: 1 }), estimateUsageTokensFromMessages: () => ({ inputTokens: 1, outputTokens: 1 }),
  getCachedBridgeContextOverhead: () => 0, contextTokensWithCachedOverhead: () => 0, updateMessageContextTokenUsage: () => 0,
}))
let database: any, store: any, inbox: any, SocketHandler: any
const root = createStudioTestRoot('notification-seam')
const children: ChildProcess[] = []
const environment = () => ({ ...studioTestEnv(root), HERMES_STUDIO_NOTIFICATION_DB: database.getStoragePath() })
async function worker(mode = 'serve') {
  const child = spawn(process.env.NATIVE_PYTHON!, ['-B', 'tests/server/studio_notification_fixture.py', mode], { env: environment(), stdio: ['pipe', 'pipe', 'pipe'] })
  children.push(child)
  let errors = ''
  child.stderr!.on('data', value => { errors += value })
  const lines = createInterface({ input: child.stdout! })
  const ready = await Promise.race([once(lines, 'line').then(([line]) => line), once(child, 'exit').then(() => { throw new Error(errors) })])
  return { child, ready: String(ready), errors: () => errors }
}
async function stop(child: ChildProcess, client?: AgentBridgeClient) {
  const exited = once(child, 'exit')
  if (client) await client.request({ action: 'fixture_stop' })
  else child.stdin!.end('\n')
  await exited
}
function source() { return new DatabaseSync(resolve(root, 'source.db')) }
function cursor() { const db = source(); try { return (db.prepare('SELECT last_event_id FROM kanban_notify_subs').get() as any).last_event_id } finally { db.close() } }
function handler() {
  const obj = Object.create(SocketHandler.prototype)
  const state: any = { messages: [], events: [], queue: [{ queue_id: 'human', input: 'human first' }], isWorking: true }
  obj.closing = false
  obj.sessionMap = new Map()
  obj.sessionStateForBackground = async () => state
  obj.socketForBackgroundRun = () => ({ data: { user: { id: 1 } } })
  obj.emitExternalEvent = vi.fn()
  obj.nsp = { adapter: { rooms: new Map() }, to: () => ({ emit: vi.fn() }) }
  obj.backgroundBridge = { completeBackgroundNotification: vi.fn(), releaseBackgroundNotification: vi.fn(async () => ({ released: false })) }
  return { obj, state }
}
beforeAll(async () => {
  database = await import('../../packages/server/src/modules/studio/infrastructure/database')
  const { initAllStores } = await import('../../packages/server/src/modules/studio/infrastructure/database/init')
  initAllStores()
  store = await import('../../packages/server/src/modules/studio/repositories/session-store')
  inbox = await import('../../packages/server/src/modules/studio/repositories/kanban-notification-store')
  SocketHandler = (await import('../../packages/server/src/modules/studio/sockets/chat-run')).ChatRunSocket
  store.createSession({ id: 'session-one', profile: 'default', source: 'cli' })
})
afterAll(async () => {
  for (const child of children) if (child.exitCode === null && child.signalCode === null) { child.kill('SIGTERM'); await once(child, 'exit') }
  database?.closeDb()
})
it('runs actual socket client → Python owner fence → Studio transaction → exact ACK → TS socket adoption and refresh', async () => {
  const { child, ready, errors } = await worker()
  const client = new AgentBridgeClient({ endpoint: JSON.parse(ready).endpoint, timeoutMs: 3000, connectRetryMs: 0 })
  const held = await worker('hold')
  expect(held.ready).toBe('held')
  expect((await client.request({ action: 'background_poll', session_ids: ['session-one'] })).notifications).toEqual([])
  expect(cursor()).toBe(3)
  expect(store.getSessionDetail('session-one').messages).toEqual([])
  await stop(held.child)
  const result: any = await client.request({ action: 'background_poll', session_ids: ['session-one'] })
  expect(result.notifications, errors()).toHaveLength(1)
  expect(cursor()).toBe(8)
  const item = result.notifications[0]
  const { obj, state } = handler()
  await obj.scheduleBackgroundNotification(item)
  expect(obj.backgroundBridge.completeBackgroundNotification).not.toHaveBeenCalled()
  expect(state.queue.map((q: any) => q.queue_id)).toEqual(['human', `delegation_${item.delegation_id}`])
  expect(state.messages).toHaveLength(1)
  database.closeDb()
  expect(store.getSessionDetail('session-one').messages).toHaveLength(1)
  expect(store.getSession('session-one').message_count).toBe(1)
  expect(inbox.kanbanNotificationReceipt(item.delegation_id)).toMatchObject({ consumer_state: 'pending', reference_state: 'visible', old_cursor: 3, new_cursor: 8 })
  const current: any = await client.request({ action: 'background_notification_status', session_id: 'session-one', delegation_id: item.delegation_id,
    notification_identity: item.identity, board_db_identity: item.board_db_identity })
  expect(current.current).toBe(true)
  expect(inbox.beginKanbanConsumption(item.delegation_id, 'session-one', 'synthetic-run')).toBe(true)
  inbox.settleKanbanConsumption(item.delegation_id, 'session-one', 'synthetic-run', 'uncertain', 'local synthetic dispatch outcome unknown')
  await obj.scheduleBackgroundNotification(item, true)
  expect(state.queue).toEqual([{ queue_id: 'human', input: 'human first' }])
  expect(inbox.pendingKanbanNotifications()).toEqual([])
  expect((await client.request({ action: 'background_poll', session_ids: ['session-one'] })).notifications).toEqual([])
  await stop(child, client)
})
it('crashes after the real target commit, restarts idempotently, and refuses foreign target after owner death', async () => {
  const db = source()
  db.prepare("INSERT INTO task_events(id,task_id,kind,payload,created_at) VALUES(9,'task','blocked','{\"reason\":\"synthetic\"}',1)").run()
  db.close()
  const crash = spawnSync(process.env.NATIVE_PYTHON!, ['-B', 'tests/server/studio_notification_fixture.py', 'crash'], { env: environment(), encoding: 'utf8' })
  expect(crash.status, crash.stderr).toBe(71)
  expect(cursor()).toBe(8)
  const before = store.getSessionDetail('session-one').messages.length
  expect(before).toBe(2)
  const foreign = resolve(root, 'foreign.db')
  const foreignDb = new DatabaseSync(foreign); foreignDb.exec('CREATE TABLE synthetic(x)'); foreignDb.close()
  const denied = spawnSync(process.env.NATIVE_PYTHON!, ['-B', 'tests/server/studio_notification_fixture.py', 'crash'], { env: { ...environment(), HERMES_STUDIO_NOTIFICATION_DB: foreign }, encoding: 'utf8' })
  expect(denied.status).not.toBe(71)
  expect(cursor()).toBe(8)
  const { child, ready, errors } = await worker()
  const client = new AgentBridgeClient({ endpoint: JSON.parse(ready).endpoint, timeoutMs: 3000 })
  const result: any = await client.request({ action: 'background_poll', session_ids: ['session-one'] })
  expect(result.notifications, errors()).toHaveLength(1)
  expect(cursor()).toBe(9)
  expect(store.getSessionDetail('session-one').messages).toHaveLength(before)
  await stop(child, client)
})
it('refuses missing provenance and a failed real target transaction without ACK or partial stats', async () => {
  const db = source(); db.prepare("INSERT INTO task_events(id,task_id,kind,payload,created_at) VALUES(10,'task','completed','{}',1)").run(); db.close()
  const count = store.getSession('session-one').message_count
  database.getDb().exec("CREATE TRIGGER refuse_adapter BEFORE INSERT ON kanban_notification_receipts BEGIN SELECT RAISE(ABORT,'synthetic refusal'); END")
  const { child, ready } = await worker()
  const client = new AgentBridgeClient({ endpoint: JSON.parse(ready).endpoint, timeoutMs: 3000 })
  expect((await client.request({ action: 'background_poll', session_ids: ['session-one'] })).notifications).toEqual([])
  expect(cursor()).toBe(9)
  expect(store.getSession('session-one').message_count).toBe(count)
  expect(store.getSessionDetail('session-one').messages).toHaveLength(count)
  database.getDb().exec('DROP TRIGGER refuse_adapter')
  const result: any = await client.request({ action: 'background_poll', session_ids: ['session-one'] })
  expect(result.notifications).toHaveLength(1)
  const item = result.notifications[0]
  database.getDb().prepare('UPDATE kanban_notification_receipts SET target_binding=NULL WHERE delivery_id=?').run(item.delegation_id)
  expect(() => inbox.receiveKanbanNotification(item, true)).toThrow('provenance')
  expect(inbox.pendingKanbanNotifications().some((n: any) => n.delegation_id === item.delegation_id)).toBe(false)
  await expect(client.request({ action: 'background_notification_status', session_id: 'session-one', delegation_id: item.delegation_id,
    notification_identity: item.identity, board_db_identity: item.board_db_identity })).rejects.toThrow('provenance')
  expect(() => inbox.beginKanbanConsumption(item.delegation_id, 'session-one', 'bypass')).toThrow('provenance')
  await stop(child, client)
})
it.each(['consumed', 'uncertain'])('runs the real queued human then automatic consumer after Python receive; %s never replays', async outcome => {
  const sid = 'joint-' + outcome
  store.createSession({ id: sid, profile: 'default', source: 'cli', user_id: '1' })
  const db = source()
  db.prepare("INSERT INTO kanban_notify_subs(task_id,platform,chat_id,thread_id,last_event_id,notifier_profile,delivery_mode,delivery_metadata,created_at) VALUES('task','tui',?,'',9,'default','notify+wake','{\"studio_auto_continue\":\"v1\"}',1)").run(sid)
  db.close()
  const { child, ready, errors } = await worker()
  const client = new AgentBridgeClient({ endpoint: JSON.parse(ready).endpoint, timeoutMs: 3000 })
  const result: any = await client.request({ action: 'background_poll', session_ids: [sid] })
  expect(result.notifications, errors()).toHaveLength(1)
  const item = result.notifications[0]
  const { obj, state } = handler()
  obj.sessionMap.set(sid, state)
  await obj.scheduleBackgroundNotification(item)
  const inputs: string[] = [], emitted: any[] = [], failures: string[] = []
  obj.nsp.to = () => ({ emit: (...args: any[]) => emitted.push(args) })
  let currentRun = ''
  const syntheticProvider: any = {
    request: (request: any) => client.request(request),
    contextEstimate: async () => ({ token_count: 1, fixed_context_tokens: 1 }),
    chat: vi.fn(async () => { currentRun = 'synthetic-' + inputs.length; return { run_id: currentRun, status: 'started' } }),
    streamOutput: async function* () {
      if (inputs.length === 2 && outcome === 'uncertain') throw new Error('Synthetic accepted dispatch lost its terminal result')
      yield { run_id: currentRun, done: true, status: 'complete', delta: 'SYNTHETIC local result', output: 'SYNTHETIC local result', cursor: 1, event_cursor: 0, events: [] }
    },
    ackResult: async () => true,
  }
  const socket: any = { connected: false, data: { user: { id: 1, username: 'synthetic', role: 'super_admin' } }, emit: vi.fn(), join: () => {}, to: () => ({ emit: vi.fn() }) }
  const { handleBridgeRun } = await import('../../packages/server/src/modules/studio/services/chat-run/handle-bridge-run')
  obj.executeRun = async (_socket: any, data: any, profile: string, skipUser: boolean) => {
    inputs.push(data.input)
    try {
      await handleBridgeRun(obj.nsp, socket, data, profile, obj.sessionMap, syntheticProvider, skipUser,
        async () => state, obj.dequeueNextQueuedRun.bind(obj))
    } catch (error) { failures.push(String(error)); throw error }
  }
  expect(obj.dequeueNextQueuedRun(socket, sid)).toBe(true)
  await vi.waitFor(() => expect(inbox.kanbanNotificationReceipt(item.delegation_id).consumer_state,
    JSON.stringify({ inputs, emitted, failures, working: state.isWorking, marker: state.activeRunMarker, providerCalls: syntheticProvider.chat.mock.calls.length, queue: state.queue, error: inbox.kanbanNotificationReceipt(item.delegation_id).error })).toBe(outcome))
  await vi.waitFor(() => expect(state.isWorking).toBe(false))
  expect(inputs).toEqual(['human first', item.message])
  expect(syntheticProvider.chat).toHaveBeenCalledTimes(2)
  database.closeDb()
  expect(inbox.kanbanNotificationReceipt(item.delegation_id).consumer_state).toBe(outcome)
  expect(store.getSessionDetail(sid).messages.filter((m: any) => m.run_marker === item.delegation_id)).toHaveLength(1)
  await obj.scheduleBackgroundNotification(item, true)
  expect(state.queue).toEqual([])
  expect(syntheticProvider.chat).toHaveBeenCalledTimes(2)
  expect((await client.request({ action: 'background_poll', session_ids: [sid] })).notifications).toEqual([])
  await stop(child, client)
})
