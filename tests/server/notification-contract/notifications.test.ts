import { beforeAll, afterAll, expect, it, vi } from 'vitest'
import { resolve } from 'node:path'
import { createStudioTestRoot, seedStudioReceipt } from '../studio-receipt-fixture'

const root = createStudioTestRoot('notification-contract')

// External model/runtime adapters are never constructed. The actual socket
// notification handler, session repositories, SQLite and resume helpers run.
vi.mock('../../../packages/server/src/modules/studio/public/chat-agent-runtime', () => ({}))
vi.mock('../../../packages/server/src/modules/studio/services/chat-run/handle-ekko-agent-run', () => ({}))
vi.mock('../../../packages/server/src/modules/studio/services/chat-run/handle-coding-agent-run', () => ({}))
vi.mock('../../../packages/server/src/modules/studio/services/chat-run/ekko-session-command', () => ({}))
vi.mock('../../../packages/server/src/modules/studio/services/chat-run/handle-bridge-run', () => ({}))

let store: any, database: any, SocketHandler: any, inbox: any
function persist(item: any) {
  if (!inbox.kanbanNotificationReceipt(item.delegation_id)) seedStudioReceipt(item, database.getStoragePath())
}
async function schedule(obj: any, item: any) {
  if (process.env.HERMES_STUDIO_COMPATIBILITY_ROLLBACK !== '1') persist(item)
  return obj.scheduleBackgroundNotification(item)
}
beforeAll(async () => {
  expect(resolve(process.env.HERMES_WEB_UI_TEST_DB_DIR!)).toBe(resolve(root, 'receiver'))
  database = await import('../../../packages/server/src/modules/studio/infrastructure/database/index')
  expect(database.getStoragePath()).toBe(resolve(process.env.HERMES_WEB_UI_TEST_DB_DIR!, 'hermes-web-ui.db'))
  const { initAllHermesTables } = await import('../../../packages/server/src/modules/studio/infrastructure/database/schemas')
  initAllHermesTables()
  inbox = await import('../../../packages/server/src/modules/studio/repositories/kanban-notification-store')
  inbox.initKanbanNotificationStore()
  store = await import('../../../packages/server/src/modules/studio/repositories/session-store')
  SocketHandler = (await import('../../../packages/server/src/modules/studio/sockets/chat-run')).ChatRunSocket
})
afterAll(() => database?.closeDb())

export function notice(id: string, extra: any = {}) {
  return { session_id: 'session-one', profile: 'default', delegation_id: `kanban:${id}`,
    claim_id: `claim-${id}`, notification_kind: 'kanban', delivery_mode: 'notify',
    message: 'same text', identity: ['board.db', 'task', id, null, 'default', 'tui', 'session-one', ''],
    event: { type: 'kanban', board: 'board', task_id: 'task', event_id: id, kind: 'completed' }, ...extra }
}
function handler() {
  const obj = Object.create(SocketHandler.prototype)
  obj.closing = false
  obj.sessionMap = new Map()
  const state: any = { messages: [], events: [], queue: [], isWorking: true }
  obj.sessionStateForBackground = async () => state
  obj.socketForBackgroundRun = () => ({ data: { user: { id: 1 } } }) // auth adapter substitute
  obj.emitExternalEvent = vi.fn()
  obj.nsp = { to: () => ({ emit: vi.fn() }) }
  obj.backgroundBridge = { completeBackgroundNotification: vi.fn(async () => ({ completed: true })),
    releaseBackgroundNotification: vi.fn(async () => ({ released: true })) }
  return { obj, state }
}

it('persists visible exact events before ACK, survives reopening, and does not queue duplicate model turns', async () => {
  store.createSession({ id: 'session-one', profile: 'default', source: 'cli' })
  const { obj, state } = handler()
  await schedule(obj,notice('e1'))
  expect(obj.emitExternalEvent).toHaveBeenCalledWith('session-one', 'delegation.updated', expect.objectContaining({
    background_pending: 0,
  }))
  database.closeDb()
  const first = store.getSessionDetail('session-one').messages
  expect(first.filter((m: any) => m.content === 'same text' && m.display_role !== 'internal')).toHaveLength(1)
  await schedule(obj,notice('e1', { claim_id: 'retry' }))
  await schedule(obj,notice('e2'))
  const saved = store.getSessionDetail('session-one').messages
  expect(saved).toHaveLength(2)
  expect(state.queue).toHaveLength(0)
  expect(obj.backgroundBridge.completeBackgroundNotification).not.toHaveBeenCalled()
})

it('keeps internal consumption pending across a supervision pause and delivers stopped-chain results directly', async () => {
  const { obj, state } = handler()
  state.backgroundWakeStopped = true
  const item = notice('wake-event', { delivery_mode: 'notify+wake' })
  await schedule(obj,item)
  const inbox = await import('../../../packages/server/src/modules/studio/repositories/kanban-notification-store')
  expect(inbox.kanbanNotificationReceipt(item.delegation_id).consumer_state).toBe('pending')
  expect(state.queue).toHaveLength(0)
  expect(obj.backgroundBridge.completeBackgroundNotification).not.toHaveBeenCalled()
  state.backgroundWakeStopped = false
  await schedule(obj,item)
  await schedule(obj,{ ...item, claim_id: 'retry' })
  expect(state.queue).toHaveLength(1)
  await schedule(obj,{ ...item, execution_stopped: true })
  expect(state.queue).toHaveLength(0)
  expect(inbox.kanbanNotificationReceipt(item.delegation_id).consumer_state).toBe('consumed')
  await schedule(obj,notice('late-real-result', { execution_stopped: true, delivery_mode: 'notify+wake' }))
  expect(state.queue).toHaveLength(0)
  expect(store.getSessionDetail('session-one').messages.filter((m: any) => m.run_marker === item.delegation_id)).toHaveLength(1)
})

it('only queues eligible events for newly marked Studio automatic subscriptions', async () => {
  const { obj, state } = handler()
  for (const [kind, eligible] of [['completed', true], ['status', false], ['unknown', false]] as const) {
    await schedule(obj,notice(`marked-${kind}`, { delivery_mode: 'notify+wake',
      studio_auto_continue: 'v1', wake_eligible: eligible, event: { kind } }))
  }
  expect(state.queue.map((item: any) => item.backgroundDelegationId)).toEqual(['kanban:marked-completed'])
})

it('claims only the exact delivery once, preserves uncertain state, and adds no reservation schema', async () => {
  const inbox = await import('../../../packages/server/src/modules/studio/repositories/kanban-notification-store')
  for (const id of ['result-one', 'result-two']) {
    const item = notice(id, { delivery_mode: 'notify+wake' })
    persist(item)
    inbox.receiveKanbanNotification(item, true)
  }
  expect(inbox.beginKanbanConsumption('kanban:result-one', 'wrong-session', 'wrong-marker')).toBe(false)
  expect(inbox.beginKanbanConsumption('kanban:result-one', 'session-one', 'marker-one')).toBe(true)
  expect(inbox.beginKanbanConsumption('kanban:result-one', 'session-one', 'marker-two')).toBe(false)
  expect(inbox.beginKanbanConsumption('kanban:result-two', 'session-one', 'marker-two')).toBe(true)
  inbox.settleKanbanConsumption('kanban:result-one', 'session-one', 'wrong-marker', 'consumed')
  expect(inbox.kanbanNotificationReceipt('kanban:result-one').consumer_state).toBe('running')
  inbox.settleKanbanConsumption('kanban:result-one', 'session-one', 'marker-one', 'uncertain', 'transport unknown')
  database.closeDb()
  expect(inbox.beginKanbanConsumption('kanban:result-one', 'session-one', 'retry')).toBe(false)
  expect(inbox.kanbanNotificationReceipt('kanban:result-one')).toMatchObject({ consumer_state: 'uncertain', error: 'transport unknown' })
  const columns = database.getDb().prepare('PRAGMA table_info(kanban_notification_receipts)').all().map((c: any) => c.name)
  expect(columns).toEqual(['delivery_id', 'session_id', 'profile', 'identity', 'notification', 'message_id', 'consumer_state', 'run_marker', 'error', 'reference_state', 'reference_error', 'target_binding', 'source_digest', 'source_event', 'source_file', 'old_cursor', 'new_cursor'])
  expect(database.getDb().prepare("SELECT name FROM sqlite_master WHERE name='kanban_notification_result'").all()).toEqual([])
})

it('adopts a durable receipt after a lost delivery and refuses an uncommitted target transaction', async () => {
  const { obj } = handler()
  const item = notice('ack-crash')
  persist(item)
  database.closeDb()
  await schedule(obj, { ...item, claim_id: 'new-claim' })
  expect(store.getSessionDetail('session-one').messages.filter((m: any) => m.run_marker === item.delegation_id)).toHaveLength(1)
  const count = obj.backgroundBridge.completeBackgroundNotification.mock.calls.length
  database.getDb().exec("CREATE TRIGGER refuse_new_receipt BEFORE INSERT ON kanban_notification_receipts BEGIN SELECT RAISE(ABORT, 'synthetic receipt failure'); END")
  try {
    await expect(schedule(obj,notice('db-refused'))).rejects.toThrow('synthetic receipt failure')
    expect(obj.backgroundBridge.completeBackgroundNotification).toHaveBeenCalledTimes(count)
    expect(store.getSessionDetail('session-one').messages.some((m: any) => m.run_marker === 'kanban:db-refused')).toBe(false)
  } finally { database.getDb().exec('DROP TRIGGER refuse_new_receipt') }
})

it('pauses actual notification entry points without ACK or data mutation in protected rollback', async () => {
  const { obj } = handler()
  const before = store.getSessionDetail('session-one').messages.length
  vi.stubEnv('HERMES_STUDIO_COMPATIBILITY_ROLLBACK', '1')
  try {
    await schedule(obj,notice('rollback-held'))
    expect(obj.backgroundBridge.completeBackgroundNotification).not.toHaveBeenCalled()
    expect(store.getSessionDetail('session-one').messages).toHaveLength(before)
  } finally { vi.unstubAllEnvs() }
  await schedule(obj,notice('rollback-held'))
  expect(obj.backgroundBridge.completeBackgroundNotification).not.toHaveBeenCalled()
  expect(store.getSessionDetail('session-one').messages).toHaveLength(before + 1)
})

it('restores orphan and uncertain diagnoses from persistent receipts through the real socket handler without replay', async () => {
  const inbox = await import('../../../packages/server/src/modules/studio/repositories/kanban-notification-store')
  const orphan = notice('socket-orphan', { delivery_mode: 'notify+wake' })
  const unknown = notice('socket-uncertain', { delivery_mode: 'notify+wake' })
  const { obj, state } = handler()
  await schedule(obj,orphan)
  await schedule(obj,unknown)
  const receipt = inbox.kanbanNotificationReceipt(orphan.delegation_id)
  database.getDb().prepare('DELETE FROM messages WHERE id=?').run(receipt.message_id)
  expect(inbox.beginKanbanConsumption(unknown.delegation_id, 'session-one', 'original-run')).toBe(true)
  inbox.settleKanbanConsumption(unknown.delegation_id, 'session-one', 'original-run', 'uncertain', 'Original dispatch outcome unproven')
  database.closeDb()
  state.queue = []
  obj.backgroundBridge.completeBackgroundNotification.mockClear()
  obj.emitExternalEvent.mockClear()
  await schedule(obj,{ ...orphan, claim_id: 'reopened' })
  expect(obj.backgroundBridge.completeBackgroundNotification).not.toHaveBeenCalled()
  expect(obj.emitExternalEvent).toHaveBeenCalledWith('session-one', 'delegation.updated', expect.objectContaining({
    delegation_id: orphan.delegation_id, reference_state: 'orphaned', error: 'Received; original message location cannot be confirmed',
  }))
  await schedule(obj,{ ...unknown, claim_id: 'reopened' })
  expect(state.queue).toEqual([])
  expect(inbox.kanbanReferenceIssues('session-one')).toEqual(expect.arrayContaining([
    expect.objectContaining({ delegation_id: orphan.delegation_id, reference_state: 'orphaned' }),
    expect.objectContaining({ delegation_id: unknown.delegation_id, delivery_status: 'uncertain', error: 'Original dispatch outcome unproven' }),
  ]))
})

it('does not hide notification recovery routes because the session is old', () => {
  store.createSession({ id: 'old-target', profile: 'default', source: 'cli', agent: 'hermes' })
  database.getDb().prepare('UPDATE sessions SET last_active=1 WHERE id=?').run('old-target')
  const { obj } = handler()
  expect(obj.backgroundRecoveryRoutes().flatMap((r: any) => r.session_ids)).toContain('old-target')
})
