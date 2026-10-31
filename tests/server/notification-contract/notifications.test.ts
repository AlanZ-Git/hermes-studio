import { beforeAll, afterAll, expect, it, vi } from 'vitest'
import { resolve } from 'node:path'

// External model/runtime adapters are never constructed. The actual socket
// notification handler, session repositories, SQLite and resume helpers run.
vi.mock('../../../packages/server/src/modules/studio/public/chat-agent-runtime', () => ({}))
vi.mock('../../../packages/server/src/modules/studio/services/chat-run/handle-ekko-agent-run', () => ({}))
vi.mock('../../../packages/server/src/modules/studio/services/chat-run/handle-coding-agent-run', () => ({}))
vi.mock('../../../packages/server/src/modules/studio/services/chat-run/ekko-session-command', () => ({}))
vi.mock('../../../packages/server/src/modules/studio/services/chat-run/handle-bridge-run', () => ({}))

let store: any, database: any, SocketHandler: any
beforeAll(async () => {
  expect(resolve(process.env.HERMES_WEB_UI_TEST_DB_DIR!)).toMatch(/^\/(work\/test-runs|scratch)\//)
  database = await import('../../../packages/server/src/modules/studio/infrastructure/database/index')
  expect(database.getStoragePath()).toBe(process.env.HERMES_WEB_UI_TEST_DB_DIR + '/hermes-web-ui.db')
  const { initAllHermesTables } = await import('../../../packages/server/src/modules/studio/infrastructure/database/schemas')
  initAllHermesTables()
  store = await import('../../../packages/server/src/modules/studio/repositories/session-store')
  SocketHandler = (await import('../../../packages/server/src/modules/studio/sockets/chat-run')).ChatRunSocket
})
afterAll(() => database?.closeDb())

export function notice(id: string, extra: any = {}) {
  return { session_id: 'session-one', profile: 'default', delegation_id: `kanban:${id}`,
    claim_id: `claim-${id}`, notification_kind: 'kanban', delivery_mode: 'notify',
    message: 'same text', identity: ['board.db', 'board', 'task', id, 'run', 'default', 'tui', 'session-one', ''],
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
  await obj.scheduleBackgroundNotification(notice('e1'))
  expect(obj.emitExternalEvent).toHaveBeenCalledWith('session-one', 'delegation.updated', expect.objectContaining({
    background_pending: 0,
  }))
  database.closeDb()
  const first = store.getSessionDetail('session-one').messages
  expect(first.filter((m: any) => m.content === 'same text' && m.display_role !== 'internal')).toHaveLength(1)
  await obj.scheduleBackgroundNotification(notice('e1', { claim_id: 'retry' }))
  await obj.scheduleBackgroundNotification(notice('e2'))
  const saved = store.getSessionDetail('session-one').messages
  expect(saved).toHaveLength(2)
  expect(state.queue).toHaveLength(0)
  expect(obj.backgroundBridge.completeBackgroundNotification).toHaveBeenCalledTimes(3)
})

it('keeps internal consumption pending across a supervision pause and delivers stopped-chain results directly', async () => {
  const { obj, state } = handler()
  state.backgroundWakeStopped = true
  const item = notice('wake-event', { delivery_mode: 'notify+wake' })
  await obj.scheduleBackgroundNotification(item)
  const inbox = await import('../../../packages/server/src/modules/studio/repositories/kanban-notification-store')
  expect(inbox.kanbanNotificationReceipt(item.delegation_id).consumer_state).toBe('pending')
  expect(state.queue).toHaveLength(0)
  expect(obj.backgroundBridge.completeBackgroundNotification).toHaveBeenCalledTimes(1)
  state.backgroundWakeStopped = false
  await obj.scheduleBackgroundNotification(item)
  await obj.scheduleBackgroundNotification({ ...item, claim_id: 'retry' })
  expect(state.queue).toHaveLength(1)
  await obj.scheduleBackgroundNotification({ ...item, execution_stopped: true })
  expect(state.queue).toHaveLength(0)
  expect(inbox.kanbanNotificationReceipt(item.delegation_id).consumer_state).toBe('consumed')
  await obj.scheduleBackgroundNotification(notice('late-real-result', { execution_stopped: true, delivery_mode: 'notify+wake' }))
  expect(state.queue).toHaveLength(0)
  expect(store.getSessionDetail('session-one').messages.filter((m: any) => m.run_marker === item.delegation_id)).toHaveLength(1)
})

it('does not hide notification recovery routes because the session is old', () => {
  store.createSession({ id: 'old-target', profile: 'default', source: 'cli', agent: 'hermes' })
  database.getDb().prepare('UPDATE sessions SET last_active=1 WHERE id=?').run('old-target')
  const { obj } = handler()
  expect(obj.backgroundRecoveryRoutes().flatMap((r: any) => r.session_ids)).toContain('old-target')
})
