import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { DatabaseSync } from 'node:sqlite'
import { mkdtempSync } from 'node:fs'
import { resolve } from 'node:path'
import { createStudioTestRoot, seedStudioReceipt } from './studio-receipt-fixture'

const root = createStudioTestRoot('reference-rollback')

const log = vi.hoisted(() => ({ warn: vi.fn(), info: vi.fn(), error: vi.fn(), debug: vi.fn() }))
vi.mock('../../packages/server/src/modules/studio/public/logging', () => ({ logger: log, bridgeLogger: log }))
let database: DatabaseSync
let targetPath: string
function receive(item: any, wake: boolean) {
  if (!inbox.kanbanNotificationReceipt(item.delegation_id)) seedStudioReceipt({ ...item, delivery_mode: wake ? 'notify+wake' : 'notify' }, targetPath)
  return inbox.receiveKanbanNotification(item, wake)
}
let inbox: typeof import('../../packages/server/src/modules/studio/repositories/kanban-notification-store')
let sessions: typeof import('../../packages/server/src/modules/studio/repositories/session-store')

const notice = (id: string) => ({ delegation_id: `kanban:${id}`, session_id: 'fixture', profile: 'default',
  identity: ['/synthetic/board.db', 'task', 1, null, 'default', 'agent_bridge', 'fixture', ''],
  message: 'Synthetic event', claim_id: 'claim', notification_kind: 'kanban' })

beforeEach(async () => {
  vi.resetModules()
  targetPath = resolve(mkdtempSync(resolve(root, 'references-')), 'studio.db')
  database = new DatabaseSync(targetPath)
  vi.doMock('../../packages/server/src/modules/studio/infrastructure/database/index', () => ({
    getDb: () => database, isSqliteAvailable: () => true, getStoragePath: () => targetPath,
  }))
  const { initAllHermesTables } = await import('../../packages/server/src/modules/studio/infrastructure/database/schemas')
  initAllHermesTables()
  sessions = await import('../../packages/server/src/modules/studio/repositories/session-store')
  inbox = await import('../../packages/server/src/modules/studio/repositories/kanban-notification-store')
  sessions.createSession({ id: 'fixture', profile: 'default', source: 'cli', user_id: '1' })
})
afterEach(() => { vi.unstubAllEnvs(); database.close(); vi.resetModules() })

describe('Kanban reference preservation and protected rollback', () => {
  it('keeps message, receipt and stats atomic on receipt insertion failure', () => {
    receive(notice('schema'), false)
    const before = sessions.getSessionDetail('fixture')!
    database.exec("CREATE TRIGGER refuse_receipt BEFORE INSERT ON kanban_notification_receipts BEGIN SELECT RAISE(ABORT, 'synthetic refusal'); END")
    expect(() => receive(notice('failure'), true)).toThrow('synthetic refusal')
    expect(sessions.getSessionDetail('fixture')!.messages).toHaveLength(before.messages.length)
    expect(sessions.getSession('fixture')!.message_count).toBe(before.message_count)
    expect(inbox.kanbanNotificationReceipt('kanban:failure')).toBeUndefined()
  })
  it.each(['TEXT', 'INTEGER PRIMARY KEY'])('rejects an existing incompatible receipt schema (%s) without accepting or migrating it', deliveryType => {
    database.exec(`CREATE TABLE kanban_notification_receipts (
      delivery_id ${deliveryType}, session_id TEXT NOT NULL, profile TEXT NOT NULL,
      identity TEXT NOT NULL, notification TEXT NOT NULL, message_id INTEGER NOT NULL,
      consumer_state TEXT NOT NULL, run_marker TEXT, error TEXT,
      FOREIGN KEY(session_id) REFERENCES sessions(id) ON DELETE CASCADE)`)
    expect(() => receive(notice('bad-schema'), true)).toThrow('Incompatible Kanban receipt schema')
    expect(sessions.getSessionDetail('fixture')!.messages).toEqual([])
    expect(database.prepare('PRAGMA table_info(kanban_notification_receipts)').all()).toHaveLength(9)
  })
  it('rejects a changed source event under an existing delivery while allowing reordered identical event fields', () => {
    const item = { ...notice('source'), event: { event_id: 1, kind: 'completed' } }
    receive(item, true)
    expect(() => receive({ ...item, event: { event_id: 1, kind: 'archived' } }, true)).toThrow('identity collision')
    expect(receive({ ...item, event: { kind: 'completed', event_id: 1 } }, true).message).toBeTruthy()
    expect(sessions.getSessionDetail('fixture')!.messages).toHaveLength(1)
  })
  it('persists orphaned diagnosis without ACK evidence or text-based resurrection', () => {
    const { receipt } = receive(notice('orphan'), true)
    database.prepare('DELETE FROM messages WHERE id=?').run(receipt.message_id)
    sessions.addMessage({ session_id: 'fixture', role: 'assistant', content: 'Synthetic event' })
    const again = receive(notice('orphan'), true)
    expect(again.message).toBeUndefined()
    expect(again.receipt.reference_state).toBe('orphaned')
    expect(inbox.kanbanNotificationReceipt('kanban:orphan').reference_error).toContain('cannot be confirmed')
    expect(inbox.pendingKanbanNotifications()).toEqual([])
    expect(inbox.kanbanReferenceIssues('fixture')).toEqual([expect.objectContaining({
      session_id: 'fixture', delegation_id: 'kanban:orphan', reference_state: 'orphaned',
      error: 'Received; original message location cannot be confirmed',
    })])
    expect(inbox.kanbanReferenceIssues('other-session')).toEqual([])
    expect(inbox.beginKanbanConsumption('kanban:orphan', 'fixture', 'new-run')).toBe(false)
    expect(sessions.getSessionDetail('fixture')!.messages).toHaveLength(1)
  })
  it('repairs only the exact immutable delivery marker, never a summary or another delivery', () => {
    const { receipt } = receive(notice('exact'), true)
    database.prepare('UPDATE kanban_notification_receipts SET message_id=-1 WHERE delivery_id=?').run('kanban:exact')
    const again = receive(notice('exact'), true)
    expect(again.message.id).toBe(receipt.message_id)
    expect(again.receipt.reference_state).toBe('visible')
    expect(inbox.beginKanbanConsumption('kanban:exact', 'fixture', 'run')).toBe(true)
    inbox.settleKanbanConsumption('kanban:exact', 'fixture', 'run', 'uncertain', 'dispatch outcome unavailable')
    expect(inbox.beginKanbanConsumption('kanban:exact', 'fixture', 'retry')).toBe(false)
    expect(inbox.kanbanReferenceIssues('fixture')).toEqual([expect.objectContaining({
      delegation_id: 'kanban:exact', reference_state: 'visible', delivery_status: 'uncertain',
      error: 'dispatch outcome unavailable',
    })])
  })
  it('honors proven history deletion without reinserting a notification', () => {
    receive(notice('deleted'), true)
    sessions.clearSessionMessages('fixture')
    expect(inbox.kanbanNotificationReceipt('kanban:deleted').reference_state).toBe('deleted')
    expect(receive(notice('deleted'), true).message).toBeUndefined()
    expect(sessions.getSessionDetail('fixture')!.messages).toEqual([])
  })
  it('preserves pending and uncertain state across the new-code compatibility pause, not an old release', () => {
    receive(notice('pending'), true)
    receive(notice('unknown'), true)
    inbox.beginKanbanConsumption('kanban:unknown', 'fixture', 'original')
    inbox.settleKanbanConsumption('kanban:unknown', 'fixture', 'original', 'uncertain')
    const saved = database.prepare('SELECT * FROM kanban_notification_receipts ORDER BY delivery_id').all()
    vi.stubEnv('HERMES_STUDIO_COMPATIBILITY_ROLLBACK', '1')
    expect(inbox.pendingKanbanNotifications()).toEqual([])
    expect(() => receive(notice('during-rollback'), true)).toThrow('Compatibility rollback')
    expect(() => inbox.beginKanbanConsumption('kanban:pending', 'fixture', 'rollback-run')).toThrow('Compatibility rollback')
    expect(database.prepare('SELECT * FROM kanban_notification_receipts ORDER BY delivery_id').all()).toEqual(saved)
    expect(sessions.getSessionDetail('fixture')!.messages).toHaveLength(2)
    vi.stubEnv('HERMES_STUDIO_COMPATIBILITY_ROLLBACK', '0')
    expect(inbox.pendingKanbanNotifications().map((item: any) => item.delegation_id)).toEqual(['kanban:pending'])
    receive(notice('during-rollback'), true)
    expect(receive(notice('pending'), true).receipt.consumer_state).toBe('pending')
    expect(inbox.kanbanNotificationReceipt('kanban:unknown').consumer_state).toBe('uncertain')
    expect(sessions.getSessionDetail('fixture')!.messages).toHaveLength(3)
  })
})
