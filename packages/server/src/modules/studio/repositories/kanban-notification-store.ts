import { isDeepStrictEqual } from 'node:util'
import { realpathSync, statSync } from 'node:fs'
import { requireCompatibleConsumer, compatibilityRollbackProtected } from '../services/compatibility-rollback'
import { getDb, getStoragePath } from '../infrastructure/database'
import { getSession, getSessionDetail } from './session-store'

// The receiving half of the existing Kanban event/cursor chain, in the same
// database/transaction as the visible session message. task_events remains the
// source until ACK; this is an exact receipt plus recoverable consumer state.
function db() {
  const db = getDb()
  if (!db) throw new Error('Durable Kanban notification storage unavailable')
  db.exec(`CREATE TABLE IF NOT EXISTS kanban_notification_receipts (
    delivery_id TEXT PRIMARY KEY, session_id TEXT NOT NULL, profile TEXT NOT NULL,
    identity TEXT NOT NULL, notification TEXT NOT NULL, message_id INTEGER NOT NULL,
    consumer_state TEXT NOT NULL, run_marker TEXT, error TEXT,
    FOREIGN KEY(session_id) REFERENCES sessions(id) ON DELETE CASCADE
  )`)
  const info = db.prepare('PRAGMA table_info(kanban_notification_receipts)').all() as any[]
  const columns = new Set(info.map(row => row.name))
  for (const [name, type] of Object.entries({ delivery_id: 'TEXT', session_id: 'TEXT', profile: 'TEXT',
    identity: 'TEXT', notification: 'TEXT', message_id: 'INTEGER', consumer_state: 'TEXT', run_marker: 'TEXT', error: 'TEXT',
    reference_state: 'TEXT', reference_error: 'TEXT' })) {
    const column = info.find(row => row.name === name)
    if ((!column && !name.startsWith('reference_')) || (column && column.type.toUpperCase() !== type)
      || (name === 'delivery_id' && column?.pk !== 1)) throw new Error('Incompatible Kanban receipt schema')
  }
  const foreignKeys = db.prepare('PRAGMA foreign_key_list(kanban_notification_receipts)').all() as any[]
  if (!foreignKeys.some(row => row.table === 'sessions' && row.from === 'session_id'
    && row.to === 'id' && row.on_delete === 'CASCADE')) throw new Error('Incompatible Kanban receipt binding')
  if (!columns.has('reference_state')) db.exec("ALTER TABLE kanban_notification_receipts ADD COLUMN reference_state TEXT NOT NULL DEFAULT 'unchecked'")
  if (!columns.has('reference_error')) db.exec('ALTER TABLE kanban_notification_receipts ADD COLUMN reference_error TEXT')
  for (const [name, type] of Object.entries({ target_binding: 'TEXT', source_digest: 'TEXT', source_event: 'TEXT',
    source_file: 'TEXT', old_cursor: 'INTEGER', new_cursor: 'INTEGER' })) {
    const column = info.find(row => row.name === name)
    if (column && column.type.toUpperCase() !== type) throw new Error('Incompatible Kanban receipt provenance schema')
    if (!column) db.exec(`ALTER TABLE kanban_notification_receipts ADD COLUMN ${name} ${type}`)
  }
  return db
}

export function initKanbanNotificationStore() { db() }

function requireProvenance(receipt: any) {
  const path = realpathSync(getStoragePath())
  const stat = statSync(path)
  const binding = JSON.stringify([path, `studio-sqlite-v1:${stat.dev}:${stat.ino}`, receipt.session_id])
  if (receipt.target_binding !== binding || !receipt.source_digest || !receipt.source_event || !receipt.source_file
    || receipt.old_cursor == null || receipt.new_cursor == null) throw new Error('Notification receipt provenance unavailable')
}

function resolveReference(database: ReturnType<typeof db>, receipt: any) {
  const original = database.prepare('SELECT * FROM messages WHERE id=? AND session_id=?').get(receipt.message_id, receipt.session_id) as any
  const visible = (message: any) => message && message.run_marker === receipt.delivery_id
    && message.role === 'assistant' && message.finish_reason === 'notification'
    && message.display_role !== 'internal' && Boolean(message.content)
  let message = receipt.reference_state !== 'deleted' && visible(original) ? original : undefined
  if (!original && receipt.reference_state !== 'deleted') {
    const matches = database.prepare("SELECT * FROM messages WHERE session_id=? AND run_marker=? AND role='assistant' AND finish_reason='notification' AND COALESCE(display_role, '')!='internal' AND LENGTH(content)>0")
      .all(receipt.session_id, receipt.delivery_id) as any[]
    if (matches.length === 1) message = matches[0]
  }
  const referenceState = message ? 'visible' : receipt.reference_state === 'deleted' ? 'deleted' : 'orphaned'
  const referenceError = message ? null : 'Received; original message location cannot be confirmed'
  database.prepare('UPDATE kanban_notification_receipts SET message_id=?, reference_state=?, reference_error=? WHERE delivery_id=?')
    .run(message?.id ?? receipt.message_id, referenceState, referenceError, receipt.delivery_id)
  Object.assign(receipt, { message_id: message?.id ?? receipt.message_id, reference_state: referenceState, reference_error: referenceError })
  return message
}

export function receiveKanbanNotification(notification: any, wake: boolean) {
  requireCompatibleConsumer()
  const database = db()
  const sessionId = String(notification.session_id)
  const profile = String(notification.profile)
  const identity = JSON.stringify(notification.identity)
  if (!Array.isArray(notification.identity) || !notification.identity.length) throw new Error('Missing exact notification identity')
  const session = getSession(sessionId)
  if (!session || (session.profile || 'default') !== profile) throw new Error('Notification target changed')
  database.exec('BEGIN IMMEDIATE')
  try {
    let receipt = database.prepare('SELECT * FROM kanban_notification_receipts WHERE delivery_id=?')
      .get(notification.delegation_id) as any
    if (receipt && (receipt.session_id !== sessionId || receipt.profile !== profile || receipt.identity !== identity
      || !isDeepStrictEqual(JSON.parse(receipt.notification).event, notification.event))) {
      throw new Error('Notification identity collision')
    }
    if (!receipt) throw new Error('Notification has not been durably received by the admitted owner')
    requireProvenance(receipt)
    if (notification.execution_stopped && receipt.consumer_state === 'pending') {
      database.prepare("UPDATE kanban_notification_receipts SET consumer_state='consumed', error='Stopped task: delivered directly' WHERE delivery_id=?")
        .run(notification.delegation_id)
      receipt.consumer_state = 'consumed'
    }
    const message = resolveReference(database, receipt)
    database.exec('COMMIT')
    return { receipt, message }
  } catch (error) {
    database.exec('ROLLBACK')
    throw error
  }
}

export function consumeStoppedKanbanNotification(deliveryId: string, sessionId: string): boolean {
  requireCompatibleConsumer()
  const database = db()
  const receipt = database.prepare('SELECT * FROM kanban_notification_receipts WHERE delivery_id=? AND session_id=?').get(deliveryId, sessionId)
  if (!receipt || !resolveReference(database, receipt)) return false
  return Number(database.prepare(`UPDATE kanban_notification_receipts SET consumer_state='consumed', error='Stopped task: delivered directly'
    WHERE delivery_id=? AND session_id=? AND consumer_state='pending'
    AND EXISTS (SELECT 1 FROM messages m WHERE m.id=message_id AND m.session_id=?
      AND COALESCE(m.display_role, '') != 'internal' AND LENGTH(m.content)>0)`)
    .run(deliveryId, sessionId, sessionId).changes) === 1
}

export function diagnosePendingKanbanNotification(deliveryId: string, sessionId: string, error: string) {
  db().prepare(`UPDATE kanban_notification_receipts SET error=?
    WHERE delivery_id=? AND session_id=? AND consumer_state='pending'`).run(error, deliveryId, sessionId)
}

export function consumePendingKanbanNotification(deliveryId: string, sessionId: string, reason: string): boolean {
  requireCompatibleConsumer()
  const database = db()
  const receipt = database.prepare('SELECT * FROM kanban_notification_receipts WHERE delivery_id=? AND session_id=?').get(deliveryId, sessionId)
  if (!receipt || !resolveReference(database, receipt)) return false
  return Number(database.prepare(`UPDATE kanban_notification_receipts SET consumer_state='consumed', error=?
    WHERE delivery_id=? AND session_id=? AND consumer_state='pending'`).run(reason, deliveryId, sessionId).changes) === 1
}

export function beginKanbanConsumption(deliveryId: string, sessionId: string, marker: string): boolean {
  requireCompatibleConsumer()
  const database = db()
  const receipt = database.prepare('SELECT * FROM kanban_notification_receipts WHERE delivery_id=? AND session_id=?').get(deliveryId, sessionId)
  if (!receipt || !resolveReference(database, receipt)) return false
  requireProvenance(receipt)
  return Number(database.prepare(`UPDATE kanban_notification_receipts SET consumer_state='running', run_marker=?, error=NULL
    WHERE delivery_id=? AND session_id=? AND consumer_state='pending' AND reference_state='visible'`).run(marker, deliveryId, sessionId).changes) === 1
}

export function settleKanbanConsumption(deliveryId: string, sessionId: string, marker: string, state: 'consumed' | 'uncertain', error?: string) {
  db().prepare(`UPDATE kanban_notification_receipts SET consumer_state=?, error=?
    WHERE delivery_id=? AND session_id=? AND run_marker=? AND consumer_state='running'`)
    .run(state, error || null, deliveryId, sessionId, marker)
}

export function completeKanbanConsumption(deliveryId: string, sessionId: string, marker: string) {
  const saved = getSessionDetail(sessionId)?.messages.some(message => message.run_marker === marker
    && message.role === 'assistant' && message.display_role !== 'internal' && Boolean(message.content))
  if (!saved) throw new Error('Kanban consumer result has not been persistently delivered')
  settleKanbanConsumption(deliveryId, sessionId, marker, 'consumed')
}

export function pendingKanbanNotifications() {
  if (compatibilityRollbackProtected()) return []
  const database = db()
  return (database.prepare("SELECT * FROM kanban_notification_receipts WHERE consumer_state='pending' ORDER BY message_id").all() as any[])
    .filter(receipt => {
      try { requireProvenance(receipt) } catch {
        database.prepare('UPDATE kanban_notification_receipts SET error=? WHERE delivery_id=?')
          .run('Notification receipt provenance unavailable; automatic replay paused', receipt.delivery_id)
        return false
      }
      return resolveReference(database, receipt)
    }).map(receipt => JSON.parse(receipt.notification))
}

export function kanbanReferenceIssues(sessionId?: string) {
  const database = getDb()
  if (!database?.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name='kanban_notification_receipts'").get()) return []
  const receipts = db().prepare(`SELECT * FROM kanban_notification_receipts WHERE consumer_state IN ('pending', 'running', 'uncertain')
    ${sessionId === undefined ? '' : 'AND session_id=?'}`).all(...(sessionId === undefined ? [] : [sessionId])) as any[]
  return receipts.filter(receipt => {
    const message = resolveReference(database, receipt)
    return !message || receipt.consumer_state === 'uncertain'
  }).map(receipt => ({
    event: 'delegation.updated', session_id: receipt.session_id, profile: receipt.profile,
    delegation_id: receipt.delivery_id, delivery_status: receipt.consumer_state,
    reference_state: receipt.reference_state,
    error: receipt.reference_error || receipt.error || 'Received; dispatch outcome cannot be confirmed. Automatic replay is paused',
  }))
}

export function kanbanNotificationReceipt(deliveryId: string) {
  return db().prepare('SELECT * FROM kanban_notification_receipts WHERE delivery_id=?').get(deliveryId) as any
}
