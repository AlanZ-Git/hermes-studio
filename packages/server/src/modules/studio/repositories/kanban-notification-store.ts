import { getDb } from '../infrastructure/database'
import { addMessage, getSession, getSessionDetail, updateSessionStats } from './session-store'

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
  return db
}

export function receiveKanbanNotification(notification: any, wake: boolean) {
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
    if (receipt && (receipt.session_id !== sessionId || receipt.profile !== profile || receipt.identity !== identity)) {
      throw new Error('Notification identity collision')
    }
    if (!receipt) {
      const messageId = addMessage({ session_id: sessionId, role: 'assistant', content: notification.message,
        run_marker: notification.delegation_id, finish_reason: 'notification' })
      if (messageId == null) throw new Error('Cannot persist visible Kanban notification')
      database.prepare(`INSERT INTO kanban_notification_receipts
        (delivery_id, session_id, profile, identity, notification, message_id, consumer_state)
        VALUES (?, ?, ?, ?, ?, ?, ?)`)
        .run(notification.delegation_id, sessionId, profile, identity, JSON.stringify(notification), messageId,
          wake ? 'pending' : 'consumed')
      updateSessionStats(sessionId)
      receipt = database.prepare('SELECT * FROM kanban_notification_receipts WHERE delivery_id=?').get(notification.delegation_id)
    }
    if (notification.execution_stopped && receipt.consumer_state === 'pending') {
      database.prepare("UPDATE kanban_notification_receipts SET consumer_state='consumed', error='Stopped task: delivered directly' WHERE delivery_id=?")
        .run(notification.delegation_id)
      receipt.consumer_state = 'consumed'
    }
    // A deleted/hidden message is not evidence of visible delivery. Do not ACK
    // an orphaned receipt or silently reconstruct user-deleted history.
    const message = database.prepare('SELECT * FROM messages WHERE id=? AND session_id=?').get(receipt.message_id, sessionId) as any
    if (!message || message.display_role === 'internal' || !message.content) throw new Error('Notification receipt lost its visible message')
    database.exec('COMMIT')
    return { receipt, message }
  } catch (error) {
    database.exec('ROLLBACK')
    throw error
  }
}

export function consumeStoppedKanbanNotification(deliveryId: string, sessionId: string): boolean {
  return Number(db().prepare(`UPDATE kanban_notification_receipts SET consumer_state='consumed', error='Stopped task: delivered directly'
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
  return Number(db().prepare(`UPDATE kanban_notification_receipts SET consumer_state='consumed', error=?
    WHERE delivery_id=? AND session_id=? AND consumer_state='pending'`).run(reason, deliveryId, sessionId).changes) === 1
}

export function beginKanbanConsumption(deliveryId: string, sessionId: string, marker: string): boolean {
  return Number(db().prepare(`UPDATE kanban_notification_receipts SET consumer_state='running', run_marker=?, error=NULL
    WHERE delivery_id=? AND session_id=? AND consumer_state='pending'`).run(marker, deliveryId, sessionId).changes) === 1
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
  return db().prepare("SELECT notification FROM kanban_notification_receipts WHERE consumer_state='pending' ORDER BY message_id")
    .all().map((row: any) => JSON.parse(row.notification))
}

export function kanbanNotificationReceipt(deliveryId: string) {
  return db().prepare('SELECT * FROM kanban_notification_receipts WHERE delivery_id=?').get(deliveryId) as any
}
