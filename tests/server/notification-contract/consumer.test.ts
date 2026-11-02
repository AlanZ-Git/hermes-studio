import { expect, it, vi } from 'vitest'

// Only external model/provider/context-cost/workspace adapters are substitutes.
// handleBridgeRun, bridge-message persistence, result ACK and receipt DB are real.
vi.mock('../../../packages/server/src/modules/studio/public/profile-config', () => ({
  readConfigYamlForProfile: async () => ({}), getProfileDir: () => process.env.TODO199_RUN + '/hermes',
  saveEnvValueForProfile: async () => {},
}))
vi.mock('../../../packages/server/src/modules/studio/public/runs/prompt', () => ({ getSystemPrompt: () => 'synthetic prompt' }))
vi.mock('../../../packages/server/src/modules/studio/services/chat-run/model-config', () => ({
  resolveBridgeRunModelConfig: async () => ({ model: 'synthetic-model', provider: 'openai' }),
}))
vi.mock('../../../packages/server/src/modules/studio/services/chat-run/model-run-source', () => ({
  createModelRunSource: async () => ({ token: 'SYNTHETIC-NOT-A-CREDENTIAL' }),
  closeModelRunSource: async () => {}, readModelRunSource: async () => ({}),
}))
vi.mock('../../../packages/server/src/modules/studio/services/chat-run/workspace', () => ({
  ensureHermesRunWorkspace: async () => process.env.TODO199_RUN + '/synthetic-workspace',
}))
vi.mock('../../../packages/server/src/modules/studio/services/chat-run/workspace-diff-tracker', () => ({
  startWorkspaceRunCheckpoint: () => {}, completeWorkspaceRunCheckpoint: () => null,
}))
vi.mock('../../../packages/server/src/modules/studio/services/chat-run/compression', () => ({
  getOrCreateSession: (m: any, id: string) => m.get(id),
  buildCompressedHistory: async () => [], buildDbSnapshotAwareHistory: async () => [],
  pushState: () => {}, replaceState: () => {}, forceCompressBridgeHistory: async () => [],
}))
vi.mock('../../../packages/server/src/modules/studio/services/chat-run/usage', () => ({
  calcAndUpdateUsage: async () => ({ inputTokens: 1, outputTokens: 1 }),
  estimateUsageTokensFromMessages: () => ({ inputTokens: 1, outputTokens: 1 }),
  getCachedBridgeContextOverhead: () => 0, contextTokensWithCachedOverhead: () => 0,
  updateMessageContextTokenUsage: () => 0,
}))

it.each(['success', 'model-failure', 'save-failure', 'source-ack-independent', 'stopped-before-consume', 'before-dispatch-failure', 'stale-result', 'stop-during-preparation'])('real consumer persistence and responsibility: %s', async failure => {
  const database = await import('../../../packages/server/src/modules/studio/infrastructure/database/index')
  expect(database.getStoragePath()).toBe(process.env.TODO199_RUN + '/webstate/hermes-web-ui.db')
  const { initAllHermesTables } = await import('../../../packages/server/src/modules/studio/infrastructure/database/schemas')
  initAllHermesTables()
  const store = await import('../../../packages/server/src/modules/studio/repositories/session-store')
  const inbox = await import('../../../packages/server/src/modules/studio/repositories/kanban-notification-store')
  const { handleBridgeRun } = await import('../../../packages/server/src/modules/studio/services/chat-run/handle-bridge-run')
  const sid = 'consumer-' + failure
  const id = 'kanban:' + sid
  store.createSession({ id: sid, profile: 'default', source: 'cli' })
  const notification = { session_id: sid, profile: 'default', delegation_id: id, claim_id: 'claim',
    studio_auto_continue: 'v1',
    message: 'predecessor completed: continue ordinary work', identity: ['board', 'task', sid, 'run', 'default', sid, ''] }
  inbox.receiveKanbanNotification(notification, true)
  const state: any = { messages: [], events: [], queue: [], isWorking: false }
  const states = new Map([[sid, state]])
  const emit = vi.fn()
  const nsp: any = { adapter: { rooms: new Map() }, to: () => ({ emit }) }
  const socket: any = { connected: false, data: { user: { id: 1, username: 'synthetic', role: 'super_admin' } }, emit, join: () => {} }
  const bridge: any = {
    contextEstimate: vi.fn(async () => ({ token_count: 1, fixed_context_tokens: 1 })),
    chat: vi.fn(async () => ({ run_id: 'model-' + sid, status: 'started' })),
    streamOutput: vi.fn(async function* () {
      if (failure === 'model-failure') {
        yield { run_id: 'model-' + sid, done: true, status: 'error', error: 'SYNTHETIC model failure', delta: '', output: '', events: [] }
      } else {
        if (failure === 'save-failure') database.getDb()!.exec("CREATE TRIGGER synthetic_consumer_save BEFORE INSERT ON messages BEGIN SELECT RAISE(ABORT, 'SYNTHETIC consumer save failure'); END")
        yield { run_id: 'model-' + sid, done: true, status: 'complete', delta: 'SYNTHETIC consumed predecessor result',
          output: 'SYNTHETIC consumed predecessor result', cursor: 1, event_cursor: 0, events: [] }
      }
    }),
    ackResult: vi.fn(async () => true), request: vi.fn(async (req: any) => req.action === 'background_notification_status'
      ? { current: true, execution_stopped: failure === 'stopped-before-consume', studio_auto_continue: 'v1',
          wake_eligible: true, stale_result: failure === 'stale-result' } : {}),
    completeBackgroundNotification: vi.fn(async () => {
      if (failure === 'source-ack-independent') throw new Error('Consumer must not re-ACK the source lease')
      return { completed: true }
    }),
    releaseBackgroundNotification: vi.fn(async () => ({ released: true })),
  }
  const data: any = { session_id: sid, input: notification.message, display_input: null,
    background_notification_kind: 'kanban', background_delegation_id: id, background_claim_id: 'claim', autonomous: true }
  if (failure === 'stop-during-preparation') data.loopBeforeDispatch = async () => { state.backgroundWakeStopped = true }
  let beforeDispatchState: string | undefined
  if (failure === 'before-dispatch-failure') data.loopBeforeDispatch = async () => {
    beforeDispatchState = inbox.kanbanNotificationReceipt(id).consumer_state
    throw new Error('SYNTHETIC known failure before dispatch')
  }
  try {
    await handleBridgeRun(nsp, socket, data, 'default', states, bridge, false, vi.fn(), vi.fn())
  } catch (error) {
    if (failure !== 'save-failure') throw error
  } finally {
    database.getDb()!.exec('DROP TRIGGER IF EXISTS synthetic_consumer_save')
  }
  if (failure === 'stop-during-preparation') {
    expect(bridge.chat).not.toHaveBeenCalled()
    expect(inbox.kanbanNotificationReceipt(id).consumer_state).toBe('pending')
    expect(state.backgroundWakeStopped).toBe(true)
    expect(state.isWorking).toBe(false)
    database.closeDb()
    return
  }
  if (failure === 'before-dispatch-failure') {
    expect(beforeDispatchState).toBe('pending')
    expect(bridge.chat).not.toHaveBeenCalled()
    expect(bridge.completeBackgroundNotification).not.toHaveBeenCalled()
    expect(inbox.kanbanNotificationReceipt(id).consumer_state).toBe('pending')
    expect(inbox.kanbanNotificationReceipt(id).error).toContain('known failure before dispatch')
    delete data.loopBeforeDispatch
    state.isWorking = false
    await handleBridgeRun(nsp, socket, data, 'default', states, bridge, false, vi.fn(), vi.fn())
    expect(bridge.chat).toHaveBeenCalledTimes(1)
    expect(inbox.kanbanNotificationReceipt(id).consumer_state).toBe('consumed')
    database.closeDb()
    return
  }
  expect(bridge.chat).toHaveBeenCalledTimes(['stopped-before-consume', 'stale-result'].includes(failure) ? 0 : 1)
  expect(bridge.completeBackgroundNotification).not.toHaveBeenCalled()
  const receipt = inbox.kanbanNotificationReceipt(id)
  expect(receipt.consumer_state).toBe(['success', 'source-ack-independent', 'stopped-before-consume', 'stale-result'].includes(failure) ? 'consumed' : 'uncertain')
  if (['success', 'source-ack-independent'].includes(failure)) {
    database.closeDb()
    expect(store.getSessionDetail(sid)!.messages.filter(m => m.content === 'SYNTHETIC consumed predecessor result')).toHaveLength(1)
  } else if (failure !== 'stopped-before-consume') {
    expect(bridge.completeBackgroundNotification).not.toHaveBeenCalled()
    expect(receipt.error).toBeTruthy()
  }
  // A replayed persisted/running event never starts a second model attempt.
  state.isWorking = false
  await handleBridgeRun(nsp, socket, data, 'default', states, bridge, false, vi.fn(), vi.fn())
  expect(bridge.chat).toHaveBeenCalledTimes(['stopped-before-consume', 'stale-result'].includes(failure) ? 0 : 1)
  database.closeDb()
})
