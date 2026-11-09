// @vitest-environment jsdom
import { beforeEach, describe, expect, it, vi } from 'vitest'
import { createPinia, setActivePinia } from 'pinia'

const chatApi = vi.hoisted(() => ({
  startRunViaSocket: vi.fn(),
  resumeSession: vi.fn(),
  registerSessionHandlers: vi.fn(),
  globalPendingHandler: undefined as undefined | ((event: any) => void),
  unregisterSessionHandlers: vi.fn(),
}))

const completionSoundMock = vi.hoisted(() => ({
  primeCompletionSound: vi.fn(),
  playCompletionSound: vi.fn(),
}))
const completionNotification=vi.hoisted(()=>vi.fn(async()=>true))
vi.mock('@/utils/completion-notification',()=>({showCompletionNotification:completionNotification}))

vi.mock('@/api/studio/chat', () => ({
  onUnobservedRunStarted: vi.fn(() => vi.fn()),
  startRunViaSocket: chatApi.startRunViaSocket,
  resumeSession: chatApi.resumeSession,
  registerSessionHandlers: chatApi.registerSessionHandlers,
  unregisterSessionHandlers: chatApi.unregisterSessionHandlers,
  getChatRunSocket: vi.fn(() => ({ emit: vi.fn() })),
  respondToolApproval: vi.fn(),
  respondClarify: vi.fn(),
  onPeerUserMessage: vi.fn((handler: (event: any) => void) => { chatApi.globalPendingHandler = handler; return vi.fn() }),
  onApprovalRequested: vi.fn(() => vi.fn()),
  onApprovalResolved: vi.fn(() => vi.fn()),
  onClarifyRequested: vi.fn(() => vi.fn()),
  onClarifyResolved: vi.fn(() => vi.fn()),
  onSessionCommand: vi.fn(() => vi.fn()),
  onSessionTitleUpdated: vi.fn(() => vi.fn()),
  onRunUsageUpdated: vi.fn(() => vi.fn()),
  onSessionWorkspaceUpdated: vi.fn(() => vi.fn()),
  onSessionSettingsUpdated: vi.fn(() => vi.fn()),
}))

vi.mock('@/api/client', () => ({
  getActiveProfileName: () => 'default',
  hasApiKey: () => false,
}))

vi.mock('@/api/studio/sessions', () => ({
  archiveSession: vi.fn(),
  deleteSession: vi.fn(),
  fetchSession: vi.fn(),
  fetchSessions: vi.fn(),
  fetchWorkspaceRunChangesForSession: vi.fn(async () => []),
  fetchWorkspaceRunChangeFile: vi.fn(async () => null),
  setSessionModel: vi.fn(),
}))

vi.mock('@/api/studio/download', () => ({
  getDownloadUrl: (_path: string, name: string) => `/download/${name}`,
}))

vi.mock('@/api/hermes/system', () => ({
  checkHealth: vi.fn(),
  fetchAvailableModels: vi.fn(),
  addCustomModel: vi.fn(),
  removeCustomModel: vi.fn(),
  updateDefaultModel: vi.fn(),
  updateModelVisibility: vi.fn(),
  updateModelAlias: vi.fn(),
}))

vi.mock('@/utils/completion-sound', () => ({
  primeCompletionSound: completionSoundMock.primeCompletionSound,
  playCompletionSound: completionSoundMock.playCompletionSound,
}))

import { useChatStore, type Message, type Session } from '@/stores/hermes/chat'
import { useSettingsStore } from '@/stores/hermes/settings'
import {respondClarify, respondToolApproval} from '@/api/studio/chat'
import { invalidateAuth } from '@/api/auth-invalidation'

function makeSession(id: string): Session {
  return {
    id,
    title: id,
    messages: [],
    createdAt: Date.now(),
    updatedAt: Date.now(),
  }
}

describe('chat store error handling - #1644', () => {
  let handlers: any

  beforeEach(() => {
    handlers = undefined
    chatApi.globalPendingHandler = undefined
    vi.resetAllMocks()
    setActivePinia(createPinia())
    chatApi.startRunViaSocket.mockReturnValue({ abort: vi.fn() })
    chatApi.resumeSession.mockImplementation((sessionId: string, onResumed: (data: any) => void) => {
      onResumed({
        session_id: sessionId,
        messages: [],
        isWorking: false,
        events: [],
      })
      return {} as any
    })
    chatApi.registerSessionHandlers.mockImplementation((_sessionId: string, registeredHandlers: any) => {
      handlers = registeredHandlers
      return vi.fn()
    })
  })

  it('displays persisted Kanban orphan and uncertain diagnoses without starting or replaying work', async () => {
    const store = useChatStore()
    store.sessions = [makeSession('notification-session')]
    store.activeSessionId = 'notification-session'
    store.activeSession = store.sessions[0]
    await store.sendMessage('ordinary human input')
    const onEvent = chatApi.startRunViaSocket.mock.calls[0][1] as (event: any) => void
    const orphan = { event: 'delegation.updated', session_id: 'notification-session',
      delegation_id: 'kanban:orphan', reference_state: 'orphaned', delivery_status: 'pending',
      error: 'Received; original message location cannot be confirmed' }
    onEvent(orphan)
    onEvent(orphan)
    onEvent({ ...orphan, delegation_id: 'kanban:uncertain', reference_state: 'visible', delivery_status: 'uncertain',
      error: 'Received; dispatch outcome cannot be confirmed. Automatic replay is paused' })
    const issues = () => store.sessions[0].messages.filter(message => message.commandAction === 'notification.issue')
    expect(issues()).toHaveLength(2)
    expect(issues()[0].content).toBe(orphan.error)
    expect(issues()[1].content).toContain('Automatic replay is paused')
    onEvent({ event: 'run.completed', session_id: 'notification-session', output: 'human response' })
    expect(issues()).toHaveLength(2)
    expect(chatApi.startRunViaSocket).toHaveBeenCalledTimes(1)
    onEvent({ ...orphan, reference_state: 'visible', delivery_status: 'consumed', error: undefined })
    expect(issues()).toHaveLength(1)
    expect(issues()[0].id).toContain('kanban:uncertain')
  })

  it('restores Kanban diagnoses from resume events without replaying a model turn', async () => {
    const store = useChatStore()
    store.sessions = [makeSession('resume-notification')]
    chatApi.resumeSession.mockImplementation((sessionId: string, onResumed: (data: any) => void) => {
      onResumed({ session_id: sessionId, messages: [], isWorking: false, events: [{
        event: 'delegation.updated', data: { event: 'delegation.updated', session_id: sessionId,
          delegation_id: 'kanban:restart', delivery_status: 'uncertain', reference_state: 'visible',
          error: 'Received; dispatch outcome cannot be confirmed. Automatic replay is paused' },
      }] })
      return {} as any
    })
    await store.switchSession('resume-notification')
    const issues = store.sessions.find(session => session.id === 'resume-notification')!.messages
      .filter(message => message.commandAction === 'notification.issue')
    expect(issues).toHaveLength(1)
    expect(issues[0].content).toContain('Automatic replay is paused')
    expect(chatApi.startRunViaSocket).not.toHaveBeenCalled()
  })

  it('reuses completion notifications for Person terminal events outside the active conversation once',()=>{
    const store=useChatStore(),settings=useSettingsStore()
    settings.display.notify_on_complete=true
    store.sessions=[makeSession('another-chat')];store.activeSessionId='another-chat'
    const event={event:'person.run.completed',session_id:'person-main-1',notification_id:'main-run',output:'Report ready'}
    chatApi.globalPendingHandler?.(event);chatApi.globalPendingHandler?.(event)
    expect(completionNotification).toHaveBeenCalledTimes(1)
    expect(completionNotification).toHaveBeenCalledWith(expect.objectContaining({title:'zhezhe',body:'Report ready',clickUrl:'/person?session=person-main-1'}))
  })

  it('answers a Person prompt on its chat transport while another runtime is selected',()=>{
    const store=useChatStore()
    chatApi.globalPendingHandler?.({event:'clarify.requested',session_id:'person-main-1',clarify_id:'q',question:'Which scope?',choices:[]})
    store.setRuntimeMode('global_agent')
    expect(store.respondToClarifyFor('person-main-1','q','Today')).toBe('submitted')
    expect(respondClarify).toHaveBeenCalledWith('person-main-1','q','Today','chat-run')
    store.setRuntimeMode('default')
    expect(store.pendingClarifies.has('person-main-1')).toBe(true)
    invalidateAuth()
    expect(store.pendingClarifies.has('person-main-1')).toBe(false)
  })

  it('primes approval sound on direct send when completion sound is disabled', async () => {
    const store = useChatStore()
    const settingsStore = useSettingsStore()
    const session = makeSession('session-1')
    store.sessions = [session]
    store.activeSessionId = 'session-1'
    store.activeSession = session
    settingsStore.display.bell_on_complete = false
    settingsStore.display.approval_bell = true

    await store.sendMessage('request approval')

    expect(completionSoundMock.primeCompletionSound).toHaveBeenCalledOnce()
  })

  it('does not prime notification sound on direct send when both sound settings are disabled', async () => {
    const store = useChatStore()
    const settingsStore = useSettingsStore()
    const session = makeSession('session-1')
    store.sessions = [session]
    store.activeSessionId = 'session-1'
    store.activeSession = session
    settingsStore.display.bell_on_complete = false
    settingsStore.display.approval_bell = false

    await store.sendMessage('silent request')

    expect(completionSoundMock.primeCompletionSound).not.toHaveBeenCalled()
  })

  it('queues independent same-session approvals and clears only the exact terminal binding', () => {
    const store = useChatStore()
    store.activeSessionId = 'session-b'
    const first = { event: 'approval.requested', session_id: 'session-b', approval_id: 'a', run_id: 'r1', request_id: 'q1', choices: ['once', 'deny'] }
    chatApi.globalPendingHandler?.(first)
    chatApi.globalPendingHandler?.({ ...first, approval_id: 'b' })
    chatApi.globalPendingHandler?.({ ...first, run_id: 'r2', request_id: 'q2' })
    expect(store.pendingApprovals.size).toBe(3)
    expect(store.respondApprovalFor('session-b', 'a', 'once', { runId: 'wrong', requestId: 'q1' })).toBe('missing')
    expect(store.respondApprovalFor('session-b', 'a', 'once')).toBe('missing')
    expect(store.activePendingApproval).toMatchObject({ approvalId: 'a', runId: 'r1' })
    chatApi.globalPendingHandler?.({ ...first, event: 'approval.resolved', resolved: true, run_id: 'wrong' })
    expect(store.pendingApprovals.size).toBe(3)
    chatApi.globalPendingHandler?.({ ...first, event: 'approval.resolved', resolved: false, terminal: true })
    expect(store.pendingApprovals.size).toBe(2)
    expect(store.activePendingApproval?.approvalId).toBe('b')
    chatApi.globalPendingHandler?.({ ...first, event: 'approval.resolved', resolved: true })
    expect(store.pendingApprovals.size).toBe(2)
  })

  it('preserves durable Person approval in-flight bindings across runtime switches and clears them on auth invalidation', () => {
    const store = useChatStore()
    const request = { event: 'approval.requested', session_id: 'person-main-1', approval_id: 'a', choices: ['once', 'deny'], durable: true }
    chatApi.globalPendingHandler?.(request)
    chatApi.globalPendingHandler?.({ ...request, approval_id: 'b' })
    store.respondApprovalFor('person-main-1', 'a', 'once')
    store.setRuntimeMode('global_agent')
    expect(store.pendingApprovals.size).toBe(2)
    store.respondApprovalFor('person-main-1', 'a', 'once')
    expect(respondToolApproval).toHaveBeenCalledOnce()
    invalidateAuth()
    expect(store.pendingApprovals.size).toBe(0)
    chatApi.globalPendingHandler?.(request)
    store.respondApprovalFor('person-main-1', 'a', 'once')
    expect(respondToolApproval).toHaveBeenCalledTimes(2)
  })

  it('does not extend a replayed approval countdown, including zero remaining', () => {
    const store = useChatStore()
    store.activeSessionId = 'session-b'
    const event = { event: 'approval.requested', session_id: 'session-b', approval_id: 'a', remaining_timeout_ms: 0, timeout_ms: 600000 }
    chatApi.globalPendingHandler?.(event)
    const deadline = store.activePendingApproval!.countdownDeadline
    chatApi.globalPendingHandler?.({ ...event, remaining_timeout_ms: 600000 })
    expect(store.activePendingApproval!.countdownDeadline).toBe(deadline)
  })

  it('retains submitted approvals until ACK and retries recoverable failures without cross-clearing', () => {
    const store = useChatStore()
    store.activeSessionId = 's'
    const request = { event: 'approval.requested', session_id: 's', approval_id: 'a', run_id: 'r', request_id: 'q', choices: ['once', 'deny'] }
    chatApi.globalPendingHandler?.(request)
    chatApi.globalPendingHandler?.({ ...request, approval_id: 'b' })
    expect(store.respondApproval('once')).toBe('submitted')
    expect(store.pendingApprovals.size).toBe(2)
    expect(store.activePendingApproval?.approvalId).toBe('a')
    expect(store.respondApproval('once')).toBe('submitted')
    expect(respondToolApproval).toHaveBeenCalledTimes(1)
    const firstAttempt = (vi.mocked(respondToolApproval).mock.calls[0] as any)[4]
    const ack = { ...request, event: 'approval.resolved', ...firstAttempt }
    chatApi.globalPendingHandler?.({ ...ack, resolved: true, run_id: 'wrong' })
    chatApi.globalPendingHandler?.({ ...ack, resolved: true, session_id: 'wrong' })
    chatApi.globalPendingHandler?.({ ...ack, resolved: true, approval_id: 'wrong' })
    chatApi.globalPendingHandler?.({ ...ack, resolved: true, request_id: 'wrong' })
    expect(store.pendingApprovals.size).toBe(2)
    chatApi.globalPendingHandler?.({ ...ack, resolved: false, terminal: false, outcome: 'notify_failed' })
    expect(store.respondApproval('once')).toBe('submitted')
    expect(respondToolApproval).toHaveBeenCalledTimes(2)
    const retry = (vi.mocked(respondToolApproval).mock.calls[1] as any)[4]
    chatApi.globalPendingHandler?.({ ...ack, resolved: false, terminal: false })
    store.respondApproval('once')
    expect(respondToolApproval).toHaveBeenCalledTimes(2)
    chatApi.globalPendingHandler?.({ ...ack, ...retry, resolved: true })
    chatApi.globalPendingHandler?.({ ...ack, ...retry, resolved: true })
    expect(store.pendingApprovals.size).toBe(1)
    expect(store.activePendingApproval?.approvalId).toBe('b')
  })

  it('sets in-flight bookkeeping before synchronous ACK and releases it on synchronous failure or throw', () => {
    const store = useChatStore()
    store.activeSessionId = 's'
    const request = { event: 'approval.requested', session_id: 's', approval_id: 'a', choices: ['once', 'deny'], durable: true }
    chatApi.globalPendingHandler?.(request)
    vi.mocked(respondToolApproval).mockImplementationOnce((...args: any[]) => {
      chatApi.globalPendingHandler?.({ ...request, ...args[4], event: 'approval.resolved', resolved: false, terminal: false })
    })
    store.respondApproval('once')
    expect(store.activePendingApproval?.countdownDeadline).toBe(Infinity)
    vi.mocked(respondToolApproval).mockImplementationOnce(() => { throw new Error('transport unavailable') })
    expect(() => store.respondApproval('once')).toThrow('transport unavailable')
    vi.mocked(respondToolApproval).mockImplementationOnce((...args: any[]) => {
      chatApi.globalPendingHandler?.({ ...request, ...args[4], event: 'approval.resolved', resolved: true })
    })
    expect(store.respondApproval('once')).toBe('submitted')
    expect(respondToolApproval).toHaveBeenCalledTimes(3)
    expect(store.pendingApprovals.size).toBe(0)
  })

  it('cleans up only approvals belonging to a failed run through the streaming event path', async () => {
    const store = useChatStore()
    const session = makeSession('s')
    store.sessions = [session]
    store.activeSessionId = session.id
    store.activeSession = session
    await store.sendMessage('approval cleanup')
    const onEvent = chatApi.startRunViaSocket.mock.calls[0][1]
    for (const run_id of ['r1', 'r2']) onEvent({ event: 'approval.requested', session_id: 's', approval_id: run_id, run_id, choices: ['once', 'deny'] })
    expect(store.pendingApprovals.size).toBe(2)
    store.respondApprovalFor('s', 'r1', 'once')
    store.respondApprovalFor('s', 'r2', 'once')
    expect(respondToolApproval).toHaveBeenCalledTimes(2)
    onEvent({ event: 'run.failed', session_id: 's', run_id: 'r1', error: 'Failed' })
    expect([...store.pendingApprovals.values()].map(pending => pending.runId)).toEqual(['r2'])
    expect(store.respondApprovalFor('s', 'r1', 'once')).toBe('missing')
    store.respondApprovalFor('s', 'r2', 'once')
    expect(respondToolApproval).toHaveBeenCalledTimes(2)
  })

  it('keeps parallel same-session submission bookkeeping independent and handles asynchronous ACKs', async () => {
    const store = useChatStore()
    store.activeSessionId = 's'
    const request = { event: 'approval.requested', session_id: 's', run_id: 'r', choices: ['once', 'deny'] }
    for (const approval_id of ['a', 'b']) chatApi.globalPendingHandler?.({ ...request, approval_id })
    store.respondApprovalFor('s', 'a', 'once')
    store.respondApprovalFor('s', 'b', 'once')
    expect(respondToolApproval).toHaveBeenCalledTimes(2)
    const firstAttempt = (vi.mocked(respondToolApproval).mock.calls[0] as any)[4]
    chatApi.globalPendingHandler?.({ ...request, ...firstAttempt, event: 'approval.resolved', approval_id: 'a', resolved: false, terminal: false })
    store.respondApprovalFor('s', 'b', 'once')
    expect(respondToolApproval).toHaveBeenCalledTimes(2)
    vi.mocked(respondToolApproval).mockImplementationOnce((...args: any[]) => {
      queueMicrotask(() => chatApi.globalPendingHandler?.({ ...request, ...args[4], event: 'approval.resolved', approval_id: 'a', resolved: true }))
    })
    store.respondApprovalFor('s', 'a', 'once')
    expect(store.pendingApprovals.size).toBe(2)
    await Promise.resolve()
    expect([...store.pendingApprovals.values()].map(pending => pending.approvalId)).toEqual(['b'])
    store.respondApprovalFor('s', 'b', 'once')
    expect(respondToolApproval).toHaveBeenCalledTimes(3)
  })

  it('tracks an approval request from an inactive session through the global socket listener', () => {
    const store = useChatStore()
    const sessionA = makeSession('session-a')
    store.sessions = [sessionA, makeSession('session-b')]
    store.activeSessionId = 'session-a'
    store.activeSession = sessionA

    chatApi.globalPendingHandler?.({
      event: 'approval.requested',
      session_id: 'session-b',
      approval_id: 'approval-b',
      command: 'pwd',
      description: 'Run command',
      choices: ['once', 'deny'],
    })

    expect([...store.pendingApprovals.values()].find(pending => pending.sessionId === 'session-b')).toMatchObject({ approvalId: 'approval-b', command: 'pwd' })
    expect(store.activePendingApproval).toBeNull()
  })

  it('anchors approval and clarification countdowns to server-provided remaining time', () => {
    const store = useChatStore()
    store.sessions = [makeSession('session-b')]
    const now = performance.now()

    chatApi.globalPendingHandler?.({
      event: 'approval.requested', session_id: 'session-b', approval_id: 'approval-b',
      choices: ['once', 'deny'], timeout_ms: 300_000, remaining_timeout_ms: 42_000,
    })
    chatApi.globalPendingHandler?.({
      event: 'clarify.requested', session_id: 'session-b', clarify_id: 'clarify-b',
      question: 'Continue?', timeout_ms: 300_000, remaining_timeout_ms: 17_000,
    })

    expect([...store.pendingApprovals.values()].find(pending => pending.sessionId === 'session-b')!.countdownDeadline - now).toBeGreaterThanOrEqual(41_900)
    expect([...store.pendingApprovals.values()].find(pending => pending.sessionId === 'session-b')!.countdownDeadline - now).toBeLessThanOrEqual(42_100)
    expect(store.pendingClarifies.get('session-b')!.countdownDeadline - now).toBeGreaterThanOrEqual(16_900)
    expect(store.pendingClarifies.get('session-b')!.countdownDeadline - now).toBeLessThanOrEqual(17_100)
  })

  it.each(['timeout', 'session_closed', 'interrupted', 'notify_failed'])('clears a terminal %s without requiring an accepted decision', outcome => {
    const store = useChatStore()
    store.sessions = [makeSession('session-b')]
    chatApi.globalPendingHandler?.({
      event: 'approval.requested', session_id: 'session-b', approval_id: 'approval-b', choices: ['once', 'deny'],
    })
    chatApi.globalPendingHandler?.({
      event: 'approval.resolved', session_id: 'session-b', approval_id: 'approval-b', resolved: false, terminal: true, outcome,
    })
    expect([...store.pendingApprovals.values()].some(pending => pending.sessionId === 'session-b')).toBe(false)
  })

  it('does not let an old terminal clear a newer approval in the same session', () => {
    const store = useChatStore()
    chatApi.globalPendingHandler?.({ event: 'approval.requested', session_id: 'session-b', approval_id: 'new', choices: ['once', 'deny'] })
    chatApi.globalPendingHandler?.({ event: 'approval.resolved', session_id: 'session-b', approval_id: 'old', terminal: true, resolved: false, outcome: 'timeout' })
    expect([...store.pendingApprovals.values()].find(pending => pending.sessionId === 'session-b')?.approvalId).toBe('new')
  })

  it.each(['binding_mismatch', 'invalid_scope', 'stale'])('keeps valid pending approvals after a non-terminal %s response', outcome => {
    const store = useChatStore()
    for (const sid of ['session-a', 'session-b']) chatApi.globalPendingHandler?.({ event: 'approval.requested', session_id: sid, approval_id: sid, choices: ['once', 'deny'] })
    chatApi.globalPendingHandler?.({ event: 'approval.resolved', session_id: 'session-a', approval_id: 'session-a', resolved: false, terminal: false, outcome })
    expect([...store.pendingApprovals.values()].map(pending => pending.sessionId)).toEqual(['session-a', 'session-b'])
  })

  it('keeps a pending approval when the authoritative response is unresolved', () => {
    const store = useChatStore()
    store.sessions = [makeSession('session-a'), makeSession('session-b')]
    chatApi.globalPendingHandler?.({
      event: 'approval.requested', session_id: 'session-b', approval_id: 'approval-b', choices: ['once', 'deny'],
    })

    chatApi.globalPendingHandler?.({
      event: 'approval.resolved', session_id: 'session-b', approval_id: 'approval-b', resolved: false, error: 'stale',
    })

    expect([...store.pendingApprovals.values()].find(pending => pending.sessionId === 'session-b')).toMatchObject({ approvalId: 'approval-b' })
  })

  it('waits for the server to decide whether old approval and clarification prompts expired', () => {
    const store = useChatStore()
    const expired = vi.fn()
    window.addEventListener('hermes:pending-interaction-expired', expired)
    store.sessions = [makeSession('session-b')]
    chatApi.globalPendingHandler?.({
      event: 'approval.requested', session_id: 'session-b', approval_id: 'approval-b', choices: ['once', 'deny'], timeout_ms: 1,
    })
    chatApi.globalPendingHandler?.({
      event: 'clarify.requested', session_id: 'session-b', clarify_id: 'clarify-b', question: 'Continue?', timeout_ms: 1,
    })
    const approval = [...store.pendingApprovals.values()].find(pending => pending.sessionId === 'session-b')!
    approval.requestedAt = Date.now() - 10
    store.pendingClarifies.get('session-b')!.requestedAt = Date.now() - 10

    expect(store.respondApprovalFor('session-b', 'approval-b', 'once')).toBe('submitted')
    expect(store.respondToClarifyFor('session-b', 'clarify-b', 'Continue')).toBe('submitted')
    expect([...store.pendingApprovals.values()].some(pending => pending.sessionId === 'session-b')).toBe(true)
    expect(store.pendingClarifies.has('session-b')).toBe(true)
    expect(expired).not.toHaveBeenCalled()

    chatApi.globalPendingHandler?.({
      event: 'approval.resolved', session_id: 'session-b', approval_id: 'approval-b', resolved: false, terminal: true,
      stale: true, error: 'Approval is no longer pending.',
    })
    chatApi.globalPendingHandler?.({
      event: 'clarify.resolved', session_id: 'session-b', clarify_id: 'clarify-b', resolved: false,
      stale: true, error: 'Clarification is no longer pending.',
    })

    expect([...store.pendingApprovals.values()].some(pending => pending.sessionId === 'session-b')).toBe(false)
    expect(store.pendingClarifies.has('session-b')).toBe(false)
    expect(expired).toHaveBeenCalledTimes(2)
    window.removeEventListener('hermes:pending-interaction-expired', expired)
  })

  it('reports a server-confirmed terminal stale response after retaining the submitted approval', () => {
    const store = useChatStore()
    const session = makeSession('session-b')
    const expired = vi.fn()
    window.addEventListener('hermes:pending-interaction-expired', expired)
    store.sessions = [session]
    store.activeSessionId = session.id
    store.activeSession = session
    chatApi.globalPendingHandler?.({
      event: 'approval.requested', session_id: session.id, approval_id: 'approval-b', choices: ['once', 'deny'], timeout_ms: 300_000,
    })

    expect(store.respondApproval('once')).toBe('submitted')
    expect(store.pendingApprovals.size).toBe(1)
    chatApi.globalPendingHandler?.({
      event: 'approval.resolved', session_id: session.id, approval_id: 'approval-b', resolved: false, terminal: true,
      stale: true, error: 'Approval is no longer pending.',
    })

    expect(expired).toHaveBeenCalledOnce()
    window.removeEventListener('hermes:pending-interaction-expired', expired)
  })

  it('preserves assistant content when run.failed fires during streaming with substantial content', async () => {
    const store = useChatStore()
    const session = makeSession('session-1')
    store.sessions = [session]
    store.activeSessionId = 'session-1'
    store.activeSession = session

    await store.sendMessage('run claude')

    const onEvent = chatApi.startRunViaSocket.mock.calls[0][1] as (event: any) => void

    // Simulate run.started
    onEvent({ event: 'run.started', session_id: 'session-1', run_id: 'run-1' })

    // Simulate message.delta with substantial content (>100 chars)
    const longContent = 'A'.repeat(200)
    onEvent({
      event: 'message.delta',
      session_id: 'session-1',
      run_id: 'run-1',
      delta: longContent,
      output: longContent,
    })

    // At this point the assistant message should be streaming with content
    let assistantMsg = store.activeSession?.messages.find(
      (m: Message) => m.role === 'assistant',
    )
    expect(assistantMsg).toBeDefined()
    expect(assistantMsg?.isStreaming).toBe(true)
    expect((assistantMsg as any)?.content).toBe(longContent)

    // Simulate run.failed (e.g., socket disconnect)
    onEvent({
      event: 'run.failed',
      session_id: 'session-1',
      run_id: 'run-1',
      error: 'Socket disconnected',
    })

    // The original assistant message should be preserved (not overwritten)
    const msgs = store.activeSession?.messages || []
    assistantMsg = msgs.find((m: Message) => m.content === longContent)
    expect(assistantMsg).toBeDefined()
    expect(assistantMsg?.content).toBe(longContent)
    expect(assistantMsg?.isStreaming).toBe(false)
    expect(assistantMsg?.systemType).toBeUndefined()

    // A separate error message should be appended
    const errorMessage = msgs.find(
      (m: Message) => m.role === 'assistant' && m.systemType === 'error',
    )
    expect(errorMessage).toBeDefined()
    expect(errorMessage?.content).toBe('Error: Socket disconnected')
  })

  it('overwrites empty streaming message when run.failed fires (no substantial content)', async () => {
    const store = useChatStore()
    const session = makeSession('session-1')
    store.sessions = [session]
    store.activeSessionId = 'session-1'
    store.activeSession = session

    await store.sendMessage('run claude')

    const onEvent = chatApi.startRunViaSocket.mock.calls[0][1] as (event: any) => void

    // Simulate run.started
    onEvent({ event: 'run.started', session_id: 'session-1', run_id: 'run-1' })

    // Simulate message.delta with only a short content (<100 chars)
    onEvent({
      event: 'message.delta',
      session_id: 'session-1',
      run_id: 'run-1',
      delta: 'Hi',
      output: 'Hi',
    })

    // Simulate run.failed
    onEvent({
      event: 'run.failed',
      session_id: 'session-1',
      run_id: 'run-1',
      error: 'Something went wrong',
    })

    const msgs = store.activeSession?.messages || []
    const assistantMsg = msgs.find((m: Message) => m.role === 'assistant')
    expect(assistantMsg).toBeDefined()
    expect(assistantMsg?.content).toBe('Error: Something went wrong')
    expect(assistantMsg?.systemType).toBe('error')
    expect(assistantMsg?.isStreaming).toBe(false)
  })

  it('appends error as separate message when streaming has finished (isStreaming false)', async () => {
    const store = useChatStore()
    const session = makeSession('session-1')
    store.sessions = [session]
    store.activeSessionId = 'session-1'
    store.activeSession = session

    await store.sendMessage('run claude')

    const onEvent = chatApi.startRunViaSocket.mock.calls[0][1] as (event: any) => void

    // Simulate run.started
    onEvent({ event: 'run.started', session_id: 'session-1', run_id: 'run-1' })

    // Simulate message.delta
    onEvent({
      event: 'message.delta',
      session_id: 'session-1',
      run_id: 'run-1',
      delta: 'Hello, how can I help you?',
      output: 'Hello, how can I help you?',
    })

    // Simulate run.completed (closes streaming)
    onEvent({
      event: 'run.completed',
      session_id: 'session-1',
      run_id: 'run-1',
    })

    // At this point isStreaming should be false
    const assistantMsg = store.activeSession?.messages.find(
      (m: Message) => m.role === 'assistant',
    )
    expect(assistantMsg?.isStreaming).toBe(false)

    // Now simulate run.failed (e.g., late socket error)
    onEvent({
      event: 'run.failed',
      session_id: 'session-1',
      run_id: 'run-1',
      error: 'Late socket error',
    })

    // Original message should be unchanged
    const msgs = store.activeSession?.messages || []
    const firstAssistant = msgs.find((m: Message) => m.content === 'Hello, how can I help you?')
    expect(firstAssistant).toBeDefined()
    expect(firstAssistant?.systemType).toBeUndefined()

    // Error appended as separate message
    const errorMessage = msgs.find((m: Message) => m.systemType === 'error')
    expect(errorMessage).toBeDefined()
    expect(errorMessage?.content).toBe('Error: Late socket error')
  })
})
