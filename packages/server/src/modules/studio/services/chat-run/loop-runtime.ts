import { randomUUID } from 'node:crypto'
import type { Socket } from 'socket.io'
import type { AuthenticatedUser } from '../../public/auth'
import type { PrimaryAgentBridgeOutput } from '../../public/chat-agent-runtime'
import { getSession, getSessionDetail, type HermesSessionRow } from '../../repositories/session-store'
import { getOrCreateSession } from './compression'
import { isHermesLoopSession } from './loop-driver'
import type { ModelRunAuth } from './model-run-source'
import type { SessionState } from './types'

/** Functions are internal capabilities, never deserialized from socket/HTTP input. */
export interface LoopRunHooks {
  loopAuthorize?: () => void
  loopBeforeDispatch?: () => Promise<void>
  loopDispatchConfirmed?: () => void
  loopProof?: (output: PrimaryAgentBridgeOutput) => void
  recoveryRegister?: (binding: ModelRunAuth) => void
  background_notification_kind?: 'kanban'
}

type Result = Record<string, any>
interface LoopHost {
  sessionMap: Map<string, SessionState>
  request: (payload: Result) => Promise<Result>
  profiles: () => string[]
  owner: (row: HermesSessionRow) => AuthenticatedUser
  socket: (sessionId: string) => Socket
  run: (socket: Socket, data: Result, profile: string) => Promise<void>
  command: (row: HermesSessionRow, state: SessionState, input: string, hooks: LoopRunHooks) => Promise<{ response?: string; model?: boolean }>
  emit: (sessionId: string, event: string, payload: Result) => void
  closing: () => boolean
}

/** Native LoopStore is authoritative; this driver only owns in-flight dispatches. */
export class LoopRuntime {
  private polling = false
  private recovered = false
  private stopped = false
  private fault?: string
  private runs = new Map<string, Promise<void>>()
  private capabilities = new Map<string, { row: HermesSessionRow; binding: Result; owner: AuthenticatedUser }>()
  constructor(private readonly host: LoopHost) {}

  async poll(): Promise<void> {
    if (this.polling || this.stopped || this.fault || this.host.closing()) return
    this.polling = true
    try {
      const profiles = this.host.profiles()
      if (!this.recovered) {
        await this.host.request({ action: 'loop_recover', profiles })
        this.recovered = true
      }
      const listed = await this.host.request({ action: 'loop_list', profiles })
      for (const item of listed.loops || []) {
        if (this.stopped || this.host.closing() || item.current_tick || this.runs.has(item.session_id)) continue
        const recovery = item.recovery?.status === 'pending'
        if (!recovery && (['held', 'failed', 'claimed', 'injecting', 'started'].includes(item.recovery?.status) || item.next_due_at * 1000 > Date.now())) continue
        const row = getSession(item.session_id)
        if (!isHermesLoopSession(row) || !row?.user_id || (row.profile || 'default') !== item.route?.profile) continue
        this.host.owner(row)
        const state = getOrCreateSession(this.host.sessionMap, row.id)
        if (state.isWorking || state.queue.length) continue
        state.isWorking = true
        const work = this.execute({ ...row }, state, item, recovery)
        this.runs.set(row.id, work)
        void work.finally(() => this.runs.delete(row.id)).catch(() => {})
      }
    } catch (error) {
      // A transport-unknown write must not automatically replay a task.
      this.fault = String(error)
    } finally { this.polling = false }
  }

  private authorize(row: HermesSessionRow): AuthenticatedUser {
    const fresh = getSession(row.id)
    if (this.stopped || this.host.closing() || !isHermesLoopSession(fresh) || !fresh
      || String(fresh.user_id) !== String(row.user_id) || fresh.profile !== row.profile) throw new Error('Loop session binding changed')
    return this.host.owner(fresh)
  }

  async recoveryOperation(marker: string, user: AuthenticatedUser | undefined, sessionId: string, profile: string, payload: Result): Promise<Result> {
    const cap = this.capabilities.get(marker)
    if (!cap || String(user?.id) !== String(cap.owner.id) || cap.row.id !== sessionId || cap.row.profile !== profile) throw new Error('Recovery binding rejected')
    this.authorize(cap.row)
    if (payload.recovery_id !== cap.binding.recovery_id || payload.expected_generation !== cap.binding.expected_generation
      || Object.keys(payload).some(key => !['recovery_id', 'expected_generation', 'decision', 'evidence'].includes(key))) throw new Error('Recovery fields rejected')
    return this.host.request({ action: 'loop_operation', session_id: sessionId, profile, ...cap.binding,
      op: payload.decision === undefined ? 'recovery_status' : 'recovery_propose', decision: payload.decision, evidence: payload.evidence })
  }

  private async execute(row: HermesSessionRow, state: SessionState, item: Result, recovery: boolean): Promise<void> {
    const base: Result = { action: 'loop_operation', session_id: row.id, profile: row.profile || 'default',
      ...(recovery ? { recovery_id: item.recovery.id, expected_generation: item.generation } : { tick_id: randomUUID() }) }
    let claimed = false, dispatched = false, runId: string | undefined, marker: string | undefined
    let proof: PrimaryAgentBridgeOutput | undefined, terminal: Result | undefined
    let updates: Promise<unknown> = Promise.resolve()
    const request = (op: string, extra: Result = {}) => this.host.request({ ...base, op, ...extra })
    try {
      const owner = this.authorize(row)
      const claim = await request(recovery ? 'recovery_claim' : 'claim')
      if (!(recovery ? claim.authorized : claim.claimed)) return
      claimed = true
      base.generation = claim.generation
      const input = recovery
        ? `Recovery verification only. Never replay the interrupted business task or perform external writes or dispatch. Inspect saved conversation and existing evidence. Missing evidence requires hold. Submit one bound resume/finish/hold proposal with evidence using POST /api/studio/chat-run/loop-recovery and header x-webui-recovery-run equal to the current run marker, with session_id, profile and recovery=${JSON.stringify({ recovery_id: item.recovery.id, expected_generation: item.generation })}. Resume only schedules future supervision. Interrupted record: ${JSON.stringify(item.recovery)}`
        : String(claim.prompt || '')
      const hooks = {
        loopAuthorize: () => { this.authorize(row) },
        recoveryRegister: recovery ? (auth: ModelRunAuth) => {
          marker = auth.run_marker
          this.capabilities.set(marker, { row, binding: { recovery_id: item.recovery.id, expected_generation: item.generation }, owner })
        } : undefined,
        loopBeforeDispatch: async () => {
          this.authorize(row)
          const gate = await request(recovery ? 'recovery_inject' : 'inject')
          if (!gate.authorized) throw new Error('Loop generation invalidated')
          this.authorize(row)
        },
        loopDispatchConfirmed: () => { dispatched = true },
        loopProof: (chunk: PrimaryAgentBridgeOutput) => { if (chunk.done) proof = chunk },
        onEvent: (name: string, event: Result) => {
          if (name === 'run.started') {
            runId = event.run_id
            base.run_id = runId
            updates = updates.then(() => request(recovery ? 'recovery_started' : 'started', { run_id: runId }))
            void updates.catch(() => {})
          }
          if (['run.completed', 'run.failed', 'abort.completed'].includes(name)) terminal = { ...event, name }
        },
      }
      let commandResult: { response?: string; model?: boolean } | undefined
      if (!recovery && input.trimStart().startsWith('/')) {
        commandResult = await this.host.command(row, state, input, hooks)
      } else {
        await this.host.run(this.host.socket(row.id), {
          ...hooks, session_id: row.id, source: 'cli', input,
          display_input: recovery ? 'Recovery verification — no replay' : '[loop]', display_role: 'command', storage_message: input,
          model: row.model || undefined, provider: row.provider || undefined, autonomous: true, background_delegation_enabled: false,
        }, row.profile || 'default')
      }
      await updates
      const normal = commandResult && !commandResult.model || runId && terminal?.name === 'run.completed' && terminal.run_id === runId
        && proof?.run_id === runId && proof.status === 'complete' && !proof.error
        && !proof.result?.failed && !proof.result?.partial && !proof.result?.interrupted
      if (normal && (!commandResult || commandResult.model)) {
        const saved = getSessionDetail(row.id)?.messages.find(message => message.id === terminal?.message_id && message.role === 'assistant')
        if (!saved?.content?.trim()) throw new Error('Loop assistant persistence readback failed')
      }
      this.authorize(row)
      const outcome = recovery ? normal ? 'recovery_commit' : 'recovery_failed'
        : normal ? 'complete' : !dispatched ? 'abandon' : proof?.result?.interrupted || terminal?.name === 'abort.completed' ? 'interrupted' : proof?.error || terminal?.name === 'run.failed' ? 'failed' : 'unknown'
      const result = await request(outcome, { response: commandResult?.response ?? terminal?.output ?? '', not_started: !dispatched, reason: 'No proven normal completion' })
      this.host.emit(row.id, 'loop.updated', { session_id: row.id, ...result })
    } catch (error) {
      if (claimed) {
        try { await updates.catch(() => {}); await request(recovery ? 'recovery_failed' : dispatched ? 'unknown' : 'abandon', { not_started: !dispatched, reason: String(error) }) }
        catch (writeError) { this.fault = String(writeError) }
      } else this.fault = String(error)
    } finally {
      if (marker) this.capabilities.delete(marker)
      if (!state.runId && !state.activeRunMarker && !state.abortController && !state.queue.length) state.isWorking = false
    }
  }

  close(): void { this.stopped = true; this.capabilities.clear() }
}
