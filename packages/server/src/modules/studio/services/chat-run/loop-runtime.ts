import { compatibilityRollbackProtected } from '../compatibility-rollback'
import { randomUUID } from 'node:crypto'
import { logger } from '../../public/logging'
import type { Socket } from 'socket.io'
import type { AuthenticatedUser } from '../../public/auth'
import type { PrimaryAgentBridgeOutput } from '../../public/chat-agent-runtime'
import { addLoopStatusMessage, getSession, getSessionDetail, type HermesSessionRow } from '../../repositories/session-store'
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
  recoveryRegister?: (binding: ModelRunAuth) => void | Promise<void>
  background_notification_kind?: 'kanban'
}

type Result = Record<string, any>
function formatLoopStatus(event: Result): string {
  const reason = String(event.reason)
  const exhausted = /exhausted|completed the requested/i.test(reason)
    || event.status === 'paused' && event.total > 0 && event.ticks_fired >= event.total
  let explanation: string
  if (reason === 'loop-created' || reason === 'loop-active') explanation = '新建监督组：按本组明确设置的周期与预算安排后续唤醒。New loop configured; no previous work is replayed.'
  else if (reason === 'manual-resume') explanation = '已手动恢复后续监督，不重放旧任务、不重置已用次数。Resume schedules future wakeups only; it does not replay work or reset the count.'
  else if (reason.startsWith('user-paused')) explanation = '用户主动暂停了后续监督；正在进行的业务任务不因此中止。User paused future supervision.'
  else if (reason.startsWith('startup-failed')) explanation = '启动确认多次失败，后续监督已暂停；这不能证明业务执行失败。请检查桥接连接和已有运行记录，确认安全后再恢复。Repeated startup confirmation failures; inspect transport and saved runs.'
  else if (reason.startsWith('execution-unknown')) explanation = '执行结果尚未确认：缺少可核对的完整完成凭据，不能认定失败或完成。请核对已保存回复与运行记录，不要重放本轮业务。An unknown result is neither a proven failure nor a proven completion.'
  else if (reason.startsWith('dispatch-unknown')) explanation = '无法确认是否已派发本轮业务；保留次数并暂停后续监督。须通过绑定恢复核验已有运行证据，不能直接恢复或重放。Dispatch is uncertain; bound recovery is required before resuming.'
  else if (reason.startsWith('user-interrupted')) explanation = '本轮运行被中断；已用次数保留。请核对部分结果及仍在进行的业务，准备好后再恢复后续监督。The run was interrupted; inspect partial results.'
  else if (reason.startsWith('execution-failed')) explanation = '本轮运行报告失败，后续监督已暂停；请检查完整错误和已保存结果，解决问题后再恢复。The run reported failure; inspect the full error and saved result.'
  else if (/recovery/i.test(reason)) explanation = '恢复核验暂缓或尚未解决，证据不足时不继续监督、不重放业务。请查看恢复记录并完成所需人工核验。Recovery is held or unresolved; resolve the recorded requirement first.'
  else if (/human|waiting|approval|unachievable/i.test(reason)) explanation = '等待人工处理：请处理所列审批、阻塞条件或调整目标，再决定是否恢复后续监督。Human action is required before future supervision continues.'
  else if (/agent signaled|stop condition met/i.test(reason)) explanation = '监督正常完成：收到 LOOP_COMPLETE 或已满足停止条件，不再安排后续唤醒；这不代替业务成果核验。Normal completion stopped future wakeups.'
  else if (/cleared|stopped|cancel/i.test(reason) || event.status === 'cleared') explanation = '用户明确停止了后续监督，不会再唤醒；已进行的业务不因此中止。User explicitly stopped future supervision.'
  else if (exhausted) explanation = '监督次数已耗尽，不再安排后续唤醒；这不表示业务任务已完成。The supervision budget is exhausted, not proof of business completion.'
  else explanation = '监督状态已改变；请按下列原始原因核对运行记录和所需操作，不要据此重放业务。Inspect the raw reason and saved evidence before taking action.'
  if (exhausted && !explanation.includes('监督次数已耗尽')) explanation += '\n监督次数已耗尽，不能恢复旧组。'
  const action = exhausted || ['done', 'cleared'].includes(event.status)
    ? '如仍需监督，请明确新建 /loop 并设置新预算；旧组不会恢复。Explicitly set a new /loop with a new budget if needed.'
    : event.status === 'active'
      ? '后续监督已启用；如需暂停使用 /loop pause，停止使用 /loop stop。Future supervision is active.'
      : /dispatch-unknown|recovery/i.test(reason)
        ? '先解决绑定恢复/人工核验要求；未解决前保持暂停，不自动恢复。Resolve bound recovery/human requirements; no automatic resume.'
        : '核对结果并解决人工/恢复要求后，可用 /loop resume 仅恢复未来唤醒；不重置次数、不重放业务。Use /loop resume only when ready; future wakeups only.'
  const scope = event.status === 'paused'
    ? 'Only future supervision is paused; this does not halt the business task. 仅暂停后续监督，不表示业务任务已停止。'
    : event.status === 'active' ? '后续监督生效，不重放历史业务。'
      : 'Future supervision has stopped; this does not halt the business task. 仅停止后续监督，不表示业务任务已停止。'
  return `[loop status event]\nState at ${new Date(event.timestamp * 1000).toISOString()}: ${event.status}\nTicks: ${event.ticks_fired}/${event.total}\nPeriod: ${event.period ?? 'unavailable (legacy event; not reconstructed)'}\nReason: ${reason}\n${explanation}\n${scope}\n${action}\n此为历史状态变化；按时间排序，最新一条状态事件为当前已知状态。This is a retained state change; a later loop status event supersedes it.`
}

export interface RecoveryBinding {
  session_id: string
  profile: string
  recovery_id: string
  expected_generation: number
  reservation_id: string
  run_marker?: string
  run_id?: string
  model_run_auth?: ModelRunAuth
  deadline_at?: number
  cancel_requested?: boolean
}

interface LoopHost {
  abortRecovery: (binding: RecoveryBinding, reason: string) => Promise<Result>
  observeRecoveryExit: (binding: RecoveryBinding) => Promise<Result>
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
  private stopped = false
  private retryAt = 0
  private startupRecovered = false
  private recoveryRetryAt = 0
  private uncertain = new Map<string, { row: HermesSessionRow; binding: Result; retryAt: number; attempts?: number }>()
  private runs = new Map<string, Promise<void>>()
  private capabilities = new Map<string, { row: HermesSessionRow; binding: Result; owner: AuthenticatedUser }>()
  private recoveryReady = false
  private resources = new Map<string, RecoveryBinding>()
  private resourceRetryAt = new Map<string, number>()
  private timer?: ReturnType<typeof setInterval>
  constructor(private readonly host: LoopHost) {}

  start(): void {
    if (this.timer || this.stopped) return
    this.timer = setInterval(() => { void this.poll() }, 500)
    this.timer.unref?.()
    void this.poll()
  }

  private log(binding: Result, resolution: string, error?: unknown, result?: Result): void {
    logger.warn({ event: 'loop.claim', session_id: binding.session_id, profile: binding.profile,
      generation: binding.generation, tick_id: binding.tick_id, resolution,
      error_class: error instanceof Error && /timeout|timed out/i.test(error.message) ? 'timeout' : error ? 'request-or-proof-error' : undefined,
      next_due_at: result?.loop_state?.next_due_at, ticks_fired: result?.loop_state?.ticks_fired,
      status: result?.loop_state?.status }, 'Loop claim lifecycle')
  }

  async poll(): Promise<void> {
    if (this.polling || this.stopped || compatibilityRollbackProtected() || Date.now() < this.retryAt || this.host.closing()) return
    this.polling = true
    try {
      const profiles = this.host.profiles()
      if (!this.startupRecovered && Date.now() >= this.recoveryRetryAt) {
        try {
          await this.host.request({ action: 'loop_recover', profiles, recovery_only: true })
          this.startupRecovered = true
        } catch { this.recoveryRetryAt = Date.now() + 2000 }
      }
      try {
        await this.refreshResources()
        await this.observeResources()
        this.recoveryReady = true
      } catch { this.recoveryReady = false }
      const listed = await this.host.request({ action: 'loop_list', profiles, include_inflight: true, include_events: true })
      const blocked = new Set(this.uncertain.keys())
      for (const [sessionId, pending] of this.uncertain) {
        if (this.runs.has(sessionId) || pending.retryAt > Date.now()) continue
        await this.reconcile(pending)
      }
      for (const item of listed.loops || []) {
        if (this.stopped || this.host.closing()) continue
        const row = getSession(item.session_id)
        if (!isHermesLoopSession(row) || !row?.user_id || (row.profile || 'default') !== item.route?.profile) continue
        try { this.authorize(row) } catch { continue }
        await this.publishStatus(row, item)
        if (blocked.has(item.session_id) || this.runs.has(item.session_id)) continue
        const recovery = item.recovery?.status === 'pending'
        if (recovery && (!this.startupRecovered || !this.recoveryReady)) continue
        const state = getOrCreateSession(this.host.sessionMap, row.id)
        if (state.isWorking || state.queue.length) continue
        if (item.status && item.status !== 'active' && !item.current_tick && !recovery) continue
        if (item.current_tick) {
          if (item.current_tick.phase !== 'unknown') {
            const pending = { row: { ...row }, binding: { action: 'loop_operation', session_id: row.id,
              profile: row.profile || 'default', tick_id: item.current_tick.id, generation: item.current_tick.generation }, retryAt: 0 }
            this.uncertain.set(row.id, pending)
            await this.reconcile(pending)
          }
          continue
        }
        if (!recovery && (['held', 'failed', 'claimed', 'injecting', 'started'].includes(item.recovery?.status) || item.next_due_at * 1000 > Date.now())) continue
        state.isWorking = true
        const work = this.execute({ ...row }, state, item, recovery)
        this.runs.set(row.id, work)
        void work.finally(() => this.runs.delete(row.id)).catch(() => {})
      }
    } catch {
      this.retryAt = Date.now() + 2000
    } finally { this.polling = false }
  }

  private async refreshResources(): Promise<void> {
    const result = await this.host.request({ action: 'recovery_resources' })
    if (!Array.isArray(result.resources)) throw new Error('Recovery enumeration incomplete')
    this.resources = new Map(result.resources.map((binding: RecoveryBinding) => [binding.reservation_id || `${binding.profile}:${binding.recovery_id}`, binding]))
  }

  async recoveryIssues(sessionId: string, profile: string): Promise<Result[]> {
    const result = await this.host.request({ action: 'recovery_resources' })
    if (!Array.isArray(result.resources)) throw new Error('Recovery enumeration incomplete')
    return result.resources.filter((binding: Result) => binding.session_id === sessionId && binding.profile === profile
      && (binding.cancel_requested || binding.outcome === 'unknown'))
      .map((binding: Result) => this.recoveryIssue(binding, binding.evidence?.reason || binding.reason || 'original_exit_unproven',
        binding.last_observed_at ? binding.last_observed_at * 1000 : undefined))
  }

  private recoveryIssue(binding: Result, reason: string, observedAt?: number): Result {
    return { event: 'agent.event', session_id: binding.session_id, kind: 'recovery.resource',
      recovery_id: binding.recovery_id, generation: binding.expected_generation,
      text: `Recovery stop result pending verification: ${reason}. ${observedAt ? `Last checked ${new Date(observedAt).toISOString()}. ` : ''}Capacity remains reserved; no automatic replay.`,
      last_observed_at: observedAt }
  }

  private async observeResources(cancelReason?: string): Promise<void> {
    for (const [id, binding] of this.resources) {
      const expired = binding.deadline_at !== undefined && binding.deadline_at * 1000 <= Date.now()
      if (!cancelReason && (this.resourceRetryAt.get(id) || 0) > Date.now()
        && (!expired || binding.cancel_requested)) continue
      const cancel = cancelReason || expired || binding.cancel_requested
      if (cancel && binding.run_marker) this.capabilities.delete(binding.run_marker)
      try {
        const result = cancel
          ? await this.host.abortRecovery(binding, cancelReason || 'Recovery decision deadline expired')
          : await this.host.observeRecoveryExit(binding)
        if (['exited_proven', 'not_dispatched_proven'].includes(result.outcome)) {
          this.resources.delete(id)
          this.resourceRetryAt.delete(id)
        } else {
          this.resourceRetryAt.set(id, Date.now() + 2000)
          this.host.emit(binding.session_id, 'loop.updated', { session_id: binding.session_id,
            recovery_id: binding.recovery_id, generation: binding.expected_generation,
            recovery_exit: result.outcome, reason: result.reason, last_observed_at: Date.now() })
          if (cancel || result.outcome === 'unknown') {
            this.host.emit(binding.session_id, 'agent.event', this.recoveryIssue(binding, result.reason || 'original_exit_unproven', Date.now()))
          }
        }
      } catch { this.resourceRetryAt.set(id, Date.now() + 2000) }
    }
  }

  private async publishStatus(row: HermesSessionRow, result: Result): Promise<void> {
    try { await this.deliverStatus(row, result) }
    catch (error) {
      // The native outbox retains unacknowledged events; delivery cannot revoke execution proof.
      this.log({ session_id: row.id, profile: row.profile }, 'status-delivery-pending', error)
    }
  }

  private emitUpdate(sessionId: string, payload: Result): void {
    try { this.host.emit(sessionId, 'loop.updated', payload) }
    catch (error) { this.log({ session_id: sessionId }, 'status-emit-pending', error) }
  }

  private async deliverStatus(row: HermesSessionRow, result: Result): Promise<void> {
    for (const event of result.status_events || []) {
      try {
      this.authorize(row)
      const content = formatLoopStatus(event)
      const saved = addLoopStatusMessage(row.id, event.id, content, event.timestamp)
      const state = this.host.sessionMap.get(row.id)
      if (state && !state.messages.some(message => String(message.id) === String(saved.id))) state.messages.push(saved)
      this.host.emit(row.id, 'session.command', { event: 'session.command', session_id: row.id,
        action: 'loop_event', command: 'loop', type: 'loop', message_id: saved.id,
        message: saved.content, timestamp: saved.timestamp, loop_event_id: event.id })
      await this.host.request({ action: 'loop_operation', op: 'event_ack', session_id: row.id,
        profile: row.profile || 'default', event_id: event.id })
      } catch (error) {
        // Keep each failed delivery in the native outbox without blocking later transitions.
        this.log({ session_id: row.id, profile: row.profile }, 'status-delivery-pending', error)
      }
    }
  }

  private async reconcile(pending: { row: HermesSessionRow; binding: Result; retryAt: number; attempts?: number }): Promise<void> {
    try {
      this.authorize(pending.row)
      const result = await this.host.request({ ...pending.binding, op: pending.binding.recovery_id ? 'recovery_failed' : 'reconcile_claim',
        ...(pending.binding.recovery_id ? { not_started: false, reason: 'Recovery receipt uncertain; no automatic replay' } : {}) })
      if (!result.claim_resolution && !pending.binding.recovery_id) throw new Error('Claim reconciliation binding unresolved')
      this.uncertain.delete(pending.row.id)
      this.log(pending.binding, result.claim_resolution || 'recovery-failed', undefined, result)
      await this.publishStatus(pending.row, result)
      this.emitUpdate(pending.row.id, { session_id: pending.row.id, ...result })
    } catch (error) {
      pending.attempts = (pending.attempts || 0) + 1
      const delay = Math.min(30_000, 1000 * 2 ** Math.min(pending.attempts, 5))
      pending.retryAt = Date.now() + delay
      this.log(pending.binding, 'receipt-uncertain', error)
      this.emitUpdate(pending.row.id, { session_id: pending.row.id,
        claim_resolution: 'receipt-uncertain', tick_id: pending.binding.tick_id, generation: pending.binding.generation,
        reason: `Bound reconciliation pending; no business replay; retry in ${delay / 1000}s` })
    }
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
      ...(recovery ? { recovery_id: item.recovery.id, expected_generation: item.generation } : { tick_id: randomUUID(), generation: item.generation }) }
    let claimed = false, dispatched = false, runId: string | undefined, marker: string | undefined
    let proof: PrimaryAgentBridgeOutput | undefined, terminal: Result | undefined
    let updates: Promise<unknown> = Promise.resolve()
    const request = async (op: string, extra: Result = {}): Promise<Result> => {
      try { return await this.host.request({ ...base, op, ...extra }) }
      catch (error) {
        if (recovery || !['started', 'complete'].includes(op)) throw error
        this.authorize(row)
        // A lost acknowledgement is not a failed write; read only this tick/run.
        const receipt = await this.host.request({ ...base, ...extra, op: 'receipt', receipt_op: op })
        if (receipt.receipt_confirmed !== true) throw error
        this.log(base, `${op}-receipt-confirmed`, error, receipt)
        return receipt
      }
    }
    try {
      const owner = this.authorize(row)
      const claim = await request(recovery ? 'recovery_claim' : 'claim')
      if (!(recovery ? claim.authorized : claim.claimed)) return
      claimed = true
      if (!recovery && claim.generation !== base.generation) throw new Error('Loop claim generation mismatch')
      if (recovery) {
        base.generation = claim.generation
        base.reservation_id = claim.reservation_id
        if (!base.reservation_id) throw new Error('Durable recovery reservation missing')
      }
      const input = recovery
        ? `Recovery verification only. Never replay the interrupted business task or perform external writes or dispatch. Inspect saved conversation and existing evidence. Missing evidence requires hold. Submit one bound resume/finish/hold proposal with evidence using POST /api/studio/chat-run/loop-recovery and header x-webui-recovery-run equal to the current run marker, with session_id, profile and recovery=${JSON.stringify({ recovery_id: item.recovery.id, expected_generation: item.generation })}. Resume only schedules future supervision. Interrupted record: ${JSON.stringify(item.recovery)}`
        : String(claim.prompt || '')
      const hooks = {
        loopAuthorize: () => { this.authorize(row) },
        recoveryRegister: recovery ? async (auth: ModelRunAuth) => {
          marker = auth.run_marker
          const registered = await request('recovery_bind', { recovery_auth: auth })
          if (!registered.authorized) throw new Error('Recovery resource binding rejected')
          this.resources.set(base.reservation_id, { ...base, model_run_auth: auth, run_marker: marker } as RecoveryBinding)
          this.capabilities.set(marker, { row, binding: { recovery_id: item.recovery.id, expected_generation: item.generation }, owner })
        } : undefined,
        loopBeforeDispatch: async () => {
          this.authorize(row)
          const gate = await request(recovery ? 'recovery_inject' : 'inject')
          if (!gate.authorized) throw new Error('Loop generation invalidated')
          if (recovery) {
            const resource = this.resources.get(base.reservation_id)
            if (resource) resource.deadline_at = gate.recovery?.deadline_at
          }
          this.authorize(row)
        },
        loopDispatchConfirmed: () => { dispatched = true },
        loopProof: (chunk: PrimaryAgentBridgeOutput) => { if (chunk.done) proof = chunk },
        onEvent: (name: string, event: Result) => {
          if (name === 'run.started') {
            runId = event.run_id
            base.run_id = runId
            updates = updates.then(async () => {
              if (recovery) await request('recovery_resource_started', { run_id: runId })
              return request(recovery ? 'recovery_started' : 'started', { run_id: runId })
            })
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
          display_input: recovery ? 'Recovery verification — no replay' : input, display_role: 'command', storage_message: input,
          model: row.model || undefined, provider: row.provider || undefined, autonomous: true, background_delegation_enabled: false,
        }, row.profile || 'default')
      }
      await updates
      const normal = commandResult && !commandResult.model || runId && terminal?.name === 'run.completed' && terminal.run_id === runId
        && proof?.run_id === runId && proof.session_id === row.id && proof.status === 'complete' && !proof.error
        && !proof.result?.failed && !proof.result?.partial && !proof.result?.interrupted
      if (normal && (!commandResult || commandResult.model)) {
        // Bridge persistence emits a string ID; repository rows use numbers.
        // Compare the exact representation, never coerce via Number/loose equality.
        const messageId = terminal?.message_id
        const saved = messageId == null ? undefined : getSessionDetail(row.id)?.messages.find(message => String(message.id) === String(messageId) && message.role === 'assistant')
        if (!saved?.content?.trim()) throw new Error('Loop assistant persistence readback failed')
      }
      this.authorize(row)
      const outcome = recovery ? normal ? 'recovery_commit' : 'recovery_failed'
        : normal ? 'complete' : !dispatched ? 'abandon' : proof?.result?.interrupted || terminal?.name === 'abort.completed' ? 'interrupted' : proof?.error || terminal?.name === 'run.failed' ? 'failed' : 'unknown'
      const result = await request(outcome, { response: commandResult?.response ?? terminal?.output ?? '', not_started: !dispatched, reason: 'No proven normal completion' })
      await this.publishStatus(row, result)
      this.emitUpdate(row.id, { session_id: row.id, ...result })
    } catch (error) {
      this.log(base, claimed ? 'execution-receipt-error' : 'claim-error', error)
      if (claimed) {
        try {
          await updates.catch(() => {})
          const result = await request(recovery ? 'recovery_failed' : dispatched ? 'unknown' : 'abandon', { not_started: !dispatched, reason: 'Loop execution receipt or proof unavailable' })
          await this.publishStatus(row, result)
          this.emitUpdate(row.id, { session_id: row.id, ...result })
        }
        catch { this.uncertain.set(row.id, { row, binding: { ...base }, retryAt: 0 }) }
      } else this.uncertain.set(row.id, { row, binding: { ...base }, retryAt: 0 })
      this.emitUpdate(row.id, { session_id: row.id, tick_id: base.tick_id, generation: base.generation,
        claim_resolution: this.uncertain.has(row.id) ? 'receipt-uncertain' : 'settled-without-replay' })
    } finally {
      if (marker) this.capabilities.delete(marker)
      if (!state.runId && !state.activeRunMarker && !state.abortController && !state.queue.length) state.isWorking = false
    }
  }

  async close(deadline = Date.now() + 5000): Promise<{ status: 'drained' | 'unresolved'; unresolved: number }> {
    this.stopped = true
    if (this.timer) clearInterval(this.timer)
    this.timer = undefined
    this.capabilities.clear()
    let enumerated = false
    const remaining = async (work: Promise<void>) => {
      let timer: ReturnType<typeof setTimeout> | undefined
      try {
        await Promise.race([work, new Promise<never>((_, reject) => {
          timer = setTimeout(() => reject(new Error('Recovery drain deadline')), Math.max(0, deadline - Date.now() - 100))
        })])
      } finally { if (timer) clearTimeout(timer) }
    }
    try {
      await remaining(this.host.request({ action: 'recovery_quiesce' }).then(() => undefined))
      await remaining(this.refreshResources())
      enumerated = true
      await remaining(this.observeResources('Studio shutdown; recovery decisions revoked'))
      while ((this.resources.size || this.polling || this.runs.size) && Date.now() + 150 < deadline) {
        await remaining(this.refreshResources())
        await remaining(this.observeResources('Studio shutdown; recovery decisions revoked'))
        if (this.resources.size || this.polling || this.runs.size) {
          await new Promise(resolve => setTimeout(resolve, Math.min(20, Math.max(0, deadline - Date.now() - 150))))
        }
      }
    } catch { /* Unsettled responsibility remains in LoopStore for the next owner. */ }
    const unresolved = (enumerated ? this.resources.size : Math.max(1, this.resources.size))
      + this.runs.size + (this.polling ? 1 : 0)
    return { status: unresolved ? 'unresolved' : 'drained', unresolved }
  }
}
