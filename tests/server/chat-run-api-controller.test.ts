import { EventEmitter } from 'events'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

const ioMock = vi.hoisted(() => vi.fn())

vi.mock('socket.io-client', () => ({
  io: ioMock,
}))


function makeSocket() {
  const emitter = new EventEmitter() as EventEmitter & {
    emit: ReturnType<typeof vi.fn>
    disconnect: ReturnType<typeof vi.fn>
    emitNative: (event: string, payload?: unknown) => boolean
  }
  const nativeEmit = EventEmitter.prototype.emit.bind(emitter)
  emitter.emitNative = nativeEmit
  emitter.emit = vi.fn((event: string, payload?: unknown) => {
    if (event === 'run') {
      process.nextTick(() => {
        nativeEmit('run.started', { event: 'run.started', run_id: 'run-1' })
        nativeEmit('message.delta', { event: 'message.delta', run_id: 'run-1', delta: 'hello' })
        nativeEmit('plan.updated', { event: 'plan.updated', run_id: 'turn-1', plan_id: 'mcp:context', revision: 1 })
        nativeEmit('run.completed', { event: 'run.completed', run_id: 'run-1' })
      })
    }
    return true
  }) as any
  emitter.disconnect = vi.fn()
  return emitter
}

describe('correlated current-session loop HTTP receipts', () => {
  afterEach(() => { vi.useRealTimers() })

  async function start(input = '/loop status') {
    const { runOnce } = await import('../../packages/server/src/modules/studio/controllers/chat-run')
    vi.useFakeTimers()
    const socket = makeSocket()
    socket.emit.mockImplementation(() => true)
    ioMock.mockReturnValue(socket)
    const ctx = {
      get: vi.fn(() => ''), state: { profile: { name: 'synthetic-profile' } },
      request: { body: { input, session_id: 'synthetic-session', profile: 'synthetic-profile', request_id: 'synthetic-request', timeout_ms: 100, include_events: true } },
      status: 200, body: undefined as any,
    }
    const pending = runOnce(ctx as any)
    socket.emitNative('connect')
    return { socket, ctx, pending }
  }

  const receipt = { event: 'session.command', session_id: 'synthetic-session', profile: 'synthetic-profile', request_id: 'synthetic-request', command: 'loop', action: 'loop', control: true, terminal: false, ok: true, message: 'synthetic loop receipt' }

  it.each(['/loop status', '/loop 1h synthetic --times 12'])('settles %s from its matching control receipt and cleans up', async input => {
    const { socket, ctx, pending } = await start(input)
    socket.emitNative('session.command', receipt)
    await vi.advanceTimersByTimeAsync(101)
    await pending
    expect(ctx.status).toBe(200)
    expect(ctx.body).toMatchObject({ ok: true, status: 'completed', event: 'session.command', request_id: 'synthetic-request', output: receipt.message, command: receipt })
    expect(ctx.body.run_id).toBeUndefined()
    expect(vi.getTimerCount()).toBe(0)
    expect(socket.eventNames()).toEqual([])
    expect(socket.disconnect).toHaveBeenCalledTimes(1)
  })

  it('ignores wrong identity, non-loop events and the executing caller turn', async () => {
    const { socket, ctx, pending } = await start()
    for (const change of [{ request_id: 'other' }, { request_id: undefined }, { session_id: 'other' }, { profile: 'other' }, { command: 'status' }, { action: 'status' }, { control: false }, { terminal: true }, { ok: undefined }]) socket.emitNative('session.command', { ...receipt, ...change })
    for (const event of ['run.started', 'message.delta', 'run.completed', 'run.failed', 'approval.requested']) socket.emitNative(event, { ...receipt, event, run_id: 'executing-caller', delta: 'must not leak', output: 'caller output' })
    await vi.advanceTimersByTimeAsync(50)
    expect(ctx.body).toBeUndefined()
    expect(socket.disconnect).not.toHaveBeenCalled()
    socket.emitNative('session.command', receipt)
    await vi.advanceTimersByTimeAsync(51)
    await pending
    expect(ctx.status).toBe(200)
    expect(ctx.body.run_id).toBeUndefined()
    expect(ctx.body.output).toBe(receipt.message)
    expect(ctx.body.events).toEqual([receipt])
  })

  it('returns a correlated command failure without terminating the caller', async () => {
    const { socket, ctx, pending } = await start()
    socket.emitNative('session.command', { ...receipt, ok: false, message: 'synthetic command error' })
    await vi.advanceTimersByTimeAsync(101)
    await pending
    expect(ctx.status).toBe(400)
    expect(ctx.body).toMatchObject({ ok: false, status: 'failed', error: 'synthetic command error', request_id: 'synthetic-request' })
    expect(vi.getTimerCount()).toBe(0)
    expect(socket.eventNames()).toEqual([])
  })

  it.each(['connect_error', 'timeout'])('cleans up on %s', async failure => {
    const { socket, ctx, pending } = await start()
    if (failure === 'connect_error') socket.emitNative('connect_error', new Error('synthetic connection failure'))
    await vi.advanceTimersByTimeAsync(101)
    await pending
    expect(ctx.status).toBe(failure === 'timeout' ? 504 : 503)
    expect(vi.getTimerCount()).toBe(0)
    expect(socket.eventNames()).toEqual([])
    expect(socket.disconnect).toHaveBeenCalledTimes(1)
  })

  it('does not settle an ordinary model turn from a loop receipt', async () => {
    const { socket, ctx, pending } = await start('ordinary synthetic model turn')
    socket.emitNative('session.command', receipt)
    await vi.advanceTimersByTimeAsync(50)
    expect(ctx.body).toBeUndefined()
    socket.emitNative('run.completed', { run_id: 'ordinary-run', output: 'ordinary output' })
    await pending
    expect(ctx.body).toMatchObject({ ok: true, run_id: 'ordinary-run', output: 'ordinary output' })
  })
})

describe('chat-run HTTP API controller', () => {
  beforeEach(() => {
    vi.clearAllMocks()
  })

  it('rejects a coding-agent run without an explicit execution identity and explains the correct request', async () => {
    const socket = makeSocket()
    ioMock.mockReturnValue(socket)

    const { runOnce } = await import('../../packages/server/src/modules/studio/controllers/chat-run')
    const ctx = {
      get: vi.fn(() => ''),
      state: { profile: { name: 'default' } },
      request: {
        body: {
          input: 'continue work',
          source: 'coding_agent',
          coding_agent_id: 'codex',
          mode: 'scoped',
        },
      },
      status: 200,
      body: undefined as any,
    }

    await runOnce(ctx as any)

    expect(ctx.status).toBe(400)
    expect(ctx.body).toMatchObject({
      ok: false,
      error: expect.stringContaining('provider, model, and apiMode are required'),
    })
    expect(ctx.body.error).toContain('"apiMode":"codex_responses"')
    expect(socket.emit).not.toHaveBeenCalledWith('run', expect.anything())
  })

  it.each([
    { provider: 'custom:corp-codex' },
    { model: 'gpt-5.6-terra' },
    { apiMode: 'codex_responses' },
    { provider: 'custom:corp-codex', model: 'gpt-5.6-terra' },
    { provider: 'custom:corp-codex', apiMode: 'codex_responses' },
    { model: 'gpt-5.6-terra', apiMode: 'codex_responses' },
  ])('rejects a partial execution identity before starting a run: %j', async (identity) => {
    const socket = makeSocket()
    ioMock.mockReturnValue(socket)

    const { runOnce } = await import('../../packages/server/src/modules/studio/controllers/chat-run')
    const ctx = {
      get: vi.fn(() => ''),
      state: { profile: { name: 'default' } },
      request: { body: { input: 'continue work', source: 'coding_agent', ...identity } },
      status: 200,
      body: undefined as any,
    }

    await runOnce(ctx as any)

    expect(ctx.status).toBe(400)
    expect(ctx.body).toMatchObject({
      ok: false,
      error: expect.stringContaining('provider, model, and apiMode are required'),
    })
    expect(ctx.body.error).toContain('"apiMode":"codex_responses"')
    expect(socket.emit).not.toHaveBeenCalledWith('run', expect.anything())
  })

  it('rejects an unsupported API mode and names the OpenAI Responses value', async () => {
    const socket = makeSocket()
    ioMock.mockReturnValue(socket)

    const { runOnce } = await import('../../packages/server/src/modules/studio/controllers/chat-run')
    const ctx = {
      get: vi.fn(() => ''),
      state: { profile: { name: 'default' } },
      request: { body: {
        input: 'continue work',
        source: 'coding_agent',
        provider: 'custom:explicit',
        model: 'explicit-model',
        apiMode: 'openai_responses',
      } },
      status: 200,
      body: undefined as any,
    }

    await runOnce(ctx as any)

    expect(ctx.status).toBe(400)
    expect(ctx.body).toMatchObject({
      ok: false,
      error: expect.stringContaining('For OpenAI Responses use "apiMode":"codex_responses"'),
    })
    expect(socket.emit).not.toHaveBeenCalledWith('run', expect.anything())
  })

  it('preserves a complete explicit execution identity without reading Profile defaults', async () => {
    const socket = makeSocket()
    ioMock.mockReturnValue(socket)

    const { runOnce } = await import('../../packages/server/src/modules/studio/controllers/chat-run')
    const ctx = {
      get: vi.fn(() => ''),
      state: { profile: { name: 'default' } },
      request: { body: {
        input: 'continue work',
        source: 'coding_agent',
        provider: 'custom:explicit',
        model: 'explicit-model',
        apiMode: 'anthropic_messages',
        apiKey: 'must-not-leave-http-boundary',
        baseUrl: 'https://must-not-leave.example/v1',
      } },
      status: 200,
      body: undefined as any,
    }

    const pending = runOnce(ctx as any)
    await new Promise(resolve => setImmediate(resolve))
    socket.emitNative('connect')
    await pending

    expect(socket.emit).toHaveBeenCalledWith('run', expect.objectContaining({
      provider: 'custom:explicit',
      model: 'explicit-model',
      apiMode: 'anthropic_messages',
    }))
    const emittedPayload = socket.emit.mock.calls.find(call => call[0] === 'run')?.[1]
    expect(emittedPayload).not.toHaveProperty('apiKey')
    expect(emittedPayload).not.toHaveProperty('baseUrl')
  })

  it('runs chat-run through Socket.IO and returns a completed HTTP response', async () => {
    const socket = makeSocket()
    ioMock.mockReturnValue(socket)

    const { runOnce } = await import('../../packages/server/src/modules/studio/controllers/chat-run')
    const ctx = {
      get: vi.fn((name: string) => name.toLowerCase() === 'authorization' ? 'Bearer token-1' : ''),
      state: { profile: { name: 'default' } },
      request: {
        body: {
          session_id: 'session-1',
          input: 'hello',
          include_events: true,
        },
      },
      status: 200,
      body: undefined as any,
    }

    const pending = runOnce(ctx as any)
    socket.emitNative('connect')
    await pending

    expect(ioMock).toHaveBeenCalledWith(expect.stringContaining('/chat-run'), expect.objectContaining({
      auth: { token: 'token-1' },
      query: { profile: 'default' },
    }))
    expect(socket.emit).toHaveBeenCalledWith('run', expect.objectContaining({
      session_id: 'session-1',
      input: 'hello',
      profile: 'default',
    }))
    expect(ctx.status).toBe(200)
    expect(ctx.body).toMatchObject({
      ok: true,
      status: 'completed',
      session_id: 'session-1',
      run_id: 'run-1',
      output: 'hello',
    })
    expect(ctx.body.events).toHaveLength(4)
    expect(ctx.body.events).toContainEqual(expect.objectContaining({ event: 'plan.updated', plan_id: 'mcp:context', revision: 1 }))
  })

  it('generates a session id when none is provided', async () => {
    const socket = makeSocket()
    ioMock.mockReturnValue(socket)

    const { runOnce } = await import('../../packages/server/src/modules/studio/controllers/chat-run')
    const ctx = {
      get: vi.fn((name: string) => name.toLowerCase() === 'authorization' ? 'Bearer token-1' : ''),
      state: { profile: { name: 'default' } },
      request: {
        body: {
          source: 'cli',
          input: 'start a new chat',
        },
      },
      status: 200,
      body: undefined as any,
    }

    const pending = runOnce(ctx as any)
    socket.emitNative('connect')
    await pending

    const emittedPayload = socket.emit.mock.calls.find(call => call[0] === 'run')?.[1] as Record<string, unknown>
    expect(emittedPayload.session_id).toEqual(expect.stringMatching(/^[0-9a-f-]{36}$/))
    expect(emittedPayload).toMatchObject({
      source: 'cli',
      input: 'start a new chat',
      profile: 'default',
    })
    expect(ctx.status).toBe(200)
    expect(ctx.body).toMatchObject({
      ok: true,
      status: 'completed',
      session_id: emittedPayload.session_id,
    })
  })

  it('generates a session id for global-agent runs when none is provided', async () => {
    const socket = makeSocket()
    ioMock.mockReturnValue(socket)

    const { runOnce } = await import('../../packages/server/src/modules/studio/controllers/chat-run')
    const ctx = {
      get: vi.fn((name: string) => name.toLowerCase() === 'authorization' ? 'Bearer token-1' : ''),
      state: { profile: { name: 'default' } },
      request: {
        body: {
          source: 'global_agent',
          input: 'start a global run',
        },
      },
      status: 200,
      body: undefined as any,
    }

    const pending = runOnce(ctx as any)
    socket.emitNative('connect')
    await pending

    const emittedPayload = socket.emit.mock.calls.find(call => call[0] === 'run')?.[1] as Record<string, unknown>
    expect(emittedPayload.session_id).toEqual(expect.stringMatching(/^[0-9a-f-]{36}$/))
    expect(emittedPayload).toMatchObject({
      source: 'global_agent',
      input: 'start a global run',
      profile: 'default',
    })
    expect(ctx.body).toMatchObject({
      ok: true,
      status: 'completed',
      session_id: emittedPayload.session_id,
    })
  })
})
