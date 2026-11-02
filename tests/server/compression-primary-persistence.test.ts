import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { DatabaseSync } from 'node:sqlite'

const runtime = vi.hoisted(() => ({ run: vi.fn(), bridge: vi.fn(), ack: vi.fn() }))
vi.mock('../../packages/server/src/modules/studio/public/chat-agent-runtime', async importOriginal => ({
  ...await importOriginal<typeof import('../../packages/server/src/modules/studio/public/chat-agent-runtime')>(),
  getChatEkkoAgent: () => ({ runIsolated: runtime.run }),
  resolveChatEkkoProviderRuntimeConfig: async () => ({ provider: 'fixture', baseUrl: 'https://fixture.invalid', apiKey: 'synthetic' }),
  resolveChatEkkoModelProviderConfigs: () => ({ providerConfig: { id: 'fixture', defaultModel: 'synthetic' } }),
  createChatEkkoModelClient: () => ({ capabilities: { streaming: true } }),
  createChatEkkoAuthorizedProviderFetch: () => vi.fn(),
  createPrimaryAgentBridge: () => ({ request: runtime.bridge, ackResult: runtime.ack }),
}))
vi.mock('../../packages/server/src/modules/studio/services/chat-run/model-run-source', () => ({
  createCompressionModelRunSource: async () => ({ version: 1, profile: 'fixture' }),
  closeModelRunSource: async () => undefined,
}))
vi.mock('../../packages/server/src/modules/studio/public/profile-config', () => ({
  readConfigYamlForProfile: async () => ({ compression: { threshold: 0.5, protect_first_n: 0, protect_last_n: 0 } }),
}))
vi.mock('../../packages/server/src/modules/studio/public/provider-runtime', () => ({ getModelContextLength: () => 1000 }))
vi.mock('../../packages/server/src/modules/studio/services/chat-run/usage', () => ({
  calcAndUpdateUsage: async () => ({ inputTokens: 10000, outputTokens: 0 }),
  estimateUsageTokensFromMessages: () => ({ inputTokens: 10000, outputTokens: 0 }),
  updateMessageContextTokenUsage: (_sid: string, state: any, _emit: unknown, tokens: number) => {
    state.contextTokens = tokens
    return tokens
  },
}))
vi.mock('../../packages/server/src/modules/studio/public/logging', () => ({
  logger: { info: vi.fn(), warn: vi.fn(), error: vi.fn(), debug: vi.fn() },
}))

describe('primary compression to private SQLite', () => {
  let db: DatabaseSync
  let directory: string

  beforeEach(async () => {
    vi.resetModules()
    vi.resetAllMocks()
    directory = mkdtempSync(join(tmpdir(), 'compression-222-'))
    db = new DatabaseSync(join(directory, 'private.sqlite'))
    vi.doMock('../../packages/server/src/modules/studio/infrastructure/database/index', () => ({
      getDb: () => db, isSqliteAvailable: () => true, getStoragePath: () => directory,
    }))
    const { initAllHermesTables } = await import('../../packages/server/src/modules/studio/infrastructure/database/schemas')
    initAllHermesTables()
    runtime.bridge.mockImplementation(async request => {
      if (request.action === 'destroy') return {}
      throw new Error('Synthetic backup unavailable')
    })
  })

  afterEach(() => {
    db.close()
    rmSync(directory, { recursive: true, force: true })
    vi.doUnmock('../../packages/server/src/modules/studio/infrastructure/database/index')
    vi.resetModules()
  })

  it.each([false, true])('preserves history and snapshot on failure, then saves real recovery (incremental=%s)', async incremental => {
    const store = await import('../../packages/server/src/modules/studio/repositories/session-store')
    const snapshots = await import('../../packages/server/src/modules/studio/repositories/compression-snapshot')
    const { ChatContextCompressor } = await import('../../packages/server/src/modules/studio/services/context-compressor')
    store.createSession({ id: 'synthetic-session', source: 'cli' })
    const first = store.addMessage({ session_id: 'synthetic-session', role: 'user', content: 'Synthetic first turn' })!
    if (incremental) snapshots.saveCompressionSnapshot('synthetic-session', 'Existing valid summary', 0, 1, {
      compressedThroughMessageId: first, expectedHistoryRevision: 0,
    })
    store.addMessage({ session_id: 'synthetic-session', role: 'assistant', content: 'Synthetic answer' })
    const boundary = store.addMessage({ session_id: 'synthetic-session', role: 'user', content: 'Synthetic follow-up' })!
    const originalHistory = store.getSessionContextMessages('synthetic-session')
    const originalSnapshot = snapshots.getCompressionSnapshot('synthetic-session')
    const compressor = new ChatContextCompressor({ config: { headMessageCount: 0, tailMessageCount: 0 } })
    const messages = originalHistory.map(row => ({ role: row.role, content: row.content, cursorId: Number(row.id) }))
    const options = { model: 'synthetic', provider: 'fixture', profile: 'fixture', historyRevision: 0, allowHermesFallback: false }

    for (const content of ['API call failed after 3 retries: Request timed out.', 'API call failed: connection reset', ' \n ']) {
      runtime.run.mockResolvedValueOnce({ output: { role: 'assistant', content, finishReason: 'stop' } })
      await expect(compressor.compress(messages, '', undefined, 'synthetic-session', options)).rejects.toThrow(/summari.*response/i)
      expect(snapshots.getCompressionSnapshot('synthetic-session')).toEqual(originalSnapshot)
      expect(store.getSessionContextMessages('synthetic-session')).toEqual(originalHistory)
    }

    runtime.run.mockResolvedValueOnce({ output: { role: 'assistant', content: 'Valid summary: the previous API error was resolved.', finishReason: 'stop' } })
    const recovered = await compressor.compress(messages, '', undefined, 'synthetic-session', options)
    expect(recovered.meta.llmCompressed).toBe(true)
    expect(snapshots.getCompressionSnapshot('synthetic-session')).toMatchObject({
      summary: 'Valid summary: the previous API error was resolved.', compressedThroughMessageId: boundary, historyRevision: 0,
    })
    expect(store.getSessionContextMessages('synthetic-session')).toEqual(originalHistory)
    expect(runtime.bridge).not.toHaveBeenCalled()
  })

  it('emits and retains automatic failure metadata from the real compressor, then replaces it on recovery', async () => {
    const store = await import('../../packages/server/src/modules/studio/repositories/session-store')
    const snapshots = await import('../../packages/server/src/modules/studio/repositories/compression-snapshot')
    const { buildCompressedHistory } = await import('../../packages/server/src/modules/studio/services/chat-run/compression')
    store.createSession({ id: 'automatic', source: 'cli', profile: 'fixture', model: 'synthetic', provider: 'fixture' })
    for (let i = 0; i < 12; i++) store.addMessage({ session_id: 'automatic', role: i % 2 ? 'assistant' : 'user', content: `Synthetic turn ${i}` })
    const original = store.getSessionContextMessages('automatic')
    const events: Array<{ event: string; data: any }> = []
    const emit = (event: string, data: any) => { events.push({ event, data }) }
    const state = new Map()
    const options = { model: 'synthetic', provider: 'fixture', allowHermesFallback: false }
    runtime.run.mockResolvedValueOnce({ output: { role: 'assistant', content: 'API call failed: synthetic timeout', finishReason: 'stop' } })
    const retained = await buildCompressedHistory('automatic', 'fixture', '', undefined, emit, state, options)
    const logger = (await import('../../packages/server/src/modules/studio/public/logging')).logger
    expect(retained.length, vi.mocked(logger.warn).mock.calls.map(call => call.map(String).join(' ')).join('\n')).toBeGreaterThan(0)
    expect(events.find(entry => entry.event === 'compression.completed')?.data).toMatchObject({ compressed: false, error: expect.stringMatching(/provider failure text/) })
    expect(state.get('automatic').events.find((entry: any) => entry.event === 'compression.completed')?.data.error).toMatch(/provider failure text/)
    expect(snapshots.getCompressionSnapshot('automatic')).toBeNull()
    expect(store.getSessionContextMessages('automatic')).toEqual(original)

    events.length = 0
    runtime.run.mockResolvedValueOnce({ output: { role: 'assistant', content: 'Healthy automatic summary', finishReason: 'stop' } })
    await buildCompressedHistory('automatic', 'fixture', '', undefined, emit, state, options)
    expect(events.find(entry => entry.event === 'compression.completed')?.data).toMatchObject({ compressed: true, llmCompressed: true })
    expect(state.get('automatic').events.find((entry: any) => entry.event === 'compression.completed')?.data.error).toBeUndefined()
    expect(snapshots.getCompressionSnapshot('automatic')?.summary).toBe('Healthy automatic summary')
  })

  it('persists a healthy Hermes backup after explicit primary failure and ACKs after the SQLite write', async () => {
    const store = await import('../../packages/server/src/modules/studio/repositories/session-store')
    const snapshots = await import('../../packages/server/src/modules/studio/repositories/compression-snapshot')
    const { ChatContextCompressor } = await import('../../packages/server/src/modules/studio/services/context-compressor')
    store.createSession({ id: 'backup', source: 'cli' })
    const cursorId = store.addMessage({ session_id: 'backup', role: 'user', content: 'Synthetic turn' })!
    runtime.run.mockResolvedValue({ output: { role: 'assistant', content: 'API call failed: timeout', finishReason: 'stop' } })
    runtime.bridge.mockImplementation(async request => request.action === 'destroy' ? {} : {
      status: 'complete', run_id: 'synthetic-backup', deltas: [], events: [], result: { final_response: 'Healthy backup summary' },
    })
    runtime.ack.mockImplementation(async () => {
      expect(snapshots.getCompressionSnapshot('backup')?.summary).toBe('Healthy backup summary')
      return true
    })
    await new ChatContextCompressor({ config: { headMessageCount: 0, tailMessageCount: 0 } }).compress([
      { role: 'user', content: 'Synthetic turn', cursorId },
    ], '', undefined, 'backup', { model: 'synthetic', provider: 'fixture', profile: 'fixture' })
    expect(runtime.ack).toHaveBeenCalledWith('synthetic-backup', 0, 0)
    expect(snapshots.getCompressionSnapshot('backup')?.compressedThroughMessageId).toBe(cursorId)
  })
})
