import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

describe('compression cursor persistence', () => {
  let db: any = null

  beforeEach(async () => {
    vi.resetModules()
    const { DatabaseSync } = await import('node:sqlite')
    db = new DatabaseSync(':memory:')
    vi.doMock('../../packages/server/src/modules/studio/infrastructure/database/index', () => ({
      getDb: () => db,
      isSqliteAvailable: () => true,
      getStoragePath: () => ':memory:',
    }))
    const { initAllHermesTables } = await import('../../packages/server/src/modules/studio/infrastructure/database/schemas')
    initAllHermesTables()
  })

  afterEach(() => {
    db?.close()
    db = null
    vi.doUnmock('../../packages/server/src/modules/studio/infrastructure/database/index')
    vi.resetModules()
  })

  it('rebuilds hidden notification causes after reload and preserves the next user turn', async () => {
    const { createSession, addMessage, getSessionDetail } = await import('../../packages/server/src/modules/studio/repositories/session-store')
    const { buildDbHistory } = await import('../../packages/server/src/modules/studio/services/chat-run/context-history')
    createSession({ id: 'notify', source: 'cli' })
    const messages = [
      { role: 'user', content: 'Original goal' },
      { role: 'user', content: 'Synthetic notification', display_role: 'internal', display_content: '' },
      { role: 'assistant', content: 'Notification consumed' },
      { role: 'user', content: 'Next ordinary question' },
    ]
    for (const [index, message] of messages.entries()) addMessage({ session_id: 'notify', ...message, timestamp: index + 1 })
    const reloaded = getSessionDetail('notify')!
    expect(reloaded.messages.map(message => [message.role, message.content])).toEqual(messages.map(message => [message.role, message.content]))
    expect(reloaded.messages[1]).toMatchObject({ role: 'user', display_role: 'internal', display_content: '' })
    expect(buildDbHistory('notify').map(message => [message.role, message.content])).toEqual(messages.map(message => [message.role, message.content]))
    expect(buildDbHistory('notify', { excludeLastUser: true }).map(message => message.content)).toEqual(messages.slice(0, 3).map(message => message.content))
  })

  it('discovers cold recovery routes without querying any message bodies or previews', async () => {
    const store = await import('../../packages/server/src/modules/studio/repositories/session-store')
    store.createSession({ id: 'recent', source: 'cli', profile: 'research', user_id: 7 })
    store.createSession({ id: 'old', source: 'cli', profile: 'research' })
    store.updateSession('recent', { last_active: 200 })
    store.updateSession('old', { last_active: 10 })
    db.exec('DROP TABLE messages')
    expect(store.listRecoverySessionMetadata(100)).toEqual([
      expect.objectContaining({ id: 'recent', profile: 'research', user_id: '7', last_active: 200 }),
    ])
  })

  it('stores a stable cursor while retaining legacy diagnostics', async () => {
    const { addMessage, createSession } = await import('../../packages/server/src/modules/studio/repositories/session-store')
    const { getCompressionSnapshot, saveCompressionSnapshot } = await import('../../packages/server/src/modules/studio/repositories/compression-snapshot')
    createSession({ id: 'session-1', source: 'cli' })
    const headId = addMessage({ session_id: 'session-1', role: 'user', content: 'head' })!
    addMessage({ session_id: 'session-1', role: 'assistant', content: 'middle' })
    const boundaryId = addMessage({ session_id: 'session-1', role: 'user', content: 'fold me' })!
    addMessage({ session_id: 'session-1', role: 'assistant', content: 'tail' })

    expect(saveCompressionSnapshot('session-1', 'summary', 0, 0, {
      compressedThroughMessageId: boundaryId,
      protectedHeadThroughMessageId: headId,
      expectedHistoryRevision: 0,
    })).toBe(true)
    expect(getCompressionSnapshot('session-1')).toEqual({
      summary: 'summary',
      lastMessageIndex: 2,
      messageCountAtTime: 4,
      compressedThroughMessageId: boundaryId,
      protectedHeadThroughMessageId: headId,
      historyRevision: 0,
      updatedAt: expect.any(Number),
    })
  })

  it('invalidates a snapshot transactionally and rejects an in-flight stale write after clear', async () => {
    const { addMessage, clearSessionMessages, createSession, getSession } = await import('../../packages/server/src/modules/studio/repositories/session-store')
    const { getCompressionSnapshot, saveCompressionSnapshot } = await import('../../packages/server/src/modules/studio/repositories/compression-snapshot')
    createSession({ id: 'session-clear', source: 'cli' })
    const boundaryId = addMessage({ session_id: 'session-clear', role: 'user', content: 'old' })!
    expect(saveCompressionSnapshot('session-clear', 'old summary', 0, 1, {
      compressedThroughMessageId: boundaryId,
      expectedHistoryRevision: 0,
    })).toBe(true)

    expect(clearSessionMessages('session-clear')).toBe(1)
    expect(getCompressionSnapshot('session-clear')).toBeNull()
    expect(getSession('session-clear')?.history_revision).toBe(1)
    const replacementId = addMessage({ session_id: 'session-clear', role: 'user', content: 'new' })!
    expect(saveCompressionSnapshot('session-clear', 'stale result', 0, 1, {
      compressedThroughMessageId: replacementId,
      expectedHistoryRevision: 0,
    })).toBe(false)
    expect(getCompressionSnapshot('session-clear')).toBeNull()
  })

  it('removes snapshots with sessions and remaps cursor IDs for complete branches', async () => {
    const {
      addMessage,
      createBranchedSession,
      createSession,
      deleteSession,
      getSessionContextMessages,
    } = await import('../../packages/server/src/modules/studio/repositories/session-store')
    const {
      copyCompressionSnapshot,
      getCompressionSnapshot,
      saveCompressionSnapshot,
    } = await import('../../packages/server/src/modules/studio/repositories/compression-snapshot')

    createSession({ id: 'parent', source: 'cli' })
    const parentHead = addMessage({ session_id: 'parent', role: 'user', content: 'head', timestamp: 1 })!
    const parentCursor = addMessage({ session_id: 'parent', role: 'assistant', content: 'middle', timestamp: 2 })!
    addMessage({ session_id: 'parent', role: 'user', content: 'tail', timestamp: 3 })
    expect(saveCompressionSnapshot('parent', 'parent summary', 1, 3, {
      compressedThroughMessageId: parentCursor,
      protectedHeadThroughMessageId: parentHead,
      expectedHistoryRevision: 0,
    })).toBe(true)

    createBranchedSession({
      id: 'child',
      parent_session_id: 'parent',
      source: 'cli',
      ended_at: 4,
      last_active: 4,
      messages: [
        { role: 'user', content: 'head', timestamp: 1 },
        { role: 'assistant', content: 'middle', timestamp: 2 },
        { role: 'user', content: 'tail', timestamp: 3 },
      ],
    })
    const childRows = getSessionContextMessages('child')
    expect(getCompressionSnapshot('child')).toEqual(expect.objectContaining({
      summary: 'parent summary',
      compressedThroughMessageId: Number(childRows[1].id),
      protectedHeadThroughMessageId: Number(childRows[0].id),
      historyRevision: 0,
    }))

    createSession({ id: 'partial-child', source: 'cli' })
    addMessage({ session_id: 'partial-child', role: 'user', content: 'head' })
    expect(copyCompressionSnapshot('parent', 'partial-child')).toBe(false)
    expect(getCompressionSnapshot('partial-child')).toBeNull()

    expect(deleteSession('child')).toBe(true)
    expect(getCompressionSnapshot('child')).toBeNull()
  })

  it('keeps legacy rows unchanged when the cursor columns are introduced', async () => {
    db.exec('DROP TABLE chat_compression_snapshots')
    db.exec(`CREATE TABLE chat_compression_snapshots (
      session_id TEXT PRIMARY KEY,
      summary TEXT NOT NULL DEFAULT '',
      last_message_index INTEGER NOT NULL DEFAULT 0,
      message_count_at_time INTEGER NOT NULL DEFAULT 0,
      updated_at INTEGER NOT NULL
    )`)
    db.prepare(`INSERT INTO chat_compression_snapshots
      (session_id, summary, last_message_index, message_count_at_time, updated_at)
      VALUES (?, ?, ?, ?, ?)`
    ).run('legacy', 'legacy summary', 42, 50, 1)

    const { initAllHermesTables } = await import('../../packages/server/src/modules/studio/infrastructure/database/schemas')
    initAllHermesTables()
    const row = db.prepare('SELECT * FROM chat_compression_snapshots WHERE session_id = ?').get('legacy') as any
    expect(row).toEqual(expect.objectContaining({
      summary: 'legacy summary',
      last_message_index: 42,
      message_count_at_time: 50,
      compressed_through_message_id: null,
      protected_head_through_message_id: null,
      history_revision: 0,
    }))
  })

  it('reads only protected head and post-cursor context for a 15,000-row session', async () => {
    const { addMessages, createSession, getSessionContextMessages } = await import('../../packages/server/src/modules/studio/repositories/session-store')
    const { getCompressionSnapshot, saveCompressionSnapshot } = await import('../../packages/server/src/modules/studio/repositories/compression-snapshot')
    const { readCursorSnapshotParts } = await import('../../packages/server/src/modules/studio/services/chat-run/context-history')
    const { buildDbExportHistory } = await import('../../packages/server/src/modules/studio/services/context-compressor/export-compressor')
    createSession({ id: 'large', source: 'cli' })
    addMessages(Array.from({ length: 15_000 }, (_, index) => ({
      session_id: 'large',
      role: index % 5 === 4 ? 'moa' : index % 2 === 0 ? 'user' : 'assistant',
      content: `message-${index}`,
      timestamp: index + 1,
    })))
    const contextRows = getSessionContextMessages('large')
    const headId = Number(contextRows[1].id)
    const boundaryId = Number(contextRows.at(-6)!.id)
    expect(saveCompressionSnapshot('large', 'large summary', 0, 0, {
      compressedThroughMessageId: boundaryId,
      protectedHeadThroughMessageId: headId,
      expectedHistoryRevision: 0,
    })).toBe(true)

    const result = readCursorSnapshotParts('large', getCompressionSnapshot('large'))
    expect(result.status).toBe('usable')
    if (result.status === 'usable') {
      expect(result.parts.head).toHaveLength(2)
      expect(result.parts.newMessages).toHaveLength(5)
      expect(result.parts.newMessages[0].content).toBe(contextRows.at(-5)!.content)
    }
    const exportHistory = buildDbExportHistory('large')
    expect(exportHistory).toHaveLength(7)
    expect(exportHistory[0].content).toBe(contextRows[0].content)
    expect(exportHistory.at(-1)?.content).toBe(contextRows.at(-1)?.content)
  })
})
