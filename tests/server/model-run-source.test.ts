import { mkdtemp, readFile, stat } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, describe, expect, it, vi } from 'vitest'

vi.mock('../../packages/server/src/modules/studio/public/auth', () => ({
  issueModelRunJwt: vi.fn(async (user: { id: number }) => `synthetic-${user.id}`),
  authenticateUserToken: vi.fn(async (token: string) => token === 'synthetic-1' ? { id: 1 } : null),
}))

const ownerState = { profile: 'alpha', status: 'active', allowed: true }
vi.mock('../../packages/server/src/modules/studio/repositories/session-store', () => ({
  getSession: vi.fn(() => ({ user_id: 1, profile: ownerState.profile })),
}))
vi.mock('../../packages/server/src/modules/studio/repositories/users-store', () => ({
  findUserById: vi.fn(() => ({ id: 1, username: 'synthetic', role: 'admin', status: ownerState.status })),
  userCanAccessProfile: vi.fn(() => ownerState.allowed),
}))

describe('immutable model-run source', () => {
  afterEach(() => vi.unstubAllEnvs())

  it('requires the current session owner and profile access for a distinct compression generation', async () => {
    vi.stubEnv('HERMES_WEB_UI_HOME', await mkdtemp(join(tmpdir(), 'studio-summary-source-')))
    const { createCompressionModelRunSource, closeModelRunSource } = await import('../../packages/server/src/modules/studio/services/chat-run/model-run-source')
    const binding = await createCompressionModelRunSource('session', 'alpha', 'summary-1')
    expect(binding.owner_id).toBe('1')
    expect(binding.run_marker).toBe('summary-1')
    await closeModelRunSource(binding)
    await expect(createCompressionModelRunSource('session', 'beta', 'summary-2')).rejects.toThrow(/owner/)
    ownerState.allowed = false
    await expect(createCompressionModelRunSource('session', 'alpha', 'summary-3')).rejects.toThrow(/owner/)
    ownerState.allowed = true
    ownerState.status = 'disabled'
    await expect(createCompressionModelRunSource('session', 'alpha', 'summary-4')).rejects.toThrow(/owner/)
    ownerState.status = 'active'
    await expect(createCompressionModelRunSource(undefined, 'alpha', 'summary-5')).rejects.toThrow(/owner/)
  })

  it('keeps same-profile runs separate and rejects ended or changed identities', async () => {
    const home = await mkdtemp(join(tmpdir(), 'studio-run-source-'))
    vi.stubEnv('HERMES_WEB_UI_HOME', home)
    const { createModelRunSource, readModelRunSource, closeModelRunSource } = await import('../../packages/server/src/modules/studio/services/chat-run/model-run-source')
    const user = { id: 1, username: 'synthetic', role: 'super_admin' as const }
    const first = await createModelRunSource(user, 'alpha', 'run-1')
    const second = await createModelRunSource(user, 'alpha', 'run-2')
    expect(first.token_path).not.toBe(second.token_path)
    expect((await readModelRunSource(first)).active).toBe(true)
    if (process.platform !== 'win32') expect((await stat(first.token_path)).mode & 0o777).toBe(0o600)
    await expect(readModelRunSource({ ...first, owner_id: '2' })).rejects.toThrow(/binding/)
    await closeModelRunSource(first)
    await expect(readModelRunSource(first)).rejects.toThrow(/inactive/)
    expect((await readModelRunSource(second)).active).toBe(true)
    expect(JSON.parse(await readFile(first.token_path, 'utf8')).active).toBe(false)
    await expect(createModelRunSource(undefined, 'alpha', 'run-3')).rejects.toThrow(/owner/)
  })
})
