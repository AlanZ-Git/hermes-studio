import { afterAll, beforeAll, describe, expect, it, vi } from 'vitest'
import Koa from 'koa'
import { mkdir, writeFile } from 'node:fs/promises'
import { reasoningDefault } from '../../packages/server/src/modules/studio/controllers/reasoning-default'

const reads = vi.hoisted(() => vi.fn())
vi.mock('../../packages/server/src/modules/studio/public/profile-config', () => ({ getProfilesBaseDir: reads }))
vi.mock('../../packages/server/src/modules/studio/repositories/users-store', () => ({
  findUserById: (id: number) => ({ id, username: 'fixture', role: Number(id) === 1 ? 'admin' : 'super_admin', status: 'active' }),
  listUserProfiles: () => [{ profile_name: 'A' }],
  userCanAccessProfile: (_: number, profile: string) => profile === 'A',
  touchUserLogin: vi.fn(),
}))
vi.mock('../../packages/server/src/modules/studio/controllers/sessions', () => new Proxy({}, { get: (_t, key) => key === 'then' ? undefined : async (ctx: any) => { ctx.status = 418 }, has: () => true }))
vi.mock('../../packages/server/src/modules/studio/controllers/session-shares', () => ({ synthesizeSpeech: vi.fn(), transcribeSpeech: vi.fn(), models: vi.fn(), contextLength: vi.fn(), setContextLength: vi.fn(), workspaces: vi.fn() }))
vi.mock('../../packages/server/src/modules/studio/controllers/workspace-directories', () => ({ list: vi.fn(), record: vi.fn(), setFavorite: vi.fn() }))

let server: any
let base: string
let token: string
let superToken: string
beforeAll(async () => {
  vi.stubEnv('AUTH_JWT_SECRET', 'reasoning-synthetic-secret')
  const root = '/tmp/reasoning-http-profile-fixture'
  reads.mockReturnValue(root)
  await mkdir(`${root}/profiles/A`, { recursive: true })
  await mkdir(`${root}/profiles/broken`, { recursive: true })
  await mkdir(`${root}/profiles/scalar`, { recursive: true })
  await mkdir(`${root}/profiles/provider`, { recursive: true })
  await mkdir(`${root}/profiles/invalid`, { recursive: true })
  await writeFile(`${root}/profiles/provider/config.yaml`, '{}')
  await writeFile(`${root}/profiles/invalid/config.yaml`, 'agent:\n  reasoning_effort: hgih\n')
  await writeFile(`${root}/config.yaml`, 'agent:\n  reasoning_effort: low\n')
  await writeFile(`${root}/profiles/A/config.yaml`, 'agent:\n  reasoning_effort: high\n  reasoning_overrides:\n    m: medium\n    off: false\nsecret: NEVER_RETURN_THIS\n')
  await writeFile(`${root}/profiles/broken/config.yaml`, 'agent: [unterminated')
  await writeFile(`${root}/profiles/scalar/config.yaml`, '42')
  const { issueUserJwt, requireUserJwt, resolveUserProfile } = await import('../../packages/server/src/modules/studio/middleware/auth')
  token = await issueUserJwt({ id: 1, username: 'fixture', role: 'admin' })
  superToken = await issueUserJwt({ id: 2, username: 'fixture', role: 'super_admin' })
  const { sessionRoutes } = await import('../../packages/server/src/modules/studio/routes/sessions')
  const app = new Koa()
  app.use(requireUserJwt).use(resolveUserProfile).use(sessionRoutes.routes())
  server = app.listen(0, '127.0.0.1')
  await new Promise<void>(resolve => server.once('listening', resolve))
  base = `http://127.0.0.1:${server.address().port}`
})
afterAll(async () => { if (server) await new Promise<void>(resolve => server.close(() => resolve())); vi.unstubAllEnvs() })
async function get(profile: string, model = '', bearer: string | null = token, header?: string) {
  reads.mockClear()
  const response = await fetch(`${base}/api/studio/sessions/reasoning-default?${new URLSearchParams({ profile, model })}`, { headers: { ...(bearer ? { Authorization: `Bearer ${bearer}` } : {}), ...(header ? { 'x-hermes-profile': header } : {}) } })
  return { code: response.status, body: await response.json() }
}
describe('authenticated exact-profile reasoning route', () => {
  it('denies missing authentication before any config read', async () => {
    expect((await get('A', '', null)).code).toBe(401)
    expect(reads).not.toHaveBeenCalled()
    const ctx = { state: {}, query: { profile: 'A' } } as any
    await reasoningDefault(ctx)
    expect(ctx.status).toBe(401)
    expect(reads).not.toHaveBeenCalled()
  })
  it('denies unauthorized exact profile before config read', async () => {
    expect((await get('default')).code).toBe(403)
    expect(reads).not.toHaveBeenCalled()
  })
  it('rejects conflicting header/query identities', async () => {
    expect((await get('default', '', token, 'A')).code).toBe(400)
    expect(reads).not.toHaveBeenCalled()
  })
  it.each([['', 'high'], ['m', 'medium'], ['off', 'none']])('returns only safe leaves for %s', async (model, effort) => {
    expect(await get('A', model)).toEqual({ code: 200, body: { profile: 'A', model, status: 'resolved', effectiveEffort: effort } })
  })
  it.each(['provider', 'invalid'])('keeps Core provider-default semantics for %s distinct from read failure', async profile => {
    expect(await get(profile, 'm', superToken)).toEqual({ code: 200, body: { profile, model: 'm', status: 'provider-default', effectiveEffort: null } })
  })
  it.each(['missing', 'broken', 'scalar'])('reports unknown for %s, never default-file low', async profile => {
    expect(await get(profile, 'm', superToken)).toEqual({ code: 503, body: { profile, model: 'm', status: 'unknown', effectiveEffort: null } })
  })
  it('rejects path traversal before reading', async () => {
    expect((await get('../default', '', superToken)).code).toBe(400)
    expect(reads).not.toHaveBeenCalled()
  })
})
