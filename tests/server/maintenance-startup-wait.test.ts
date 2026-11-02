import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { mkdtempSync, writeFileSync, existsSync, readFileSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'

vi.mock('node:http', () => ({
  createServer: () => {
    const server = {
      on: vi.fn(),
      once: vi.fn(),
      listen: (_port: number, _host: string, done: () => void) => done(),
      close: (done: () => void) => done(),
    }
    return server
  },
}))
vi.mock('../../packages/server/src/bootstrap/maintenance-state', () => ({ prepareState: vi.fn().mockResolvedValue(undefined) }))
vi.mock('../../packages/server/src/modules/studio/infrastructure/database/init', () => ({ initAllStores: vi.fn() }))
vi.mock('../../packages/server/src/modules/studio/infrastructure/database/index', () => ({ withMaintenanceDatabase: vi.fn() }))
vi.mock('../../packages/server/src/bootstrap/http', () => ({}))

let root: string
const identity = { attempt: 'synthetic', nonce: 'nonce' }
function store(name: string, value: unknown) {
  writeFileSync(join(root, name), JSON.stringify(value), { mode: 0o600 })
}

beforeEach(() => {
  vi.useFakeTimers()
  root = mkdtempSync(join(tmpdir(), 'maintenance-wait-'))
  store('request.json', { ...identity, version: 1, pair: 'fixture', write_sets: { webui: [] }, ports: { webui: 0 } })
  vi.stubEnv('HERMES_CUTOVER_ATTEMPT', root)
})
afterEach(() => {
  vi.useRealTimers()
  vi.unstubAllEnvs()
  rmSync(root, { recursive: true, force: true })
})

describe('maintenance release wait', () => {
  it('keeps waiting past five minutes and activates only after a matching durable fence', async () => {
    const { startWithMaintenanceGate } = await import('../../packages/server/src/bootstrap/maintenance')
    let settled = false
    const result = startWithMaintenanceGate().finally(() => { settled = true })
    const check = expect(result).resolves.toBeUndefined()
    await vi.advanceTimersByTimeAsync(300_100)
    expect(settled).toBe(false)
    const ready = JSON.parse(readFileSync(join(root, 'webui.ready.json'), 'utf8'))
    expect(ready).toMatchObject({ ...identity, role: 'webui', business_started: false })
    expect(existsSync(join(root, 'webui.activated.json'))).toBe(false)
    store('release.json', identity)
    store('business-fence.json', { ...identity, status: 'BUSINESS_MAY_HAVE_STARTED' })
    await vi.advanceTimersByTimeAsync(50)
    await check
    expect(JSON.parse(readFileSync(join(root, 'webui.activated.json'), 'utf8'))).toMatchObject(identity)
  })

  it.each([{ nonce: 'foreign' }, { attempt: 'foreign' }, { status: 'NOT_RELEASED' }])('rejects foreign or unreleased fence %j', async mismatch => {
    const { startWithMaintenanceGate } = await import('../../packages/server/src/bootstrap/maintenance')
    const result = startWithMaintenanceGate()
    const check = expect(result).rejects.toThrow('release without durable fence')
    await vi.advanceTimersByTimeAsync(100)
    store('release.json', identity)
    store('business-fence.json', { ...identity, status: 'BUSINESS_MAY_HAVE_STARTED', ...mismatch })
    await vi.advanceTimersByTimeAsync(50)
    await check
    expect(existsSync(join(root, 'webui.activated.json'))).toBe(false)
  })
})
