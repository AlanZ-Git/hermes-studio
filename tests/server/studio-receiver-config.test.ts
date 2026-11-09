import { beforeAll, afterAll, expect, it, vi } from 'vitest'
import { createServer } from 'node:net'
import { realpathSync, statSync } from 'node:fs'
import { execFileSync } from 'node:child_process'
import { createStudioTestRoot, studioTestEnv } from './studio-receipt-fixture'

const root = createStudioTestRoot('receiver-config')

vi.mock('child_process', async original => ({ ...await original<any>(), spawn: vi.fn(() => { throw new Error('Unexpected process launch') }) }))
let database: any, managerModule: any
beforeAll(async () => {
  database = await import('../../packages/server/src/modules/studio/infrastructure/database')
  const { initAllStores } = await import('../../packages/server/src/modules/studio/infrastructure/database/init')
  initAllStores()
  managerModule = await import('../../packages/server/src/modules/hermes/services/bridge/manager')
})
afterAll(() => { vi.unstubAllEnvs(); database?.closeDb() })
it('passes only the internal repository database in the launch environment', () => {
  vi.stubEnv('HERMES_STUDIO_NOTIFICATION_DB', '/synthetic/arbitrary-rpc-path.db')
  const env = managerModule.buildAgentBridgeProcessEnv('tcp://127.0.0.1:1', process.env.STUDIO_TEST_ROOT, undefined)
  expect(env.HERMES_STUDIO_NOTIFICATION_DB).toBe(realpathSync(database.getStoragePath()))
  expect(env.HERMES_WORKER_SESSION_ID).toBe(process.env.HERMES_WORKER_SESSION_ID)
})
it.each(['missing', 'path', 'inode', 'matching'])('checks reused broker configuration before attach/readiness: %s', async variant => {
  const path = realpathSync(database.getStoragePath()), stat = statSync(path)
  const receiver = variant === 'missing' ? undefined : variant === 'path' ? [path + '.other', stat.dev, stat.ino]
    : [path, stat.dev, stat.ino + (variant === 'inode' ? 1 : 0)]
  const actions: string[] = []
  const server = createServer(socket => socket.once('data', data => {
    const request = JSON.parse(String(data))
    actions.push(request.action)
    socket.end(JSON.stringify({ ok: true, pong: true, notification_receiver: receiver }) + '\n')
  }))
  await new Promise<void>(resolve => server.listen(0, '127.0.0.1', resolve))
  const endpoint = `tcp://127.0.0.1:${(server.address() as any).port}`
  const manager = new managerModule.AgentBridgeManager({ endpoint })
  try {
    if (variant === 'matching') {
      await manager.start()
      expect(manager.getRuntimeState()).toMatchObject({ ready: true, attached: true })
      expect(await manager.checkReadiness()).toMatchObject({ ready: true, reachable: true })
      await manager.stop()
      expect(actions).toEqual(['ping', 'ping', 'shutdown'])
    } else {
      await expect(manager.start()).rejects.toThrow('receiver configuration mismatch')
      await expect(manager.ensureReady()).rejects.toThrow('receiver configuration mismatch')
      expect(manager.getRuntimeState()).toMatchObject({ ready: false, attached: false, restartScheduled: false })
      expect(actions).toEqual(['ping', 'ping'])
    }
  } finally { await new Promise<void>(resolve => server.close(() => resolve())) }
})
it('checks the actual existing Python worker receiver before forwarding requests', () => {
  const output = execFileSync(process.env.NATIVE_PYTHON!, ['-B', 'tests/server/studio_receiver_worker_fixture.py'], {
    encoding: 'utf8', env: { ...studioTestEnv(root), HERMES_STUDIO_NOTIFICATION_DB: database.getStoragePath() },
  })
  expect(output).toContain('actual worker matched; reused receiver mismatch refused; owned child exited')
})
