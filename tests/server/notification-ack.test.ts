import { execFileSync } from 'node:child_process'
import { expect, it } from 'vitest'
import { createStudioTestRoot, studioTestEnv } from './studio-receipt-fixture'

const root = createStudioTestRoot('notification-ack')

it('uses exact source cursor CAS and refuses changed source, target, provenance and schema under the live fence', async () => {
  expect(process.env.NATIVE_PYTHON).toBeTruthy()
  expect(process.env.STUDIO_TEST_ROOT).toBeTruthy()
  const database = await import('../../packages/server/src/modules/studio/infrastructure/database')
  const { initAllStores } = await import('../../packages/server/src/modules/studio/infrastructure/database/init')
  initAllStores()
  const sessions = await import('../../packages/server/src/modules/studio/repositories/session-store')
  sessions.createSession({ id: 'session', profile: 'default', source: 'cli' })
  const output = execFileSync(process.env.NATIVE_PYTHON!, ['-B', 'tests/server/notification_ack_fixture.py'],
    { encoding: 'utf8', env: { ...studioTestEnv(root), HERMES_STUDIO_NOTIFICATION_DB: database.getStoragePath() } })
  expect(output).toContain('Exact source ACK: normal CAS')
  for (const control of ['cursor-2', 'cursor-4', 'cursor-8', 'cursor-10', 'event-inside-ack', 'source-inode-inside-ack',
    'target-inode', 'receipt-digest', 'receipt-source', 'schema-missing', 'schema-incompatible', 'transfer', 'release']) expect(output).toContain(control)
  database.closeDb()
})
