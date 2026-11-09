import { execFileSync } from 'node:child_process'
import { it, expect } from 'vitest'
import { createStudioTestRoot, studioTestEnv } from './studio-receipt-fixture'

const root = createStudioTestRoot('recovery-resources')

it('enforces persistent broker-wide recovery reservations and exact lifecycle exit', () => {
  expect(process.env.NATIVE_PYTHON).toBeTruthy()
  execFileSync(process.env.NATIVE_PYTHON!, ['-B', 'tests/server/recovery_resources_fixture.py'], { stdio: 'pipe', env: studioTestEnv(root) })
})
