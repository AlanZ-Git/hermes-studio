import { execFileSync } from 'node:child_process'
import { it, expect } from 'vitest'
import { createStudioTestRoot, studioTestEnv } from './studio-receipt-fixture'

const root = createStudioTestRoot('retention-wrapper')

it('validates retention pages before merge and authenticates partial scan receipts', () => {
  expect(process.env.NATIVE_PYTHON).toBeTruthy()
  execFileSync(process.env.NATIVE_PYTHON!, ['-B', 'tests/server/retention_wrapper_fixture.py'], { stdio: 'pipe', env: studioTestEnv(root) })
})
