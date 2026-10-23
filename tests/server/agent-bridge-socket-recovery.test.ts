import { execFileSync } from 'node:child_process'
import { it } from 'vitest'

it.skipIf(process.platform !== 'linux')('recovers only owned broker endpoints after graceful and abnormal exit', () => {
  execFileSync('python3', ['-B', 'tests/fixtures/bridge-socket-recovery.py'], {
    cwd: process.cwd(), encoding: 'utf8', stdio: 'pipe', timeout: 60_000,
  })
}, 65_000)

it.skipIf(process.platform !== 'linux')('preserves unknown objects across lease races and releases failed leases', () => {
  execFileSync('python3', ['-B', 'tests/fixtures/broker-socket-safety.py'], {
    cwd: process.cwd(), encoding: 'utf8', stdio: 'pipe', timeout: 30_000,
  })
})
