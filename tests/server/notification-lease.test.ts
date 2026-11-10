import { execFileSync } from 'node:child_process'
import { mkdtempSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { resolve } from 'node:path'
import { expect, it } from 'vitest'

it('releases idle notification acquisitions without losing active or reserved session ownership', () => {
  const root = mkdtempSync(resolve(tmpdir(), 'notification-lease-'))
  const output = execFileSync(process.env.NATIVE_PYTHON || 'python3',
    ['-B', 'tests/server/notification_lease_fixture.py'], {
      encoding: 'utf8',
      env: { PATH: process.env.PATH, SYSTEMROOT: process.env.SYSTEMROOT,
        HOME: root, HERMES_HOME: root, TMPDIR: root, STUDIO_TEST_ROOT: root },
      timeout: 30_000,
    })
  expect(output).toContain('Bridge source:')
})
