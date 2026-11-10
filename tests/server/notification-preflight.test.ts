import { execFileSync } from 'node:child_process'
import { mkdtempSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { resolve } from 'node:path'
import { expect, it } from 'vitest'

it('releases context-only preparations before notification revalidation without weakening run ownership', () => {
  const root = mkdtempSync(resolve(tmpdir(), 'notification-preflight-'))
  const output = execFileSync(process.env.NATIVE_PYTHON || 'python3',
    ['-B', 'tests/server/notification_preflight_fixture.py'], {
      encoding: 'utf8',
      env: { PATH: process.env.PATH, SYSTEMROOT: process.env.SYSTEMROOT,
        HOME: root, HERMES_HOME: root, TMPDIR: root, STUDIO_TEST_ROOT: root },
      timeout: 30_000,
    })
  expect(output).toContain('Preflight actual-method fixture source:')
})
