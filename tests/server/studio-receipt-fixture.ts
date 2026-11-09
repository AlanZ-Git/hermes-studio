import { execFileSync } from 'node:child_process'
import { mkdirSync, mkdtempSync, realpathSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { isAbsolute, relative, resolve } from 'node:path'

// NATIVE_PYTHON must be a native interpreter; PYTHONPATH must expose Studio's bridge
// modules, Core hermes_cli.active_sessions owner API and agent.recovery_lifecycle.
export function createStudioTestRoot(suite: string) {
  const root = realpathSync(mkdtempSync(resolve(tmpdir(), `studio-${suite}-`)))
  Object.assign(process.env, studioTestEnv(root))
  return root
}

export function studioTestEnv(root: string) {
  root = realpathSync(root)
  const home = resolve(root, 'home'), coreHome = resolve(root, 'core-home'), receiver = resolve(root, 'receiver')
  for (const dir of [home, coreHome, receiver]) mkdirSync(dir, { recursive: true })
  return { ...process.env, STUDIO_TEST_ONLY: '1', STUDIO_TEST_ROOT: root, HOME: home,
    HERMES_HOME: coreHome, HERMES_AGENT_BRIDGE_BASE_HOME: coreHome,
    HERMES_WEB_UI_TEST_DB_DIR: receiver, HERMES_STUDIO_NOTIFICATION_DB: '' }
}

export function seedStudioReceipt(item: any, targetPath: string) {
  if (process.env.STUDIO_TEST_ONLY !== '1' || !process.env.STUDIO_TEST_ROOT) throw new Error('Explicit test-only root required')
  const root = realpathSync(process.env.STUDIO_TEST_ROOT)
  const target = relative(root, realpathSync(targetPath))
  if (target === '..' || target.startsWith('../') || isAbsolute(target)) throw new Error('Receipt target must be inside the test root')
  return execFileSync(process.env.NATIVE_PYTHON!, ['-B', 'tests/server/studio_notification_fixture.py', 'seed'], {
    encoding: 'utf8', input: JSON.stringify(item),
    env: { ...studioTestEnv(root), HERMES_STUDIO_NOTIFICATION_DB: targetPath },
  })
}
