import { execFileSync } from 'child_process'
import { mkdirSync, mkdtempSync, symlinkSync, writeFileSync } from 'fs'
import { tmpdir } from 'os'
import { join, resolve } from 'path'
import { afterEach, describe, expect, it, vi } from 'vitest'

const state = vi.hoisted(() => ({ appHome: '' }))
vi.mock('../../packages/server/src/modules/studio/public/config', () => ({
  config: { get appHome() { return state.appHome } },
}))

function fixture() {
  const base = mkdtempSync(join(tmpdir(), 'explicit-binding-'))
  state.appHome = join(base, 'studio')
  const root = join(base, 'release core', 'source')
  const runtime = join(base, 'release core', 'runtime')
  mkdirSync(join(root, 'hermes_cli'), { recursive: true })
  writeFileSync(join(root, 'run_agent.py'), 'raise RuntimeError("must not import run_agent")\n')
  writeFileSync(join(root, 'cli.py'), '')
  writeFileSync(join(root, 'hermes_constants.py'), '__version__ = "0.99.1"\n')
  writeFileSync(join(root, 'hermes_cli', '__init__.py'), '')
  writeFileSync(join(root, 'hermes_cli', 'main.py'), 'import json, sys, hermes_constants\nprint(json.dumps({"version": hermes_constants.__version__, "python": sys.executable, "args": sys.argv[1:]}))\n')
  execFileSync('python3', ['-m', 'venv', '--without-pip', runtime], { timeout: 15000 })
  const python = join(runtime, 'bin', 'python3')
  const site = execFileSync(python, ['-I', '-c', 'import sysconfig; print(sysconfig.get_path("purelib"))'], { encoding: 'utf8' }).trim()
  writeFileSync(join(site, 'core.pth'), `${root}\n`)
  const home = join(base, 'home')
  mkdirSync(join(home, '.local', 'bin'), { recursive: true })
  const oldRuntime = join(base, 'old-runtime')
  execFileSync('python3', ['-m', 'venv', '--without-pip', oldRuntime], { timeout: 15000 })
  const oldPython = join(oldRuntime, 'bin', 'python3')
  const oldSite = execFileSync(oldPython, ['-I', '-c', 'import sysconfig; print(sysconfig.get_path("purelib"))'], { encoding: 'utf8' }).trim()
  writeFileSync(join(oldSite, 'hermes_constants.py'), '__version__ = "0.20.0"\n')
  mkdirSync(join(oldSite, 'hermes_cli'))
  writeFileSync(join(oldSite, 'hermes_cli', '__init__.py'), '__version__ = "0.20.0"\n')
  writeFileSync(join(oldSite, 'hermes_cli', 'main.py'), '')
  writeFileSync(join(home, '.local', 'bin', 'hermes'), `#!${oldPython}\n`, { mode: 0o755 })
  const env: NodeJS.ProcessEnv = {
    HOME: home, HERMES_HOME: join(home, '.hermes'), HERMES_WEB_UI_HOME: state.appHome,
    PATH: '/usr/bin:/bin', HERMES_AGENT_ROOT: root, HERMES_AGENT_BRIDGE_PYTHON: python,
  }
  return { base, root, runtime, python, site, oldSite, env }
}

afterEach(() => { vi.unstubAllEnvs() })

describe.skipIf(process.platform === 'win32')('explicit Hermes binding', () => {
  it('probes the selected CLI version directly through the pair, not launcher inference or PYTHONPATH', async () => {
    const f = fixture()
    const { configurePreferredHermesRuntime } = await import('../../packages/server/src/modules/hermes/services/runtime/selection')
    await configurePreferredHermesRuntime(f.env)
    f.env.PYTHONPATH = f.oldSite
    const { probeHermesCliVersion } = await import('../../packages/server/src/modules/hermes/services/runtime/discovery')
    expect(await probeHermesCliVersion(f.env.HERMES_BIN!, f.env)).toEqual({ version: '0.99.1', error: '' })
  })

  it('accepts and displays unknown version_info when the actual Core paths and Python match', async () => {
    const f = fixture()
    writeFileSync(join(f.root, 'hermes_cli', 'version_info.py'), 'from types import SimpleNamespace\ndef get_version_info(): return SimpleNamespace(display_version="unknown")\n')
    const { configurePreferredHermesRuntime } = await import('../../packages/server/src/modules/hermes/services/runtime/selection')
    const selected = await configurePreferredHermesRuntime(f.env)
    expect(selected).toMatchObject({ source: 'explicit-binding', agentRoot: f.root, pythonPath: f.python, version: 'unknown' })
    const { probeHermesCliVersion } = await import('../../packages/server/src/modules/hermes/services/runtime/discovery')
    expect(await probeHermesCliVersion(f.env.HERMES_BIN!, f.env)).toEqual({ version: 'unknown', error: '' })
    for (const [key, value] of Object.entries(f.env)) vi.stubEnv(key, value)
    const { getRuntimeVersionStatus } = await import('../../packages/server/src/modules/hermes/services/runtime/version-manager')
    const status = await getRuntimeVersionStatus({ includeRemote: false })
    expect(status.hermes).toMatchObject({ source: 'user-cli', agentVersion: 'unknown', pythonPath: f.python, agentRoot: f.root })
    expect(status.hermes.cliInstallations).toEqual([{ source: 'user-cli', path: f.python, version: 'unknown', selected: true }])
    const { getAgentStatusSnapshot } = await import('../../packages/server/src/modules/studio/public/agent-status-registry')
    expect(getAgentStatusSnapshot().agents.find(agent => agent.id === 'hermes')).toMatchObject({ installed: true, version: 'unknown', path: f.python })
  })

  it('reports the bound Core in inventory rather than the older user launcher', async () => {
    const f = fixture()
    const { configurePreferredHermesRuntime } = await import('../../packages/server/src/modules/hermes/services/runtime/selection')
    await configurePreferredHermesRuntime(f.env)
    for (const [key, value] of Object.entries(f.env)) vi.stubEnv(key, value)
    const { getRuntimeVersionStatus } = await import('../../packages/server/src/modules/hermes/services/runtime/version-manager')
    const status = await getRuntimeVersionStatus({ includeRemote: false })
    expect(status.hermes).toMatchObject({ source: 'user-cli', agentVersion: '0.99.1', pythonPath: f.python, agentRoot: f.root })
    expect(status.hermes.cliInstallations).toEqual([{ source: 'user-cli', path: f.python, version: '0.99.1', selected: true }])
    const { getAgentStatusSnapshot } = await import('../../packages/server/src/modules/studio/public/agent-status-registry')
    expect(getAgentStatusSnapshot().agents.find(agent => agent.id === 'hermes')).toMatchObject({ installed: true, source: 'user-cli', version: '0.99.1', path: f.python })
  })
  it.each([
    'root-only', 'python-only', 'cli-only', 'empty-python', 'missing-root', 'missing-python',
    'missing-agent-file', 'non-executable', 'wrong-default-source', 'cli-conflict', 'missing-main', 'empty-version', 'nonstring-version',
  ])('rejects %s without fallback or env mutation', async (kind) => {
    const f = fixture()
    if (kind === 'root-only') delete f.env.HERMES_AGENT_BRIDGE_PYTHON
    if (kind === 'python-only') delete f.env.HERMES_AGENT_ROOT
    if (kind === 'cli-only') {
      delete f.env.HERMES_AGENT_ROOT
      delete f.env.HERMES_AGENT_BRIDGE_PYTHON
      f.env.HERMES_AGENT_CLI_PYTHON = f.python
    }
    if (kind === 'empty-python') f.env.HERMES_AGENT_BRIDGE_PYTHON = ' '
    if (kind === 'missing-root') f.env.HERMES_AGENT_ROOT = join(f.base, 'absent')
    if (kind === 'missing-python') f.env.HERMES_AGENT_BRIDGE_PYTHON = join(f.base, 'absent')
    if (kind === 'missing-agent-file') {
      f.env.HERMES_AGENT_ROOT = f.base
    }
    if (kind === 'non-executable') {
      const fake = join(f.base, 'python')
      writeFileSync(fake, 'not executable')
      f.env.HERMES_AGENT_BRIDGE_PYTHON = fake
    }
    if (kind === 'wrong-default-source') {
      f.env.HERMES_AGENT_BRIDGE_PYTHON = join(f.base, 'old-runtime', 'bin', 'python3')
      f.env.PYTHONPATH = f.root
    }
    if (kind === 'cli-conflict') f.env.HERMES_AGENT_CLI_PYTHON = join(f.base, 'old-runtime', 'bin', 'python3')
    if (kind === 'missing-main') writeFileSync(join(f.root, 'hermes_cli', '__init__.py'), '__path__ = []\n')
    if (kind === 'empty-version') writeFileSync(join(f.root, 'hermes_constants.py'), '__version__ = ""\n')
    if (kind === 'nonstring-version') writeFileSync(join(f.root, 'hermes_cli', 'version_info.py'), 'from types import SimpleNamespace\ndef get_version_info(): return SimpleNamespace(display_version=None)\n')
    const before = { ...f.env }
    const { configurePreferredHermesRuntime } = await import('../../packages/server/src/modules/hermes/services/runtime/selection')
    await expect(configurePreferredHermesRuntime(f.env)).rejects.toMatchObject({ name: 'ExplicitHermesBindingError', message: expect.stringMatching(/explicit Hermes binding/i) })
    expect(f.env).toEqual(before)
  })

  it('probes afresh and rejects a changed interpreter environment even with PYTHONPATH masking', async () => {
    const f = fixture()
    const { configurePreferredHermesRuntime } = await import('../../packages/server/src/modules/hermes/services/runtime/selection')
    await configurePreferredHermesRuntime(f.env)
    const other = join(f.base, 'other core')
    mkdirSync(join(other, 'hermes_cli'), { recursive: true })
    writeFileSync(join(other, 'hermes_constants.py'), '__version__ = "0.20.0"\n')
    writeFileSync(join(other, 'hermes_cli', '__init__.py'), '__version__ = "0.20.0"\n')
    writeFileSync(join(other, 'hermes_cli', 'main.py'), '')
    writeFileSync(join(f.site, 'core.pth'), `${other}\n`)
    f.env.PYTHONPATH = f.root
    const before = { ...f.env }
    await expect(configurePreferredHermesRuntime(f.env)).rejects.toThrow(/default Core does not match/)
    expect(f.env).toEqual(before)
  })

  it('bounds a stuck safe-import probe', async () => {
    const f = fixture()
    writeFileSync(join(f.root, 'hermes_constants.py'), 'import time\ntime.sleep(30)\n')
    const before = { ...f.env }
    const { configurePreferredHermesRuntime } = await import('../../packages/server/src/modules/hermes/services/runtime/selection')
    await expect(configurePreferredHermesRuntime(f.env)).rejects.toThrow(/probe timed out after 5000ms/)
    expect(f.env).toEqual(before)
  }, 10000)

  it('keeps discovery available when no binding and no CLI are present', async () => {
    const f = fixture()
    delete f.env.HERMES_AGENT_ROOT
    delete f.env.HERMES_AGENT_BRIDGE_PYTHON
    f.env.HOME = join(f.base, 'empty-home')
    f.env.PATH = ''
    const { configurePreferredHermesRuntime } = await import('../../packages/server/src/modules/hermes/services/runtime/selection')
    expect(await configurePreferredHermesRuntime(f.env)).toEqual({ source: 'none', path: '', version: '' })
  })

  it('accepts a symlinked root with spaces and matching CLI interpreter', async () => {
    const f = fixture()
    const alias = join(f.base, 'source alias')
    symlinkSync(f.root, alias, 'dir')
    f.env.HERMES_AGENT_ROOT = alias
    f.env.HERMES_AGENT_CLI_PYTHON = f.python
    const { configurePreferredHermesRuntime } = await import('../../packages/server/src/modules/hermes/services/runtime/selection')
    expect(await configurePreferredHermesRuntime(f.env)).toMatchObject({ source: 'explicit-binding', agentRoot: alias })
  })
  it('executes and spawns the CLI with the paired Python on POSIX despite an old launcher, poisoned PYTHONPATH and cwd', async () => {
    const f = fixture()
    const { configurePreferredHermesRuntime } = await import('../../packages/server/src/modules/hermes/services/runtime/selection')
    await configurePreferredHermesRuntime(f.env)
    f.env.PYTHONPATH = f.oldSite
    const { execHermesWithBin, spawnHermesWithBin } = await import('../../packages/server/src/modules/hermes/services/runtime/process')
    const old = join(f.env.HOME!, '.local', 'bin', 'hermes')
    const result = await execHermesWithBin(old, ['probe', 'argument with spaces'], { env: f.env, cwd: f.oldSite, timeout: 5000 })
    expect(JSON.parse(result.stdout)).toEqual({ version: '0.99.1', python: f.python, args: ['probe', 'argument with spaces'] })
    const child = spawnHermesWithBin(old, ['spawn-probe'], { env: f.env, cwd: f.oldSite, stdio: ['ignore', 'pipe', 'pipe'] })
    let stdout = ''
    child.stdout!.on('data', data => { stdout += data })
    await new Promise<void>((resolve, reject) => {
      child.on('error', reject)
      child.on('close', code => code === 0 ? resolve() : reject(new Error(`CLI exited ${code}`)))
    })
    expect(JSON.parse(stdout)).toMatchObject({ version: '0.99.1', python: f.python, args: ['spawn-probe'] })
  })

  it.each(['broker', 'worker'])('starts a fresh isolated %s with the selected Python and Core', async mode => {
    const f = fixture()
    const marker = join(f.base, `${mode}-identity.json`)
    writeFileSync(join(f.root, 'hermes_bootstrap.py'), 'import json, os, sys, hermes_constants\nfrom pathlib import Path\nPath(os.environ["BINDING_MARKER"]).write_text(json.dumps({"python": sys.executable, "prefix": sys.prefix, "core": hermes_constants.__file__}))\n')
    writeFileSync(join(f.root, 'model_tools.py'), 'def get_tool_definitions(**kwargs): return []\n')
    writeFileSync(join(f.root, 'hermes_cli', 'config.py'), 'def load_config(): return {}\n')
    mkdirSync(join(f.root, 'agent'))
    writeFileSync(join(f.root, 'agent', '__init__.py'), '')
    writeFileSync(join(f.root, 'agent', 'prompt_builder.py'), 'PLATFORM_HINTS = {}\n')
    const { configurePreferredHermesRuntime } = await import('../../packages/server/src/modules/hermes/services/runtime/selection')
    await configurePreferredHermesRuntime(f.env)
    const script = String.raw`
import json, os, queue, socket, subprocess, sys, threading, time
from pathlib import Path
bridge, root, mode, marker = sys.argv[1:]
with socket.socket() as reservation:
    reservation.bind(('127.0.0.1', 0))
    port = reservation.getsockname()[1]
command = [sys.executable, bridge, '--endpoint', f'tcp://127.0.0.1:{port}', '--agent-root', root, '--hermes-home', os.environ['HERMES_HOME']]
if mode == 'worker':
    command += ['--worker-profile', 'default']
proc = subprocess.Popen(command, env=dict(os.environ, BINDING_MARKER=marker), cwd=root, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
lines = queue.Queue()
def read_stdout():
    for line in proc.stdout:
        lines.put(line)
    lines.put(None)
threading.Thread(target=read_stdout, daemon=True).start()
def request(action):
    with socket.create_connection(('127.0.0.1', port), timeout=3) as conn:
        conn.sendall((json.dumps({'action': action}) + '\n').encode())
        with conn.makefile('rb') as wire:
            return json.loads(wire.readline())
try:
    deadline = time.monotonic() + 8
    while time.monotonic() < deadline:
        line = lines.get(timeout=max(0.1, deadline - time.monotonic()))
        if line is None:
            raise RuntimeError('bridge exited before ready: ' + proc.stderr.read())
        try:
            if json.loads(line).get('event') == 'ready': break
        except ValueError: pass
    else: raise RuntimeError('bridge ready timeout')
    identity = json.loads(Path(marker).read_text())
    pong = request('ping')['pong']
    request('shutdown')
    proc.wait(timeout=5)
    print(json.dumps(dict(identity=identity, pong=pong, code=proc.returncode)))
finally:
    if proc.poll() is None:
        proc.kill()
        proc.wait(timeout=5)
`
    const result = JSON.parse(execFileSync(f.python, ['-I', '-c', script,
      resolve('packages/server/src/modules/hermes/services/bridge/python/hermes_bridge.py'), f.root, mode, marker],
    { env: f.env, encoding: 'utf8', timeout: 18000 }))
    expect(result).toEqual({ identity: { python: f.python, prefix: f.runtime, core: join(f.root, 'hermes_constants.py') }, pong: true, code: 0 })
  }, 20000)

  it('keeps a split source/runtime pair ahead of an old user CLI', async () => {
    const f = fixture()
    const { configurePreferredHermesRuntime } = await import('../../packages/server/src/modules/hermes/services/runtime/selection')
    const selected = await configurePreferredHermesRuntime(f.env)
    expect(selected).toMatchObject({ source: 'explicit-binding', agentRoot: f.root, pythonPath: f.python, version: '0.99.1' })
    expect(f.env.HERMES_AGENT_ROOT).toBe(f.root)
    expect(f.env.HERMES_AGENT_BRIDGE_PYTHON).toBe(f.python)
    expect(f.env.HERMES_AGENT_CLI_PYTHON).toBe(f.python)
    const configured = { ...f.env }
    expect(await configurePreferredHermesRuntime(f.env)).toEqual(selected)
    expect(f.env).toEqual(configured)
  })
})
