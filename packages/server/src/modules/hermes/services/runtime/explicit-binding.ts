import { execFile } from 'child_process'
import { accessSync, constants, realpathSync, statSync } from 'fs'
import { isAbsolute, join } from 'path'
import { promisify } from 'util'

const execFileAsync = promisify(execFile)
const PROBE = [
  'import sys, json, importlib.util, hermes_constants, hermes_cli',
  "main = importlib.util.find_spec('hermes_cli.main')",
  'try:',
  '    from hermes_cli.version_info import get_version_info',
  'except ModuleNotFoundError as exc:',
  "    if exc.name != 'hermes_cli.version_info': raise",
  "    version = getattr(hermes_cli, '__version__', getattr(hermes_constants, '__version__', ''))",
  'else:',
  '    version = get_version_info().display_version',
  "print(json.dumps(dict(executable=sys.executable, prefix=sys.prefix, constants=hermes_constants.__file__, cli=hermes_cli.__file__, main=main.origin if main else None, version=version)))",
].join('\n')

export async function probeExplicitHermesPython(python: string, env: NodeJS.ProcessEnv) {
  const { stdout } = await execFileAsync(python, ['-I', '-c', PROBE], {
    env, timeout: 5000, maxBuffer: 64 * 1024, encoding: 'utf8', windowsHide: true,
  })
  return JSON.parse(stdout) as {
    executable: string; prefix: string; constants: string; cli: string; main: string; version: string
  }
}

function canonicalBindingPath(path: string): string {
  const canonical = realpathSync(path)
  return process.platform === 'win32' ? canonical.toLowerCase() : canonical
}

export class ExplicitHermesBindingError extends Error {
  constructor(detail: string) {
    super(`Invalid explicit Hermes binding: ${detail}`)
    this.name = 'ExplicitHermesBindingError'
  }
}

export function hasExplicitHermesBinding(env: NodeJS.ProcessEnv): boolean {
  return ['HERMES_AGENT_ROOT', 'HERMES_AGENT_BRIDGE_PYTHON', 'HERMES_AGENT_CLI_PYTHON']
    .some(key => env[key] !== undefined)
}

export async function validateExplicitHermesBinding(env: NodeJS.ProcessEnv) {
  try {
    const root = env.HERMES_AGENT_ROOT?.trim()
    const python = env.HERMES_AGENT_BRIDGE_PYTHON?.trim()
    if (!root || !python) throw new Error('HERMES_AGENT_ROOT and HERMES_AGENT_BRIDGE_PYTHON must both be set')
    if (!isAbsolute(root) || !isAbsolute(python)) throw new Error('root and Python must be absolute paths')
    for (const name of ['run_agent.py', 'cli.py', 'hermes_constants.py', 'hermes_cli/__init__.py', 'hermes_cli/main.py']) {
      if (!statSync(join(root, name)).isFile()) throw new Error(`required Core file missing: ${name}`)
    }
    function checkPython(path: string) {
      if (!isAbsolute(path) || !statSync(path).isFile()) throw new Error('Python must be an executable file')
      accessSync(path, constants.X_OK)
    }
    checkPython(python)
    const probe = await probeExplicitHermesPython(python, env)
    if (canonicalBindingPath(probe.executable) !== canonicalBindingPath(python)) throw new Error('Python executable mismatch')
    for (const [actual, expected] of [
      [probe.constants, 'hermes_constants.py'], [probe.cli, 'hermes_cli/__init__.py'], [probe.main, 'hermes_cli/main.py'],
    ]) {
      if (!actual || canonicalBindingPath(actual) !== canonicalBindingPath(join(root, expected))) {
        throw new Error(`Python default Core does not match HERMES_AGENT_ROOT (${expected})`)
      }
    }
    if (typeof probe.version !== 'string' || !probe.version.trim()) {
      throw new Error('Core CLI version is unavailable')
    }
    if (!probe.prefix || !statSync(probe.prefix).isDirectory()) throw new Error('Python environment prefix is unavailable')
    if (env.HERMES_AGENT_CLI_PYTHON !== undefined) {
      const cliPython = env.HERMES_AGENT_CLI_PYTHON.trim()
      checkPython(cliPython)
      const cliProbe = await probeExplicitHermesPython(cliPython, env)
      for (const field of ['executable', 'prefix', 'constants', 'cli', 'main'] as const) {
        if (!cliProbe[field] || canonicalBindingPath(cliProbe[field]) !== canonicalBindingPath(probe[field])) {
          throw new Error('HERMES_AGENT_CLI_PYTHON conflicts with the bridge Python environment')
        }
      }
      if (cliProbe.version !== probe.version) throw new Error('CLI Python Core version mismatch')
    }
    return { root, python, ...probe, version: probe.version.trim() }
  } catch (error) {
    const detail = error as NodeJS.ErrnoException & { killed?: boolean }
    throw new ExplicitHermesBindingError(detail.killed ? 'Python probe timed out after 5000ms' : detail.message || String(error))
  }
}
