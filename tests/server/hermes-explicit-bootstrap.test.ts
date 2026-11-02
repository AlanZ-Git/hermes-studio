import { readFileSync, mkdirSync, mkdtempSync } from 'fs'
import { tmpdir } from 'os'
import { join, resolve } from 'path'
import { runInNewContext } from 'vm'
import ts from 'typescript'
import { describe, expect, it, vi } from 'vitest'
import { ExplicitHermesBindingError } from '../../packages/server/src/modules/hermes/services/runtime/explicit-binding'

function isolatedBootstrap(selectionError: Error) {
  const base = mkdtempSync(join(tmpdir(), 'binding-bootstrap-'))
  mkdirSync(join(base, 'uploads'))
  const reachedInventory = vi.fn(async () => undefined)
  const warn = vi.fn()
  const stop = new Error('isolated bootstrap boundary')
  const dependencies: Record<string, unknown> = {
    config: { uploadDir: join(base, 'uploads'), dataDir: join(base, 'data') },
    shouldCreateWebUiDataDir: () => false,
    runRegisteredStartupTasks: async () => {},
    readLockedDesktopHermesSelection: () => null,
    configurePreferredHermesRuntime: async () => { throw selectionError },
    ExplicitHermesBindingError,
    logger: { warn },
    updateAgentStatus: () => {},
    getRuntimeVersionStatus: reachedInventory,
    getCodingAgentsStatus: reachedInventory,
    isHermesAgentAvailable: () => false,
    initLoginLimiter: async () => { throw stop },
  }
  const exports = {}
  const path = resolve('packages/server/src/bootstrap/http.ts')
  const source = ts.createSourceFile(path, readFileSync(path, 'utf8'), ts.ScriptTarget.Latest, true)
  // Load the exported bootstrap without the entrypoint's automatic invocation.
  const moduleOnly = ts.factory.updateSourceFile(source, source.statements.slice(0, -1))
  const compiled = ts.transpileModule(ts.createPrinter().printFile(moduleOnly), {
    compilerOptions: { module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2022, esModuleInterop: true },
  }).outputText
  const stub = new Proxy({}, { get: (_, key) => key === '__esModule' ? true : dependencies[String(key)] || class {} })
  runInNewContext(compiled, {
    exports, require: (id: string) => id === 'path' ? { resolve } : id === 'fs/promises' ? { mkdir: async () => {} } : stub,
    process: { env: {}, once() {}, on() {} },
    console: { log() {}, warn() {} }, __APP_VERSION__: 'test', __dirname: base,
  })
  return { bootstrap: (exports as { bootstrap: () => Promise<void> }).bootstrap, reachedInventory, warn, stop }
}

describe('explicit binding bootstrap boundary', () => {
  it('propagates binding failure before inventory or runtime services', async () => {
    const error = new ExplicitHermesBindingError('fixture mismatch')
    const f = isolatedBootstrap(error)
    await expect(f.bootstrap()).rejects.toBe(error)
    expect(f.reachedInventory).not.toHaveBeenCalled()
  })

  it('keeps ordinary selection failures nonfatal', async () => {
    const f = isolatedBootstrap(new Error('ordinary discovery failure'))
    await expect(f.bootstrap()).rejects.toBe(f.stop)
    expect(f.reachedInventory).toHaveBeenCalledTimes(2)
    expect(f.warn).toHaveBeenCalledWith(expect.any(Error), expect.stringContaining('continuing'))
  })
})
