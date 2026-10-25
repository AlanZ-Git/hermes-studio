// Offline startup gate. Normal bootstrap is not loaded before durable release.
import { createServer } from 'node:http'
import { readFileSync, writeFileSync, openSync, closeSync, fsyncSync, lstatSync, existsSync, realpathSync, createReadStream } from 'node:fs'
import { resolve } from 'node:path'
import { createHash } from 'node:crypto'

function readPrivate(path: string): any {
  const s = lstatSync(path)
  if (!s.isFile() || s.uid !== process.getuid!() || (s.mode & 0o777) !== 0o600) throw new Error('private owned regular file required')
  return JSON.parse(readFileSync(path, 'utf8'))
}
function writeOnce(path: string, value: unknown): void {
  const fd = openSync(path, 'wx', 0o600)
  try { writeFileSync(fd, JSON.stringify(value)); fsyncSync(fd) } finally { closeSync(fd) }
  const dir = openSync(resolve(path, '..'), 'r')
  try { fsyncSync(dir) } finally { closeSync(dir) }
}
async function confirmWrites(request: any): Promise<Record<string, unknown>> {
  const rows: Record<string, unknown> = {}
  for (const path of request.write_sets.webui) {
    if (!existsSync(path)) { rows[path] = { type: 'absent' }; continue }
    const s = lstatSync(path)
    if (!s.isFile()) throw new Error('maintenance target must be regular')
    const fd = openSync(path, 'r')
    try { fsyncSync(fd) } finally { closeSync(fd) }
    const hash = createHash('sha256')
    for await (const chunk of createReadStream(path)) hash.update(chunk)
    rows[path] = { type: 'file', sha256: hash.digest('hex'), mode: s.mode & 0o777, bytes: s.size }
  }
  return rows
}
async function gate(attempt: string): Promise<void> {
  const root = resolve(attempt)
  const s = lstatSync(root)
  if (realpathSync(root) !== root || !s.isDirectory() || s.uid !== process.getuid!() || (s.mode & 0o777) !== 0o700) throw new Error('private attempt required')
  const request = readPrivate(resolve(root, 'request.json'))
  if (request.version !== 1 || !request.nonce) throw new Error('invalid maintenance request')
  if (existsSync(resolve(root, 'webui.activated.json'))) {
    const fence = readPrivate(resolve(root, 'business-fence.json'))
    const activated = readPrivate(resolve(root, 'webui.activated.json'))
    if (['attempt', 'nonce'].some(k => fence[k] !== request[k] || activated[k] !== request[k]) || fence.status !== 'BUSINESS_MAY_HAVE_STARTED') throw new Error('foreign activation')
    return
  }
  if (existsSync(resolve(root, 'webui.ready.json'))) throw new Error('unreleased maintenance restart requires recovery')
  const { initAllStores } = await import('../modules/studio/infrastructure/database/init')
  const { closeDb } = await import('../modules/studio/infrastructure/database/index')
  try { initAllStores(); closeDb() }
  catch (error) {
    closeDb()
    writeOnce(resolve(root, 'webui.failed.json'), { status: 'maintenance_failed', attempt: request.attempt, nonce: request.nonce, pair: request.pair, role: 'webui', writes: await confirmWrites(request) })
    throw error
  }
  const status = { status: 'maintenance_ready', attempt: request.attempt, nonce: request.nonce, pair: request.pair, role: 'webui', pid: process.pid, business_started: false, writes: await confirmWrites(request), start_time: readFileSync('/proc/self/stat', 'utf8').split(') ').pop()!.split(' ')[19] }
  const server = createServer((req, res) => {
    if (req.method === 'GET' && req.url === '/health/cutover') { res.writeHead(200); res.end(JSON.stringify(status)); return }
    res.writeHead(503); res.end('maintenance')
  })
  server.on('upgrade', (_req, socket) => socket.end('HTTP/1.1 503 Service Unavailable\r\nConnection: close\r\n\r\n'))
  await new Promise<void>((done, fail) => { server.once('error', fail); server.listen(request.ports.webui, '127.0.0.1', done) })
  try {
    writeOnce(resolve(root, 'webui.ready.json'), status)
    const start = Date.now()
    await new Promise<void>((done, fail) => {
      const timer = setInterval(() => {
        try {
          if (Date.now() - start > 300000) throw new Error('maintenance timeout')
          if (!existsSync(resolve(root, 'release.json'))) return
          const release = readPrivate(resolve(root, 'release.json'))
          const fence = readPrivate(resolve(root, 'business-fence.json'))
          if (['attempt', 'nonce'].some(key => release[key] !== request[key] || fence[key] !== request[key]) || fence.status !== 'BUSINESS_MAY_HAVE_STARTED') throw new Error('release without durable fence')
          clearInterval(timer); done()
        } catch (error) { clearInterval(timer); fail(error) }
      }, 50)
    })
  } finally {
    await new Promise<void>(done => server.close(() => done()))
  }
  writeOnce(resolve(root, 'webui.activated.json'), { attempt: request.attempt, nonce: request.nonce, pid: process.pid, status: 'activating_normal_entry' })

}
export async function startWithMaintenanceGate(): Promise<void> {
  const attempt = process.env.HERMES_CUTOVER_ATTEMPT
  if (attempt) await gate(attempt)
  delete process.env.HERMES_CUTOVER_ATTEMPT
  await import('./http')
}
