import { createHash, randomUUID } from 'node:crypto'
import { chmodSync, chownSync, closeSync, copyFileSync, createReadStream, existsSync, fsyncSync, lstatSync, mkdirSync, openSync, renameSync } from 'node:fs'
import { basename, dirname, resolve } from 'node:path'

type Image = { type: 'absent' } | { type: 'file'; sha256: string; mode: number; bytes: number; uid: number; gid: number }
export async function stateImage(path: string): Promise<Image> {
  if (!existsSync(path)) return { type: 'absent' }
  const s = lstatSync(path)
  if (!s.isFile()) throw new Error('maintenance target must be regular')
  const hash = createHash('sha256')
  for await (const chunk of createReadStream(path)) hash.update(chunk)
  return { type: 'file', sha256: hash.digest('hex'), mode: s.mode & 0o777, bytes: s.size, uid: s.uid, gid: s.gid }
}
export async function prepareState(root: string, paths: string[], writes: Record<string, Image>, migrate: (stage: string) => void): Promise<void> {
  const stage = resolve(root, `web-maintenance-${randomUUID()}`)
  mkdirSync(stage, { mode: 0o700 })
  const before: Record<string, Image> = {}
  for (const path of paths) {
    before[path] = await stateImage(path)
    if (before[path].type === 'file') {
      copyFileSync(path, resolve(stage, basename(path)))
      chmodSync(resolve(stage, basename(path)), (before[path] as Exclude<Image, { type: 'absent' }>).mode)
    }
  }
  migrate(stage)
  const after: Record<string, Image> = {}
  for (const path of paths) after[path] = await stateImage(resolve(stage, basename(path)))
  for (const path of paths) if (JSON.stringify(await stateImage(path)) !== JSON.stringify(before[path])) throw new Error(`unknown target drift: ${path}`)
  for (const path of paths) {
    const value = after[path]
    if (JSON.stringify(value) === JSON.stringify(before[path])) continue
    for (const other of paths) if (JSON.stringify(await stateImage(other)) !== JSON.stringify(writes[other] || before[other])) throw new Error(`unknown target drift: ${other}`)
    let intended: Image = value
    if (value.type === 'absent') {
      renameSync(path, resolve(stage, `retained-${basename(path)}`))
    } else {
      const temp = resolve(dirname(path), `.maintenance-${randomUUID()}`)
      copyFileSync(resolve(stage, basename(path)), temp)
      const original = before[path]
      const uid = original.type === 'file' ? original.uid : process.getuid!()
      const gid = original.type === 'file' ? original.gid : process.getgid!()
      chownSync(temp, uid, gid); chmodSync(temp, value.mode)
      const fd = openSync(temp, 'r'); try { fsyncSync(fd) } finally { closeSync(fd) }
      intended = { ...value, uid, gid }
      renameSync(temp, path)
    }
    const fd = openSync(dirname(path), 'r'); try { fsyncSync(fd) } finally { closeSync(fd) }
    if (JSON.stringify(await stateImage(path)) !== JSON.stringify(intended)) throw new Error('maintenance publication readback differs')
    writes[path] = intended
  }
}
