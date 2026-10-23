import { lstatSync, readFileSync } from 'node:fs'
import { isAbsolute } from 'node:path'

const fields = ['version', 'owner_id', 'profile', 'run_marker', 'token_path']

export function parseModelRunBinding(raw) {
  if (raw === undefined) return null
  const binding = JSON.parse(raw)
  if (!binding || binding.version !== 1 || Object.keys(binding).length !== fields.length
    || fields.slice(1).some(key => typeof binding[key] !== 'string' || !binding[key].trim()
      || binding[key].trim() !== binding[key] || binding[key].includes('\0'))
    || !isAbsolute(binding.token_path)) throw new Error('Invalid model run binding')
  return Object.freeze(binding)
}

export function readBoundModelRunToken(binding, profile) {
  if (!binding || profile !== binding.profile) throw new Error('Model run profile binding mismatch')
  const info = lstatSync(binding.token_path)
  if (!info.isFile() || info.isSymbolicLink()
    || (process.platform !== 'win32' && (info.mode & 0o777) !== 0o600)) throw new Error('Model run source is not private')
  const source = JSON.parse(readFileSync(binding.token_path, 'utf8'))
  if (!source.binding || fields.some(key => source.binding[key] !== binding[key])) throw new Error('Model run source binding mismatch')
  if (source.active !== true || typeof source.token !== 'string') throw new Error('Model run source inactive')
  return source.token
}
