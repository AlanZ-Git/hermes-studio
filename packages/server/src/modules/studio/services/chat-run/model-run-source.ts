import { createHash, randomUUID } from 'node:crypto'
import { lstat, mkdir, readFile, rename, writeFile } from 'node:fs/promises'
import { dirname, isAbsolute, join } from 'node:path'
import { getWebUiHome } from '../../public/config'
import { authenticateUserToken, issueModelRunJwt, type AuthenticatedUser } from '../../public/auth'
import { getSession } from '../../repositories/session-store'
import { findUserById, userCanAccessProfile } from '../../repositories/users-store'

export interface ModelRunAuth {
  version: 1
  owner_id: string
  profile: string
  run_marker: string
  token_path: string
}

const fields = ['version', 'owner_id', 'profile', 'run_marker', 'token_path'] as const

type ModelRunSource = { binding: ModelRunAuth; token: string; active: boolean }

export function validateModelRunAuth(binding: ModelRunAuth): void {
  if (!binding || binding.version !== 1 || Object.keys(binding).length !== fields.length
    || fields.slice(1).some(key => typeof binding[key] !== 'string'
      || !String(binding[key]).trim() || String(binding[key]).trim() !== binding[key]
      || String(binding[key]).includes('\0')) || !isAbsolute(binding.token_path)) {
    throw new Error('Invalid model run binding')
  }
}

async function assertOwner(token: string, ownerId: string): Promise<void> {
  const owner = await authenticateUserToken(token)
  if (!owner || String(owner.id) !== ownerId) throw new Error('Model run owner is unavailable')
}

async function readSource(binding: ModelRunAuth): Promise<ModelRunSource> {
  validateModelRunAuth(binding)
  const info = await lstat(binding.token_path)
  if (!info.isFile() || info.isSymbolicLink()
    || (process.platform !== 'win32' && (info.mode & 0o777) !== 0o600)) {
    throw new Error('Model run source is not private')
  }
  const source = JSON.parse(await readFile(binding.token_path, 'utf8')) as ModelRunSource
  if (!source.binding || fields.some(key => source.binding[key] !== binding[key])) {
    throw new Error('Model run source binding mismatch')
  }
  return source
}

export async function readModelRunSource(binding: ModelRunAuth): Promise<ModelRunSource> {
  const source = await readSource(binding)
  if (source.active !== true || typeof source.token !== 'string') throw new Error('Model run source inactive')
  await assertOwner(source.token, binding.owner_id)
  return source
}

export async function createModelRunSource(
  user: AuthenticatedUser | undefined, profile: string, runMarker: string,
): Promise<Readonly<ModelRunAuth>> {
  if (user?.id == null || !String(user.id).trim()) throw new Error('Model run requires an authenticated owner')
  const ownerId = String(user.id)
  const key = createHash('sha256').update(JSON.stringify([ownerId, profile, runMarker])).digest('hex')
  const directory = join(getWebUiHome(), 'model-runs', key)
  const binding = Object.freeze({ version: 1 as const, owner_id: ownerId, profile, run_marker: runMarker, token_path: join(directory, 'source.json') })
  validateModelRunAuth(binding)
  await mkdir(dirname(directory), { recursive: true, mode: 0o700 })
  const info = await lstat(dirname(directory))
  if (!info.isDirectory() || info.isSymbolicLink()
    || (process.platform !== 'win32' && (info.mode & 0o777) !== 0o700)) throw new Error('Model run directory is not private')
  await mkdir(directory, { mode: 0o700 })
  const token = await issueModelRunJwt(user)
  await assertOwner(token, ownerId)
  await writeSource(binding, { binding, token, active: true })
  return binding
}

async function writeSource(binding: ModelRunAuth, source: ModelRunSource): Promise<void> {
  const temporary = join(dirname(binding.token_path), `.source-${randomUUID()}.tmp`)
  await writeFile(temporary, `${JSON.stringify(source)}\n`, { mode: 0o600, flag: 'wx' })
  await rename(temporary, binding.token_path)
}

export async function closeModelRunSource(binding: ModelRunAuth): Promise<void> {
  const source = await readSource(binding)
  if (source.active === false) return
  await writeSource(binding, { ...source, active: false })
}

export async function createCompressionModelRunSource(
  sessionId: string | undefined, profile: string, runMarker: string,
): Promise<Readonly<ModelRunAuth>> {
  const session = sessionId ? getSession(sessionId) : null
  const user = session?.user_id != null ? findUserById(session.user_id) : null
  if (!session || (session.profile || 'default') !== profile || !user || user.status !== 'active'
    || (user.role !== 'super_admin' && !userCanAccessProfile(user.id, profile))) {
    throw new Error('Compression requires an active session owner for this profile')
  }
  return createModelRunSource({ id: user.id, username: user.username, role: user.role }, profile, runMarker)
}
