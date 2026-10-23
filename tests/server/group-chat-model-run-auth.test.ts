import { mkdtemp, readFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach, expect, it, vi } from 'vitest'
import { AgentClient, type MentionMessage } from '../../packages/server/src/modules/studio/services/group-chat/agent-clients'
import { readModelRunSource } from '../../packages/server/src/modules/studio/services/chat-run/model-run-source'

const users = vi.hoisted(() => ({
  findUserById: vi.fn(), userCanAccessProfile: vi.fn(),
  listUserProfiles: vi.fn(() => [{ profile_name: 'alpha' }]),
}))
const bridge = vi.hoisted(() => ({ chat: vi.fn(), streamOutput: vi.fn(), request: vi.fn(), destroy: vi.fn(), interrupt: vi.fn() }))
vi.mock('../../packages/server/src/modules/studio/repositories/users-store', () => users)
vi.mock('../../packages/server/src/modules/studio/public/auth', () => ({
  issueModelRunJwt: vi.fn(async (user: { id: number }) => `synthetic-${user.id}`),
  authenticateUserToken: vi.fn(async (token: string) => token === 'synthetic-7' ? { id: 7 } : null),
}))
vi.mock('../../packages/server/src/modules/studio/public/profile-config', () => ({
  readConfigYamlForProfile: vi.fn(async () => ({ model: { default: 'fixture', provider: 'custom' } })),
}))
vi.mock('../../packages/server/src/modules/studio/public/group-chat-agent-runtime', () => ({
  createGroupPrimaryAgentBridge: () => bridge,
}))

beforeEach(async () => {
  vi.clearAllMocks()
  vi.stubEnv('HERMES_WEB_UI_HOME', await mkdtemp(join(tmpdir(), 'studio-group-source-')))
  users.findUserById.mockReturnValue({ id: 7, username: 'fixture', role: 'admin', status: 'active' })
  users.userCanAccessProfile.mockReturnValue(true)
  bridge.chat.mockResolvedValue({ run_id: 'native-run' })
  bridge.request.mockResolvedValue({ synced: true })
  bridge.destroy.mockResolvedValue({})
  bridge.streamOutput.mockImplementation(async function* () {
    yield { run_id: 'native-run', delta: 'GROUP_BOUND_OK', status: 'complete', done: true }
  })
})
afterEach(() => vi.unstubAllEnvs())

function harness() {
  const sink = { connected: true, id: 'worker', emit: vi.fn(), sendMessage: vi.fn(async (..._args: any[]) => 'reply') }
  const client = new AgentClient({ agentId: 'worker', agent: 'hermes', profile: 'alpha', name: 'Worker', description: '', invited: 0, backgroundDelegationEnabled: false }, {}, sink)
  const storage = {
    getRoom: vi.fn(() => ({ workspace: '' })),
    getMemberByUserId: vi.fn((room: string, sender: string) => room === 'room' && sender === 'auth:7' ? { authUserId: 7 } : null),
  }
  client.setStorage(storage)
  const message: MentionMessage = { senderId: 'auth:7', senderName: 'Fixture', role: 'user', content: '@Worker hello', timestamp: 1 }
  return { client, storage, sink, message }
}

it('binds a local authenticated mention to its own generation and closes the source after bound cleanup', async () => {
  const { client, sink, message } = harness()
  let binding: any
  bridge.chat.mockImplementation(async (...args: any[]) => {
    binding = args[5].model_run_auth
    expect(binding).toMatchObject({ version: 1, owner_id: '7', profile: 'alpha' })
    expect((await readModelRunSource(binding)).active).toBe(true)
    return { run_id: 'native-run' }
  })
  await client.replyToMention('room', message)
  expect(sink.sendMessage).toHaveBeenCalledWith('room', 'GROUP_BOUND_OK', expect.any(String), expect.objectContaining({ role: 'assistant' }), expect.any(String))
  expect(bridge.request).toHaveBeenCalledWith(expect.objectContaining({ action: 'destroy', model_run_auth: binding }))
  expect(JSON.parse(await readFile(binding.token_path, 'utf8')).active).toBe(false)
  await expect(readModelRunSource(binding)).rejects.toThrow('inactive')
})

it.each(['disabled', 'profile-denied', 'unknown-sender', 'foreign-room', 'assistant', 'remote-relay', 'legacy-member-spoof'])('rejects %s without borrowing the owner identity', async reason => {
  const { client, storage, sink, message } = harness()
  if (reason === 'disabled') users.findUserById.mockReturnValue({ id: 7, role: 'user', status: 'disabled' })
  if (reason === 'profile-denied') users.userCanAccessProfile.mockReturnValue(false)
  if (reason === 'unknown-sender') message.senderId = 'spoofed'
  if (reason === 'assistant') message.role = 'assistant'
  if (reason === 'remote-relay') client.setStorage({ getRoom: storage.getRoom, getRoomMembers: () => [{ authUserId: 7, userId: 'auth:7' }] })
  if (reason === 'legacy-member-spoof') { message.senderId = 'legacy-client-id'; storage.getMemberByUserId.mockReturnValue({ authUserId: 7 }) }
  await client.replyToMention(reason === 'foreign-room' ? 'other-room' : 'room', message)
  expect(bridge.chat).not.toHaveBeenCalled()
  expect(bridge.request).not.toHaveBeenCalled()
  expect(sink.sendMessage.mock.calls[0][1]).toMatch(/active local member.*not authorized/)
})

it('keeps simultaneous rooms and repeated mentions in distinct generations', async () => {
  const { client, storage, message } = harness()
  storage.getMemberByUserId.mockReturnValue({ authUserId: 7 })
  await Promise.all([client.replyToMention('room', message), client.replyToMention('second-room', message)])
  await client.replyToMention('room', message)
  const bindings = bridge.chat.mock.calls.map(args => args[5].model_run_auth)
  expect(new Set(bindings.map(binding => binding.run_marker)).size).toBe(3)
  expect(new Set(bindings.map(binding => binding.token_path)).size).toBe(3)
  for (const binding of bindings) await expect(readModelRunSource(binding)).rejects.toThrow('inactive')
})

it.each(['owner-revoked', 'replaced-session'])('drops late output and interrupts only its bound generation after %s', async reason => {
  const { client, storage, sink, message } = harness()
  bridge.streamOutput.mockImplementation(async function* () {
    if (reason === 'owner-revoked') storage.getMemberByUserId.mockReturnValue(null)
    else (client as any).activeSessions.set('room', 'replacement')
    yield { run_id: 'native-run', delta: 'STALE_OUTPUT', done: true }
  })
  await client.replyToMention('room', message)
  const binding = bridge.chat.mock.calls[0][5].model_run_auth
  expect(bridge.request).toHaveBeenCalledWith(expect.objectContaining({ action: 'interrupt', model_run_auth: binding }))
  expect(bridge.request).toHaveBeenCalledWith(expect.objectContaining({ action: 'destroy', model_run_auth: binding }))
  expect(sink.sendMessage.mock.calls.filter(args => args[3]?.role === 'assistant')).toEqual([])
  await expect(readModelRunSource(binding)).rejects.toThrow('inactive')
  if (reason === 'replaced-session') expect(client.getActiveSessionId('room')).toBe('replacement')
})

it.each(['launch', 'stream', 'cleanup'])('deactivates the source after a %s failure', async phase => {
  const { client, message } = harness()
  if (phase === 'launch') bridge.chat.mockRejectedValueOnce(new Error('launch failure'))
  if (phase === 'stream') bridge.streamOutput.mockImplementation(async function* () { throw new Error('stream failure') })
  if (phase === 'cleanup') bridge.request.mockRejectedValueOnce(new Error('cleanup failure'))
  await client.replyToMention('room', message)
  const binding = bridge.chat.mock.calls[0][5].model_run_auth
  await expect(readModelRunSource(binding)).rejects.toThrow('inactive')
  expect(client.getActiveSessionId('room')).toBeUndefined()
})

it('uses the active generation for user interrupt and disconnect even with no further output', async () => {
  const { client, message } = harness()
  let release!: () => void
  const gate = new Promise<void>(resolve => { release = resolve })
  bridge.streamOutput.mockImplementation(async function* () { await gate })
  const pending = client.replyToMention('room', message)
  await vi.waitFor(() => expect(bridge.streamOutput).toHaveBeenCalled())
  const binding = bridge.chat.mock.calls[0][5].model_run_auth
  try {
    expect(await client.interrupt('room')).toBe(true)
    expect(bridge.request).toHaveBeenCalledWith(expect.objectContaining({ action: 'interrupt', model_run_auth: binding }))
    await client.disconnect()
    await expect(readModelRunSource(binding)).rejects.toThrow('inactive')
    expect(bridge.request).toHaveBeenCalledWith(expect.objectContaining({ action: 'destroy', model_run_auth: binding }))
  } finally { release(); await pending }
})

it('responds to approvals and clarification only in the current originating generation', async () => {
  const { client, message, storage } = harness()
  let release!: () => void
  const gate = new Promise<void>(resolve => { release = resolve })
  bridge.streamOutput.mockImplementation(async function* () {
    yield { run_id: 'native-run', events: [
      { event: 'approval.requested', approval_id: 'approval' },
      { event: 'clarify.requested', clarify_id: 'clarify' },
    ] }
    await gate
  })
  bridge.request.mockResolvedValue({ resolved: true })
  const pending = client.replyToMention('room', message)
  await vi.waitFor(() => expect((client as any).modelRuns.size).toBe(1))
  await vi.waitFor(() => expect(bridge.streamOutput).toHaveBeenCalled())
  // Let the event batch reach recordBridgeEvents before responding.
  await new Promise(resolve => setTimeout(resolve, 10))
  const binding = bridge.chat.mock.calls[0][5].model_run_auth
  try {
    expect(await client.respondApproval('foreign', 'once')).toBe(false)
    expect(await client.respondApproval('approval', 'once')).toBe(true)
    expect(await client.respondClarify('clarify', 'fixture response')).toBe(true)
    for (const action of ['approval_respond', 'clarify_respond']) expect(bridge.request).toHaveBeenCalledWith(expect.objectContaining({ action, session_id: binding.run_marker, profile: 'alpha', model_run_auth: binding }))
    storage.getMemberByUserId.mockReturnValue(null)
    const count = bridge.request.mock.calls.length
    expect(await client.respondApproval('approval', 'once')).toBe(false)
    expect(bridge.request.mock.calls.length).toBe(count)
  } finally { release(); await pending }
  expect(await client.respondClarify('clarify', 'stale response')).toBe(false)
})
