import assert from 'node:assert/strict'
import { spawn } from 'node:child_process'
import { createServer } from 'node:http'
import { mkdtemp, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { createInterface } from 'node:readline'
import { test } from 'node:test'

const session = 'synthetic-session'
const profile = 'synthetic-profile'
const endpoint = '/api/studio/chat-run/runs'
const body = (input = '/loop status') => ({ input, session_id: session, profile, request_id: 'synthetic-request-1' })
const request = (value = body()) => ({ method: 'POST', path: endpoint, body: value })
const openapi = { info: { title: 'Synthetic fixture API', version: '1' }, paths: {
  [endpoint]: { post: { tags: ['chat-run'], requestBody: { required: true, content: { 'application/json': { schema: { type: 'object', required: ['input'] } } } }, responses: {} } },
  '/api/studio/unrelated': { get: { tags: ['unrelated'], responses: {} } },
} }

async function fixture(t, overrides = {}, sourceChange = {}) {
  const home = await mkdtemp(join(tmpdir(), 'loop-mcp-'))
  const binding = { version: 1, owner_id: 'synthetic-owner', profile, run_marker: 'synthetic-model-run', token_path: join(home, 'source.json') }
  await writeFile(binding.token_path, JSON.stringify({ binding, token: 'synthetic-private-token', active: true, ...sourceChange }), { mode: 0o600 })
  const received = []
  const server = createServer(async (req, res) => {
    let text = ''
    for await (const chunk of req) text += chunk
    received.push({ path: req.url, headers: req.headers, body: text ? JSON.parse(text) : undefined })
    res.setHeader('content-type', 'application/json')
    if (req.url === '/api/openapi.json') res.end(JSON.stringify(openapi))
    else if (req.headers.authorization !== 'Bearer synthetic-private-token') { res.statusCode = 401; res.end('{"error":"fixture unauthorized"}') }
    else res.end(JSON.stringify({ ok: true, fixture: true }))
  })
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve))
  const child = spawn(process.execPath, ['bin/ekko-studio-mcp.mjs', 'api'], { env: {
    ...process.env, HERMES_WEB_UI_URL: `http://127.0.0.1:${server.address().port}`,
    HERMES_WEB_UI_MODEL_RUN_AUTH: JSON.stringify(binding), HERMES_WEB_UI_REQUIRE_MODEL_RUN_AUTH: '1',
    HERMES_WEB_UI_PROFILE: profile, HERMES_STUDIO_SESSION_ID: session,
    HERMES_WEB_UI_API_SCOPE: 'current-session-loop', ...overrides,
  } })
  let sequence = 0
  let stderr = ''
  child.stderr.on('data', data => { stderr += data })
  const pending = new Map()
  createInterface({ input: child.stdout }).on('line', line => {
    const response = JSON.parse(line)
    pending.get(response.id)?.(response)
  })
  t.after(async () => { child.kill(); await new Promise(resolve => server.close(resolve)) })
  const rpc = (method, params = {}) => new Promise((resolve, reject) => {
    const id = ++sequence
    const timer = setTimeout(() => { pending.delete(id); reject(new Error(`MCP timeout: ${stderr}`)) }, 4000)
    pending.set(id, value => { clearTimeout(timer); pending.delete(id); resolve(value) })
    child.stdin.write(`${JSON.stringify({ jsonrpc: '2.0', id, method, params })}\n`)
  })
  const call = (args, name = 'ekko_studio_api_request') => rpc('tools/call', { name, arguments: args })
  return { call, rpc, received, binding }
}

for (const input of ['/loop status', '/loop 1h synthetic --times 12']) {
  test(`OLD scoped stdio contract sends intact ${input}`, async t => {
    const f = await fixture(t)
    const value = body(input)
    const response = await f.call(request(value))
    assert.notEqual(response.result.isError, true, JSON.stringify(response))
    const sent = f.received.find(entry => entry.path === endpoint)
    assert.deepEqual(sent?.body, value)
    assert.equal(sent.headers.authorization, 'Bearer synthetic-private-token')
    assert.equal(sent.headers['x-hermes-profile'], profile)
    const explicit = { ...value, request_id: 'synthetic-request-2', timeout_ms: 1000, include_events: true }
    const withProfile = await f.call({ ...request(explicit), profile })
    assert.notEqual(withProfile.result.isError, true, JSON.stringify(withProfile))
    assert.deepEqual(f.received.at(-1).body, explicit)
  })
}

test('scoped manual and tool descriptions advertise only current-session loop control', async t => {
  const f = await fixture(t)
  const init = await f.rpc('initialize', { protocolVersion: '2024-11-05', capabilities: {}, clientInfo: { name: 'fixture', version: '1' } })
  assert.match(init.result.instructions, /current.session.*loop/i)
  const listing = await f.rpc('tools/list')
  for (const tool of listing.result.tools) assert.match(tool.description, /current.session.*loop/i)
  for (const args of [{}, { full: true }, { path: '/api/studio/unrelated' }]) {
    const response = await f.call(args, 'ekko_studio_api_openapi_get')
    const text = response.result.content[0].text
    assert.match(text, /request_id/)
    assert.match(text, /synthetic-session/)
    assert.match(text, /synthetic-profile/)
    assert.doesNotMatch(text, /unrelated|module index|delegation/i)
  }
})

const invalid = [
  ['raw general document request', () => ({ method: 'GET', path: '/api/openapi.json' })],
  ['other endpoint', () => ({ method: 'GET', path: '/api/studio/unrelated' })],
  ['wrong method', () => ({ ...request(), method: 'GET' })],
  ['missing request', () => { const b = body(); delete b.request_id; return request(b) }],
  ...[null, '', ' ', 'a\0b', 'a\n', 1, {}, 'x'.repeat(129), ' spaced '].map(value => [`invalid request ${JSON.stringify(value)}`, () => request({ ...body(), request_id: value })]),
  ['cross-session', () => request({ ...body(), session_id: 'other-session' })],
  ['cross-profile', () => request({ ...body(), profile: 'other-profile' })],
  ['top-level profile', () => ({ ...request(), profile: 'other-profile' })],
  ['ordinary model turn', () => request(body('do ordinary model work'))],
  ['body override', () => request({ ...body(), provider: 'override' })],
  ['token override', () => ({ ...request(), token: 'override' })],
  ['headers override', () => ({ ...request(), headers: { authorization: 'override' } })],
  ['query override', () => ({ ...request(), query: { profile: 'other-profile' } })],
  ['path query override', () => ({ ...request(), path: `${endpoint}?profile=other-profile` })],
  ['top-level input override', () => ({ ...request(), input: 'normal' })],
]
for (const [label, args] of invalid) {
  test(`rejects ${label} before network`, async t => {
    const f = await fixture(t)
    const response = await f.call(args())
    assert.equal(response.result.isError, true, JSON.stringify(response))
    assert.equal(f.received.length, 0)
  })
}

for (const [label, env, source] of [
  ['missing identity', { HERMES_WEB_UI_MODEL_RUN_AUTH: undefined, HERMES_WEB_UI_REQUIRE_MODEL_RUN_AUTH: '0' }, {}],
  ['missing session', { HERMES_STUDIO_SESSION_ID: '' }, {}],
  ['inactive identity', {}, { active: false }],
  ['tampered identity', {}, { binding: { owner_id: 'other-owner' } }],
]) {
  test(`rejects ${label} without static-token fallback`, async t => {
    const f = await fixture(t, { HERMES_WEB_UI_TOKEN: 'synthetic-private-token', ...env }, source)
    const response = await f.call(request())
    assert.equal(response.result.isError, true, JSON.stringify(response))
    assert.equal(f.received.length, 0)
  })
}

test('scoped manual rejects overrides and inactive identity before network', async t => {
  const f = await fixture(t)
  for (const args of [{ token: 'override' }, { profile: 'other' }, { headers: { authorization: 'override' } }, { query: { profile: 'other' } }]) {
    const response = await f.call(args, 'ekko_studio_api_openapi_get')
    assert.equal(response.result.isError, true)
  }
  await writeFile(f.binding.token_path, JSON.stringify({ binding: f.binding, token: 'synthetic-private-token', active: false }), { mode: 0o600 })
  const response = await f.call({}, 'ekko_studio_api_openapi_get')
  assert.equal(response.result.isError, true)
  assert.equal(f.received.length, 0)
})

test('server authentication remains required for an otherwise valid scoped request', async t => {
  const f = await fixture(t, {}, { token: 'synthetic-unaccepted-token' })
  const response = await f.call(request())
  const envelope = JSON.parse(response.result.content[0].text)
  assert.equal(envelope.status, 401)
  assert.equal(envelope.body.error, 'fixture unauthorized')
  assert.equal(f.received.at(-1).headers.authorization, 'Bearer synthetic-unaccepted-token')
})

test('non-scoped manual and ordinary API behavior remain general', async t => {
  const f = await fixture(t, { HERMES_WEB_UI_API_SCOPE: '' })
  const listing = await f.rpc('tools/list')
  assert.match(listing.result.tools.find(tool => tool.name === 'ekko_studio_api_openapi_get').description, /module index/)
  const doc = await f.call({ full: true }, 'ekko_studio_api_openapi_get')
  assert.deepEqual(JSON.parse(doc.result.content[0].text), openapi)
  const response = await f.call({ method: 'GET', path: '/api/studio/unrelated' })
  assert.notEqual(response.result.isError, true)
  assert.equal(f.received.at(-1).path, '/api/studio/unrelated')
})
