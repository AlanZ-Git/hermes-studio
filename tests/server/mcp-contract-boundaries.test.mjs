import assert from 'node:assert/strict'
import { spawn, spawnSync } from 'node:child_process'
import { createInterface } from 'node:readline'
import { test } from 'node:test'
import { createServer } from 'node:http'

const encode = value => Buffer.from(JSON.stringify(value)).toString('base64url')
const jwt = (changes = {}) => `${encode({ alg: 'HS256', typ: 'JWT' })}.${encode({ type: 'access', aud: 'hermes-studio', sub: '1', username: 'fixture', role: 'super_admin', exp: Math.floor(Date.now() / 1000) + 3600, ...changes })}.c3ludGhldGljLXNpZ25hdHVyZQ`

async function fixture(t, token) {
  const received = []
  const server = createServer((req, res) => {
    received.push({ path: req.url, authorization: req.headers.authorization })
    res.setHeader('content-type', 'application/json')
    if (req.url === '/api/openapi.json') res.end(JSON.stringify({ paths: { '/api/synthetic-read': { get: { responses: {} } } } }))
    else { res.statusCode = 401; res.end('{"error":"synthetic server rejects signature"}') }
  })
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve))
  const child = spawn(process.execPath, ['bin/ekko-studio-mcp.mjs', 'api'], { env: { ...process.env,
    HERMES_WEB_UI_URL: `http://127.0.0.1:${server.address().port}`, HERMES_WEB_UI_TOKEN: token,
    HERMES_WEB_UI_PROFILE: 'synthetic',
  } })
  t.after(async () => { child.kill(); server.closeAllConnections(); await new Promise(resolve => server.close(resolve)) })
  let sequence = 0
  const pending = new Map()
  createInterface({ input: child.stdout }).on('line', line => { const value = JSON.parse(line); pending.get(value.id)?.(value) })
  const call = (name, args = {}) => new Promise((resolve, reject) => {
    const id = ++sequence
    const timer = setTimeout(() => reject(new Error('RPC timeout')), 4000)
    pending.set(id, value => { clearTimeout(timer); pending.delete(id); resolve(value) })
    child.stdin.write(JSON.stringify({ jsonrpc: '2.0', id, method: 'tools/call', params: { name, arguments: args } }) + '\n')
  })
  return { call, received }
}

for (const [label, token] of [
  ['malformed', 'synthetic-not-a-JWT'],
  ['missing', ''],
  ['expired', jwt({ exp: 1 })],
  ['wrong audience', jwt({ aud: 'other' })],
]) {
  test(`management ${label} JWT is rejected before schema or business HTTP`, async t => {
    const f = await fixture(t, token)
    const response = await f.call('ekko_studio_api_request', { method: 'GET', path: '/api/synthetic-read' })
    assert.equal(response.result.isError, true, JSON.stringify(response))
    assert.match(response.result.content[0].text, /JWT|credential/i)
    assert.deepEqual(f.received, [])
    const doc = await f.call('ekko_studio_api_openapi_get')
    assert.notEqual(doc.result.isError, true)
    assert.deepEqual(f.received, [{ path: '/api/openapi.json', authorization: undefined }])
  })
}

test('local JWT decoding is not authentication: structurally valid token still reaches server rejection', async t => {
  const f = await fixture(t, jwt())
  const response = await f.call('ekko_studio_api_request', { method: 'GET', path: '/api/synthetic-read' })
  assert.equal(JSON.parse(response.result.content[0].text).status, 401)
  assert.deepEqual(f.received.map(entry => entry.path), ['/api/openapi.json', '/api/synthetic-read'])
})

test('plan uses default profile only when no profile was supplied', async t => {
  const received = []
  const server = createServer(async (req, res) => {
    let raw = ''; for await (const chunk of req) raw += chunk
    received.push({ profile: req.headers['x-hermes-profile'], body: JSON.parse(raw) })
    res.setHeader('content-type', 'application/json'); res.end('{"ok":true}')
  })
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve))
  const child = spawn(process.execPath, ['bin/ekko-studio-mcp.mjs', 'plan'], { env: { ...process.env,
    HERMES_WEB_UI_URL: `http://127.0.0.1:${server.address().port}`, HERMES_WEB_UI_TOKEN: jwt(),
    HERMES_WEB_UI_PROFILE: '', HERMES_PROFILE: '', PROFILE: '',
  } })
  t.after(async () => { child.kill(); server.closeAllConnections(); await new Promise(resolve => server.close(resolve)) })
  const response = await new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error('RPC timeout')), 4000)
    createInterface({ input: child.stdout }).on('line', line => { clearTimeout(timer); resolve(JSON.parse(line)) })
    child.stdin.write(JSON.stringify({ jsonrpc: '2.0', id: 1, method: 'tools/call', params: { name: 'ekko_studio_update_plan', arguments: { context_id: 'synthetic', plan: [] } } }) + '\n')
  })
  assert.notEqual(response.result.isError, true)
  assert.equal(received[0].profile, 'default')
})

test('plan discovery, instructions and alias calls agree on the narrowed tool set', async t => {
  const child = spawn(process.execPath, ['bin/ekko-studio-mcp.mjs', 'plan'], { env: { ...process.env, HERMES_WEB_UI_URL: 'http://127.0.0.1:1' } })
  t.after(() => child.kill())
  const pending = new Map()
  createInterface({ input: child.stdout }).on('line', line => { const value = JSON.parse(line); pending.get(value.id)?.(value) })
  let sequence = 0
  const rpc = (method, params = {}) => new Promise((resolve, reject) => {
    const id = ++sequence; const timer = setTimeout(() => reject(new Error('RPC timeout')), 4000)
    pending.set(id, value => { clearTimeout(timer); resolve(value) })
    child.stdin.write(JSON.stringify({ jsonrpc: '2.0', id, method, params }) + '\n')
  })
  const initialized = await rpc('initialize')
  assert.doesNotMatch(initialized.result.instructions, /session_tasks/)
  for (const name of ['ekko_studio_list_session_tasks', 'hermes_studio_list_session_tasks', 'ekko_studio_update_session_tasks', 'hermes_studio_update_session_tasks']) {
    const response = await rpc('tools/call', { name, arguments: {} })
    assert.equal(response.result.isError, true)
    assert.match(response.result.content[0].text, /not available/)
  }
})

test('plan defaults to exactly the plan update capability', async t => {
  const child = spawn(process.execPath, ['bin/ekko-studio-mcp.mjs', 'plan'], { env: { ...process.env, HERMES_MCP_USER_CLARIFICATION: '0', HERMES_MCP_NATIVE_TASK_PLAN: '0' } })
  t.after(() => child.kill())
  const response = await new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error('RPC timeout')), 4000)
    createInterface({ input: child.stdout }).on('line', line => { clearTimeout(timer); resolve(JSON.parse(line)) })
    child.stdin.write(JSON.stringify({ jsonrpc: '2.0', id: 1, method: 'tools/list' }) + '\n')
  })
  assert.deepEqual(response.result.tools.map(tool => tool.name), ['ekko_studio_update_plan'])
})

for (const args of [['not-a-toolset'], []]) {
  test(`unknown toolset fails closed from ${args.length ? 'argv' : 'environment'}`, () => {
    const child = spawnSync(process.execPath, ['bin/ekko-studio-mcp.mjs', ...args], {
      env: { ...process.env, HERMES_MCP_TOOLSET: 'not-a-toolset' }, encoding: 'utf8', timeout: 4000,
      input: JSON.stringify({ jsonrpc: '2.0', id: 1, method: 'tools/list' }) + '\n',
    })
    assert.notEqual(child.status, 0)
    assert.match(child.stderr, /unknown.*toolset/i)
    assert.doesNotMatch(child.stdout, /ekko_studio_api_request/)
  })
}
