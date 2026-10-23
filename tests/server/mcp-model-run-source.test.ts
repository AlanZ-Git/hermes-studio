import { spawn, spawnSync } from 'node:child_process'
import { mkdtemp, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { describe, expect, it } from 'vitest'

function call(env: NodeJS.ProcessEnv, args: Record<string, unknown>): Promise<any> {
  return new Promise((resolve, reject) => {
    const child = spawn(process.execPath, ['bin/ekko-studio-mcp.mjs', 'api'], { env: { ...process.env, ...env } })
    const timer = setTimeout(() => { child.kill(); reject(new Error('MCP timeout')) }, 5000)
    let output = ''
    child.stdout.on('data', data => {
      output += data
      const line = output.split('\n').find(value => value.includes('"id":1'))
      if (line) { clearTimeout(timer); child.kill(); resolve(JSON.parse(line)) }
    })
    child.stdin.write(`${JSON.stringify({ jsonrpc: '2.0', id: 1, method: 'tools/call', params: { name: 'ekko_studio_api_request', arguments: args } })}\n`)
  })
}

describe('bound MCP model source', () => {
  it('refuses bridge-required launches with no binding before serving requests', () => {
    const result = spawnSync(process.execPath, ['bin/ekko-studio-mcp.mjs', 'api'], {
      env: { ...process.env, HERMES_WEB_UI_MODEL_RUN_AUTH: undefined,
        HERMES_WEB_UI_REQUIRE_MODEL_RUN_AUTH: '1', HERMES_WEB_UI_TOKEN: 'synthetic-unrelated' },
      input: '', encoding: 'utf8', timeout: 5000,
    })
    expect(result.status).not.toBe(0)
    expect(result.stderr).toContain('requires model run binding')
  })

  it('rejects inactive bound sources without falling back to explicit credentials', async () => {
    const home = await mkdtemp(join(tmpdir(), 'mcp-source-'))
    const binding = { version: 1, owner_id: '1', profile: 'alpha', run_marker: 'run', token_path: join(home, 'source.json') }
    await writeFile(binding.token_path, JSON.stringify({ binding, token: 'synthetic', active: false }), { mode: 0o600 })
    const response = await call({ HERMES_WEB_UI_MODEL_RUN_AUTH: JSON.stringify(binding), HERMES_WEB_UI_PROFILE: 'alpha', HERMES_WEB_UI_TOKEN: 'different', HERMES_WEB_UI_URL: 'http://127.0.0.1:1' }, { method: 'GET', path: '/api/studio/sessions/list' })
    expect(response.result.isError).toBe(true)
    expect(response.result.content[0].text).toMatch(/inactive/)
  })
})
