import { describe, expect, it, vi } from 'vitest'
import { execFileSync } from 'node:child_process'
import { resolveReasoningConfig, readReasoningDefault } from '../../packages/server/src/modules/studio/services/chat-run/reasoning-default'

vi.mock('../../packages/server/src/modules/studio/public/profile-config', () => ({ getProfilesBaseDir: () => '/tmp/reasoning-exact-profile-fixture' }))

const vectors = [
  [{ agent: { reasoning_effort: 'ultra' } }, 'm'],
  [{ agent: { reasoning_effort: 'high' } }, 'm'],
  [{ agent: { reasoning_effort: false } }, 'm'],
  ...[true, null, '', 'hgih', 2048, '2048', { enabled: false }, { effort: 2048 }, { effort: false }, { effort: ' fast ' }].map(raw => [{ agent: { reasoning_effort: raw } }, 'm']),
  ...['x-4.5', 'x-4-5', 'x.4.5', 'anthropic/x-4.5', 'openrouter/anthropic/x-4.5'].map(model => [{ agent: { reasoning_effort: 'high', reasoning_overrides: { 'anthropic/x-4-5': 'medium' } } }, model]),
  [{ agent: { reasoning_overrides: { 'ollama-local/qwen3.6:27b': false } } }, 'qwen3.6:27b'],
  [{ agent: { reasoning_overrides: { 'custom/another/qwen3.6:27b': { effort: 'thinking' } } } }, 'another/qwen3-6:27b'],
  [{ model: ' m ', agent: { reasoning_overrides: { m: 'medium' } } }, ''],
  ...[{ default: 'm' }, { model: 'm' }, { default: '', model: 'm' }].map(model => [{ model, agent: { reasoning_overrides: { m: false } } }, '']),
  [{ agent: { reasoning_overrides: { m: 'bad', 'openai/m': 'low', 'custom/m': 'high' } } }, 'm'],
  [{ agent: { reasoning_overrides: { 'custom/m': 'high', m: 'medium' } } }, 'm'],
  [{ agent: { reasoning_overrides: { 'custom/m': 'bad', 'custom2/m': 'high' } } }, 'm'],
  [{ agent: [] }, 'm'], [null, 'm'],
] as Array<[unknown, string]>

describe('Core reasoning parity', () => {
  it.each(vectors)('matches supplied public Core for %j / %s', (cfg, model) => {
    expect(process.env.HERMES_CORE_SOURCE, 'HERMES_CORE_SOURCE must name the public Core checkout').toBeTruthy()
    const script = 'import json,os,sys\nsys.path.insert(0,os.environ["HERMES_CORE_SOURCE"])\nfrom hermes_constants import resolve_reasoning_config\ncfg,model=json.load(sys.stdin)\nprint(json.dumps(resolve_reasoning_config(cfg,model)))\n'
    const expected = JSON.parse(execFileSync(process.env.HERMES_PYTHON || 'python3', ['-I', '-B', '-c', script], { input: JSON.stringify([cfg, model]), encoding: 'utf8' }))
    expect(resolveReasoningConfig(cfg, model)).toEqual(expected)
  })
  it('does not fall back from a missing profile to default', async () => {
    await expect(readReasoningDefault('missing', 'm')).rejects.toThrow()
  })
})
