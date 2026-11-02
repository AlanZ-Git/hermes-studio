import { readFile } from 'node:fs/promises'
import { join } from 'node:path'
import { parseDocument } from 'yaml'
import { getProfilesBaseDir } from '../../public/profile-config'

type ReasoningConfig = { enabled: false } | { enabled: true; effort: string }
const levels = new Set(['minimal', 'low', 'medium', 'high', 'xhigh', 'max', 'ultra'])
const record = (value: unknown): value is Record<string, any> => value !== null && typeof value === 'object' && !Array.isArray(value)
const truthy = (value: unknown): boolean => Boolean(value) && (!Array.isArray(value) || value.length > 0) && (!record(value) || Object.keys(value).length > 0)
function pythonString(value: unknown): string {
  if (value === true) return 'True'
  if (value === false) return 'False'
  if (value === null) return 'None'
  if (Array.isArray(value)) return `[${value.map(item => typeof item === 'string' ? `'${item}'` : pythonString(item)).join(', ')}]`
  if (record(value)) return `{${Object.entries(value).map(([key, item]) => `'${key}': ${typeof item === 'string' ? `'${item}'` : pythonString(item)}`).join(', ')}}`
  return String(value)
}
function parseEffort(raw: unknown): ReasoningConfig | null {
  if (raw == null || raw === true) return null
  if (record(raw)) {
    if (raw.enabled === false) return { enabled: false }
    const level = truthy(raw.effort) ? pythonString(raw.effort).trim() : ''
    return level ? { enabled: true, effort: level } : null
  }
  const level = pythonString(raw).trim().toLowerCase()
  if (['none', 'false', 'disabled'].includes(level)) return { enabled: false }
  return levels.has(level) ? { enabled: true, effort: level } : null
}

function modelVariants(model: string): string[] {
  const values = new Set<string>()
  const add = (...items: string[]) => items.forEach(item => { if (item) values.add(item) })
  const dashToDot = (s: string) => s.replace(/(\d)-(\d)/g, '$1.$2')
  const dotToDash = (s: string) => s.replace(/(\d)\.(\d)/g, '$1-$2')
  const derivatives = (s: string) => {
    const dashed = s.replaceAll('.', '-'), dotted = s.replaceAll('-', '.')
    add(s, dashed, dotted, dashToDot(s), dotToDash(s), dashToDot(dashed), dotToDash(dotted))
  }
  derivatives(model)
  const parts = model.split('/')
  if (parts.length >= 2) derivatives(parts.at(-1)!)
  if (parts.length >= 3) derivatives(parts.slice(1).join('/'))
  for (const v of [...values].filter(v => !v.includes('/'))) {
    add(...['anthropic', 'openai', 'google', 'openrouter', 'groq', 'mistral', 'xai', 'cohere', 'perplexity', 'together', 'fireworks', 'deepseek'].map(p => `${p}/${v}`))
  }
  for (const v of [...values].filter(v => v.split('/').length === 2)) {
    add(...['openrouter', 'opencode', 'fireworks', 'groq', 'together'].map(p => `${p}/${v}`))
  }
  return [...values]
}

export function resolveReasoningConfig(config: unknown, model = ''): ReasoningConfig | null {
  const cfg = record(config) ? config : {}
  const agent = record(cfg.agent) ? cfg.agent : {}
  if (!model) {
    let configured = cfg.model
    if (record(configured)) configured = truthy(configured.default) ? configured.default : configured.model || ''
    model = typeof configured === 'string' ? configured.trim() : ''
  }
  const overrides = agent.reasoning_overrides
  if (model && record(overrides)) {
    const variants = modelVariants(model)
    for (const v of variants) {
      if (Object.hasOwn(overrides, v)) {
        const result = parseEffort(overrides[v])
        if (result !== null) return result
      }
    }
    for (const [key, raw] of Object.entries(overrides)) {
      if (!key.includes('/')) continue
      const parts = key.split('/')
      const forms = modelVariants(parts.at(-1)!)
      if (parts.length >= 3) forms.push(...modelVariants(parts.slice(1).join('/')))
      if (forms.some(form => variants.includes(form))) {
        const result = parseEffort(raw)
        if (result !== null) return result
      }
    }
  }
  return parseEffort(agent.reasoning_effort)
}

export async function readReasoningDefault(profile: string, model: string) {
  if (!/^[A-Za-z0-9_-]+$/.test(profile)) throw new Error('Invalid profile')
  // getProfileDir and readYaml can silently fall back; this read must target the authorized file exactly.
  const root = getProfilesBaseDir()
  const path = join(profile === 'default' ? root : join(root, 'profiles', profile), 'config.yaml')
  const document = parseDocument(await readFile(path, 'utf8'), { uniqueKeys: true })
  if (document.errors.length) throw new Error('Invalid profile configuration')
  const cfg: unknown = document.toJS()
  if (!record(cfg)) throw new Error('Invalid profile configuration')
  const resolved = resolveReasoningConfig(cfg, model)
  return { profile, model, status: resolved === null ? 'provider-default' as const : 'resolved' as const,
    effectiveEffort: resolved === null ? null : resolved.enabled ? resolved.effort : 'none' }
}
