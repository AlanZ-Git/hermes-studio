import type { Context } from 'koa'
import { listUserProfiles } from '../public/users'
import { readReasoningDefault } from '../services/chat-run/reasoning-default'

export async function reasoningDefault(ctx: Context): Promise<void> {
  const user = ctx.state.user
  if (!user) { ctx.status = 401; ctx.body = { error: 'Authentication required' }; return }
  const profile = ctx.query.profile
  const model = ctx.query.model ?? ''
  if (typeof profile !== 'string' || !/^[A-Za-z0-9_-]+$/.test(profile) || typeof model !== 'string') {
    ctx.status = 400; ctx.body = { error: 'Exact profile and model required' }; return
  }
  const header = ctx.get('x-hermes-profile')
  if ((header && header.trim() !== profile) || (ctx.state.profile?.name && ctx.state.profile.name !== profile)) {
    ctx.status = 400; ctx.body = { error: 'Conflicting profile selectors' }; return
  }
  if (user.role !== 'super_admin' && !listUserProfiles(user.id).some(p => p.profile_name === profile)) {
    ctx.status = 403; ctx.body = { error: 'Profile not available' }; return
  }
  try {
    ctx.body = await readReasoningDefault(profile, model)
  } catch {
    ctx.status = 503
    ctx.body = { profile, model, effectiveEffort: null, status: 'unknown' }
  }
}
