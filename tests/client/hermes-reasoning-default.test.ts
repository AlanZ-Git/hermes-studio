// @vitest-environment jsdom
import { describe, expect, it, vi } from 'vitest'
import { effectScope, ref } from 'vue'
import { flushPromises } from '@vue/test-utils'
import { useHermesReasoningDefault } from '@/composables/useHermesReasoningDefault'
import type { ReasoningDefault } from '@/api/studio/sessions'
const fetchDefault = vi.hoisted(() => vi.fn())
vi.mock('@/api/studio/sessions', () => ({ fetchReasoningDefault: fetchDefault }))
function deferred() {
  let resolve!: (r: ReasoningDefault) => void
  let reject!: (e: Error) => void
  const promise = new Promise<ReasoningDefault>((a, b) => { resolve = a; reject = b })
  return { promise, resolve, reject }
}
describe('profile/model default request lifetime', () => {
  it.each(['success', 'failure'])('discards late %s across profile and model switches', async late => {
    const a = deferred(), b = deferred(), c = deferred()
    fetchDefault.mockReset().mockReturnValueOnce(a.promise).mockReturnValueOnce(b.promise).mockReturnValueOnce(c.promise)
    const selected = ref<{ profile: string; model: string } | null>({ profile: 'A', model: 'one' })
    const scope = effectScope()
    const result = scope.run(() => useHermesReasoningDefault(selected))!
    selected.value = { profile: 'B', model: 'one' }
    b.resolve({ profile: 'B', model: 'one', status: 'resolved', effectiveEffort: 'medium' })
    await flushPromises()
    expect(result.value?.effectiveEffort).toBe('medium')
    selected.value = { profile: 'B', model: 'two' }
    expect(result.value).toBeNull()
    c.resolve({ profile: 'B', model: 'two', status: 'provider-default', effectiveEffort: null })
    await flushPromises()
    if (late === 'success') a.resolve({ profile: 'A', model: 'one', status: 'resolved', effectiveEffort: 'high' })
    else a.reject(new Error('late A failure'))
    await flushPromises()
    expect(result.value).toEqual({ profile: 'B', model: 'two', status: 'provider-default', effectiveEffort: null })
    expect(fetchDefault.mock.calls).toEqual([['A', 'one'], ['B', 'one'], ['B', 'two']])
    selected.value = null
    expect(result.value).toBeNull()
    scope.stop()
  })
  it('reports current failure as unknown and ignores completion after unmount', async () => {
    const request = deferred()
    fetchDefault.mockReturnValueOnce(request.promise)
    const selected = ref({ profile: 'A', model: 'one' })
    const scope = effectScope()
    const result = scope.run(() => useHermesReasoningDefault(selected))!
    request.reject(new Error('unavailable'))
    await flushPromises()
    expect(result.value?.status).toBe('unknown')
    const pending = deferred()
    fetchDefault.mockReturnValueOnce(pending.promise)
    selected.value = { profile: 'A', model: 'two' }
    scope.stop()
    pending.resolve({ profile: 'A', model: 'two', status: 'resolved', effectiveEffort: 'low' })
    await flushPromises()
    expect(result.value).toBeNull()
  })
})
