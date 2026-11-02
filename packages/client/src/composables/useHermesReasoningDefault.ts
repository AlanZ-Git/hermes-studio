import { computed, onScopeDispose, shallowRef, watch, type Ref } from 'vue'
import { fetchReasoningDefault, type ReasoningDefault } from '@/api/studio/sessions'

export function useHermesReasoningDefault(selection: Ref<{ profile: string; model: string } | null>) {
  const result = shallowRef<ReasoningDefault | null>(null)
  let generation = 0
  watch(selection, async selected => {
    const current = ++generation
    result.value = null
    if (!selected) return
    try {
      const response = await fetchReasoningDefault(selected.profile, selected.model)
      if (current === generation && response.profile === selected.profile && response.model === selected.model) result.value = response
    } catch {
      if (current === generation) result.value = { ...selected, status: 'unknown', effectiveEffort: null }
    }
  }, { immediate: true, flush: 'sync' })
  onScopeDispose(() => { generation++ })
  return computed(() => result.value)
}
