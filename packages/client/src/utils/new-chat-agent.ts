import { AGENT_OPTIONS } from './agent-options'

export const DEFAULT_NEW_CHAT_AGENT = 'hermes' as const
export const NEW_CHAT_AGENT_STORAGE_KEY = 'hermes_new_chat_agent_v1'

export function resolveNewChatAgent(saved: unknown) {
  return AGENT_OPTIONS.find(option => option.value === saved)?.value || DEFAULT_NEW_CHAT_AGENT
}

export function loadNewChatAgent() {
  try {
    return resolveNewChatAgent(localStorage.getItem(NEW_CHAT_AGENT_STORAGE_KEY))
  } catch {
    return DEFAULT_NEW_CHAT_AGENT
  }
}
