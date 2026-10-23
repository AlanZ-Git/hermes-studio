import type { HermesSessionRow } from '../../repositories/session-store'

type LoopSession = Partial<HermesSessionRow>

export function isHermesLoopSession(row: LoopSession | null | undefined): row is LoopSession {
  return Boolean(row && ['cli', 'api_server'].includes(row.source || '')
    && (!row.agent || row.agent === 'hermes')
    && !['agent_mode', 'agent_preset', 'agent_session_id', 'agent_native_session_id']
      .some(key => row[key as keyof LoopSession]))
}
