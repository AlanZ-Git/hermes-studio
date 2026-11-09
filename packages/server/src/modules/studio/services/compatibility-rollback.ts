export function compatibilityRollbackProtected(): boolean {
  return ['1', 'true', 'on'].includes((process.env.HERMES_STUDIO_COMPATIBILITY_ROLLBACK || '').toLowerCase())
}

export function requireCompatibleConsumer(): void {
  if (compatibilityRollbackProtected()) throw new Error('Compatibility rollback: automatic consumers and scheduling paused')
}
