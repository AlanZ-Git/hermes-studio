import { startWithMaintenanceGate } from './bootstrap/maintenance'

void startWithMaintenanceGate().catch(error => {
  console.error('[bootstrap] startup failed', error)
  process.exit(1)
})
