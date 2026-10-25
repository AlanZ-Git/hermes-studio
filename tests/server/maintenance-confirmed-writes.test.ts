import { mkdtempSync, writeFileSync, chmodSync, readFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { describe, expect, it } from 'vitest'
import { prepareState, stateImage } from '../../packages/server/src/bootstrap/maintenance-state'

describe('maintenance producer only publishes staged confirmed deltas', () => {
  it('never admits a foreign live byte or mode change on migration failure', async () => {
    for (const mode of [false,true]) {
      const root=mkdtempSync(join(tmpdir(),'owner-web-'));const target=join(root,'hermes-web-ui.db');writeFileSync(target,'original');chmodSync(target,0o640)
      const writes={}
      await expect(prepareState(root,[target],writes,() => { if(mode) chmodSync(target,0o600);else writeFileSync(target,'foreign');throw new Error('migration failed') })).rejects.toThrow('migration failed')
      expect(writes).toEqual({})
      expect(mode ? (await stateImage(target) as any).mode : readFileSync(target,'utf8')).toEqual(mode ? 0o600 : 'foreign')
    }
  })
  it('publishes only real deltas with current owner metadata, leaving unwritten paths out', async () => {
    const root=mkdtempSync(join(tmpdir(),'owner-web-ok-'));const a=join(root,'hermes-web-ui.db');const b=a+'-wal';writeFileSync(a,'before');chmodSync(a,0o640)
    const writes: Record<string,unknown>={}
    await prepareState(root,[a,b],writes,stage => writeFileSync(join(stage,'hermes-web-ui.db'),'migrated'))
    expect(Object.keys(writes)).toEqual([a]);expect(writes[a]).toEqual(await stateImage(a));expect((writes[a] as any).mode).toBe(0o640)
    expect(writes[b]).toBeUndefined()
  })
})
