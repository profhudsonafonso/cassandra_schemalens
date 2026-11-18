#!/usr/bin/env python3
"""SchemaLens Rev3 Fase 3 (EMENDA_04): adds campaigns E7 (replicas) and E9 (new holdout + big partitions) to the
unchanged Fase 2 engine. Same CLI as fase2_core.py:

    python fase3_core.py materialize|flush|validate|preregister|run|audit --campaign C [--dataset D] [--confirm ...]

fase2_core.py is imported, never modified (its hash stays the one in every preregistration). This module's own hash is
written next to each Fase 3 preregistration (preregistration*_f3.json) and re-checked before every measurement.
"""
from __future__ import annotations

import sys
from pathlib import Path

import fase2_core as C

HERE = Path(__file__).resolve()
F2 = C.F2
AMENDMENT = F2 / 'EMENDA_04_FASE3.md'

NEW = {
    'e3r2': dict(stage='E7', datasets=['h1', 'h2', 'h3'], conds=['sst'], seed=20261311, rounds=60),
    'e4r2': dict(stage='E7', datasets=['n3', 'n18'], conds=['sst'], seed=20261411, rounds=60),
    'e9': dict(stage='E9', datasets=['h2b', 'h3b', 'n6', 'p1'], conds=['sst'], seed=20261501, rounds=60, rev4=True, rev5=True),
}
for k in NEW:
    if k in C.CAMPAIGNS:
        raise ValueError('campaign already defined in fase2_core: ' + k)
C.CAMPAIGNS.update(NEW)

# P1 (EMENDA_04): drop the whole-history control and the 330-day window, which would read ~480k rows per query.
P1_DROP = ('Q5_CONTROL',)
_orig_cases_for = C.cases_for


def cases_for(ds):
    out = _orig_cases_for(ds)
    if ds == 'p1':
        out = [q for q in out if q['s1_case_id'] not in P1_DROP and (q['s1_window_days'] or 0) <= 80]
        for q in out:
            q['f3_case_subset'] = 'EMENDA_04: P1 keeps 1/7/14/21/80-day windows and Q8_CONTROL'
    return out


C.cases_for = cases_for


def sidecar(campaign, ds=None):
    c = C.CAMPAIGNS[campaign]
    return C.cdir(campaign) / (f'preregistration_{ds}_f3.json' if c.get('per_dataset') else 'preregistration_f3.json')


def rev5_paths(campaign):
    return F2 / 'rev5' / 'rev5_frozen.json', F2 / 'rev5' / f'predictions_{campaign}.json'


def preregister(campaign, ds=None):
    c = C.CAMPAIGNS[campaign]
    if campaign not in NEW:
        return C.cmd_preregister(campaign, ds)
    side = sidecar(campaign, ds)
    if side.exists():
        raise FileExistsError(side)
    rec = dict(status='F3_PREREGISTRATION_SIDECAR', campaign=campaign, created_utc=C.now_iso(),
               fase3_core_sha256=C.digest(HERE), amendment_sha256=C.digest(AMENDMENT))
    if c.get('rev5'):
        frozen, pred = rev5_paths(campaign)
        if not frozen.exists() or not pred.exists():
            raise FileNotFoundError('Rev5 must be frozen and its predictions written before preregistering ' + campaign)
        rec.update(rev5_frozen_sha256=C.digest(frozen), rev5_predictions_sha256=C.digest(pred))
    side.parent.mkdir(parents=True, exist_ok=True)
    C.cmd_preregister(campaign, ds)          # writes the engine preregistration (core hash, inputs, Rev4, amendments)
    C.write_new(side, rec)
    C.log('PASS Fase 3 sidecar', side)


def verify_sidecar(campaign, ds):
    if campaign not in NEW:
        return
    s = C.load(sidecar(campaign, ds))
    if s['fase3_core_sha256'] != C.digest(HERE) or s['amendment_sha256'] != C.digest(AMENDMENT):
        raise ValueError('fase3_core.py or EMENDA_04 changed after preregistration')
    if C.CAMPAIGNS[campaign].get('rev5'):
        if s['rev5_predictions_sha256'] != C.digest(rev5_paths(campaign)[1]):
            raise ValueError('Rev5 predictions changed after preregistration')


def main():
    args = sys.argv[1:]
    if args[:1] == ['preregister']:
        camp = args[args.index('--campaign') + 1]
        ds = args[args.index('--dataset') + 1] if '--dataset' in args else None
        return preregister(camp, ds)
    if args[:1] == ['run']:
        camp = args[args.index('--campaign') + 1]
        verify_sidecar(camp, args[args.index('--dataset') + 1])
    C.main()


if __name__ == '__main__':
    main()
