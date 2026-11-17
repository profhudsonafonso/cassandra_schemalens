#!/usr/bin/env python3
"""SchemaLens Rev3 Fase 4 (EMENDA_05): adds campaign e9r2 (replica of the E9 holdout) to the unchanged engine.

    python fase4_core.py materialize|flush|validate|preregister|run|audit --campaign e9r2 [--dataset D] [--confirm ...]

fase2_core.py and fase3_core.py are imported, never modified. This module's hash, EMENDA_05 and the frozen tie rule are
written next to the preregistration (preregistration_f4.json) and re-checked before every measurement.
"""
from __future__ import annotations

import sys
from pathlib import Path

import fase3_core  # noqa: F401  (Fase 3 campaigns + P1 case subset)
import fase2_core as C

HERE = Path(__file__).resolve()
F2 = C.F2
AMENDMENT = F2 / 'EMENDA_05_FASE4.md'
REV5 = F2 / 'rev5'

NEW = {
    'e9r2': dict(stage='E15', datasets=['h2b', 'h3b', 'n6', 'p1'], conds=['sst'], seed=20261601, rounds=60, rev4=True, rev5=True),
}
for k in NEW:
    if k in C.CAMPAIGNS:
        raise ValueError('campaign already defined: ' + k)
C.CAMPAIGNS.update(NEW)


def guarded(campaign):
    return {'rev5_frozen': REV5 / 'rev5_frozen.json', 'tie_rule': REV5 / 'tie_rule_frozen.json',
            'rev5_predictions': REV5 / f'predictions_{campaign}.json', 'tie_predictions': REV5 / f'tie_predictions_{campaign}.json'}


def sidecar(campaign):
    return C.cdir(campaign) / 'preregistration_f4.json'


def preregister(campaign):
    side = sidecar(campaign)
    if side.exists():
        raise FileExistsError(side)
    files = guarded(campaign)
    missing = [str(p) for p in files.values() if not p.exists()]
    if missing:
        raise FileNotFoundError('must exist before preregistering: ' + ', '.join(missing))
    rec = dict(status='F4_PREREGISTRATION_SIDECAR', campaign=campaign, created_utc=C.now_iso(),
               fase4_core_sha256=C.digest(HERE), fase3_core_sha256=C.digest(Path(fase3_core.__file__).resolve()),
               amendment_sha256=C.digest(AMENDMENT), files={k: C.digest(p) for k, p in files.items()})
    side.parent.mkdir(parents=True, exist_ok=True)
    C.cmd_preregister(campaign)
    C.write_new(side, rec)
    C.log('PASS Fase 4 sidecar', side)


def verify_sidecar(campaign):
    s = C.load(sidecar(campaign))
    if s['fase4_core_sha256'] != C.digest(HERE) or s['amendment_sha256'] != C.digest(AMENDMENT):
        raise ValueError('fase4_core.py or EMENDA_05 changed after preregistration')
    if s['fase3_core_sha256'] != C.digest(Path(fase3_core.__file__).resolve()):
        raise ValueError('fase3_core.py changed after preregistration')
    for k, h in s['files'].items():
        if C.digest(guarded(campaign)[k]) != h:
            raise ValueError(k + ' changed after preregistration')


def main():
    args = sys.argv[1:]
    camp = args[args.index('--campaign') + 1] if '--campaign' in args else None
    if camp not in NEW:
        sys.exit('fase4_core.py only handles: ' + ', '.join(NEW))
    if args[:1] == ['preregister']:
        return preregister(camp)
    if args[:1] == ['run']:
        verify_sidecar(camp)
    C.main()


if __name__ == '__main__':
    main()
