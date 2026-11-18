"""Passo 2 (servidor) - hashes de referencia pelo ORACULO INDEPENDENTE da V9 (schemalens_v9/oracle_v9.py).

No Mac a corretude da V9 usava os hashes das respostas da V8 (repositorio da V8). Aqui, com dados
regenerados, a referencia e o oraculo da V9: avalia o contrato E6 das 36 instrucoes sobre o estado-fonte
(model_state do load_raw_state, o mesmo estado dos adaptadores), simulando aquecimento e escritas na mesma
ordem do executor. Saida no formato de phase6.reference_hashes(): {(carga, repeticao, requisicao, instrucao): hash}.
"""
from __future__ import annotations

import argparse
import pickle
import time

from common_paths import ARMS, FROZEN, MEASURED, REPS, WARMUP, WORKLOADS, dump, read, sha, use_target_port


def compute(state, reps=REPS, warmup=WARMUP, measured=MEASURED, progress=None):
    from schemalens_v9 import oracle_v9 as O
    from schemalens_v9.phase1 import _benchmark_domain
    from reused.methodology.semantic_replay_011 import hash_value
    from reused.methodology.workload_005 import histories
    dom = _benchmark_domain()
    ref = {}
    for w in WORKLOADS:
        for rep in range(reps):
            st = O.fresh(state)
            for phase, n in (('warmup', warmup), ('measured', measured)):
                for req in histories(w, rep, n, dom, 'v9', phase=phase):
                    ctx = {}
                    for sid in req['constituent_statement_ids']:
                        exp = O.evaluate(st, sid, req['parameters'], ctx)
                        if phase == 'measured':
                            ref[(w, rep, req['logical_request_id'], sid)] = hash_value(exp)
            if progress:
                progress(w, rep)
    return ref


def state_from(arm):
    use_target_port(ARMS[arm]['port'])
    from schemalens_v9 import phase5_run as PR
    raw, ids, state = PR.cohort()
    return state


def main():
    ap = argparse.ArgumentParser(); ap.add_argument('--arm', default='v41')
    a = ap.parse_args()
    out = FROZEN / 'ORACLE_REF.pkl'
    if out.exists():
        raise FileExistsError('ORACLE_REF.pkl ja existe')
    t0 = time.time()
    state = state_from(a.arm)
    ref = compute(state, progress=lambda w, r: print(f'oraculo: {w} repeticao {r + 1}/{REPS} ({time.time() - t0:.0f} s)', flush=True))
    out.write_bytes(pickle.dumps(ref, protocol=4))
    dump(FROZEN / 'ORACLE_REF_MANIFEST.json', {'oracle': 'schemalens_v9/oracle_v9.py (contrato E6 das 36 instrucoes)', 'state_from_arm': a.arm,
                                               'entries': len(ref), 'reps': REPS, 'warmup': WARMUP, 'measured': MEASURED,
                                               'sha256': sha(out), 'seconds': round(time.time() - t0, 1)})
    print('ORACULO OK', len(ref), 'respostas esperadas')


if __name__ == '__main__':
    main()
