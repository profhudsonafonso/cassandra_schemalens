#!/usr/bin/env python3
"""Fase 2 orchestrator: runs preregistered steps in order, one process each, and stops at the
first failure. Completed steps (evidence file present) are skipped, so relaunching resumes.

    python fase2_run.py --stages e1         # E1 (r2 then r3)
    python fase2_run.py --stages e2 e3 e4 e5 e6
After every step the dashboard (experiments/rev3_fase2/dashboard.html) is regenerated.
"""
from __future__ import annotations

import argparse
import json
import os
import signal
import socket
import threading
import subprocess
import sys
import time
import datetime as dt
from pathlib import Path

PROJECT = Path(__file__).resolve().parent
F2 = PROJECT / 'experiments' / 'rev3_fase2'
STATUS = F2 / 'status.json'
LOGS = F2 / 'logs'
PY = sys.executable
CORE = [PY, '-u', 'fase2_core.py']
REV4 = [PY, '-u', 'fase2_rev4.py']
ANALYSIS = [PY, '-u', 'fase2_analysis.py']
CORE3 = [PY, '-u', 'fase3_core.py']
REV5 = [PY, '-u', 'fase3_rev5.py']
AN3 = [PY, '-u', 'fase3_analysis.py']
CORE4 = [PY, '-u', 'fase4_core.py']
F4 = [PY, '-u', 'fase4.py']
MAT, FLU = '--confirm=CREATE_NEW_F2_KEYSPACES', '--confirm=FLUSH_F2_SST_ONLY'


def d(c, ds):
    return F2 / c / ds


def campaign_steps(c, datasets, materialize=True, core=CORE):
    s = []
    if materialize:
        for ds in datasets:
            s.append((f'{c}:mat:{ds}', f'Materializar {ds}', core + ['materialize', '--campaign', c, '--dataset', ds, MAT],
                      d(c, ds) / 'materialization' / 'materialization_manifest.json'))
        for ds in datasets:
            s.append((f'{c}:flush:{ds}', f'Flush + gate {ds}', core + ['flush', '--campaign', c, '--dataset', ds, FLU],
                      d(c, ds) / 'flush_manifest.json'))
        for ds in datasets:
            s.append((f'{c}:val:{ds}', f'Validação semântica {ds}', core + ['validate', '--campaign', c, '--dataset', ds],
                      d(c, ds) / 'semantic_validation.json'))
    return s


def run_steps(c, datasets, core=CORE):
    s = [(f'{c}:prereg', 'Pré-registro', core + ['preregister', '--campaign', c], F2 / c / 'preregistration.json')]
    for ds in datasets:
        s.append((f'{c}:run:{ds}', f'Medição {ds}', core + ['run', '--campaign', c, '--dataset', ds],
                  d(c, ds) / 'post_timing_manifest.json'))
        s.append((f'{c}:audit:{ds}', f'Auditoria {ds}', core + ['audit', '--campaign', c, '--dataset', ds],
                  d(c, ds) / 'benchmark' / 'audit.json'))
    return s


E1D = ['d1', 'd2', 'd4', 'd8']


def per_dataset_steps(c, datasets):
    """Amendment 01: load, gate, validate, preregister, time, audit and retire one factor at a time."""
    s = []
    for ds in datasets:
        s += campaign_steps(c, [ds])
        s.append((f'{c}:prereg:{ds}', f'Pré-registro {ds}', CORE + ['preregister', '--campaign', c, '--dataset', ds],
                  F2 / c / f'preregistration_{ds}.json'))
        s += run_steps(c, [ds])[1:]
        s.append((f'{c}:retire:{ds}', f'Aposentar memtables {ds}', CORE + ['retire', '--campaign', c, '--dataset', ds],
                  d(c, ds) / 'retire_manifest.json'))
    return s


STAGES = {
    'e1': [('E1', (f'e1r2:retire:{ds}', f'Aposentar e1r2 abortada {ds}', CORE + ['retire', '--campaign', 'e1r2', '--dataset', ds],
                   d('e1r2', ds) / 'retire_manifest.json')) for ds in E1D]
    + [('E1', st) for c in ('e1r2b', 'e1r3') for st in per_dataset_steps(c, E1D)]
    + [('E1', ('e1:analysis', 'Análise E1', ANALYSIS + ['e1'], F2 / 'analysis' / 'e1_summary.json'))],
    'e2': [('E2', ('e2:fit', 'Calibrar e congelar Rev4', REV4 + ['fit'], F2 / 'rev4' / 'rev4_frozen.json'))],
    'e3': [('E3', st) for st in campaign_steps('e3', ['h1', 'h2', 'h3'])]
    + [('E3', ('e3:predict', 'Previsões Rev4 (antes de medir)', REV4 + ['predict', 'e3'], F2 / 'rev4' / 'predictions_e3.json'))]
    + [('E3', st) for st in run_steps('e3', ['h1', 'h2', 'h3'])],
    'e4': [('E4', st) for st in campaign_steps('e4', ['n3', 'n18'])]
    + [('E4', ('e4:predict', 'Previsões Rev4 (antes de medir)', REV4 + ['predict', 'e4'], F2 / 'rev4' / 'predictions_e4.json'))]
    + [('E4', st) for st in run_steps('e4', ['n3', 'n18'])],
    'e5': [('E5', ('e5:analysis', 'Avaliar Rev4 + análise E3/E4', ANALYSIS + ['e345'], F2 / 'analysis' / 'e345_summary.json'))],
    'e6': [('E6', st) for st in run_steps('e6', ['h1', 'h3'])]
    + [('E6', ('e6:analysis', 'Análise cache frio', ANALYSIS + ['e6'], F2 / 'analysis' / 'e6_summary.json'))],
    # ---- Fase 3 (EMENDA_04) ----
    'e7': [('E7', st) for c, dss in (('e3r2', ['h1', 'h2', 'h3']), ('e4r2', ['n3', 'n18']))
           for st in campaign_steps(c, dss, core=CORE3) + run_steps(c, dss, core=CORE3)]
    + [('E7', ('e7:analysis', 'Análise das réplicas', AN3 + ['e7'], F2 / 'analysis' / 'e7_summary.json'))],
    'e8': [('E8', ('e8:fit', 'Calibrar e congelar Rev5', REV5 + ['fit'], F2 / 'rev5' / 'rev5_frozen.json'))],
    'e9': [('E9', st) for st in campaign_steps('e9', ['h2b', 'h3b', 'n6', 'p1'], core=CORE3)]
    + [('E9', ('e9:predict4', 'Previsões Rev4 (antes de medir)', REV5 + ['predict-rev4', 'e9'], F2 / 'rev4' / 'predictions_e9.json')),
       ('E9', ('e9:predict', 'Previsões Rev5 (antes de medir)', REV5 + ['predict', 'e9'], F2 / 'rev5' / 'predictions_e9.json'))]
    + [('E9', st) for st in run_steps('e9', ['h2b', 'h3b', 'n6', 'p1'], core=CORE3)],
    'e10': [('E10', ('e10:trace', 'Tracing do servidor (amostra)', [PY, '-u', 'fase3_trace.py', 'run'], F2 / 'e10' / 'trace_summary.json'))],
    'e11': [('E11', ('e11:analysis', 'Avaliar Rev5 + análise E9', AN3 + ['e11'], F2 / 'analysis' / 'e11_summary.json'))],
    'e12': [('E12', ('e12:draft', 'Rascunho de resultados (dos JSON)', AN3 + ['draft'], F2 / 'analysis' / 'draft_summary.json'))],
    # ---- Fase 4 (EMENDA_05) ----
    'e13': [('E13', ('e13:record', 'Registrar a correção do E10 (hashes)', F4 + ['e13'], F2 / 'e13' / 'e10_correction_record.json'))],
    'e14': [('E14', ('e14:freeze', 'Congelar a regra T5 (zona de empate)', F4 + ['freeze-rule'], F2 / 'rev5' / 'tie_rule_frozen.json')),
            ('E14', ('e14:analysis', 'T5 no holdout E9 (exploratório)', F4 + ['e14'], F2 / 'analysis' / 'e14_summary.json'))],
    'e15': [('E15', st) for st in campaign_steps('e9r2', ['h2b', 'h3b', 'n6', 'p1'], core=CORE4)]
    + [('E15', ('e9r2:predict4', 'Previsões Rev4 (antes de medir)', F4 + ['predict4', 'e9r2'], F2 / 'rev4' / 'predictions_e9r2.json')),
       ('E15', ('e9r2:predict', 'Previsões Rev5 (antes de medir)', F4 + ['predict5', 'e9r2'], F2 / 'rev5' / 'predictions_e9r2.json')),
       ('E15', ('e9r2:predicttie', 'Previsões T5 (antes de medir)', F4 + ['predict-tie', 'e9r2'], F2 / 'rev5' / 'tie_predictions_e9r2.json'))]
    + [('E15', st) for st in run_steps('e9r2', ['h2b', 'h3b', 'n6', 'p1'], core=CORE4)],
    'e16': [('E16', ('e16:trace', 'Tracing do P1 (1 e 7 dias, corrigido)', [PY, '-u', 'fase4_trace.py', 'run'], F2 / 'e16' / 'trace_summary.json'))],
    'e17': [('E17', ('e17:analysis', 'Teste confirmatório T5 + réplica E9', F4 + ['e17'], F2 / 'analysis' / 'e17_summary.json'))],
    'e18': [('E18', ('e18:pack', 'Zip das Fases 3+4 e rascunho', F4 + ['e18'], F2 / 'analysis' / 'e18_summary.json'))],
}


def now():
    return dt.datetime.now().astimezone().isoformat(timespec='seconds')


def load_status():
    try:
        return json.loads(STATUS.read_text())
    except Exception:
        return dict(steps={}, runs=[])


_SAVE_LOCK = threading.Lock()
HEARTBEAT_SECONDS = 60  # the dashboard flags the run as stale after ~2.5 heartbeats without a write


def save_status(st):
    with _SAVE_LOCK:
        if (st.get('current_run') or {}).get('state') == 'RUNNING':
            st['current_run']['heartbeat'] = now()
        tmp = STATUS.with_suffix('.tmp')
        tmp.write_text(json.dumps(st, indent=2, ensure_ascii=False))
        tmp.replace(STATUS)
        subprocess.run([PY, 'fase2_dashboard.py'], cwd=PROJECT, capture_output=True)


def start_heartbeat(st):
    """Rewrite status + dashboard every minute, so a silent step still shows the process is alive."""
    def beat():
        while True:
            time.sleep(HEARTBEAT_SECONDS)
            try:
                save_status(st)
            except RuntimeError:  # dict mutated by the main thread mid-dump; next beat will do
                pass
    threading.Thread(target=beat, daemon=True).start()


def _on_signal(signum, _frame):
    raise SystemExit(f'interrompido pelo sinal {signal.Signals(signum).name}')


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--stages', nargs='+', required=True, choices=list(STAGES))
    a = ap.parse_args()
    LOGS.mkdir(parents=True, exist_ok=True)
    st = load_status()
    plan = [(stage, *step) for s in a.stages for stage, step in STAGES[s]]
    st['current_run'] = dict(stages=a.stages, started=now(), state='RUNNING', error=None,
                             pid=os.getpid(), host=socket.gethostname())
    for sig in (signal.SIGTERM, signal.SIGHUP):
        signal.signal(sig, _on_signal)
    for stage, sid, title, cmd, done, in plan:
        st['steps'].setdefault(sid, dict(stage=stage, title=title, state='pending'))
    save_status(st)
    start_heartbeat(st)
    try:
        execute(st, plan)
    except (KeyboardInterrupt, SystemExit) as exc:
        if st['current_run'].get('state') == 'RUNNING':  # sys.exit(1) after a FAILED step is already recorded
            for rec in st['steps'].values():
                if rec['state'] == 'running':
                    rec.update(state='interrupted', ended=now())
            reason = str(exc) if isinstance(exc, SystemExit) and exc.code not in (None, 0, 1) else 'interrompido (Ctrl-C)'
            st['current_run'].update(state='STOPPED', error=reason, ended=now())
            save_status(st)
        raise
    st['current_run'].update(state='COMPLETE', ended=now(), current=None)
    save_status(st)
    print('COMPLETE', a.stages)


PARTIAL_RUN_OUTPUTS = ('benchmark', 'storage_pre_timing', 'storage_post_timing')


def quarantine_partial_run(st, sid, done):
    """Amendment 02: a measurement that ended without its post-timing manifest is discarded whole. Its partial
    outputs are moved (never deleted) to aborted_attempts/ and the measurement is repeated from the pre-timing gate."""
    folder = Path(done).parent
    found = [folder / n for n in PARTIAL_RUN_OUTPUTS if (folder / n).exists()]
    if not found:
        return
    k = len(list((folder / 'aborted_attempts').glob('*'))) + 1 if (folder / 'aborted_attempts').exists() else 1
    dest = folder / 'aborted_attempts' / f'{k:02d}_{dt.datetime.now().strftime("%Y%m%dT%H%M%S")}'
    dest.mkdir(parents=True)
    for f in found:
        f.rename(dest / f.name)
    files = sum(1 for _ in dest.rglob('*') if _.is_file())
    (dest / 'README.txt').write_text(f'Tentativa interrompida de {sid} (EMENDA_02). Saídas parciais preservadas, '
                                     f'não usadas em nenhuma análise. {files} arquivos movidos em {now()}.\n')
    st.setdefault('notes', []).append(f'{sid}: medição interrompida descartada; saídas parciais movidas para '
                                      f'{dest.relative_to(F2)} e medição refeita do zero (EMENDA_02).')
    print('QUARANTINED', sid, '->', dest, flush=True)


def execute(st, plan):
    for stage, sid, title, cmd, done in plan:
        rec = st['steps'][sid]
        if Path(done).exists():
            if rec['state'] != 'done':
                rec.update(state='done', note='evidência já existente')
            continue
        if ':run:' in sid:
            quarantine_partial_run(st, sid, done)
        if ':prereg' in sid:  # campaigns that reuse another campaign's keyspaces (E6) have no folder yet
            Path(done).parent.mkdir(parents=True, exist_ok=True)
        rec.update(state='running', started=now(), last='')
        st['current_run']['current'] = sid
        save_status(st)
        t0 = time.time()
        log = LOGS / (sid.replace(':', '_') + '.log')
        with log.open('a') as lf, subprocess.Popen(cmd, cwd=PROJECT, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                                   text=True) as p:
            last_save = 0
            for line in p.stdout:
                lf.write(line)
                lf.flush()
                rec['last'] = line.strip()[-200:]
                if time.time() - last_save > 30:
                    save_status(st)
                    last_save = time.time()
            rc = p.wait()
        rec['seconds'] = round(time.time() - t0, 1)
        rec['ended'] = now()
        if rc != 0 or not Path(done).exists():
            rec['state'] = 'failed'
            tail = log.read_text().splitlines()[-15:]
            st['current_run'].update(state='FAILED', error=f'{sid} rc={rc}\n' + '\n'.join(tail), ended=now())
            save_status(st)
            print('FAILED', sid, '\n'.join(tail))
            sys.exit(1)
        rec['state'] = 'done'
        save_status(st)
        print('DONE', sid, rec['seconds'], 's', flush=True)


if __name__ == '__main__':
    main()
