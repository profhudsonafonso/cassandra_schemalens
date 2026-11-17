#!/usr/bin/env python3
"""SchemaLens Rev3 S3 - automatic orchestrator with a live HTML dashboard.

Runs every s3_physical.py step in the preregistered order, as separate processes,
stopping at the first failure. Completed steps (detected by their evidence files)
are skipped, so re-launching resumes safely; nothing is ever overwritten.

    python s3_run_all.py            # run everything
    python s3_run_all.py --dry-run  # only (re)write the dashboard with the plan

Dashboard: experiments/rev3_s3_physical_storage/dashboard.html
(open it in a browser; it reloads itself every 30 seconds).
"""
from __future__ import annotations

import argparse
import html
import json
import os
import re
import subprocess
import sys
import threading
import time
import datetime as dt
from pathlib import Path

PROJECT = Path(__file__).resolve().parent
S3 = PROJECT / 'experiments' / 'rev3_s3_physical_storage'
CAMP = S3 / 'campaign_r1'
LOGS = S3 / 'logs'
STATUS = S3 / 'status.json'
DASH = S3 / 'dashboard.html'
LOCK = S3 / '.orchestrator.lock'
PY = sys.executable
RUN = [PY, '-u', 's3_physical.py']


def step(sid, phase, title, what, est, args, done):
    return dict(id=sid, phase=phase, title=title, what=what, est=est, args=args, done=str(done),
                state='pending', started=None, ended=None, seconds=None, progress=0.0, last='')


MAT_EST = {1: 30, 2: 45, 4: 95, 8: 190}
VAL_EST = {1: 15, 2: 20, 4: 35, 8: 60}
RUN_EST = {1: 60, 2: 100, 4: 195, 8: 420}

STEPS = [
    step('preflight', '0 · Preflight', 'Preflight somente leitura',
         'Verifica hashes S2, gate do B congelado, Cassandra 4.1.12 e ausência de sl_s3_*; salva o ambiente.',
         20, ['preflight'], S3 / 'preflight' / 'environment_before.json'),
    step('tests', '1 · Offline', 'Testes de segurança offline',
         'Testes unitários: nenhum DROP/TRUNCATE, regex de keyspace, whitelist do nodetool, sessão protegida.',
         10, None, S3 / 'logs' / 'tests.ok'),
    step('draft', '1 · Offline', 'Congelar rascunho do protocolo',
         'Grava s3_preregister_draft.json com o hash do protocolo, seed, ordem e gate antes de qualquer DDL.',
         10, ['draft'], S3 / 's3_preregister_draft.json'),
]
for f in (1, 2, 4, 8):
    STEPS.append(step(f'mat{f}', '2 · Materialização', f'Materializar {f}×',
                      f'Cria sl_s3_r1_d{f}_ref/u/d (CREATE-only) e carrega {27000 * f:,} wide rows em cada.',
                      MAT_EST[f], ['materialize', '--factor', str(f), '--confirm', 'CREATE_NEW_S3_KEYSPACES'],
                      CAMP / f'{f}x' / 'materialization' / 'materialization_manifest.json'))
for f in (1, 2, 4, 8):
    STEPS.append(step(f'flush{f}', '3 · Flush + gate físico', f'Flush e gate {f}×',
                      'nodetool flush só nas 3 keyspaces S3; espera compactação quiescer; exige SSTable ≥ 1 e memtable = 0.',
                      30, ['flush', '--factor', str(f), '--confirm', 'FLUSH_S3_KEYSPACES_ONLY'],
                      CAMP / f'{f}x' / 'flush_manifest.json'))
for f in (1, 2, 4, 8):
    STEPS.append(step(f'val{f}', '4 · Validação semântica', f'Validar {f}× (54 casos)',
                      'Oráculo source_oracle() para 18 casos × 3 variantes; traço de acesso deve ser idêntico ao S2.',
                      VAL_EST[f], ['validate', '--factor', str(f)], CAMP / f'{f}x' / 'semantic_validation.json'))
STEPS.append(step('prereg', '5 · Pré-registro', 'Pré-registro final',
                  'Congela hashes de código, protocolo, manifests e snapshots de armazenamento ANTES de cronometrar.',
                  10, ['preregister'], S3 / 's3_benchmark_preregister.json'))
for f in (1, 2, 4, 8):
    STEPS.append(step(f'run{f}', '6 · Benchmark', f'Benchmark {f}× (3.240 medições)',
                      'Snapshot pre-timing + gate; 54 oráculos; 270 warm-ups; 60 rodadas aleatorizadas (seed do S2); snapshot pós.',
                      RUN_EST[f], ['run', '--factor', str(f)], CAMP / f'{f}x' / 'post_timing_manifest.json'))
for f in (1, 2, 4, 8):
    STEPS.append(step(f'audit{f}', '7 · Auditoria', f'Auditar {f}×',
                      'Recalcula p95/p99 a partir das latências brutas, confere hashes, posições e ordem idêntica ao S2.',
                      8, ['audit', '--factor', str(f)], CAMP / f'{f}x' / 'benchmark' / 'audit.json'))
STEPS += [
    step('auditall', '7 · Auditoria', 'Auditoria completa', 'Re-audita os 4 fatores e grava all_density_comparison.csv.',
         20, ['audit-all'], S3 / 'all_density_comparison.csv'),
    step('compare', '8 · Análise', 'Comparação S3 × S2', 'Comparação pareada por fator/caso/variante + resumo do armazenamento.',
         10, ['compare-s2'], S3 / 's3_summary.json'),
    step('report', '8 · Análise', 'Relatório final', 'Gera FINAL_AUDIT_PT.md com tabelas e ameaças à validade.',
         5, ['report'], S3 / 'FINAL_AUDIT_PT.md'),
]

state = dict(status='PLANNED', started=None, ended=None, current=None, error=None, log_tail=[])
lock = threading.Lock()


def now():
    return time.time()


def fmt_dur(s):
    if s is None:
        return '—'
    s = int(round(s))
    return f'{s // 60}m{s % 60:02d}s' if s >= 60 else f'{s}s'


def fmt_ts(t):
    return dt.datetime.fromtimestamp(t).strftime('%H:%M:%S') if t else '—'


def eta_seconds():
    rem = 0.0
    for s in STEPS:
        if s['state'] == 'pending':
            rem += s['est']
        elif s['state'] == 'running':
            rem += max(s['est'] * (1 - s['progress']), s['est'] - (now() - s['started'])) if s['progress'] < 1 else 0
            rem = max(rem, 0)
    return rem


def overall_fraction():
    total = sum(s['est'] for s in STEPS)
    done = sum(s['est'] for s in STEPS if s['state'] in ('done', 'skipped'))
    run = sum(s['est'] * s['progress'] for s in STEPS if s['state'] == 'running')
    return (done + run) / total


def load_json(p):
    try:
        return json.loads(Path(p).read_text(encoding='utf8'))
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Dashboard rendering
# ---------------------------------------------------------------------------
CSS = """
:root{--bg:#f7f7f5;--card:#ffffff;--ink:#1d1d1b;--mute:#6b6b66;--line:#e3e2dc;--accent:#2f6f9f;
--ok:#2e7d4f;--okbg:#e6f3ea;--run:#9a6700;--runbg:#fff4d6;--bad:#b42318;--badbg:#fde8e6;--pend:#8a8a84;--pendbg:#f0efeb;--bar:#2f6f9f}
@media (prefers-color-scheme:dark){:root:not([data-theme="light"]){--bg:#151514;--card:#1f1f1d;--ink:#ecebe6;--mute:#a3a29b;--line:#34342f;
--accent:#7fb4dc;--ok:#7fd1a0;--okbg:#18301f;--run:#f0c060;--runbg:#3a2f12;--bad:#ff8a7f;--badbg:#3c1915;--pend:#9a9a93;--pendbg:#2a2a27;--bar:#7fb4dc;color-scheme:dark}}
:root[data-theme="dark"]{--bg:#151514;--card:#1f1f1d;--ink:#ecebe6;--mute:#a3a29b;--line:#34342f;--accent:#7fb4dc;--ok:#7fd1a0;--okbg:#18301f;
--run:#f0c060;--runbg:#3a2f12;--bad:#ff8a7f;--badbg:#3c1915;--pend:#9a9a93;--pendbg:#2a2a27;--bar:#7fb4dc;color-scheme:dark}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--ink);font:14px/1.5 system-ui,-apple-system,"Segoe UI",sans-serif}
.wrap{max-width:1180px;margin:0 auto;padding-block:24px 48px;padding-inline:16px}
h1{font-size:22px;margin:0 0 4px}h2{font-size:15px;margin:28px 0 10px;text-transform:uppercase;letter-spacing:.06em;color:var(--mute)}
.sub{color:var(--mute);margin:0 0 18px}
.kpis{display:grid;grid-template-columns:repeat(auto-fit,minmax(170px,1fr));gap:10px}
.kpi{background:var(--card);border:1px solid var(--line);border-radius:10px;padding:12px 14px}
.kpi b{display:block;font-size:22px;font-variant-numeric:tabular-nums}.kpi span{color:var(--mute);font-size:12px}
.bar{height:10px;background:var(--pendbg);border-radius:6px;overflow:hidden;margin:14px 0 4px}
.bar i{display:block;height:100%;background:var(--bar)}
.card{background:var(--card);border:1px solid var(--line);border-radius:10px;padding:14px 16px}
.tbl{overflow-x:auto;background:var(--card);border:1px solid var(--line);border-radius:10px}
table{border-collapse:collapse;width:100%;font-variant-numeric:tabular-nums}
th,td{text-align:left;padding:7px 10px;border-bottom:1px solid var(--line);vertical-align:top}
th{font-size:12px;color:var(--mute);font-weight:600;background:var(--card);position:sticky;top:0}
tr:last-child td{border-bottom:0}td.num,th.num{text-align:right}
.ph td{background:var(--pendbg);font-weight:600;font-size:12px;color:var(--mute)}
.b{display:inline-block;padding:1px 8px;border-radius:999px;font-size:12px;font-weight:600;white-space:nowrap}
.b.done,.b.skipped{background:var(--okbg);color:var(--ok)}.b.running{background:var(--runbg);color:var(--run)}
.b.failed{background:var(--badbg);color:var(--bad)}.b.pending{background:var(--pendbg);color:var(--pend)}
.what{color:var(--mute);font-size:12.5px}
pre{margin:0;white-space:pre-wrap;word-break:break-word;font:12px/1.45 ui-monospace,SFMono-Regular,Menlo,monospace;color:var(--ink)}
.err{border-color:var(--bad);background:var(--badbg)}
.grid2{display:grid;grid-template-columns:repeat(auto-fit,minmax(320px,1fr));gap:12px}
ul{margin:6px 0 0;padding-left:18px}li{margin:2px 0}
.mini{height:6px;background:var(--pendbg);border-radius:4px;overflow:hidden;margin-top:4px;min-width:80px}.mini i{display:block;height:100%;background:var(--run)}
.pass{color:var(--ok);font-weight:600}.fail{color:var(--bad);font-weight:600}
"""


def results_html():
    parts = []
    # Storage gate table (post-flush / pre-timing)
    rows = []
    for f in (1, 2, 4, 8):
        for tag in ('pre_timing', 'post_flush'):
            s = load_json(CAMP / f'{f}x' / f'storage_{tag}' / 'summary.json')
            if s:
                for label, v in s['variants'].items():
                    m = v['metrics']
                    rows.append(f"<tr><td>{f}×</td><td>{html.escape(tag)}</td><td>{html.escape(v['keyspace'])}</td>"
                                f"<td class=num>{m.get('sstable_count')}</td><td class=num>{m.get('space_live_bytes') or 0:,}</td>"
                                f"<td class=num>{m.get('memtable_cells')}</td><td class=num>{m.get('partition_mean_bytes') or 0:,}</td>"
                                f"<td class=num>{m.get('partition_max_bytes') or 0:,}</td><td class=num>{m.get('partitions_estimate')}</td>"
                                f"<td class={'pass' if v['gate'] == 'PASS' else 'fail'}>{v['gate']}</td></tr>")
                break
    if rows:
        parts.append('<h2>Estado físico S3 (último snapshot por fator)</h2><div class=tbl><table><tr><th>Fator</th><th>Snapshot</th>'
                     '<th>Keyspace</th><th class=num>SSTables</th><th class=num>Live bytes</th><th class=num>Memtable cells</th>'
                     '<th class=num>Partição média B</th><th class=num>Partição máx B</th><th class=num>Partições est.</th><th>Gate</th></tr>'
                     + ''.join(rows) + '</table></div>')
    # Benchmark progress per factor
    s = load_json(S3 / 's3_summary.json')
    if s:
        r = []
        for f, v in s['per_factor'].items():
            r.append(f"<tr><td>{f}×</td><td class=num>{v['s2_wins_u']}/{v['s2_wins_d']}</td><td class=num>{v['s3_wins_u']}/{v['s3_wins_d']}</td>"
                     f"<td class=num>{v['s2_median_d_over_u']:.3f}</td><td class=num>{v['s3_median_d_over_u']:.3f}</td>"
                     f"<td class=num>{v['winners_changed']}</td><td class=num>{v['median_s3_over_s2']['ref']:.3f}</td>"
                     f"<td class=num>{v['median_s3_over_s2']['u']:.3f}</td><td class=num>{v['median_s3_over_s2']['d']:.3f}</td></tr>")
        t = s['totals']
        r.append(f"<tr><td><b>Total</b></td><td class=num><b>{t['s2_wins_u']}/{t['s2_wins_d']}</b></td>"
                 f"<td class=num><b>{t['s3_wins_u']}/{t['s3_wins_d']}</b></td><td></td><td></td><td class=num><b>{t['winners_changed']}</b></td><td></td><td></td><td></td></tr>")
        parts.append('<h2>Resultado S3 × S2 (16 comparações temporais por fator)</h2><div class=tbl><table><tr><th>Fator</th>'
                     '<th class=num>Vitórias U/D S2</th><th class=num>Vitórias U/D S3</th><th class=num>Mediana D/U S2</th>'
                     '<th class=num>Mediana D/U S3</th><th class=num>Vencedor mudou</th><th class=num>p95 S3/S2 REF</th>'
                     '<th class=num>p95 S3/S2 U</th><th class=num>p95 S3/S2 D</th></tr>' + ''.join(r) + '</table></div>'
                     '<p class=what>Descritivo, uma réplica. S3×S2 mistura estado físico com data/carga do host; ver FINAL_AUDIT_PT.md.</p>')
    return ''.join(parts)


def render():
    with lock:
        frac = overall_fraction()
        done_n = sum(s['state'] in ('done', 'skipped') for s in STEPS)
        elapsed = (state['ended'] or now()) - state['started'] if state['started'] else None
        eta = eta_seconds()
        finish = (now() + eta) if state['status'] == 'RUNNING' else None
        rows, last_phase = [], None
        for s in STEPS:
            if s['phase'] != last_phase:
                rows.append(f"<tr class=ph><td colspan=6>{html.escape(s['phase'])}</td></tr>")
                last_phase = s['phase']
            prog = ''
            if s['state'] == 'running':
                prog = f"<div class=mini><i style='width:{s['progress'] * 100:.0f}%'></i></div>"
            actual = s['seconds'] if s['seconds'] is not None else ((now() - s['started']) if s['started'] else None)
            label = {'done': 'concluído', 'skipped': 'já feito', 'running': 'executando', 'failed': 'falhou', 'pending': 'pendente'}[s['state']]
            rows.append(f"<tr><td><span class='b {s['state']}'>{label}</span>{prog}</td><td><b>{html.escape(s['title'])}</b>"
                        f"<div class=what>{html.escape(s['what'])}</div></td><td class=num>{fmt_dur(s['est'])}</td>"
                        f"<td class=num>{fmt_dur(actual)}</td><td class=num>{fmt_ts(s['started'])}</td>"
                        f"<td class=what>{html.escape(s['last'][-140:])}</td></tr>")
        cur = next((s for s in STEPS if s['state'] == 'running'), None)
        tail = html.escape('\n'.join(state['log_tail'][-18:])) or '(sem saída ainda)'
        err = (f"<div class='card err'><b>Falha:</b> <pre>{html.escape(state['error'])}</pre>"
               "<p>Nada foi apagado. Evidência preservada; corrigir a causa e criar nova réplica se o benchmark tiver começado.</p></div>"
               if state['error'] else '')
        status_txt = {'PLANNED': 'Planejado (não iniciado)', 'RUNNING': 'Em execução', 'COMPLETE': 'Concluído', 'FAILED': 'Interrompido por falha'}[state['status']]
        page = f"""<title>S3 Physical Storage</title>
<meta http-equiv="refresh" content="30">
<style>{CSS}</style>
<div class=wrap>
<h1>SchemaLens Rev3 · S3 Physical Storage Controlled Experiment</h1>
<p class=sub>Campanha R1 · Cassandra 4.1.12 @ 127.0.0.1:9043 · keyspaces novas <code>sl_s3_r1_d{{1,2,4,8}}_{{ref,u,d}}</code> ·
atualizado às {dt.datetime.now().strftime('%H:%M:%S')} · a página recarrega a cada 30 s</p>
<div class=kpis>
<div class=kpi><span>Status</span><b>{status_txt}</b></div>
<div class=kpi><span>Etapas</span><b>{done_n}/{len(STEPS)}</b></div>
<div class=kpi><span>Decorrido</span><b>{fmt_dur(elapsed)}</b></div>
<div class=kpi><span>Restante estimado</span><b>{fmt_dur(eta) if state['status'] != 'COMPLETE' else '0s'}</b></div>
<div class=kpi><span>Término previsto</span><b>{fmt_ts(finish) if finish else ('—' if state['status'] != 'COMPLETE' else fmt_ts(state['ended']))}</b></div>
</div>
<div class=bar><i style="width:{frac * 100:.1f}%"></i></div>
<p class=what>{frac * 100:.1f}% do tempo estimado total ({fmt_dur(sum(s['est'] for s in STEPS))}). Estimativas baseadas nos tempos medidos do S2.</p>
{err}
<h2>Agora</h2>
<div class=card><b>{html.escape(cur['title']) if cur else ('—' if state['status'] != 'COMPLETE' else 'Todas as etapas concluídas')}</b>
<div class=what>{html.escape(cur['what']) if cur else ''}</div><pre style="margin-top:10px">{tail}</pre></div>
<h2>Plano e andamento</h2>
<div class=tbl><table><tr><th>Estado</th><th>Etapa / o que será feito</th><th class=num>Estimado</th><th class=num>Real</th><th class=num>Início</th><th>Última saída</th></tr>
{''.join(rows)}</table></div>
{results_html()}
<h2>Salvaguardas ativas</h2>
<div class=grid2><div class=card><ul>
<li>Só CREATE de keyspaces novas <code>sl_s3_r1_*</code>; aborta se alguma já existir.</li>
<li>Sessão protegida recusa DROP, TRUNCATE, ALTER, DELETE, UPDATE e BATCH.</li>
<li>As rotinas <code>_create_keyspace</code> e <code>run_live</code> do stage9 (que contêm DROP) nunca são usadas.</li>
<li><code>nodetool flush</code> só com keyspace S3 explícita; nenhum compact.</li></ul></div>
<div class=card><ul>
<li>B, S1 e S2 apenas lidos e verificados por SHA-256.</li>
<li>Toda evidência é gravada com modo <code>'x'</code> (nunca sobrescreve).</li>
<li>Pré-registro com hashes antes de qualquer medição cronometrada.</li>
<li>Seed 20261127 e ordem 1→2→4→8, iguais ao S2 (pareamento).</li></ul></div></div>
</div>"""
        tmp = DASH.with_suffix('.tmp')
        tmp.write_text(page, encoding='utf8')
        os.replace(tmp, DASH)
        snapshot = dict(state, steps=[{k: v for k, v in s.items() if k != 'args'} for s in STEPS])
        tmp = STATUS.with_suffix('.tmp')
        tmp.write_text(json.dumps(snapshot, indent=1, default=str), encoding='utf8')
        os.replace(tmp, STATUS)


def ticker(stop):
    while not stop.wait(5):
        try:
            render()
        except Exception:
            pass


PROG = re.compile(r'rounds=(\d+)/(\d+)')


def run_step(s):
    LOGS.mkdir(parents=True, exist_ok=True)
    logf = LOGS / f"{s['id']}.log"
    if s['id'] == 'tests':
        cmd = [PY, '-u', '-m', 'unittest', '-v', 'experiments.rev3_s3_physical_storage.tests.test_s3']
    else:
        cmd = RUN + s['args']
    with lock:
        s['state'], s['started'] = 'running', now()
        state['current'] = s['id']
        state['log_tail'].append(f"$ {' '.join(cmd[1:] if cmd[0] == PY else cmd)}")
    render()
    env = dict(os.environ, PYTHONUNBUFFERED='1')
    with logf.open('a', encoding='utf8') as lf:
        lf.write(f'\n===== {dt.datetime.now().isoformat()} {cmd}\n')
        proc = subprocess.Popen(cmd, cwd=PROJECT, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, env=env)
        for line in proc.stdout:
            lf.write(line)
            lf.flush()
            line = line.rstrip()
            print(f"[{s['id']}] {line}", flush=True)
            with lock:
                s['last'] = line
                state['log_tail'] = (state['log_tail'] + [line])[-60:]
                m = PROG.search(line)
                if m:
                    s['progress'] = 0.1 + 0.85 * int(m.group(1)) / int(m.group(2))
                elif s['id'].startswith('run') and 'Timing starts' in line:
                    s['progress'] = 0.1
        rc = proc.wait()
    with lock:
        s['ended'] = now()
        s['seconds'] = s['ended'] - s['started']
        if rc == 0:
            s['state'], s['progress'] = 'done', 1.0
            if s['id'] == 'tests':
                Path(s['done']).write_text(dt.datetime.now().isoformat() + '\n')
        else:
            s['state'] = 'failed'
            state['error'] = f"Etapa '{s['title']}' terminou com código {rc}.\n" + '\n'.join(state['log_tail'][-12:])
    render()
    return rc == 0


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--dry-run', action='store_true')
    a = ap.parse_args()
    S3.mkdir(parents=True, exist_ok=True)
    for s in STEPS:
        if Path(s['done']).exists():
            s['state'], s['progress'] = 'skipped', 1.0
    if a.dry_run:
        render()
        print('Dashboard written:', DASH)
        return
    try:
        fd = os.open(LOCK, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
    except FileExistsError:
        sys.exit(f'Another orchestrator seems active ({LOCK}). Remove the lock only if no s3_run_all.py is running.')
    os.write(fd, str(os.getpid()).encode())
    os.close(fd)
    stop = threading.Event()
    threading.Thread(target=ticker, args=(stop,), daemon=True).start()
    state['status'], state['started'] = 'RUNNING', now()
    ok = True
    try:
        for s in STEPS:
            if s['state'] == 'skipped':
                continue
            if not run_step(s):
                ok = False
                break
    finally:
        state['status'] = 'COMPLETE' if ok else 'FAILED'
        state['ended'] = now()
        state['current'] = None
        stop.set()
        render()
        LOCK.unlink(missing_ok=True)
    print('S3 orchestrator finished:', state['status'], '| dashboard:', DASH, flush=True)
    sys.exit(0 if ok else 1)


if __name__ == '__main__':
    main()
