#!/usr/bin/env python3
"""SchemaLens Rev3 Fase 4 (EMENDA_05) — non-measurement steps:

    python fase4.py e13                 # record the post-hoc UTC re-match of E10 (hashes; nothing is re-measured)
    python fase4.py freeze-rule         # E14: freeze the tie-zone rule T5 (delta, Rev5 hash) -> rev5/tie_rule_frozen.json
    python fase4.py e14                 # E14: T5 on the already-seen E9 holdout (exploratory)
    python fase4.py predict4 e9r2       # Rev4 predictions before timing (unchanged fase2_rev4)
    python fase4.py predict5 e9r2       # Rev5 predictions before timing (unchanged fase3_rev5)
    python fase4.py predict-tie e9r2    # T5 predictions before timing
    python fase4.py e17                 # E17: confirmatory test of H5.1-H5.4 on e9r2, e9 x e9r2, P1 anomaly
    python fase4.py e18                 # E18: zip of Fases 3+4 and draft section
"""
from __future__ import annotations

import datetime as dt
import json
import math
import statistics as st
import sys
import zipfile
from pathlib import Path

PROJECT = Path(__file__).resolve().parent
F2 = PROJECT / 'experiments' / 'rev3_fase2'
REV5 = F2 / 'rev5'
AN = F2 / 'analysis'
AMENDMENT = F2 / 'EMENDA_05_FASE4.md'
DELTA = 0.10
DATASETS = ('h2b', 'h3b', 'n6', 'p1')
P1_ANOMALY = (1.10, 1.30)  # EMENDA_05 §3: <=1.10 not reproduced, >=1.30 reproduced


def now():
    return dt.datetime.now(dt.timezone.utc).isoformat()


def sha(p):
    import hashlib
    return hashlib.sha256(Path(p).read_bytes()).hexdigest()


def jl(p):
    return json.loads(Path(p).read_text())


def write_x(p, obj):
    p = Path(p)
    p.parent.mkdir(parents=True, exist_ok=True)
    with p.open('x') as f:
        json.dump(obj, f, indent=2, ensure_ascii=False)
    print('WROTE', p)


# ---------------------------------------------------------------- E13
def e13():
    rm = F2 / 'e10' / 'rematch_utc'
    files = ['system_traces_sessions_dump.csv', 'trace_raw_utc.csv', 'trace_summary_utc.json']
    s = jl(rm / 'trace_summary_utc.json')
    write_x(F2 / 'e13' / 'e10_correction_record.json', dict(
        status='E13_E10_CORRECTION_RECORDED', created_utc=now(), amendment_sha256=sha(AMENDMENT),
        cause='fase3_trace.py read naive-UTC system_traces.sessions.started_at as local time (UTC-3): 0 sessions matched',
        correction='sessions re-read before TTL expiry and matched as UTC; originals untouched; fase3_trace.py unchanged',
        sessions_total=s['sessions_total'], matched=s['matched'],
        limitation='+-2 ms non-exclusive window: matched > sessions; only windows >= 7 days used as evidence',
        rematch_files={f: sha(rm / f) for f in files},
        original_files={f: sha(F2 / 'e10' / f) for f in ('trace_raw.csv', 'trace_summary.json')}))
    print(f"PASS e13: {s['matched']} matches of {s['sessions_total']} sessions recorded")


# ---------------------------------------------------------------- T5 rule
def t5(pred_du):
    l = math.log(pred_du)
    if l >= DELTA:
        return 'TS2-U', True
    if l <= -DELTA:
        return 'TS2-D', True
    return 'TS2-U', False  # technical tie; default recommendation TS2-U (Rev3 selection)


def freeze_rule():
    write_x(REV5 / 'tie_rule_frozen.json', dict(
        status='T5_TIE_RULE_FROZEN_BEFORE_E15', created_utc=now(), delta=DELTA,
        rule='ln(rev5 D/U) >= delta -> TS2-U firm; <= -delta -> TS2-D firm; else tie, default TS2-U',
        measured_class='tie if |ln(p95_D/p95_U)| < delta, else winner by p95',
        rev5_frozen_sha256=sha(REV5 / 'rev5_frozen.json'), amendment_sha256=sha(AMENDMENT),
        hypotheses={'H5.1': 'severe errors <= 5% of firm decisions', 'H5.2': 'firm accuracy >= 90%',
                    'H5.3': 'p90 regret of T5 <= 0.10', 'H5.4': 'p90 regret of T5 <= p90 regret of always-U'}))


def evaluate(cases):
    """cases: dicts with p95_u, p95_d, rev5_pred, rev4_winner, fewer_calls."""
    best = lambda c: min(c['p95_u'], c['p95_d'])
    p95 = lambda c, v: c['p95_u'] if v == 'TS2-U' else c['p95_d']
    rules = {
        'T5': lambda c: t5(c['rev5_pred']),
        'Rev5': lambda c: ('TS2-D' if c['rev5_pred'] < 1 else 'TS2-U', True),
        'Rev4': lambda c: (c['rev4_winner'], True),
        'sempre U': lambda c: ('TS2-U', True),
        'menos chamadas': lambda c: (c['fewer_calls'], True),
    }
    out = {}
    for name, f in rules.items():
        firm = ok = severe = 0
        regret = []
        for c in cases:
            rec, is_firm = f(c)
            lm = math.log(c['p95_d'] / c['p95_u'])
            win = 'TS2-U' if lm > 0 else 'TS2-D'
            regret.append(math.log(p95(c, rec) / best(c)))
            if is_firm:
                firm += 1
                ok += rec == win
                severe += rec != win and abs(lm) >= DELTA
        rs = sorted(regret)
        p90 = rs[min(len(rs) - 1, math.ceil(0.9 * len(rs)) - 1)]
        out[name] = dict(n=len(cases), firm=firm, firm_correct=ok, firm_accuracy=round(ok / firm, 4) if firm else None,
                         severe=severe, severe_rate=round(severe / firm, 4) if firm else None,
                         coverage=round(firm / len(cases), 4), regret_median=round(st.median(regret), 4),
                         regret_p90=round(p90, 4), regret_max=round(max(regret), 4))
    meas_ties = sum(abs(math.log(c['p95_d'] / c['p95_u'])) < DELTA for c in cases)
    return out, meas_ties


def holdout_cases(campaign):
    sys.path.insert(0, str(PROJECT))
    import fase2_analysis as A
    r5 = {(p['dataset'], p['case_id']): p for p in jl(REV5 / f'predictions_{campaign}.json')['predictions']}
    r4 = {(p['dataset'], p['case_id']): p for p in jl(F2 / 'rev4' / f'predictions_{campaign}.json')['predictions']}
    out = []
    for ds in DATASETS:
        idx = A.p95_index(A.audited(campaign, ds))
        for (d, case), p in sorted(r5.items()):
            if d != ds:
                continue
            q = r4[d, case]
            out.append(dict(dataset=ds, case_id=case, window_days=p['window_days'],
                            p95_u=float(idx[case, 'sst', '', 'u']['p95_ms']), p95_d=float(idx[case, 'sst', '', 'd']['p95_ms']),
                            rev5_pred=p['pred_d_over_u'], rev4_winner=q['rev4_winner'], fewer_calls=q['fewer_calls']))
    return out


def e14():
    rule = jl(REV5 / 'tie_rule_frozen.json')
    cases = holdout_cases('e9')
    res, ties = evaluate(cases)
    t = res['T5']
    find = [f"EXPLORATÓRIO (E9 já visto): T5 decidiu firme em {t['firm']} de {t['n']} casos e acertou {t['firm_correct']}; "
            f"erros graves {t['severe']}; {t['n'] - t['firm']} declarados empate.",
            f"Arrependimento p90: T5 {t['regret_p90']:.3f}, sempre U {res['sempre U']['regret_p90']:.3f}, "
            f"Rev5 {res['Rev5']['regret_p90']:.3f}, Rev4 {res['Rev4']['regret_p90']:.3f}.",
            f"Casos com empate medido (|ln D/U| < {DELTA}): {ties} de {len(cases)}."]
    write_x(AN / 'e14_summary.json', dict(status='E14_EXPLORATORY', created_utc=now(), delta=rule['delta'],
                                          tie_rule_sha256=sha(REV5 / 'tie_rule_frozen.json'), rules=res,
                                          measured_ties=ties, findings=find))
    print('\n'.join(find))


# ---------------------------------------------------------------- predictions before timing
def _register():
    sys.path.insert(0, str(PROJECT))
    import fase4_core  # noqa: F401  (registers e9r2)


def predict4(campaign):
    _register()
    import fase2_rev4
    fase2_rev4.cmd_predict(campaign)


def predict5(campaign):
    _register()
    import fase3_rev5
    fase3_rev5.cmd_predict(campaign)


def predict_tie(campaign):
    rule = jl(REV5 / 'tie_rule_frozen.json')
    if rule['rev5_frozen_sha256'] != sha(REV5 / 'rev5_frozen.json'):
        raise ValueError('Rev5 changed after the tie rule was frozen')
    if any((F2 / campaign / ds / 'benchmark').exists() for ds in DATASETS):
        raise ValueError('timing already exists; prediction would not be blind')
    preds = jl(REV5 / f'predictions_{campaign}.json')
    out = []
    for p in preds['predictions']:
        rec, firm = t5(p['pred_d_over_u'])
        out.append(dict(dataset=p['dataset'], case_id=p['case_id'], window_days=p['window_days'],
                        rev5_pred_d_over_u=p['pred_d_over_u'], t5_recommendation=rec, t5_firm=firm))
    write_x(REV5 / f'tie_predictions_{campaign}.json', dict(
        status='T5_PREDICTIONS_BEFORE_TIMING', campaign=campaign, created_utc=now(),
        tie_rule_sha256=sha(REV5 / 'tie_rule_frozen.json'), rev5_predictions_sha256=sha(REV5 / f'predictions_{campaign}.json'),
        predictions=out))
    print(f"{campaign}: {len(out)} T5 predictions; firm {sum(p['t5_firm'] for p in out)}, "
          f"firm D {sum(p['t5_firm'] and p['t5_recommendation'] == 'TS2-D' for p in out)}")


# ---------------------------------------------------------------- E17
def e17():
    tp = jl(REV5 / 'tie_predictions_e9r2.json')
    if tp['tie_rule_sha256'] != sha(REV5 / 'tie_rule_frozen.json'):
        raise ValueError('tie rule changed after predictions')
    c2 = holdout_cases('e9r2')
    c1 = {(c['dataset'], c['case_id']): c for c in holdout_cases('e9')}
    res, ties = evaluate(c2)
    t, u = res['T5'], res['sempre U']
    hyp = {'H5.1': dict(value=t['severe_rate'], threshold='<= 0.05', ok=t['severe_rate'] is not None and t['severe_rate'] <= 0.05),
           'H5.2': dict(value=t['firm_accuracy'], threshold='>= 0.90', ok=t['firm_accuracy'] is not None and t['firm_accuracy'] >= 0.90),
           'H5.3': dict(value=t['regret_p90'], threshold='<= 0.10', ok=t['regret_p90'] <= 0.10),
           'H5.4': dict(value=t['regret_p90'], threshold=f"<= {u['regret_p90']} (sempre U)", ok=t['regret_p90'] <= u['regret_p90'])}
    rows, agree = [], 0
    for ds in DATASETS:
        a = [c for c in c2 if c['dataset'] == ds]
        du1 = [c1[ds, c['case_id']]['p95_d'] / c1[ds, c['case_id']]['p95_u'] for c in a]
        du2 = [c['p95_d'] / c['p95_u'] for c in a]
        same = sum((x > 1) == (y > 1) for x, y in zip(du1, du2))
        agree += same
        rows.append(dict(dataset=ds, n=len(a), r1_median_du=round(st.median(du1), 4), r2_median_du=round(st.median(du2), 4),
                         r1_wins_d=sum(x < 1 for x in du1), r2_wins_d=sum(y < 1 for y in du2), same_winner=same))
    p1w1 = [c['p95_d'] / c['p95_u'] for c in c2 if c['dataset'] == 'p1' and int(c['window_days']) == 1]
    m = st.median(p1w1)
    verdict = 'reproduzida' if m >= P1_ANOMALY[1] else 'não reproduzida' if m <= P1_ANOMALY[0] else 'inconclusiva'
    yes = sum(h['ok'] for h in hyp.values())
    find = [f"Teste confirmatório da regra T5 em e9r2: {yes} de 4 hipóteses atendidas "
            f"({', '.join(k + (' ✔' if h['ok'] else ' ✘') for k, h in hyp.items())}).",
            f"T5: decisão firme em {t['firm']} de {t['n']} casos, acerto firme {t['firm_correct']}/{t['firm']}, erros graves {t['severe']}; "
            f"arrependimento p90 {t['regret_p90']:.3f} (sempre U {u['regret_p90']:.3f}).",
            f"Réplica E9 × e9r2: mesmo vencedor em {agree} de {len(c2)} casos temporais.",
            f"Anomalia P1 janela de 1 dia: mediana D/U em e9r2 = {m:.3f} → {verdict} (critério EMENDA_05: ≤1,10 não; ≥1,30 sim)."]
    write_x(AN / 'e17_summary.json', dict(status='E17_CONFIRMATORY', created_utc=now(), rules=res, measured_ties=ties,
                                          hypotheses=hyp, replica_rows=rows, winner_agreement=dict(agree=agree, total=len(c2)),
                                          p1_w1=dict(median_du=round(m, 4), values=[round(x, 4) for x in p1w1], verdict=verdict),
                                          findings=find))
    import csv
    with (AN / 'e17_cases.csv').open('x', newline='') as f:
        w = csv.DictWriter(f, fieldnames=list(c2[0]) + ['t5', 't5_firm'])
        w.writeheader()
        for c in c2:
            rec, firm = t5(c['rev5_pred'])
            w.writerow(dict(c, t5=rec, t5_firm=firm))
    print('\n'.join(find))


# ---------------------------------------------------------------- E18
def e18():
    name = PROJECT / 'SchemaLens_Rev3_Fase3_Fase4_results.zip'
    code = ['fase2_core.py', 'fase2_run.py', 'fase2_dashboard.py', 'fase2_analysis.py', 'fase2_rev4.py', 'fase2_java.py',
            'fase3_core.py', 'fase3_rev5.py', 'fase3_analysis.py', 'fase3_trace.py', 'fase4_core.py', 'fase4.py',
            'fase4_trace.py', 's3_physical.py']
    camps = ['e3r2', 'e4r2', 'e9', 'e9r2', 'e10', 'e13', 'e16', 'rev4', 'rev5', 'analysis', 'logs', 'data', '_protocol']
    files = [PROJECT / c for c in code]
    files += [p for p in F2.glob('*') if p.is_file()]
    for c in camps:
        files += [p for p in (F2 / c).rglob('*') if p.is_file() and not (c == 'data' and p.suffix == '.csv')]
    with zipfile.ZipFile(name, 'x', zipfile.ZIP_DEFLATED) as z:
        for p in sorted(set(files)):
            z.write(p, p.relative_to(PROJECT))
    e17s = jl(AN / 'e17_summary.json')
    e14s = jl(AN / 'e14_summary.json')
    tr = jl(F2 / 'e16' / 'trace_summary.json')
    L = ['', '## Fase 4 (EMENDA_05): regra com zona de empate, réplica de E9 e tracing do P1', '',
         '_Seção acrescentada por `fase4.py e18` a partir dos JSON auditados._', '',
         '- E10 corrigido: a associação de traces usava hora local em vez de UTC; reassociação registrada em E13.']
    L += [f'- {x}' for x in e14s['findings'] + e17s['findings']]
    L += [f"- E16 {r['dataset_campaign']} {r['case_id']} {r['variant']}: cliente mediana {r['client_ms_median']} ms, "
          f"p95 {r['client_ms_p95']} ms; servidor {r['server_ms_median']} ms em {r['server_requests_median']} requisições."
          for r in tr['rows']]
    draft = F2 / 'RESULTADOS_FASES_2_3_rascunho.md'
    if '## Fase 4' not in draft.read_text():
        with draft.open('a', encoding='utf-8') as f:
            f.write('\n'.join(L) + '\n')
    n = len(zipfile.ZipFile(name).namelist())
    write_x(AN / 'e18_summary.json', dict(status='E18_PACKED', created_utc=now(), zip=name.name, files=n, sha256=sha(name),
                                          draft=str(draft.relative_to(F2))))


if __name__ == '__main__':
    a = sys.argv[1:]
    cmds = {'e13': e13, 'freeze-rule': freeze_rule, 'e14': e14, 'e17': e17, 'e18': e18}
    if len(a) == 1 and a[0] in cmds:
        cmds[a[0]]()
    elif len(a) == 2 and a[0] in ('predict4', 'predict5', 'predict-tie'):
        {'predict4': predict4, 'predict5': predict5, 'predict-tie': predict_tie}[a[0]](a[1])
    else:
        sys.exit(__doc__)
