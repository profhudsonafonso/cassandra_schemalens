#!/usr/bin/env python3
"""Fase 3 analyses (descriptive), reading only audited evidence:  python fase3_analysis.py e7 | e11 | draft"""
from __future__ import annotations

import json
import math
import statistics as st
import sys
import datetime as dt
from pathlib import Path

import fase2_analysis as A

F2 = A.F2
OUT = A.OUT
LABEL = dict(A.LABEL, h2b='H2b · 83 dias · 9 disp.', h3b='H3b · 333 dias · 9 disp.', n6='N6 · 21 dias · 6 disp.',
             p1='P1 · 333 dias · 1 leitura/min · 3 disp.')
fx = A.fmt


def save(name, obj):
    obj['created'] = dt.datetime.now().astimezone().isoformat(timespec='seconds')
    OUT.mkdir(parents=True, exist_ok=True)
    (OUT / f'{name}_summary.json').write_text(json.dumps(obj, indent=2, ensure_ascii=False))
    print(json.dumps(obj, indent=1, ensure_ascii=False)[:3000])


def temporal(idx):
    return sorted({k[0] for k in idx if k[0].split('_')[0] in A.TEMPORAL})


def e7():
    rows, agree, total, noise = [], 0, 0, {'ref': [], 'u': [], 'd': []}
    for a, b, dss in (('e3', 'e3r2', ('h1', 'h2', 'h3')), ('e4', 'e4r2', ('n3', 'n18'))):
        for ds in dss:
            ia, ib = A.p95_index(A.audited(a, ds)), A.p95_index(A.audited(b, ds))
            du_a, du_b, same = [], [], 0
            cases = temporal(ia)
            for c in cases:
                ra = float(ia[c, 'sst', '', 'd']['p95_ms']) / float(ia[c, 'sst', '', 'u']['p95_ms'])
                rb = float(ib[c, 'sst', '', 'd']['p95_ms']) / float(ib[c, 'sst', '', 'u']['p95_ms'])
                du_a.append(ra)
                du_b.append(rb)
                same += (ra > 1) == (rb > 1)
                for v in noise:
                    noise[v].append(abs(math.log(float(ib[c, 'sst', '', v]['p95_ms']) / float(ia[c, 'sst', '', v]['p95_ms']))))
            agree += same
            total += len(cases)
            rows.append(dict(dataset=ds, label=LABEL[ds], n=len(cases), r1_median_du=A.med(du_a), r2_median_du=A.med(du_b),
                             r1_wins_u=sum(x > 1 for x in du_a), r2_wins_u=sum(x > 1 for x in du_b), same_winner=same))
    nz = {v: round(math.exp(st.median(x)) - 1, 4) for v, x in noise.items()}
    find = [f"Mesmo vencedor U/D nas duas réplicas em {agree} de {total} casos temporais.",
            f"Diferença típica de p95 entre réplicas (mediana de |ln r2/r1|): REF {nz['ref'] * 100:.1f}%, "
            f"U {nz['u'] * 100:.1f}%, D {nz['d'] * 100:.1f}%."]
    find += [f"{r['label']}: mediana D/U {fx(r['r1_median_du'])} (r1) e {fx(r['r2_median_du'])} (r2); "
             f"U venceu {r['r1_wins_u']} e {r['r2_wins_u']} de {r['n']}." for r in rows]
    save('e7', dict(rows=rows, winner_agreement=dict(agree=agree, total=total), replicate_noise_rel=nz, findings=find))


def e11():
    r5 = {(p['dataset'], p['case_id']): p for p in json.loads((F2 / 'rev5' / 'predictions_e9.json').read_text())['predictions']}
    r4 = {(p['dataset'], p['case_id']): p for p in json.loads((F2 / 'rev4' / 'predictions_e9.json').read_text())['predictions']}
    by, cases_out = {}, []
    tot = dict(n=0, wins_u=0, wins_d=0, rev5=0, rev4=0, always_u=0, fewer_calls=0)
    err5, err4 = [], []
    for ds in ('h2b', 'h3b', 'n6', 'p1'):
        idx = A.p95_index(A.audited('e9', ds))
        stv = json.loads((F2 / 'e9' / ds / 'storage_pre_timing' / 'summary.json').read_text())['variants']
        r = dict(label=LABEL[ds], n=0, wins_u=0, wins_d=0, rev5=0, rev4=0, always_u=0, fewer_calls=0, du=[],
                 u_partition_kb=stv['sst:TS2-U']['metrics']['partition_mean_bytes'] / 1024,
                 d_partition_kb=stv['sst:TS2-D']['metrics']['partition_mean_bytes'] / 1024,
                 u_partition_max_kb=stv['sst:TS2-U']['metrics']['partition_max_bytes'] / 1024)
        for (d, case), p in sorted(r5.items()):
            if d != ds:
                continue
            mu, md = float(idx[case, 'sst', '', 'u']['p95_ms']), float(idx[case, 'sst', '', 'd']['p95_ms'])
            win = 'TS2-U' if mu < md else 'TS2-D'
            q = r4[d, case]
            r['n'] += 1
            r['wins_u'] += win == 'TS2-U'
            r['wins_d'] += win == 'TS2-D'
            r['rev5'] += p['rev5_winner'] == win
            r['rev4'] += q['rev4_winner'] == win
            r['always_u'] += win == 'TS2-U'
            r['fewer_calls'] += q['fewer_calls'] == win
            r['du'].append(md / mu)
            err5.append(abs(math.log(md / mu) - math.log(p['pred_d_over_u'])))
            err4.append(abs(math.log(md / mu) - math.log(q['pred_d_over_u'])))
            cases_out.append(dict(dataset=ds, case_id=case, window_days=p['window_days'], calls_u=p['calls_u'],
                                  calls_d=p['calls_d'], rows=p['rows'], p95_u=mu, p95_d=md, measured_d_over_u=round(md / mu, 4),
                                  rev5_pred=p['pred_d_over_u'], rev4_pred=q['pred_d_over_u'], measured_winner=win,
                                  rev5=p['rev5_winner'], rev4=q['rev4_winner'], fewer_calls=q['fewer_calls'],
                                  rev5_pred_p95_u=p['pred_p95_u'], rev5_pred_p95_d=p['pred_p95_d'],
                                  extrapolates=p['extrapolates_partition'] or p['extrapolates_calls']))
        r['median_du'] = A.med(r.pop('du'))
        for k in tot:
            tot[k] += r[k]
        by[ds] = r
    A.wcsv(OUT / 'e11_holdout_cases.csv', cases_out)
    m5, m4 = st.median(err5), st.median(err4)
    p1 = by['p1']
    find = [f"Holdout E9: TS2-U venceu {tot['wins_u']} e TS2-D {tot['wins_d']} de {tot['n']} casos temporais.",
            f"Acerto do vencedor: Rev5 {tot['rev5']}/{tot['n']}, Rev4 {tot['rev4']}/{tot['n']}, "
            f"'sempre U' {tot['always_u']}/{tot['n']}, 'menos chamadas' {tot['fewer_calls']}/{tot['n']}.",
            f"Erro mediano de ln(D/U): Rev5 {m5:.3f} (×{math.exp(m5):.2f}) contra Rev4 {m4:.3f} (×{math.exp(m4):.2f}).",
            f"Partições grandes (P1): partição de U {p1['u_partition_kb'] / 1024:.1f} MB (máx. {p1['u_partition_max_kb'] / 1024:.1f} MB) "
            f"contra {p1['d_partition_kb']:.0f} KB de D; mediana D/U {fx(p1['median_du'])}; D venceu {p1['wins_d']} de {p1['n']}."]
    save('e11', dict(by_dataset=by, totals=tot, median_abs_log_error=dict(rev5=round(m5, 4), rev4=round(m4, 4)), findings=find))


def draft():
    j = lambda p: json.loads((F2 / p).read_text())
    e1, e345, e6, e7s, e11s = (j(f'analysis/{n}_summary.json') for n in ('e1', 'e345', 'e6', 'e7', 'e11'))
    r4, r5, tr = j('rev4/rev4_frozen.json'), j('rev5/rev5_frozen.json'), j('e10/trace_summary.json')
    L = ['# SchemaLens Rev3 — Fases 2 e 3: rascunho de resultados', '',
         '_Gerado automaticamente por `fase3_analysis.py draft` a partir dos JSON auditados; nenhum número foi digitado._', '',
         '## Métodos (resumo)', '',
         'Todas as campanhas usaram keyspaces novos, gate de estado físico antes e depois da medição, oráculo semântico em toda '
         'execução, 5 aquecimentos e 60 rodadas aleatorizadas por caso e variante (30 em E6), p95 por posto mais próximo e '
         'auditoria que recalcula as estatísticas a partir das latências brutas. Protocolo e emendas 01–04 em '
         '`experiments/rev3_fase2/`.', '', '## Réplicas e efeito do armazenamento (E1, E7)', '']
    L += [f'- {x}' for x in e1['findings'] + e7s['findings']]
    L += ['', '## Histórico, dispositivos e janelas (E3, E4, E9)', '']
    L += [f'- {x}' for x in e345['findings'] + e11s['findings'][:1] + e11s['findings'][3:]]
    L += ['', '## Preditores pré-benchmark (Rev4, Rev5)', '',
          f"- Rev4 (log-linear): {r4['n_dev']} observações de desenvolvimento, R² {r4['r2']}, coeficientes {r4['coefficients']}.",
          f"- Rev5 (aditivo): {r5['n_dev']} observações, erro relativo mediano {r5['dev_median_abs_rel_error']}, "
          f"coeficientes {r5['coefficients']}."]
    L += [f'- {x}' for x in e345['findings'][1:3] + e11s['findings'][1:3]]
    L += ['', '## Decomposição do tempo (E10, tracing)', '']
    for r in tr['rows']:
        L.append(f"- {r['dataset']} {r['case_id']} {r['variant'].upper()}: {r['calls']} chamada(s), {r['server_requests_median']} "
                 f"requisições ao servidor, {r['client_ms_median']} ms no cliente, {r['server_ms_median']} ms no servidor "
                 f"(fração {r['server_fraction_median']}).")
    L += ['', '## Cache frio (E6)', ''] + [f'- {x}' for x in e6['findings']]
    L += ['', '## Limitações', '', '- Nó único; latência medida no cliente Python; p95 de 60 amostras; sem teste de significância.',
          '- Planos fixos da seleção B; consultas derivadas de objetos Java, não replay do mix oficial.',
          '- E6 é cache parcialmente frio (mmap); cache frio real não foi executado (aguarda autorização).',
          '- O tracing (E10) acrescenta custo; serve para decompor o tempo, não como latência de referência.', '']
    out = F2 / 'RESULTADOS_FASES_2_3_rascunho.md'
    out.write_text('\n'.join(L), encoding='utf-8')
    save('draft', dict(file=str(out.relative_to(F2)), lines=len(L)))


if __name__ == '__main__':
    {'e7': e7, 'e11': e11, 'draft': draft}[sys.argv[1]]()
