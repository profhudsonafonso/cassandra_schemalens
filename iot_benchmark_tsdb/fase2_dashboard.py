#!/usr/bin/env python3
"""Render experiments/rev3_fase2/dashboard.html (plan, progress and results of Fase 2).

The page follows the claude.ai Artifact page contract (no <html>/<body> wrapper, tokens
for light/dark), so the same file is opened locally and published as an artifact.
"""
from __future__ import annotations

import html
import json
import datetime as dt
from pathlib import Path

PROJECT = Path(__file__).resolve().parent
F2 = PROJECT / 'experiments' / 'rev3_fase2'
OUT = F2 / 'dashboard.html'
E = html.escape


def j(p):
    try:
        return json.loads(Path(p).read_text())
    except Exception:
        return None


STAGES = [
    ('E0', 'Empacotar resultados do S3 R1',
     'Deixar a evidência do S3 pronta para revisão independente, como foi feito com o S2.',
     'Zip com protocolo, dados de medição, auditorias e scripts do S3.'),
    ('E1', 'Réplicas r2 e r3 · memória × disco na mesma sessão',
     'O S3 tem uma única réplica e foi medido em outro dia que o S2: não dá para separar o efeito do flush do efeito da data/carga, nem saber o ruído entre campanhas.',
     'Duas réplicas novas (r2, r3). Em cada uma, os mesmos dados ficam em 6 keyspaces: 3 variantes em memtable e 3 em SSTable, medidos intercalados na mesma sessão. 51.840 medições.'),
    ('E2', 'Rev4: calibrar e congelar o preditor U × D',
     'Objetivo final do SchemaLens: prever a melhor modelagem antes de medir. Precisa ser congelado antes dos dados novos para que E3/E4 sejam holdout verdadeiro (sem leakage).',
     'Modelo log-linear pré-especificado (chamadas CQL, linhas lidas, tamanho de partição, estado físico) ajustado em S2, S3 e E1. Coeficientes congelados com hash.'),
    ('E3', 'Histórico real Java: 21, 83 e 333 dias',
     'S2/S3 usaram densificação sintética de 21 dias. Partições de U ficaram em ~1 MB, longe do cenário em que o bucket diário (D) deveria compensar.',
     'Três conjuntos novos do gerador Java oficial (sementes inéditas), mais janelas de 80 e 330 dias. Planos fixos do B. Previsões Rev4 gravadas antes de medir.'),
    ('E4', 'Número de dispositivos: 3, 9 e 18',
     'Verificar se a conclusão depende do número de dispositivos (volume total maior, partições do mesmo tamanho).',
     'Dois conjuntos Java novos (N3, N18); H1 é o ponto de 9 dispositivos.'),
    ('E5', 'Avaliar a Rev4 no holdout',
     'Medir se o preditor acerta fora dos dados em que foi calibrado.',
     'Acerto do vencedor e erro de D/U previsto × medido, contra "sempre U" (Rev3) e a regra "menos chamadas vence".'),
    ('E6', 'Cache frio (best-effort)',
     'S3 mediu SSTable com page cache quente. Falta saber quanto pesa ler do disco.',
     'Evicção do page cache por keyspace antes de cada execução "fria" + invalidação do key cache. Limitação: páginas mapeadas pelo Cassandra (mmap) não saem; a residência é medida.'),
    # ---- Fase 3 (EMENDA_04_FASE3.md) ----
    ('E7', 'Réplicas de E3 e E4',
     'E3 e E4 tinham uma réplica só; na E1 a razão D/U do 1× variou até 0,23 entre réplicas.',
     'Keyspaces novos com os mesmos dados (H1, H2, H3, N3, N18) e sementes de medição novas. Mede quanto o vencedor e a razão D/U mudam de uma réplica para outra.'),
    ('E8', 'Rev5: calibrar e congelar',
     'A Rev4 empatou com "sempre U" e subestimou o custo por chamada em janelas longas (fora da faixa de calibração).',
     'Modelo aditivo pré-especificado (custo fixo + por chamada + por chamada × ln(partição) + por linha), ajustado uma vez em todos os casos em SSTable já medidos (S3, E1, E3, E4, E7) e congelado com hash.'),
    ('E9', 'Holdout novo + partições grandes',
     'O holdout de E3/E4 já foi usado para avaliar a Rev4; a Rev5 precisa de dados nunca vistos. E partições de ~2 MB ainda não testavam o cenário em que o bucket diário deveria compensar.',
     'Quatro conjuntos Java inéditos: H2b (83 d), H3b (333 d), N6 (6 dispositivos) e P1 (1 leitura/min por 333 dias, ~18 MB por partição de U). Previsões da Rev4 e da Rev5 gravadas antes de medir.'),
    ('E10', 'Tracing do servidor (amostra)',
     'Até aqui só há latência medida no cliente Python. Falta saber quanto do custo de cada chamada extra do TS2-D é servidor e quanto é rede/driver.',
     'Tracing do Cassandra ligado só durante uma amostra (Q2 e Q12, janelas de 1 a 80 dias, H3 e P1, 5 execuções) e desligado em seguida.'),
    ('E11', 'Avaliar a Rev5 no holdout novo',
     'Verificar se o modelo aditivo corrige o problema da Rev4 em dados que ele não viu.',
     'Acerto do vencedor e erro de D/U previsto × medido para Rev5, Rev4, "sempre U" e "menos chamadas".'),
    ('E12', 'Rascunho de resultados para o artigo',
     'Os resultados das Fases 2 e 3 precisam virar texto, sem nenhum número digitado à mão.',
     'Markdown gerado automaticamente a partir dos JSON auditados: métodos, réplicas, histórico, preditores, tracing, cache frio e limitações.'),
    # ---- Fase 4 (EMENDA_05_FASE4.md) ----
    ('E13', 'Registrar a correção do E10',
     'O E10 terminou com PASS mas associou 0 traces: o horário UTC do Cassandra foi lido como hora local. A reassociação feita a tempo (antes de os traces expirarem) precisa ficar registrada no protocolo.',
     'Registro com os hashes dos arquivos originais (intocados), do dump das 1.779 sessões e da reassociação em UTC. Nenhuma medição nova.'),
    ('E14', 'Regra T5: Rev5 com zona de empate',
     'Na Fase 3, Rev4 e Rev5 erraram o vencedor só em casos em que U e D diferem menos que o ruído entre réplicas (5–8%). Um preditor honesto deve dizer "empate técnico" nesses casos, em vez de chutar.',
     'Regra congelada antes de medir: D/U previsto pela Rev5 ≥ +10% → U; ≤ −10% → D; entre os dois → empate (recomenda U). δ = 0,10 vem do ruído da E7, não de E9. Avaliação em E9 só exploratória (já visto).'),
    ('E15', 'Réplica do holdout E9 (e9r2) · teste confirmatório da T5',
     'E9 teve uma réplica só, e é o único lugar em que D venceu com alguma frequência (N6, P1). Também é o teste limpo da regra T5 e da anomalia do P1 em 1 dia.',
     'Mesmos dados de E9 (H2b, H3b, N6, P1) em keyspaces novos, semente de medição nova (20261601), 15.660 medições. Previsões Rev4, Rev5 e T5 gravadas antes de medir; hashes no pré-registro.'),
    ('E16', 'Tracing do P1 em 1 e 7 dias (corrigido)',
     'No P1 com janela de 1 dia, D foi 1,7× mais lento no p95, mas não na mediana do tracing. Precisamos saber se é custo no servidor ou cauda no cliente.',
     'Q2 e Q12, janelas 1 e 7 dias, U e D, keyspaces de E9 e e9r2, 30 execuções cada (480 no total). Horário em UTC, cada sessão atribuída a uma única execução; falha explícita se nada casar.'),
    ('E17', 'Análise: hipóteses H5.1–H5.4 e estabilidade de E9',
     'Decidir, com regras fixadas antes, se a T5 funciona e se E9 se repete.',
     'H5.1 erros graves ≤ 5% · H5.2 acerto firme ≥ 90% · H5.3 arrependimento p90 ≤ 10% · H5.4 T5 não pior que "sempre U". Mais: mesmo vencedor E9 × e9r2 e veredito da anomalia do P1 (≤ 1,10 não / ≥ 1,30 sim).'),
    ('E18', 'Pacote das Fases 3 e 4 + rascunho',
     'Deixar a evidência pronta para revisão independente e o texto do artigo atualizado.',
     'Zip com protocolo, emendas 04–05, pré-registros, latências brutas, auditorias, traces e código; seção da Fase 4 acrescentada ao rascunho, gerada dos JSON.'),
    ('A1', 'Cache frio real',
     'A E6 só tirou do cache de 7% a 52% dos bytes (mmap). Ler do disco de verdade exige reiniciar o Cassandra e limpar o cache do sistema.',
     'Não será executado sem sua autorização: reinicia o container do Cassandra (afeta tudo que estiver nele) e precisa de acesso root no host.'),
    ('A2', 'Limpeza dos keyspaces sl_f2_* (e S2/S3)',
     'Ocupam disco e memória do Cassandra; os resultados já estão nos zips.',
     'Não será executado sem sua autorização: apagar keyspaces é irreversível.'),
]
APPROVAL = {'A1', 'A2'}

STEP_LABEL = {'done': 'concluído', 'running': 'executando', 'failed': 'falhou', 'pending': 'pendente',
              'interrupted': 'interrompido', 'waiting': 'aguardando sua aprovação'}


def stage_state(sid, steps):
    if sid in APPROVAL:
        return 'waiting'
    if sid == 'E0':
        return 'done' if (PROJECT / 'SchemaLens_Rev3_S3_results.zip').exists() else 'pending'
    mine = [s for s in steps.values() if s['stage'] == sid]
    if not mine:
        return 'pending'
    for st in ('failed', 'interrupted', 'running'):
        if any(s['state'] == st for s in mine):
            return st
    if all(s['state'] == 'done' for s in mine):
        return 'done'
    return 'running' if any(s['state'] == 'done' for s in mine) else 'pending'


# ---------------------------------------------------------------------------
# Liveness: is the orchestrator process really there?
# ---------------------------------------------------------------------------
STALE_SECONDS = 150  # fase2_run.py rewrites status every 60 s while alive


def parse_ts(x):
    try:
        return dt.datetime.fromisoformat(x)
    except Exception:
        return None


def orchestrator_pids():
    pids = []
    for d in Path('/proc').glob('[0-9]*'):
        try:
            cmd = (d / 'cmdline').read_bytes().split(b'\0')
        except Exception:
            continue
        if any(c.endswith(b'fase2_run.py') for c in cmd):
            pids.append(int(d.name))
    return pids


def liveness(cur):
    """'alive' | 'dead' | 'idle'. Only a RUNNING batch can be alive or dead."""
    if cur.get('state') != 'RUNNING':
        return 'idle'
    pids = orchestrator_pids()
    if cur.get('pid'):
        return 'alive' if cur['pid'] in pids else 'dead'
    return 'alive' if pids else 'dead'  # batches started before the pid was recorded


# ---------------------------------------------------------------------------
# Time estimates: simple per-kind cost models, calibrated on the steps already measured
# ---------------------------------------------------------------------------
ROWS = {'d1': 27000, 'd2': 54000, 'd4': 108000, 'd8': 216000,
        'h1': 27000, 'h2': 108000, 'h3': 432000, 'n3': 9000, 'n18': 54000}  # rows per keyspace
# timed cases per dataset as (count, relative rows read per query): 18 S1 cases at the data density, plus the
# W80 (H2, H3) and W330 (H3) window extensions of Q2/Q7/Q9/Q12, which read roughly 4x and 16x more.
CASES = {'h2': [(18, 1), (4, 4)], 'h3': [(18, 1), (4, 4), (4, 16)], 'h2b': [(18, 1), (4, 4)],
         'h3b': [(18, 1), (4, 4), (4, 16)], 'p1': [(17, 1), (4, 4)]}
PRIOR = dict(mat=0.36e-3, flush=21.0, val=1.1e-5, run_a=0.005, run_b=0.012, run_overhead=15.0, evict=1.5,
             small=3.0, analysis=30.0, fit=30.0, predict=10.0, trace=180.0)


def campaigns():
    try:
        try:
            import fase4_core  # noqa: F401  (registers Fase 3 and Fase 4 campaigns)
        except Exception:
            pass
        import fase2_core
        return fase2_core.CAMPAIGNS
    except Exception:
        return {}


def ds_rows(ds):
    m = j(F2 / 'data' / ds / 'manifest.json')
    return m['cells'] / 2 if m else ROWS.get(ds, 27000)


def density(ds):
    """Rows per device per day, relative to 144 (one reading every 10 min)."""
    m = j(F2 / 'data' / ds / 'manifest.json')
    if m:
        return m['cells'] / 2 / m['devices'] / max(m['history_days'], 1e-9) / 144
    return ds_rows(ds) / 27000 if ds.startswith('d') else 1.0


def cases(ds):
    return CASES.get(ds, [(18, 1)])


def parse_sid(sid):
    parts = sid.split(':')
    return parts[0], parts[1], (parts[2] if len(parts) > 2 else None)


class Estimator:
    def __init__(self, steps):
        self.C = campaigns()
        self.k = dict(PRIOR)
        self.calibrated = set()
        by = {}
        for sid, s in steps.items():
            if s['state'] == 'done' and s.get('seconds') and not s.get('note'):
                camp, kind, ds = parse_sid(sid)
                by.setdefault(kind, []).append((camp, ds, s['seconds']))
        med = lambda xs: sorted(xs)[len(xs) // 2]
        if by.get('mat'):
            self.k['mat'] = med([sec / (ds_rows(ds) * self.nks(c)) for c, ds, sec in by['mat']])
            self.calibrated.add('mat')
        if by.get('flush'):
            self.k['flush'] = med([sec for *_, sec in by['flush']])
            self.calibrated.add('flush')
        vals = [(c, ds, sec) for c, ds, sec in by.get('val', []) if sec >= 1]
        if vals:
            self.k['val'] = med([sec / (ds_rows(ds) * self.nks(c)) for c, ds, sec in vals])
            self.calibrated.add('val')
        runs = [(self.obs(c, ds), density(ds), sec) for c, ds, sec in by.get('run', []) if not self.cold(c)]
        if runs:  # per-observation cost a + b*density; least squares when two densities were seen
            pts = [((sec - self.k['run_overhead']) / n, x) for n, x, sec in runs]
            xs = {x for _, x in pts}
            if len(xs) >= 2:
                mx = sum(x for _, x in pts) / len(pts)
                my = sum(y for y, _ in pts) / len(pts)
                b = sum((x - mx) * (y - my) for y, x in pts) / sum((x - mx) ** 2 for _, x in pts)
                self.k['run_b'] = max(b, 0.0)
                self.k['run_a'] = max(my - self.k['run_b'] * mx, 0.0)
            else:
                y, x = pts[0]
                self.k['run_a'] = y * PRIOR['run_a'] / (PRIOR['run_a'] + PRIOR['run_b'] * x)
                self.k['run_b'] = (y - self.k['run_a']) / x
            self.calibrated.add('run')
        for kind in ('prereg', 'audit', 'retire'):
            if by.get(kind):
                self.calibrated.add(kind)
        self.small = {kind: med([sec for *_, sec in by[kind]]) for kind in ('prereg', 'audit', 'retire') if by.get(kind)}

    def nks(self, camp):
        return len(self.C.get(camp, {}).get('conds', ['mem', 'sst'])) * 3

    def cold(self, camp):
        return bool(self.C.get(camp, {}).get('cache'))

    def obs(self, camp, ds):
        c = self.C.get(camp, {})
        arms = self.nks(camp) * (2 if c.get('cache') else 1)
        return sum(n for n, _ in cases(ds)) * arms * c.get('rounds', 60)

    def seconds(self, sid):
        """(estimated seconds, calibrated?)"""
        camp, kind, ds = parse_sid(sid)
        k = self.k
        if kind == 'mat':
            return k['mat'] * ds_rows(ds) * self.nks(camp) + 5, 'mat' in self.calibrated
        if kind == 'flush':
            return k['flush'], 'flush' in self.calibrated
        if kind == 'val':
            return k['val'] * ds_rows(ds) * self.nks(camp) + 1, 'val' in self.calibrated
        if kind == 'run':
            c = self.C.get(camp, {})
            arms = self.nks(camp) * (2 if c.get('cache') else 1)
            rounds = c.get('rounds', 60)
            t = k['run_overhead']
            for n, mult in cases(ds):
                t += n * arms * rounds * (k['run_a'] + k['run_b'] * density(ds) * mult)
            if c.get('cache'):  # one page-cache eviction (docker exec) before every cold execution
                t += sum(n for n, _ in cases(ds)) * self.nks(camp) * rounds * k['evict']
            return t, 'run' in self.calibrated and not c.get('cache') and ds.startswith('d')
        if kind in self.small:
            return self.small[kind], True
        if kind == 'analysis':
            return k['analysis'], False
        if kind == 'fit':
            return k['fit'], False
        if kind in ('predict', 'predict4'):
            return k['predict'], False
        if kind == 'trace':
            return k['trace'], False
        return k['small'], False


ROUND_RE = __import__('re').compile(r'round (\d+)/(\d+)')


def fmt_dur(sec):
    if sec is None:
        return '—'
    sec = max(0, sec)
    if sec < 1:
        return '&lt;1 s'
    if sec < 60:
        return f'{sec:.0f} s'
    m = round(sec / 60)
    if m < 60:
        return f'{m} min'
    return f'{m // 60} h {m % 60:02d} min'


WD = ['seg', 'ter', 'qua', 'qui', 'sex', 'sáb', 'dom']


def fmt_clock(t, ref):
    if t is None:
        return '—'
    if t.date() == ref.date():
        return t.strftime('%H:%M')
    return f"{WD[t.weekday()]} {t.strftime('%d/%m %H:%M')}"


def f3(x, n=3):
    return '—' if x is None else f'{x:.{n}f}'.replace('.', ',')


def pct(x):
    return '—' if x is None else f'{(x - 1) * 100:+.1f}%'.replace('.', ',')


def table(head, rows, num_from=1):
    th = ''.join(f'<th class="{"n" if i >= num_from else ""}">{E(h)}</th>' for i, h in enumerate(head))
    tr = ''.join('<tr>' + ''.join(f'<td class="{"n" if i >= num_from else ""}">{c}</td>' for i, c in enumerate(r)) + '</tr>'
                 for r in rows)
    return f'<div class="tw"><table><thead><tr>{th}</tr></thead><tbody>{tr}</tbody></table></div>'


def results_e1():
    s = j(F2 / 'analysis' / 'e1_summary.json')
    if not s:
        return ''
    rows = []
    for f in ('1', '2', '4', '8'):
        r = s['by_factor'][f]
        rows.append([f'{f}×', f3(r['s2_median_du']), f3(r['s3_median_du']),
                     f3(r['r2']['mem_median_du']), f3(r['r2']['sst_median_du']),
                     f3(r['r3']['mem_median_du']), f3(r['r3']['sst_median_du']),
                     f"{r['wins_sst_u']}/{r['wins_sst_d']}"])
    eff = []
    for f in ('1', '2', '4', '8'):
        r = s['by_factor'][f]
        eff.append([f'{f}×'] + [pct(r['flush_effect'][v]) for v in ('ref', 'u', 'd')] + [pct(r['s3_over_s2'][v]) for v in ('ref', 'u', 'd')])
    out = ['<h3>Mediana da razão p95 D/U (16 casos temporais por fator)</h3>',
           table(['Fator', 'S2 (mem)', 'S3 R1 (sst)', 'r2 mem', 'r2 sst', 'r3 mem', 'r3 sst', 'Vitórias U/D sst (r2+r3)'], rows),
           '<h3>Efeito isolado do flush (mesma sessão) × diferença S3/S2 (dias diferentes)</h3>',
           table(['Fator', 'Flush REF', 'Flush U', 'Flush D', 'S3/S2 REF', 'S3/S2 U', 'S3/S2 D'], eff),
           '<ul class="find">' + ''.join(f'<li>{E(x)}</li>' for x in s.get('findings', [])) + '</ul>']
    return ''.join(out)


def results_e2():
    s = j(F2 / 'rev4' / 'rev4_frozen.json')
    if not s:
        return ''
    c = s['coefficients']
    rows = [[E(k), f3(v, 4)] for k, v in c.items()]
    return ('<h3>Coeficientes congelados</h3>' + table(['Termo', 'Valor'], rows) +
            f"<p class='note'>Ajuste em {s['n_dev']} observações de desenvolvimento; R² = {f3(s['r2'])}; "
            f"acerto do vencedor no próprio desenvolvimento: {s['dev_winner_accuracy']}. "
            f"SHA-256 do arquivo congelado registrado nos pré-registros de E3 e E4.</p>")


def results_e345():
    s = j(F2 / 'analysis' / 'e345_summary.json')
    if not s:
        return ''
    rows = []
    for ds, r in s['by_dataset'].items():
        rows.append([E(r['label']), f"{r['wins_u']}/{r['wins_d']}", f3(r['median_du']), f3(r['u_partition_kb'], 0),
                     f3(r['d_partition_kb'], 1), f"{r['rev4_hits']}/{r['n']}", f"{r['always_u_hits']}/{r['n']}",
                     f"{r['fewer_calls_hits']}/{r['n']}"])
    t = s['totals']
    rows.append(['<b>Total</b>', f"<b>{t['wins_u']}/{t['wins_d']}</b>", '', '', '', f"<b>{t['rev4_hits']}/{t['n']}</b>",
                 f"<b>{t['always_u_hits']}/{t['n']}</b>", f"<b>{t['fewer_calls_hits']}/{t['n']}</b>"])
    return ('<h3>Holdout E3 + E4: vencedor medido e acerto dos preditores</h3>' +
            table(['Conjunto', 'Vitórias U/D', 'Mediana D/U', 'Partição U (KB)', 'Partição D (KB)', 'Rev4 acertou',
                   'Sempre U', 'Menos chamadas'], rows) +
            '<ul class="find">' + ''.join(f'<li>{E(x)}</li>' for x in s.get('findings', [])) + '</ul>')


def results_e6():
    s = j(F2 / 'analysis' / 'e6_summary.json')
    if not s:
        return ''
    rows = [[E(r['label']), pct(r['cold_over_warm']['ref']), pct(r['cold_over_warm']['u']), pct(r['cold_over_warm']['d']),
             f3(r['warm_median_du']), f3(r['cold_median_du']), f"{r['resident_pct']:.0f}%"] for r in s['rows']]
    return ('<h3>Frio × quente (mediana p95 frio/quente nos casos temporais)</h3>' +
            table(['Conjunto', 'REF', 'TS2-U', 'TS2-D', 'D/U quente', 'D/U frio', 'Residência após evicção'], rows) +
            '<ul class="find">' + ''.join(f'<li>{E(x)}</li>' for x in s.get('findings', [])) + '</ul>')


def results_e7():
    s = j(F2 / 'analysis' / 'e7_summary.json')
    if not s:
        return ''
    rows = [[E(r['label']), f3(r['r1_median_du']), f3(r['r2_median_du']), f"{r['r1_wins_u']}/{r['n']}",
             f"{r['r2_wins_u']}/{r['n']}", f"{r['same_winner']}/{r['n']}"] for r in s['rows']]
    return ('<h3>Réplica 1 × réplica 2</h3>' + table(['Conjunto', 'D/U r1', 'D/U r2', 'U venceu r1', 'U venceu r2', 'Mesmo vencedor'], rows)
            + '<ul class="find">' + ''.join(f'<li>{E(x)}</li>' for x in s['findings'][:2]) + '</ul>')


def results_e8():
    s = j(F2 / 'rev5' / 'rev5_frozen.json')
    if not s:
        return ''
    rows = [[E(k), f'{v:.6f}'.replace('.', ',')] for k, v in s['coefficients'].items()]
    return ('<h3>Coeficientes congelados (ms)</h3>' + table(['Termo', 'Valor'], rows) +
            f"<p class='note'>{s['n_dev']} observações de desenvolvimento; erro relativo mediano "
            f"{100 * s['dev_median_abs_rel_error']:.1f}%; acerto do vencedor no desenvolvimento {s['dev_winner_accuracy']}; "
            f"faixa calibrada: até {s['dev_max_calls']} chamadas e partições de até {s['dev_max_partition_bytes'] / 1024:.0f} KB.</p>")


def data_table_e9():
    rows = []
    for gid, label in (('h2b', 'H2b · 9 disp. · 83 d'), ('h3b', 'H3b · 9 disp. · 333 d'), ('n6', 'N6 · 6 disp. · 21 d'),
                       ('p1', 'P1 · 3 disp. · 333 d · 1/min')):
        m = j(F2 / 'data' / gid / 'manifest.json')
        if m:
            rows.append([label, f"{m['cells']:,}".replace(',', '.'), f"{m['settings']['DATA_SEED']}",
                         f"<code>{m['csv_sha256'][:12]}…</code>"])
    return ('<h3>Dados Java inéditos (gerados sem Cassandra)</h3>' + table(['Conjunto', 'Células', 'DATA_SEED', 'SHA-256'], rows)) if rows else ''


def results_e10():
    s = j(F2 / 'e10' / 'rematch_utc' / 'trace_summary_utc.json')
    if not s:
        return ''
    s['rows'] = [dict(r, variant=r['variant']) for r in s['rows']]
    rows = [[f"{E(r['dataset'])} {E(r['case_id'])}", r['variant'].upper(), r['calls'], r['server_requests_median'],
             f3(r['client_ms_median'], 1), f3(r['server_ms_median'], 1), f"{100 * r['server_fraction_median']:.0f}%"]
            for r in s['rows']]
    return ('<h3>Tempo no servidor × no cliente (mediana de 5 execuções com tracing)</h3>' +
            table(['Caso', 'Var.', 'Chamadas', 'Requisições', 'Cliente (ms)', 'Servidor (ms)', 'Fração servidor'], rows) +
            "<p class='note'><b>Valores da reassociação em UTC (E13).</b> O arquivo original associou 0 traces por ler o horário UTC como hora local. Nas janelas de 1 dia a janela de ±2 ms às vezes pega sessões vizinhas (fração &gt; 100%); use só janelas ≥ 7 dias. O tracing acrescenta custo: estes valores decompõem o tempo, não substituem as latências medidas.</p>")


def results_e11():
    s = j(F2 / 'analysis' / 'e11_summary.json')
    if not s:
        return ''
    rows = [[E(r['label']), f"{r['wins_u']}/{r['wins_d']}", f3(r['median_du']), f3(r['u_partition_kb'], 0), f3(r['d_partition_kb'], 1),
             f"{r['rev5']}/{r['n']}", f"{r['rev4']}/{r['n']}", f"{r['always_u']}/{r['n']}", f"{r['fewer_calls']}/{r['n']}"]
            for r in s['by_dataset'].values()]
    t = s['totals']
    rows.append(['<b>Total</b>', f"<b>{t['wins_u']}/{t['wins_d']}</b>", '', '', '', f"<b>{t['rev5']}/{t['n']}</b>",
                 f"<b>{t['rev4']}/{t['n']}</b>", f"<b>{t['always_u']}/{t['n']}</b>", f"<b>{t['fewer_calls']}/{t['n']}</b>"])
    return ('<h3>Holdout E9: vencedor medido e acerto dos preditores</h3>' +
            table(['Conjunto', 'Vitórias U/D', 'Mediana D/U', 'Partição U (KB)', 'Partição D (KB)', 'Rev5', 'Rev4', 'Sempre U',
                   'Menos chamadas'], rows) + '<ul class="find">' + ''.join(f'<li>{E(x)}</li>' for x in s['findings']) + '</ul>')


def results_e12():
    s = j(F2 / 'analysis' / 'draft_summary.json')
    return (f"<p class='note'>Arquivo: <code>experiments/rev3_fase2/{E(s['file'])}</code> ({s['lines']} linhas).</p>") if s else ''


def results_e13():
    s = j(F2 / 'e13' / 'e10_correction_record.json')
    return (f"<p class='note'>Registrado: {s['matched']} associações de {s['sessions_total']} sessões; arquivos originais "
            f"intocados (hashes no registro <code>e13/e10_correction_record.json</code>).</p>") if s else ''


def rg(x):
    return f"{(__import__('math').exp(x) - 1) * 100:.1f}%".replace('.', ',')


def rules_table(rules):
    rows = [[E(k), f"{r['firm']}/{r['n']}", f"{r['firm_correct']}/{r['firm']}", str(r['severe']), rg(r['regret_median']),
             rg(r['regret_p90']), rg(r['regret_max'])] for k, r in rules.items()]
    return table(['Regra', 'Decisões firmes', 'Acerto firme', 'Erros graves', 'Arrep. mediano', 'Arrep. p90', 'Arrep. máx.'], rows)


def results_e14():
    s = j(F2 / 'analysis' / 'e14_summary.json')
    if not s:
        return ''
    return ('<h3>Exploratório: regras no holdout E9 (já visto)</h3>' + rules_table(s['rules']) +
            '<ul class="find">' + ''.join(f'<li>{E(x)}</li>' for x in s['findings']) + '</ul>' +
            "<p class='note'>Arrependimento = quanto a variante recomendada ficou mais lenta que a melhor (ln da razão de p95, mostrado em %).</p>")


def results_e16():
    s = j(F2 / 'e16' / 'trace_summary.json')
    if not s:
        return ''
    rows = [[E(r['dataset_campaign']) + ' ' + E(r['case_id']), r['variant'], r['calls'], r['server_requests_median'],
             f3(r['client_ms_median'], 1), f3(r['client_ms_p95'], 1), f3(r['server_ms_median'], 1),
             f"{100 * (r['server_fraction_median'] or 0):.0f}%"] for r in s['rows']]
    return (f"<h3>Tracing P1 ({s['reps']} execuções por linha; {s['matched']} sessões atribuídas, {s['unassigned']} sem execução)</h3>" +
            table(['Caso', 'Var.', 'Chamadas', 'Requisições', 'Cliente med. (ms)', 'Cliente p95 (ms)', 'Servidor (ms)', 'Fração servidor'], rows))


def results_e17():
    s = j(F2 / 'analysis' / 'e17_summary.json')
    if not s:
        return ''
    hy = [[k, E(str(h['value'])), E(h['threshold']), '✔ atendida' if h['ok'] else '✘ não atendida'] for k, h in s['hypotheses'].items()]
    rp = [[E(r['dataset']), f3(r['r1_median_du']), f3(r['r2_median_du']), f"{r['r1_wins_d']}/{r['n']}", f"{r['r2_wins_d']}/{r['n']}",
           f"{r['same_winner']}/{r['n']}"] for r in s['replica_rows']]
    return ('<h3>Hipóteses confirmatórias (e9r2)</h3>' + table(['Hipótese', 'Valor', 'Critério', 'Resultado'], hy) +
            '<h3>Regras em e9r2</h3>' + rules_table(s['rules']) +
            '<h3>E9 × e9r2</h3>' + table(['Conjunto', 'D/U E9', 'D/U e9r2', 'D venceu E9', 'D venceu e9r2', 'Mesmo vencedor'], rp) +
            '<ul class="find">' + ''.join(f'<li>{E(x)}</li>' for x in s['findings']) + '</ul>')


def results_e18():
    s = j(F2 / 'analysis' / 'e18_summary.json')
    return (f"<p class='note'>Arquivo: <code>{E(s['zip'])}</code> ({s['files']} arquivos, SHA <code>{s['sha256'][:8]}…</code>); "
            f"rascunho: <code>experiments/rev3_fase2/{E(s['draft'])}</code>.</p>") if s else ''


RESULTS = {'E1': results_e1, 'E2': results_e2, 'E5': results_e345, 'E6': results_e6, 'E7': results_e7, 'E8': results_e8,
           'E10': results_e10, 'E11': results_e11, 'E12': results_e12, 'E13': results_e13, 'E14': results_e14,
           'E16': results_e16, 'E17': results_e17, 'E18': results_e18}


def data_table():
    rows = []
    for gid, label in (('h1', 'H1 · 9 disp. · 21 d'), ('h2', 'H2 · 9 disp. · 83 d'), ('h3', 'H3 · 9 disp. · 333 d'),
                       ('n3', 'N3 · 3 disp. · 21 d'), ('n18', 'N18 · 18 disp. · 21 d')):
        m = j(F2 / 'data' / gid / 'manifest.json')
        if m:
            rows.append([label, f"{m['cells']:,}".replace(',', '.'), f"{m['settings']['DATA_SEED']}",
                         f"<code>{m['csv_sha256'][:12]}…</code>"])
    if not rows:
        return ''
    return ('<h3>Dados Java novos (gerados sem Cassandra)</h3>' +
            table(['Conjunto', 'Células', 'DATA_SEED', 'SHA-256'], rows) +
            "<p class='note'>Controle: o mesmo caminho reproduziu byte a byte os dados da campanha B (SHA <code>047c269a…</code>).</p>")


CSS = """
:root{--bg:#f4f6f8;--card:#ffffff;--fg:#14202b;--mute:#5b6b78;--line:#dde3e8;--accent:#1f6f8b;
--ok:#1d7a4a;--okbg:#e3f3ea;--run:#8a5a00;--runbg:#fff1d1;--bad:#b42318;--badbg:#fde7e4;--pend:#6f7d88;--pendbg:#eceff2;
--live:#16a34a;
--sans:"IBM Plex Sans",system-ui,-apple-system,"Segoe UI",sans-serif;--mono:"IBM Plex Mono",ui-monospace,Menlo,monospace}
@media (prefers-color-scheme:dark){:root:not([data-theme="light"]){--bg:#0f1519;--card:#172027;--fg:#e4eaee;--mute:#9aaab5;--line:#2a3640;
--accent:#6fb7d2;--ok:#7fd3a3;--okbg:#14301f;--run:#f0c46a;--runbg:#372a0e;--bad:#ff8f84;--badbg:#3b1714;--pend:#9aa7b1;--pendbg:#222c33;
--live:#4ade80;color-scheme:dark}}
:root[data-theme="dark"]{--bg:#0f1519;--card:#172027;--fg:#e4eaee;--mute:#9aaab5;--line:#2a3640;--accent:#6fb7d2;--ok:#7fd3a3;--okbg:#14301f;
--run:#f0c46a;--runbg:#372a0e;--bad:#ff8f84;--badbg:#3b1714;--pend:#9aa7b1;--pendbg:#222c33;--live:#4ade80;color-scheme:dark}
*{box-sizing:border-box}
body{background:var(--bg);color:var(--fg);font:15px/1.55 var(--sans);margin:0}
.wrap{max-width:1080px;margin:0 auto;padding-block:28px 56px;padding-left:max(16px,env(safe-area-inset-left,0px));padding-right:max(16px,env(safe-area-inset-right,0px))}
header{display:grid;gap:6px;margin-bottom:18px}
.eyebrow{font:600 12px/1 var(--mono);letter-spacing:.08em;text-transform:uppercase;color:var(--accent)}
h1{font-size:28px;line-height:1.15;margin:0;text-wrap:balance}
h2{font-size:18px;margin:0}
h3{font-size:14px;margin:18px 0 8px;color:var(--mute);font-weight:600}
.sub{color:var(--mute);margin:0;max-width:70ch}
.bar{height:8px;background:var(--pendbg);border-radius:4px;overflow:hidden}
.bar i{display:block;height:100%;background:var(--accent)}
.meta{display:flex;flex-wrap:wrap;gap:8px 20px;font:13px var(--mono);color:var(--mute)}
/* status panel */
.now{background:var(--card);border:1px solid var(--line);border-left:6px solid var(--pend);border-radius:10px;padding:18px 20px;
display:grid;gap:14px;margin-bottom:22px;min-width:0}
.now.alive{border-left-color:var(--live)}.now.dead,.now.failed{border-left-color:var(--bad)}.now.stale{border-left-color:var(--run)}
.state{display:flex;flex-wrap:wrap;align-items:center;gap:10px 14px}
.badge{display:inline-flex;align-items:center;gap:8px;font:700 13px var(--mono);letter-spacing:.06em;padding:5px 12px;border-radius:999px;
background:var(--pendbg);color:var(--pend)}
.alive .badge{background:var(--okbg);color:var(--ok)}.dead .badge,.failed .badge{background:var(--badbg);color:var(--bad)}
.stale .badge{background:var(--runbg);color:var(--run)}
.dot{width:10px;height:10px;border-radius:50%;background:currentColor;flex:none}
.alive .dot{background:var(--live)}
.headline{font-size:19px;font-weight:600;margin:0}
.why{margin:0;color:var(--mute);font-size:14px}
.kpis{display:grid;grid-template-columns:repeat(auto-fit,minmax(190px,1fr));gap:10px}
.kpi{border:1px solid var(--line);border-radius:8px;padding:10px 12px;min-width:0}
.kpi b{display:block;font:600 20px/1.2 var(--sans);font-variant-numeric:tabular-nums;margin-top:2px}
.kpi small{color:var(--mute);font-size:12.5px}
.stepbar{display:grid;gap:4px}
.stepbar .bar{height:10px}.stepbar .bar i{background:var(--live)}
.dead .stepbar .bar i,.stale .stepbar .bar i{background:var(--run)}
.hint{background:var(--badbg);color:var(--bad);border-radius:6px;padding:10px 12px;font-size:13.5px;margin:0}
.hint code{color:inherit}
.stalemsg{display:none}.stale .stalemsg{display:block;background:var(--runbg);color:var(--run)}
.snap{background:var(--runbg);color:var(--run);border-radius:8px;padding:10px 14px;font-size:14px;margin:0 0 12px}
.lastline{font:12px/1.45 var(--mono);color:var(--mute);overflow-wrap:anywhere;margin:0}
/* timeline */
.timeline{display:grid;gap:10px}
.stage{background:var(--card);border:1px solid var(--line);border-radius:8px;padding:16px 18px;display:grid;gap:8px;min-width:0}
.stage.running{border-color:var(--live);box-shadow:0 0 0 1px var(--live)}
.stage.failed,.stage.interrupted{border-color:var(--bad)}
.stage.done{opacity:.92}
.stage>*{min-width:0}
td:nth-child(2){min-width:9em}td:last-child{overflow-wrap:anywhere;min-width:16em}
.top{display:flex;flex-wrap:wrap;align-items:center;gap:10px}
.code{font:600 13px var(--mono);color:var(--accent);min-width:2.2em}
.pill{font:600 12px var(--mono);padding:2px 9px;border-radius:999px;white-space:nowrap;display:inline-flex;align-items:center;gap:6px}
.pill.done{background:var(--okbg);color:var(--ok)}.pill.running{background:var(--okbg);color:var(--ok)}
.pill.running::before{content:"";width:7px;height:7px;border-radius:50%;background:var(--live)}
.pill.failed,.pill.interrupted{background:var(--badbg);color:var(--bad)}.pill.pending{background:var(--pendbg);color:var(--pend)}
.pill.waiting{background:var(--runbg);color:var(--run)}.stage.waiting{border-style:dashed;border-color:var(--run)}
.phase{font:600 12px var(--mono);letter-spacing:.08em;text-transform:uppercase;color:var(--mute);margin:14px 0 2px}
.when{margin-left:auto;font:13px var(--mono);color:var(--mute);text-align:right}
.when b{color:var(--fg);font-weight:600}
.grid2{display:grid;grid-template-columns:repeat(auto-fit,minmax(280px,1fr));gap:6px 22px}
.lbl{font:600 11px var(--mono);letter-spacing:.06em;text-transform:uppercase;color:var(--mute)}
.grid2 p{margin:2px 0 0}
details{margin-top:4px}summary{cursor:pointer;color:var(--accent);font-size:13px}
.tw{overflow-x:auto;border:1px solid var(--line);border-radius:6px;margin-top:6px}
table{border-collapse:collapse;width:100%;font-size:13px;font-variant-numeric:tabular-nums}
th,td{padding:6px 10px;border-bottom:1px solid var(--line);text-align:left;vertical-align:top}
th{font-weight:600;color:var(--mute);background:var(--pendbg)}
tr:last-child td{border-bottom:0}.n{text-align:right;white-space:nowrap}
tr.cur td{background:var(--okbg)}tr.cur.bad td{background:var(--badbg)}
code{font:12px var(--mono)}
.note{color:var(--mute);font-size:13px;margin:6px 0 0}
.est{color:var(--mute)}
.find{margin:8px 0 0;padding-left:20px}.find li{margin:3px 0}
.err{background:var(--badbg);color:var(--bad);border-radius:6px;padding:10px;font:12px/1.45 var(--mono);white-space:pre-wrap;overflow-x:auto;margin:0}
footer{margin-top:28px;color:var(--mute);font-size:13px}
@media (prefers-reduced-motion:no-preference){.bar i{transition:width .4s}
.alive .dot,.pill.running::before{animation:pulse 1.6s ease-in-out infinite}
@keyframes pulse{0%,100%{box-shadow:0 0 0 0 color-mix(in srgb,var(--live) 60%,transparent)}50%{box-shadow:0 0 0 6px transparent}}}
"""

JS = """
(function(){
  var gen = +document.body.dataset.gen || +document.querySelector('[data-gen]').dataset.gen;
  var panel = document.querySelector('.now');
  function ago(ms){var s=Math.max(0,Math.round(ms/1000)); if(s<60) return s+' s'; var m=Math.round(s/60);
    return m<60? m+' min' : Math.floor(m/60)+' h '+String(m%60).padStart(2,'0')+' min';}
  function tick(){
    var now = Date.now();
    document.querySelectorAll('[data-ago]').forEach(function(el){ el.textContent = ago(now - (+el.dataset.ago)); });
    document.querySelectorAll('[data-elapsed]').forEach(function(el){ el.textContent = ago(now - (+el.dataset.elapsed)); });
    if(panel && panel.classList.contains('alive') && now - gen > STALE*1000 && location.protocol === 'file:'){
      panel.classList.add('stale');
      var b = panel.querySelector('.badge span'); if(b) b.textContent = 'SEM SINAL';
    }
    var snap = document.getElementById('snapnote');
    if(snap && location.protocol !== 'file:' && now - gen > STALE*1000){
      snap.hidden = false;
      snap.querySelector('b').textContent = ago(now - gen);
    }
  }
  tick(); setInterval(tick, 5000);
  if(location.protocol === 'file:') setTimeout(function(){ location.reload(); }, 30000);
})();
"""


def status_panel(cur, live, steps, order, est, now):
    """The 'Agora' block: is something executing, what, for how long, and when it should end."""
    run_sid = next((k for k in order if steps[k]['state'] in ('running', 'interrupted')), None)
    if cur.get('current') in steps and steps[cur['current']]['state'] in ('running', 'interrupted'):
        run_sid = cur['current']
    # last sign of life: heartbeat, start of the running step, or the last write of status.json
    st_file = F2 / 'status.json'
    signs = [parse_ts(cur.get('heartbeat') or ''), parse_ts(cur.get('started') or ''),
             parse_ts(steps[run_sid].get('started') or '') if run_sid else None,
             dt.datetime.fromtimestamp(st_file.stat().st_mtime).astimezone() if st_file.exists() else None]
    hb = max([t for t in signs if t], default=None)

    # remaining schedule from now: running step first, then the rest in plan order
    rem_step, frac, elapsed = None, None, None
    if run_sid:
        s = steps[run_sid]
        e, _ = est.seconds(run_sid)
        t0 = parse_ts(s.get('started') or '')
        elapsed = (now - t0).total_seconds() if t0 else 0
        m = ROUND_RE.search(s.get('last') or '')
        if m and live == 'alive':
            frac = int(m.group(1)) / int(m.group(2))
        if live == 'alive':
            rem_step = max(e - elapsed, 0.08 * e)
            if frac and elapsed > 60:
                rem_step = elapsed * (1 - frac) / max(frac, 0.02)
        else:
            rem_step = e  # an interrupted step has to be redone from scratch
    batch = set(cur.get('stages') or [])
    stage_of = {k: steps[k]['stage'] for k in order}
    t, batch_end, finish = now, None, {}
    for k in order:
        s = steps[k]
        if s['state'] == 'done':
            continue
        t = t + dt.timedelta(seconds=rem_step if k == run_sid and rem_step is not None else est.seconds(k)[0])
        finish[k] = t
        if stage_of[k].lower() in batch and live in ('alive', 'dead'):
            batch_end = t
    phase_end = t if finish else None
    calibrated_all = all(est.seconds(k)[1] for k in finish)

    run_txt = (steps[run_sid]['title'] + f" · <code>{E(run_sid.split(':')[0])}</code> · {E(steps[run_sid]['stage'])}") if run_sid else ''
    kpis = []
    if live == 'alive' and run_sid:
        est_step = est.seconds(run_sid)[0]
        cls, badge = 'alive', 'EXECUTANDO'
        headline = f"Executando agora: {run_txt}"
        why = 'O processo do orquestrador está ativo nesta máquina' + (f" (PID {cur['pid']})" if cur.get('pid') else '') + '.'
        pct_step = frac if frac is not None else min(elapsed / est_step, 0.97) if est_step else 0
        prog_txt = f"rodada {ROUND_RE.search(steps[run_sid]['last']).group(0)[6:]}" if frac is not None else 'progresso estimado pelo tempo'
        t0 = parse_ts(steps[run_sid]['started'])
        stepbar = (f"<div class='stepbar'><div class='meta'><span>passo rodando há <b data-elapsed='{int(t0.timestamp() * 1000)}'>"
                   f"{fmt_dur(elapsed)}</b></span><span>previsto {fmt_dur(est_step)}</span><span>{prog_txt}</span>"
                   f"<span>termina ~{fmt_clock(now + dt.timedelta(seconds=rem_step), now)}</span></div>"
                   f"<div class='bar'><i style='width:{100 * pct_step:.0f}%'></i></div></div>")
        kpis.append(('Passo atual termina', fmt_clock(now + dt.timedelta(seconds=rem_step), now), f'faltam ~{fmt_dur(rem_step)}'))
        if batch_end:
            kpis.append((f"Lote atual ({', '.join(x.upper() for x in cur.get('stages', []))}) termina", fmt_clock(batch_end, now),
                         f'faltam ~{fmt_dur((batch_end - now).total_seconds())}'))
    elif live == 'dead':
        cls, badge = 'dead', 'PARADO'
        if batch_end:
            kpis.append((f"Lote {', '.join(x.upper() for x in cur.get('stages', []))} terminaria", fmt_clock(batch_end, now),
                         f'se retomado agora · ~{fmt_dur((batch_end - now).total_seconds())}'))
        headline = 'Nada está executando: o orquestrador parou sem registrar o fim'
        why = (f"O lote {', '.join(x.upper() for x in cur.get('stages', []))} consta como em execução, mas não há processo "
               f"<code>fase2_run.py</code> ativo. Último sinal às <b>{fmt_clock(hb, now)}</b> "
               f"(há <span data-ago='{int(hb.timestamp() * 1000)}'>{fmt_dur((now - hb).total_seconds())}</span>).") if hb else \
              'O lote consta como em execução, mas não há processo fase2_run.py ativo.'
        stepbar = ''
        if run_sid:
            why += f" Passo interrompido: <b>{run_txt}</b>."
            camp, kind, ds = parse_sid(run_sid)
            partial = F2 / camp / ds / 'benchmark' if kind == 'run' and ds else None
            if partial and partial.exists() and not (F2 / camp / ds / 'post_timing_manifest.json').exists():
                stepbar = (f"<p class='hint'>A pasta parcial <code>{E(str(partial.relative_to(PROJECT)))}</code> ficou no disco; "
                           f"<code>fase2_core.py run</code> recusa diretório existente, então relançar o lote vai falhar neste passo "
                           f"até que se decida (e registre) como tratá-la.</p>")
    elif cur.get('state') == 'FAILED':
        cls, badge = 'failed', 'FALHOU'
        headline = 'Nada está executando: o último lote parou por falha'
        why = f"Encerrado às {fmt_clock(parse_ts(cur.get('ended') or ''), now)}."
        stepbar = f"<div class='err'>{E(cur['error'])}</div>" if cur.get('error') else ''
    elif cur.get('state') == 'STOPPED':
        cls, badge = 'dead', 'INTERROMPIDO'
        headline = 'Nada está executando: o lote foi interrompido'
        why = f"{E(cur.get('error') or '')} às {fmt_clock(parse_ts(cur.get('ended') or ''), now)}."
        stepbar = ''
    else:
        cls, badge = 'idle', 'PARADO'
        headline = 'Nada está executando' + (' — último lote concluído' if cur.get('state') == 'COMPLETE' else '')
        why = ''
        stepbar = ''
    if cls != 'alive' and not (live == 'dead' and run_sid):
        nxt = next((k for k in order if steps[k]['state'] != 'done'), None)
        if nxt:
            why += (f" Próximo passo: <b>{E(steps[nxt]['title'])}</b> (<code>{E(nxt.split(':')[0])}</code>, "
                    f"{E(steps[nxt]['stage'])}). Para continuar: <code>python fase2_run.py --stages "
                    f"{E(steps[nxt]['stage'].lower())} …</code>")
    if phase_end:
        cond = 'no ritmo atual' if cls == 'alive' else 'se retomada agora'
        kpis.append(('Tudo que não depende de você', fmt_clock(phase_end, now),
                     f"{cond}, lotes em sequência sem pausa · ~{fmt_dur((phase_end - now).total_seconds())} de máquina"))
    done_n = sum(steps[k]['state'] == 'done' for k in order)
    kpis.append(('Passos concluídos', f'{done_n}/{len(order)}', f'{len(order) - done_n} restantes'))
    stale = (f"<p class='hint stalemsg'>A página não é atualizada há mais de {STALE_SECONDS // 60} min, mas o orquestrador "
             f"regrava o status a cada minuto: ele provavelmente parou. Rode <code>python fase2_dashboard.py</code> para confirmar.</p>")
    last = ''
    if run_sid and steps[run_sid].get('last'):
        last = f"<p class='lastline'>última saída: {E(steps[run_sid]['last'][-160:])}</p>"
    kp = ''.join(f"<div class='kpi'><span class='lbl'>{E(a)}</span><b>{b}</b><small>{c}</small></div>" for a, b, c in kpis)
    note = '' if calibrated_all else ("<p class='note'>Previsões de passos já medidos (materializar, flush, validar, medir E1) "
                                      "usam as durações observadas; para E2–E6 ainda não há passo medido e o valor é uma "
                                      "estimativa grosseira (escala pelo tamanho dos dados e número de execuções). "
                                      "Ela se recalibra à medida que os passos terminam.</p>")
    html_ = (f"<section class='now {cls}' aria-live='polite'><div class='state'><span class='badge'><i class='dot'></i>"
             f"<span>{badge}</span></span><p class='headline'>{headline}</p></div>"
             f"{f'<p class=why>{why}</p>' if why else ''}{stepbar}{last}{stale}<div class='kpis'>{kp}</div>{note}</section>")
    return html_, finish, run_sid, live if run_sid else None


def render():
    st = j(F2 / 'status.json') or dict(steps={}, current_run=None)
    steps = dict(st.get('steps', {}))
    order = []
    try:
        import fase2_run
        for stages in fase2_run.STAGES.values():
            for stage, (sid, title, *_rest) in stages:
                steps.setdefault(sid, dict(stage=stage, title=title, state='pending'))
                order.append(sid)
    except Exception:
        pass
    order += [k for k in steps if k not in order]
    cur = st.get('current_run') or {}
    live = liveness(cur)
    if live == 'dead':  # a 'running' step without a process is shown as interrupted
        steps = {k: (dict(v, state='interrupted') if v['state'] == 'running' else v) for k, v in steps.items()}
    now = dt.datetime.now().astimezone()
    est = Estimator(steps)
    panel, finish, run_sid, run_live = status_panel(cur, live, steps, order, est, now)

    blocks = []
    for sid, title, why, what in STAGES:
        state = stage_state(sid, steps)
        mine = [k for k in order if steps[k]['stage'] == sid]
        prog, when = '', ''
        if mine:
            rows = []
            for k in mine:
                s = steps[k]
                secs = s.get('seconds')
                e_sec, calib = est.seconds(k)
                if s['state'] == 'done':
                    dur = fmt_dur(secs) if secs is not None else '—'
                    fin = fmt_clock(parse_ts(s.get('ended') or ''), now) if s.get('ended') else '—'
                else:
                    dur = f"<span class='est'>~{fmt_dur(e_sec)}{'' if calib else '?'}</span>"
                    fin = f"<span class='est'>~{fmt_clock(finish[k], now)}</span>" if k in finish else '—'
                cls = (" class='cur bad'" if s['state'] == 'interrupted' else " class='cur'") if k == run_sid else ''
                rows.append((cls, [f"<span class='pill {s['state']}'>{STEP_LABEL[s['state']]}</span>", E(s['title']),
                                   f'<code>{E(k.split(":")[0])}</code>', dur, fin, E((s.get('last') or '')[-110:])]))
            dn = sum(steps[k]['state'] == 'done' for k in mine)
            th = ''.join(f'<th class="{"n" if i in (3, 4) else ""}">{h}</th>' for i, h in
                         enumerate(['Estado', 'Passo', 'Campanha', 'Duração', 'Término', 'Última saída']))
            tr = ''.join(f'<tr{c}>' + ''.join(f'<td class="{"n" if i in (3, 4) else ""}">{x}</td>' for i, x in enumerate(r))
                         + '</tr>' for c, r in rows)
            open_ = ' open' if state in ('running', 'interrupted', 'failed') else ''
            prog = (f'<details{open_}><summary>{dn}/{len(mine)} passos concluídos — ver lista</summary>'
                    f'<div class="tw"><table><thead><tr>{th}</tr></thead><tbody>{tr}</tbody></table></div>'
                    f"<p class='note'>Duração e término com “~” são previsões; “?” = ainda sem passo do mesmo tipo medido.</p>"
                    f'</details>')
            todo = [k for k in mine if k in finish]
            if state == 'done':
                t0 = min((parse_ts(steps[k].get('started') or '') for k in mine if steps[k].get('started')), default=None)
                t1 = max((parse_ts(steps[k].get('ended') or '') for k in mine if steps[k].get('ended')), default=None)
                tot = sum(steps[k].get('seconds') or 0 for k in mine)
                when = (f"<span class='when'>levou <b>{fmt_dur(tot)}</b>" +
                        (f"<br>terminou {fmt_clock(t1, now)}" if t1 else '') + '</span>') if t0 or tot else ''
            elif todo:
                rem = sum(est.seconds(k)[0] for k in todo if k != run_sid)
                if run_sid in todo:
                    rem += (finish[run_sid] - now).total_seconds() if run_live == 'alive' else est.seconds(run_sid)[0]
                rough = sum(est.seconds(k)[0] for k in todo if not est.seconds(k)[1])
                guess = ' (estimativa grosseira)' if rough > 0.2 * max(rem, 1) else ''
                when = (f"<span class='when'>faltam <b>~{fmt_dur(rem)}</b>{guess}<br>"
                        f"término previsto <b>{fmt_clock(finish[todo[-1]], now)}</b></span>")
        res = ''
        if sid == 'E3':
            res = data_table()
        if sid == 'E9':
            res = data_table_e9()
        if sid in RESULTS:
            res += RESULTS[sid]()
        if sid == 'E0' and state == 'done':
            res = "<p class='note'>Arquivo: <code>SchemaLens_Rev3_S3_results.zip</code> (253 arquivos, SHA <code>c5bad7d6…</code>).</p>"
        if sid in ('E0', 'E7', 'E13', 'A1'):
            blocks.append({'E0': "<p class='phase'>Fase 2 · concluída</p>", 'E7': "<p class='phase'>Fase 3 · concluída</p>",
                           'E13': "<p class='phase'>Fase 4 · EMENDA_05 · recomendações da Fase 3</p>",
                           'A1': "<p class='phase'>Aguardando sua decisão</p>"}[sid])
        blocks.append(f"""<section class="stage {state}"><div class="top"><span class="code">{sid}</span><h2>{E(title)}</h2>
<span class="pill {state}">{STEP_LABEL[state]}</span>{when}</div>
<div class="grid2"><div><span class="lbl">Por quê</span><p>{E(why)}</p></div><div><span class="lbl">O que é feito</span><p>{E(what)}</p></div></div>
{prog}{res}</section>""")
    notes = st.get('notes') or []
    reg = ''.join(f"<p class='note'>Registro: {E(n)}</p>" for n in notes)
    n_done = sum(steps[k]['state'] == 'done' for k in order)
    n_total = len(order) or 1
    gen_ms = int(now.timestamp() * 1000)
    page = f"""<title>SchemaLens Fases 2–4</title>
<meta name="claude:viewport-fit-sides" content="cover">
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;600&family=IBM+Plex+Sans:wght@400;600;700&display=swap">
<style>{CSS}</style>
<div class="wrap" data-gen="{gen_ms}">
<header><span class="eyebrow">SchemaLens · Rev3 · Cassandra IoT</span>
<h1>Fases 2 a 4: robustez e alcance do resultado U × D</h1>
<p class="sub">Plano, andamento e previsão do que falta. Fases 2 (E0–E6) e 3 (E7–E12) concluídas. A Fase 4 (E13–E18, EMENDA_05) executa as recomendações do relatório da Fase 3: registra a correção do E10, congela uma regra de decisão com zona de empate, testa essa regra numa réplica do holdout E9, investiga a anomalia do P1 e empacota tudo. A1 e A2 só rodam com sua autorização. Em cada fase, o preditor é congelado antes de medir os dados novos.</p>
<div class="meta"><span>página gerada {now.strftime('%d/%m/%Y %H:%M:%S')} (há <span data-ago="{gen_ms}">0 s</span>)</span><span>{n_done}/{n_total} passos concluídos</span></div>
<div class="bar"><i style="width:{100 * n_done / n_total:.0f}%"></i></div>{reg}</header>
<p id="snapnote" class="snap" hidden>Esta é uma cópia publicada, gerada há <b></b>: o estado abaixo é o daquele momento e pode já ter mudado.
O acompanhamento ao vivo está em <code>experiments/rev3_fase2/dashboard.html</code> (abre no navegador e se atualiza sozinho).</p>
{panel}
<div class="timeline">{''.join(blocks)}</div>
<footer>TS2-U = sem bucket (uma partição por dispositivo); TS2-D = bucket diário (uma partição por dispositivo e dia); REF = baseline.
Razão D/U &gt; 1 significa D mais lento. Latência p95 medida no cliente Python, nó único, planos fixos da seleção B. Leitura descritiva, sem significância estatística.
Previsões de tempo: modelos simples por tipo de passo (linhas × keyspaces; execuções × densidade), recalibrados a cada passo concluído; supõem lotes em sequência, sem pausa.
Protocolo: <code>experiments/rev3_fase2/PROTOCOLO_FASE2.md</code>.</footer>
</div>
<script>var STALE={STALE_SECONDS};{JS}</script>"""
    OUT.write_text(page, encoding='utf-8')
    return OUT


if __name__ == '__main__':
    print(render())
