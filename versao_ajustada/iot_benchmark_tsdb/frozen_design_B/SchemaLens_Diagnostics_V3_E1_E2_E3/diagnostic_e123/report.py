"""Post-run read-only summary; doesn't rerun models or touch Cassandra."""
from __future__ import annotations
import argparse
import csv
import json
from pathlib import Path
from .core import EXPECTED,write_csv


def readcsv(p):
    if not p.exists():return []
    with p.open(newline='',encoding='utf-8') as f:return list(csv.DictReader(f))


def fmt(n):return f'{float(n):.4f}'


def main():
    a=argparse.ArgumentParser(description='Summarize existing E1/E2/E3 outputs without Cassandra access')
    a.add_argument('--results',type=Path,required=True,help='experiments/outputs/diagnostics_v3_e123 directory')
    args=a.parse_args();dir=args.results.resolve()
    if not dir.is_dir():raise FileNotFoundError(dir)
    all_dirs=[x for x in dir.iterdir() if x.is_dir() and (x/'manifest.json').exists()]
    if not all_dirs:raise ValueError('No measured diagnostic subfolders')
    for child in all_dirs:
        m=json.loads((child/'manifest.json').read_text())
        if m['frozen_hbench_sha256']!=EXPECTED or not m.get('frozen_sha256_unchanged_postrun'):
            raise ValueError('Refusing nonverified measurement '+str(child))
    lines=['# Diagnóstico E1–E3 — resumo dos resultados medidos',
           '',f'- HBench imutável: `{EXPECTED}`',
           '- Dados originais: 1.800 células métricas e 12 consultas amostradas do Java.',
           '- Métricas em milissegundos; p95 é calculado por consulta, não é p95 misto oficial.',
           '- TS2-U/TS2-D: variantes físicas do HBench já congelado; sem nova seleção.', '']
    r1=readcsv(dir/'e1'/'e1_summary.csv')
    if r1:
        lines+=['## E1 — decomposição de latência (rotas originais)','',
                'O tempo de *db_fetch* inclui driver e materialização das linhas; **não** é tempo exclusivamente no servidor.',
                '', '| Implantação | Q | p95 total | Requisições médias | Linhas CQL médias | DB fetch médio | Decodificação média | Lógica média |',
                '|---|---|---:|---:|---:|---:|---:|---:|']
        for x in r1:
            lines.append('| '+ ' | '.join([x['variant'],x['query_id'],fmt(x['p95_total_ms']),
                fmt(x['mean_requests']),fmt(x['mean_cql_rows']),fmt(x['mean_db_fetch_ms']),
                fmt(x['mean_decode_ms']),fmt(x['mean_logic_ms'])])+' |')
        lines.append('')
    r2=readcsv(dir/'e2'/'e2_summary.csv')
    if r2:
        lines+=['## E2 — baseline e TS2-U com projeções idênticas e pareamento AB/BA','',
                '| Q | Baseline p95 | TS2-U p95 | Média delta pareada (TS2-U − baseline) | Pares vencidos por TS2-U |',
                '|---|---:|---:|---:|---:|']
        for x in r2:
            lines.append('| '+' | '.join([x['query_id'],fmt(x['baseline_p95_ms']),fmt(x['ts2u_p95_ms']),
                       fmt(x['paired_mean_ts2u_minus_baseline_ms']),
                       f"{x['ts2u_lower_latency_pairs']}/{x['pairs']}"])+' |')
        lines+=['','**Interpretação:** E2 testa equivalência física/controlada, não altera o roteamento original do HBench.','']
    r3=[]
    for child in sorted(all_dirs):
        if child.name.startswith('e3_d'):
            r3.extend(readcsv(child/'e3_summary.csv'))
    if r3:
        lines+=['## E3 — escalas 1, 7 e 30 dias (sintéticas)','',
                'Dados dos dias adicionais resultam de repetição temporal dos valores originais. '
                'Não são novas amostras independentes do IoT Benchmark.', '',
                '| Dias | Configuração | Consulta | p95 total | Requisições médias | Linhas médias |',
                '|---:|---|---|---:|---:|---:|']
        for x in sorted(r3,key=lambda r:(int(r['scale_days']),r['query_id'],r['variant'])):
            lines.append('| '+' | '.join([x['scale_days'],x['variant'],x['query_id'],
                fmt(x['p95_total_ms']),fmt(x['mean_requests']),fmt(x['mean_cql_rows'])])+' |')
        lines+=['','As consultas `NATIVE_FIXED_*` mantêm parâmetros originais. '
                'As `EXTENDED_*` são sondas temporais deliberadamente novas.','']
    if not (r1 or r2 or r3):raise ValueError('No measured CSV records')
    lines+=['## Decisões e cautelas','',
            '- Nunca usar este relatório para alterar retroativamente Stage 4–8 ou o HBench congelado.',
            '- Diferenças pequenas de p95 requerem rodadas independentes e intervalos de confiança.',
            '- O experimento usa Python/Cassandra com objetos de parâmetros Java, não o replay SQL/frequência oficial.',
            '- E3 só varia a duração temporal. Escalas de dispositivos/sensores ficam para novo protocolo.','']
    out=dir/'DIAGNOSTIC_REPORT_PT.md'
    out.write_text('\n'.join(lines),encoding='utf-8')
    print('Report created:',out)

if __name__=='__main__':main()
