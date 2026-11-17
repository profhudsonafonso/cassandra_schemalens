# TSBS (cpu-only) — versão ajustada (protocolo comum de execução v1)

Corrige a lacuna mais grave apontada pela análise de consistência: até aqui, o TSBS tinha executado só **1 de 7**
deployments do HBench, com fixture sintético, oráculo não independente e sem medir armazenamento.

Relatório: [`relatorio_overleaf/main.tex`](relatorio_overleaf/main.tex) (e `main.pdf`).

## O que foi feito

1. **Fase de projeto** (`frozen_design/`): cópia de `SchemaLens_TSBS_TSDB_Only/` com os Stages 1–8 regenerados de forma
   determinística (as saídas não eram versionadas). Os SHA-256 de specs, planos, writes e deployments coincidem com
   os da execução anterior. O espaço admissível completo (2 + 24) foi reconstruído com o próprio consolidador.
2. **Dados oficiais** (`gen_data.sh`): `tsbs_generate_data` (Go, commit `8323e59c`) compilado num container
   `golang:1.22.5` descartável; 40 hosts, 600 s, 36 h. O gerador emite `series_bigint`, e um adaptador sem perda
   converte o tipo para o loader do Stage 9. Os dois arquivos e os dois hashes estão em `data/` e no pré-registro.
3. **Oráculo independente** (`tsbs_v1/oracle.py`): implementa os 16 templates sobre os eventos-fonte, sem usar
   planner, runtime ou materializer. Foi validado em 960/960 respostas antes de medir.
4. **Medição v1** (`tsbs_v1/measure.py`): 7 HBench + 3 Pareto não selecionados + baseline `series_bigint` + A/A;
   5 repetições a frio; gate de flush; guarda de isolamento; duas coortes (natural = decisão, aligned = secundária).
   Desvio declarado: 12 rodadas por repetição (60 amostras por classe e variante no total).
5. **Análise, ablações (pipeline inteiro regenerado com a regra neutralizada), aceitação e relatório.**

## Passo a passo

```bash
cd versao_ajustada/tsbs_tsdb
./gen_data.sh
./run_all.sh smoke      # opcional
./run_all.sh freeze     # PREREGISTRATION_TSBS_V1.json
./run_all.sh measure    # ~45 min (recomenda-se: setsid nohup ./run_all.sh measure &)
./run_all.sh analyze && ./run_all.sh ablation && ./run_all.sh accept
../.venv/bin/python tsbs_v1/make_tables.py --run outputs/v1_run1 && ./run_all.sh report
```
