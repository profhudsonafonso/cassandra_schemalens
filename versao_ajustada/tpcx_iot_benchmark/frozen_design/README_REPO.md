# TPCx-IoT / Cassandra — pacote no repositório

Origem: `SchemaLens_TPCx_IoT_Cassandra_Stage9_1_TARGET_FIX` (versão final e cumulativa das Etapas 1–3 → 9.1).
As pastas intermediárias (`Stage1_3`, `Stage4` … `Stage9_1_STORAGE_FIX`) têm o mesmo código e
diferem só no README da etapa. Os guias de cada etapa estão em `docs/ETAPA*_PT.md`.

## Conteúdo

| Pasta | Conteúdo |
|---|---|
| `methodology/tpcx_iot/` | pipeline SchemaLens (stage4 … stage91) |
| `config/`, `inputs/` | políticas e esquema EER do TPCx-IoT |
| `tests/` | testes unitários e de regressão (`python -m unittest discover -s tests -q`) |
| `docs/` | guias em português e mapeamento para o artigo DKE |
| `outputs/tpcx_iot_stage*/` | saídas offline das Etapas 1–9 |
| `outputs/stage91_main/`, `stage91_verification/`, `stage91_pilot_fixed/` | execuções reais no Cassandra (manifestos, resumos CSV, análise, medições `nodetool`) |

## Fora do repositório

`outputs/**/outboxes/*.sqlite` (cerca de 290 MB): os ledgers de escrita de cada repetição. São recriados a cada
execução e não entram no cálculo dos resultados publicados (`72_*_summary.csv`, `74_*_analysis.json`).
