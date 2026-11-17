# SchemaLens + TSBS — Etapa 7 (somente arquivos adicionais)

**Objetivo:** adicionar diagnósticos analíticos de viabilidade e custo físico para os **nove candidatos TS0/TS1/TS2/TS5/TS6** e **37 QueryPlans** das Etapas 4–6. Não altera o motor de ativação, C0–C9, PK/CK/CQL, WritePlans ou QueryPlans e **não escolhe o vencedor/HBench**.

## Arquivos novos

```text
methodology/tsbs/stage7/__init__.py
methodology/tsbs/stage7/estimator.py
methodology/tsbs/stage7/operators.py
methodology/tsbs/stage7/cli.py
config/tsbs_stage7_cost_policy.json
inputs/tsbs_stage7_mozaffari_evidence.json
run_tsbs_stage7.sh
tests/test_tsbs_stage7.py
examples/show_stage7_costs.py
docs/TSBS_STAGE7_GUIA_PT.md
README_STAGE7_ADDON.md
```

O ZIP é **incremental**: copie o conteúdo de suas subpastas para a raiz do projeto que já possui a ativação TSDB-only e os add-ons Stage 4, 5 e 6. **Não** crie uma segunda pasta de projeto por dentro. A instalação não substitui arquivos anteriores.

## Executar no Ubuntu / VS Code

```bash
cd /path/to/SchemaLens_TSBS_TSDB_Only
python3 -m methodology.tsbs.stage7.cli
# ou: ./run_tsbs_stage7.sh
python3 examples/show_stage7_costs.py
python3 -m unittest discover -s tests -v
```

Dependências: **somente biblioteca padrão do Python**. Antes, execute as Etapas 4, 5 e 6 para criar seus artefatos em `outputs/`.

## Saídas novas (geradas ao executar, NÃO distribuídas dentro do ZIP)

```text
outputs/tsbs_stage7/
  21_physical_footprint_proxies.json
  22_write_amplification_proxies.json
  23_read_cost_proxies.json
  24_structural_transformations.json
  25_mozaffari_signal_audit.json
  26_query_cost_matrix.csv
  27_stage7_manifest.json
```

Os arquivos trazem SHA-256 dos insumos e rejeitam alterações inesperadas nos artefatos das Etapas 4–6. Não executam DDL, DML, consultas reais ou benchmarking.

## Delimitação científica crucial

1. O cenário ilustrativo já registrado no JSON da Etapa 3 contém **100 hosts, um ponto/minuto/host e três dias**. As estimativas se referem SOMENTE a esse cenário e à data de início prespecificada em `config/tsbs_stage7_cost_policy.json`. Esse não é um valor medido, nem um padrão obrigatório do TSBS.
2. Os números de rows, partitions, CQL fan-out e bytes são **proxies analíticos**. Byte proxy usa larguras presumidas de campos e overhead por linha; não representa tamanho SSTable, leitura de rede, p95 ou custo monetário.
3. Todas as 16 consultas têm `relative_frequency` **UNSPECIFIED**. Nenhuma pontuação ponderada por workload nem ordenação de candidatos é calculada.
4. TS5/TS6 exigem uma fonte canônica para recomputações e manutenção consistente, não oferecida como serviço persistente nesta etapa. Os rollups condicionais continuam exigindo alinhamento temporal; `all-hosts` ainda requer catálogo de hosts externo.
5. FR/FU/RC/TI de Mozaffari são sinais direcionais próprios, **não** iguais a `schema_rc`, frequência de amostras ou flags de atualização. Estão `null`. Não é implementada a fórmula publicada de `Cost_R`/`Cost_D`.
6. O arquivo de operadores descreve transformações de projeto inspiradas nos tópicos estudados (materialização/bucketing/co-localização). **Não** implementa o otimizador NoSE nem operadores de Chen/QPG.
7. As regras TS0–TS8 não são reexecutadas ou alteradas. Nenhum resultado TSBS antigo ou novo escolhe famílias, candidatos ou limiares nesta etapa.

Leia `docs/TSBS_STAGE7_GUIA_PT.md` antes de interpretar as estimativas como resultados experimentais.
