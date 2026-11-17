# SchemaLens + TSBS — Stage 8 Add-on (TSDB-only)

**Entrega incremental:** este pacote contém **somente arquivos novos**. Extraia **o conteúdo** do ZIP na raiz do projeto que já possui as Etapas 3, 4, 5, 6 e 7. Nenhum arquivo anterior é substituído; o motor de regras TS0–TS8 e os modificadores M1–M5 não são reexecutados ou modificados.

## O que esta etapa faz

Executa **consolidação tardia por workload** (*late aggregation*): usa as famílias TSDB já ativadas, os nove `CassandraCandidateSpec` da Etapa 4, os 37 QueryPlans da Etapa 5, os nove WritePlans da Etapa 6 e os proxies analíticos da Etapa 7 para enumerar **deployments completos** que cobrem as 16 consultas TSBS `cpu-only`. O algoritmo agrega os planos por consulta **depois** de sua geração, jamais substituindo essa etapa por novos gatilhos de ativação.

- Busca exata no pequeno universo de nove tabelas candidatas, **sem truncamento oculto**; enumera até 256 subconjuntos contendo a fonte TS0 obrigatória por cenário.
- Reutiliza a mesma tabela entre consultas, exige WritePlan de cada tabela e remove combinações com tabelas que não são utilizadas em nenhum plano escolhido.
- Identifica uma frente **não dominada** considerando três proxies independentes: leitura, armazenamento e chaves afetadas por escrita. Em seguida ordena essa frente com pesos **hipotéticos predefinidos**, para planejar um **Top-k HBench pré-benchmark**.
- Produz **dois cenários**: `conservative` (não presume alinhamento; evita rollups condicionais) e `conditional_aligned` (permite TS5/TS6 como **hipóteses**, exigindo posterior verificação do alinhamento nos parâmetros de consulta).
- **Não** usa latência p95, throughput, resultados publicados do TSBS, desempenho antigo, nem score calibrado com benchmark. **Não** executa Cassandra/TSBS.

## Novos arquivos

```text
methodology/tsbs/stage8/__init__.py
methodology/tsbs/stage8/consolidator.py
methodology/tsbs/stage8/cli.py
config/tsbs_stage8_consolidation_policy.json
run_tsbs_stage8.sh
tests/test_tsbs_stage8.py
examples/show_stage8_hbench.py
docs/TSBS_STAGE8_GUIA_PT.md
README_STAGE8_ADDON.md
```

## Instalação e execução no VS Code (Ubuntu)

1. Abra no VS Code a **mesma raiz** que contém `methodology/`, `config/`, `inputs/`, `outputs/` e `tests/` das etapas anteriores.
2. Extraia os arquivos do ZIP preservando a estrutura acima. Não copie uma pasta aninhada `SchemaLens_TSBS_Stage8_ADDON` para dentro do projeto.
3. Verifique que os artefatos das Etapas 3–7 estão gerados (caso não estejam, execute na ordem os scripts dessas etapas).
4. Na raiz:

```bash
python3 -m methodology.tsbs.stage8.cli
# ou: bash run_tsbs_stage8.sh
python3 examples/show_stage8_hbench.py
python3 -m unittest discover -s tests -v
```

Somente biblioteca padrão Python é necessária. A etapa gera os arquivos em `outputs/tsbs_stage8/`:

```text
28_hbench_selected_deployments.json   # cada deployment e escolha de QueryPlan por consulta
29_hbench_selected_query_routes.json  # linhas por consulta e deployment selecionado
30_hbench_summary.csv                 # tabela curta para análise
31_stage8_manifest.json               # hashes de entradas, limites e advertências
32_stage9_execution_plan.csv          # referências a DDL/WritePlan, AINDA não executáveis
```

## Política analítica e observações essenciais

O arquivo `config/tsbs_stage8_consolidation_policy.json` declara **antecipadamente**:

- Pesos de leitura: 20% fan-out CQL + 80% linhas lógicas (ambos normalizados pela TS0 de cada consulta).
- Pesos de deployment: 80% proxy médio de leitura + 10% proxy de armazenamento + 10% proxy de chaves afetadas na escrita.
- Frequência **uniforme ilustrativa** para os 16 templates, pois não há frequências medidas da workload no conjunto de evidências disponível. Esses valores **não** foram ajustados por benchmarks.
- `top_k=5`; pesquisa exata `<=16` candidatos; todas as dependências de fonte canônica são preservadas.

**HBench não significa aqui “melhor desempenho demonstrado”**: é somente uma seleção prévia para testes futuros. **Não apresente redução de espaço, Top-k, regret ou near-best como garantias de qualidade** sem oráculo retrospectivo e experimentos reais (Etapa 9).

No perfil conservador, as consultas ainda exigem condições da Etapa 5 (ex.: catálogo completo de hosts), não verificadas em Cassandra. No perfil condicional, os resultados das consultas que usam TS5/TS6 **somente** serão corretos se os limites efetivos do TSBS estiverem alinhados à granularidade materializada; caso contrário, usar a TS0 como fallback e medir esse caminho. A Etapa 6 ainda exige fonte durável e manutenção consistente para produção.

O baseline Cassandra **original do TSBS** é uma implantação externa obrigatória para a comparação experimental, e não deve ser confundido com a TS0 gerada na Etapa 4.

Consulte `docs/TSBS_STAGE8_GUIA_PT.md` para conceitos, fórmulas, exemplos, interpretações e limitações.
