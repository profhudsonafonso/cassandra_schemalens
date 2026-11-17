# Emenda 05 — Fase 4 (E13–E18): correção do E10, regra com zona de empate e réplica do holdout E9

**Data:** 12/11/2026, depois da Fase 3 e antes de qualquer DDL, previsão ou medição da Fase 4. O hash deste arquivo entra
em todos os pré-registros seguintes (o motor grava o hash de todas as `EMENDA_*.md`). Continuam valendo o
`PROTOCOLO_FASE2.md` e as emendas 01–04: keyspaces novos `sl_f2_*`, sem DROP/TRUNCATE/ALTER, gates físicos, oráculo em
toda execução, evidência com modo `x`, medição interrompida descartada inteira e refeita (EMENDA_02).

`fase2_core.py`, `fase3_core.py`, `fase3_rev5.py` e `fase2_rev4.py` **não são alterados**. A campanha nova entra por
`fase4_core.py`, que importa `fase3_core` (campanhas da Fase 3 e o subconjunto de casos do P1) sem mudá-lo; o hash de
`fase4_core.py`, desta emenda e do arquivo da regra congelada vão num arquivo ao lado do pré-registro
(`preregistration_f4.json`).

## 1. Correção do E10 (registro; nenhuma medição nova)

`fase3_trace.py` leu `system_traces.sessions.started_at` (UTC sem fuso, como o driver devolve) como hora local (UTC−3), e
por isso associou 0 de 1.779 sessões às execuções, embora tenha terminado com PASS. Em 12/11 ~15:05, antes de os traces
expirarem (TTL de 24 h), as 1.779 sessões foram relidas, gravadas em `e10/rematch_utc/system_traces_sessions_dump.csv` e
associadas como UTC (`e10/rematch_utc/trace_raw_utc.csv`, `trace_summary_utc.json`). Os arquivos originais de `e10/`
ficam como estão. Limitação conhecida da reassociação: janela de ±2 ms sem exclusividade, de modo que em consultas de
1–5 ms uma sessão pode cair em duas execuções vizinhas (1.891 associações para 1.779 sessões); só janelas ≥ 7 dias são
usadas como evidência.

`fase3_trace.py` também fica como está (é o código que gerou a evidência de E10). O tracing novo (E16) usa
`fase4_trace.py`, que: (a) converte `started_at` como UTC; (b) atribui cada sessão a **uma única** execução — a que
contém o início da sessão; (c) separa execuções consecutivas por 20 ms de pausa.

## 2. E14 — Regra de decisão com zona de empate (congelada antes de E15)

A Fase 3 mostrou que os erros de vencedor das Revs estão em casos em que |ln(D/U)| medido fica dentro do ruído entre
réplicas (E7: diferença típica de p95 entre réplicas de 5,3–8,3%). A regra T5 usa a Rev5 congelada sem nenhum ajuste novo:

    r = D/U previsto pela Rev5 (rev5/rev5_frozen.json, inalterado)
    se ln r ≥ +δ  → recomenda TS2-U (decisão firme)
    se ln r ≤ −δ  → recomenda TS2-D (decisão firme)
    senão         → EMPATE TÉCNICO; recomendação por omissão TS2-U (seleção Rev3)
    δ = 0,10 (≈ 10,5%), fixado pelo ruído entre réplicas da E7 (D: 8,3%), não pelo resultado de E9

**Classe medida** de cada caso: empate se |ln(p95_D / p95_U)| < δ; caso contrário, o vencedor por p95.

**Métricas** (para T5, Rev5 sem zona, Rev4, "sempre U" e "menos chamadas"):

- *acerto firme*: entre os casos com decisão firme, fração em que o vencedor medido por p95 é o recomendado;
- *erro grave*: decisão firme e vencedor medido oposto com |ln medido| ≥ δ;
- *arrependimento* de seguir a recomendação: ln(p95 da variante recomendada / menor p95 entre U e D); mediana, p90 e máximo;
- *cobertura*: fração de casos com decisão firme.

**E14 (exploratório)**: T5 é aplicada aos casos do holdout E9 já medidos. Como E9 já foi visto, o resultado é só
descritivo e não conta como teste.

**Hipóteses confirmatórias, testadas só em E15 (e9r2):**

- H5.1: erros graves de T5 ≤ 5% das decisões firmes;
- H5.2: acerto firme de T5 ≥ 90%;
- H5.3: p90 do arrependimento de T5 ≤ 0,10 (≤ 10,5% mais lento que a melhor variante em 90% dos casos);
- H5.4 (comparação): o p90 do arrependimento de T5 não é maior que o de "sempre U".

A regra (δ, código e hash da Rev5) é gravada em `rev5/tie_rule_frozen.json` e as previsões T5 de e9r2 em
`rev5/tie_predictions_e9r2.json`, antes da primeira medição de E15. Ambos os hashes entram no pré-registro.

## 3. E15 — Réplica do holdout E9 (`e9r2`)

Os mesmos quatro conjuntos de E9 (H2b, H3b, N6, P1), já gerados e com hash conhecido, recarregados em keyspaces novos
`sl_f2_e9r2_*`, só SSTable, 60 rodadas, semente de medição nova **20261601**, mesmo subconjunto de casos do P1 (EMENDA_04).
Previsões Rev4, Rev5 e T5 gravadas antes de medir. Objetivos: (1) teste confirmatório de T5; (2) estabilidade de E9 entre
réplicas (mesmo vencedor, mediana D/U por conjunto); (3) a anomalia do P1 em janela de 1 dia (E9: D/U p95 ≈ 1,7).
Critério pré-declarado para (3): "reproduzida" se a mediana D/U dos casos de 1 dia do P1 em e9r2 for ≥ 1,30; "não
reproduzida" se ≤ 1,10; "inconclusiva" entre os dois.

## 4. E16 — Tracing do P1 (janelas de 1 e 7 dias), com a correção

Q2 e Q12, janelas de 1 e 7 dias, TS2-U e TS2-D, nos keyspaces de E9 e de e9r2 do P1, 30 execuções sequenciais por
combinação, fora de qualquer medição de latência. Probabilidade de tracing 1 só durante a amostra e 0 ao final (também em
erro). Saída: latência no cliente (mediana, p95), tempo de servidor por requisição e número de requisições. Serve para
explicar a anomalia do P1; o tracing acrescenta custo, então não substitui as latências de E9/E15.

## 5. E17 — Análise e E18 — Pacote

E17 avalia H5.1–H5.4 em e9r2, compara e9 × e9r2 e classifica a anomalia do P1, a partir só de evidência auditada. E18
gera o zip das Fases 3 e 4 (protocolo, emendas, pré-registros, latências brutas, auditorias, código; CSVs Java ficam fora
por serem reproduzíveis pela semente, com hash nos manifests) e acrescenta a seção da Fase 4 ao rascunho de resultados.

Ficam **fora**, aguardando decisão do responsável: cache frio real (A1) e remoção dos keyspaces `sl_f2_*` (A2).
