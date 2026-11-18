# Emenda 04 — Fase 3 (E7–E12): pendências após a Fase 2

**Data:** 12/11/2026, antes de qualquer DDL, validação ou medição da Fase 3. O hash deste arquivo entra em todos os
pré-registros seguintes. Todas as regras do `PROTOCOLO_FASE2.md` e das emendas 01–03 continuam valendo (keyspaces novos
`sl_f2_*`, sem DROP/TRUNCATE/ALTER, gates físicos, oráculo em toda execução, evidência com modo `x`, medição interrompida
descartada inteira).

`fase2_core.py` **não é alterado**: as campanhas novas são acrescentadas ao catálogo por `fase3_core.py`, que importa o
motor sem mudá-lo. O hash de `fase3_core.py` é gravado num arquivo ao lado de cada pré-registro (`preregistration*_f3.json`).

## Ordem

| Ordem | Etapa | O que | Por quê |
|---:|---|---|---|
| 1 | E7 | Réplicas `e3r2` (H1, H2, H3) e `e4r2` (N3, N18): keyspaces novos, mesmos dados, sementes de medição novas (20261311, 20261411) | E3/E4 tinham réplica única |
| 2 | E8 | Rev5: calibrar e congelar | A Rev4 empatou com "sempre U" e subestimou o custo por chamada em janelas longas |
| 3 | E9 | Holdout novo `e9`: H2b (83 d), H3b (333 d), N6 (6 disp.) com sementes Java inéditas 5512xx–5514xx, e P1 (partições grandes) | Teste da Rev5 sem leakage; cenário em que o bucket diário poderia compensar |
| 4 | E10 | Tracing do servidor numa amostra (H3 de E3 e P1 de E9) | Separar tempo do servidor e tempo de cliente/rede |
| 5 | E11 | Avaliação da Rev5 (e da Rev4) no holdout E9; análise das réplicas | — |
| 6 | E12 | Rascunho do texto de resultados, gerado só a partir dos JSON | — |

Ficam **fora** desta emenda, aguardando decisão do responsável: cache frio real (reinício do container + root) e remoção
dos keyspaces `sl_f2_*` (irreversível).

## P1 — partições grandes

3 dispositivos, `POINT_STEP` = 60 s, 96.000 loops (2.880.000 células, 333 dias): ~480 mil linhas por partição de TS2-U
(~18 MB) contra ~1.440 linhas por partição diária de TS2-D. Para manter o tempo de medição viável, em P1 os casos são os 16
temporais (1/7/14/21 d), a extensão W80 e o controle Q8; **saem** W330 e Q5_CONTROL (que leem o histórico inteiro, ~480 mil
linhas por consulta). A remoção é determinística e registrada em cada caso.

## Rev5 (pré-especificada)

Modelo aditivo, por ser o custo físico esperado de uma consulta paginada (custo fixo + custo por chamada + custo por linha):

    p95_ms = b0 + b1·calls + b2·calls·ln(partição média em bytes) + b3·rows

- Desenvolvimento: todos os casos temporais **em SSTable** já medidos e auditados (S3 R1, E1 r2b/r3 condição sst, E3, E4,
  E7), variantes U e D.
- Ajuste: mínimos quadrados ponderados por 1/p95² (erro relativo), uma única vez, coeficientes congelados com hash em
  `rev5/rev5_frozen.json`.
- Entradas na previsão: `execute_calls`, `iterated_rows` (validação semântica) e partição média (tablestats pós-flush).
- Regra: prever TS2-D se p95_D previsto < p95_U previsto. Previsões de E9 gravadas antes da primeira medição de E9, junto
  com as da Rev4 congelada, e incluídas no pré-registro.
- Comparação: Rev5, Rev4, "sempre U" e "menos chamadas vence"; acerto do vencedor e erro mediano de ln(D/U).

## E10 — tracing

`nodetool settraceprobability 1` só durante a amostra e volta a 0 ao final (também em caso de erro). 10 execuções sequenciais
por caso temporal × {U, D}, fora de qualquer medição de latência da Fase 3. As sessões de `system_traces` são associadas a
cada execução pelo intervalo de tempo. Saída: tempo de servidor por requisição, requisições (páginas) por consulta e fração
do tempo total que passa no servidor. O tracing acrescenta custo: os valores servem para decompor o tempo, não como latência
de referência.
