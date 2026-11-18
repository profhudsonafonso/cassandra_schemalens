# Emenda 01 — E1 executada fator por fator (antes de qualquer medição)

**Data:** 11/11/2026, antes de qualquer latência da Fase 2. O hash deste arquivo entra em todos os pré-registros seguintes.

## O que aconteceu

A primeira tentativa da réplica 2 (`e1r2`) materializou os 4 fatores (24 keyspaces) antes de qualquer flush. Durante a carga do 8×, o uso total das memtables passou do limite de limpeza do Cassandra e o `SlabPoolCleaner` descarregou sozinho as duas maiores memtables da condição memória (`sl_f2_e1r2_d8_mem_ref` e `sl_f2_e1r2_d8_mem_u`, `Reason: MEMTABLE_LIMIT`, 20:58–20:59 UTC). O gate pós-flush reprovou (SSTable = 1 na condição memória) e a campanha parou, como previsto no protocolo. Nenhuma validação ou medição de `e1r2` foi feita.

## Mudanças

1. `e1r2` fica preservada como evidência de falha (`abandoned`). Suas tabelas de memória são "aposentadas" por `nodetool flush` (dados mantidos, nada é apagado) para liberar memória.
2. A réplica 2 é refeita como `e1r2b` (semente 20261212); a réplica 3 continua `e1r3` (semente 20261223).
3. Sequência por fator, nas duas réplicas: carregar as 3 tabelas SSTable e descarregar cada uma logo após a carga → carregar as 3 tabelas de memória → gate (SSTable = 0 e memtable > 0 na memória) → validação semântica → **pré-registro do fator** → medição → auditoria → aposentar as tabelas de memória do fator.
4. O pré-registro passa a ser por fator nas campanhas E1 (`preregistration_d{f}.json`), sempre antes da medição daquele fator. As demais regras (randomização, rodadas, gates, travas de segurança) não mudam.

## Efeito sobre a interpretação

Nenhum. A ordem por fator já era a ordem de medição; só muda quando cada fator é carregado. O pico de memória passa a ser o de um único fator (~243 MB on-heap no 8×), abaixo do limite observado (~600 MB).
