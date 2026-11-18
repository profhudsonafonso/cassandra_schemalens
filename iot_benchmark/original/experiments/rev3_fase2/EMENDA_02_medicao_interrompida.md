# Emenda 02 — medição interrompida é descartada inteira e refeita

**Data:** 11/11/2026, 18:5x (horário local). Escrita antes de refazer `e1r2b/d4`.

## O que aconteceu

O orquestrador (`fase2_run.py`) parou sem registrar falha durante `e1r2b:run:d4`, logo depois do snapshot
`storage_pre_timing` (gate PASS nos 6 keyspaces), por volta das 18:18. Não houve erro do Python nem morte por falta de memória
registrada no kernel; a causa mais provável é o encerramento do terminal/sessão que havia iniciado o processo.
`e1r2b/d4/benchmark/` ficou vazia: **nenhuma latência foi gravada**.

## Regra (vale para toda a Fase 2)

1. Uma medição sem `post_timing_manifest.json` é descartada inteira. Nenhum dado parcial é usado.
2. As saídas parciais (`benchmark/`, `storage_pre_timing/`, `storage_post_timing/`) são **movidas**, nunca apagadas,
   para `<campanha>/<conjunto>/aborted_attempts/NN_<data>/`, com um README.
3. A medição é refeita do zero com o mesmo código, pré-registro e semente: gate de armazenamento → oráculos → aquecimento →
   rodadas. Se o gate reprovar (ex.: a memtable foi descarregada nesse meio-tempo), a medição para, como no protocolo.
4. A movimentação é feita pelo orquestrador (`quarantine_partial_run` em `fase2_run.py`); `fase2_core.py` não muda, então os
   hashes de pré-registro continuam válidos.
5. O orquestrador passa a rodar desacoplado do terminal (`setsid nohup`) e grava PID + sinal de vida a cada 60 s.

## Efeito sobre a interpretação

Nenhum na randomização ou nas rodadas. Os keyspaces tiveram um ciclo extra de leituras (oráculos/aquecimento da tentativa
interrompida) antes da medição; como toda medição já começa com oráculos e aquecimento, o estado de cache resultante é o
mesmo do protocolo.

## Registro das tentativas de `e1r2b/d4`

- `aborted_attempts/01_…`: tentativa original, interrompida ~18:18 após o gate pre-timing (nenhuma latência).
- `aborted_attempts/02_…` e `03_…`: relançamentos às 18:53 com o interpretador errado (Python do sistema, sem o driver
  `cassandra`); falharam em `connect()` logo após o gate pre-timing (PASS), sem nenhuma consulta temporizada.
- Tentativa válida: relançada às 18:54 com `/home/hudson/Documents/schemalens_cassandra/venv/bin/python`.
