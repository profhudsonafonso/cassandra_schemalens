# Emenda 03 — correção do pré-registro das campanhas não divididas por fator

**Data:** 11/11/2026, ~20:05 (horário local). Escrita antes de qualquer pré-registro ou medição de E3, E4 e E6.

## O que aconteceu

`e3:prereg` falhou com `per-dataset campaigns need --dataset; others must not pass it`. Em `cmd_preregister`, a
comparação `c.get('per_dataset') != bool(only)` dava `None != False` → verdadeiro para toda campanha sem a chave
`per_dataset` (E3, E4, E6), recusando o pré-registro correto (sem `--dataset`). Foi um defeito introduzido junto com a
EMENDA_01, que só o caminho por fator (E1) havia exercitado.

## Mudança

Uma linha: `bool(c.get('per_dataset')) != bool(only)`. Nenhuma regra de randomização, rodada, gate ou trava mudou.

- `fase2_core.py` antes: `e347cad58880e6e7870502420f71839f331ddc05cc60b189b438e5c243919398` (registrado nos pré-registros de `e1r2b` e `e1r3`; E1 está todo medido e auditado com ele)
- `fase2_core.py` depois: `f7e3433079d5c46b7c8657e7086a28e0885691d96013c9cd59f15f3b29418b64` (será registrado nos pré-registros de E3, E4 e E6)

## Efeito sobre a interpretação

Nenhum. Os passos já feitos de E3 (materialização, flush, validação de h1–h3 e previsões Rev4) não dependem dessa função, e
as previsões Rev4 continuam gravadas antes de qualquer medição.
