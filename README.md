# SchemaLens V9 — geração automática de esquemas (RUBiS / Cassandra)

> **Esta é a execução V9. A V1–V8 está em `~/Documents/rubis3_claude/experiments/rubis_cassandra` e não foi alterada.**
> A V9 só lê aquele repositório (código e evidência da V8-012), e registra cada arquivo lido com SHA-256 em
> `runs/v9_generation_001/provenance/V8_INPUTS.json`.

## O que muda em relação à V8

A auditoria da V8 mostrou que os dois esquemas medidos (AUTO_H_norm e AUTO_H_c3c5c7) e as suas rotas foram
escritos à mão, que os mecanismos analíticos não influenciavam a decisão e que as famílias C3/C5/C7 eram
acréscimos, nunca substituições. A V9 refaz o pipeline E1–E23 de forma que:

- o catálogo C0–C8 declara `provides`, `supersedes`, `maintenance` e `activation` (predicado quantitativo);
- a ativação lê a matriz de evidências calculada de `rubis_full` e da carga;
- as rotas de leitura e de escrita são sintetizadas por código;
- H (pelo menos 5 esquemas) sai de uma otimização com modelo de custo calibrado nos dados da V8;
- Hbench é congelado com hash antes de qualquer medição da V9.

## Estrutura

| Pasta | Conteúdo |
|---|---|
| `schemalens_v9/` | código novo (estágios, monitor, isolamento, inventário; depois catálogo, gerador, rotas, custo, composição) |
| `reused/` | código e entradas copiados da V8 (`REUSE_MANIFEST.json`: origem, SHA-256 de origem e da cópia) |
| `runs/v9_generation_001/` | todas as saídas desta execução (`stages/Exx_*/RESULT.json`, inventário, proveniência) |
| `monitor/` | `MONITOR.html` (abre com duplo clique) e `status.json`, atualizados a cada 15 s |
| `docs/` | `DECISION_LOG.md`, `METHOD_V9.md`, `LIMITATIONS.md` |

## Como rodar

```
.venv/bin/python -m schemalens_v9.monitor --loop     # monitor
.venv/bin/python -m schemalens_v9.phase0 E1          # preflight
.venv/bin/python -m schemalens_v9.phase0 E2          # contagem de rubis_full (somente leitura)
.venv/bin/python -m schemalens_v9.inventory          # inventário da Fase 0
```

Run id: `v9-generation-20261112-001`. Keyspaces criados pela V9 começam com `rubis_v9_`.
