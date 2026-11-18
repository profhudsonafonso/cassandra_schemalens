# Método V9

Pipeline E1–E23, em fases 0–8. Cada estágio grava `runs/v9_generation_001/stages/Exx_<nome>/RESULT.json` com o status,
as entradas e saídas (SHA-256), as verificações e a duração. Houve duas paradas obrigatórias, ambas aprovadas: no fim da
Fase 0 e no fim da Fase 4.

- **Fase 0 (INV, E1, E2):** inventário do repositório da V8 (somente leitura), preflight e fonte `rubis_full`.
- **Fase 1 (E3–E7):** esquema conceitual, estatísticas, carga, contrato semântico das 36 instruções e catálogo C0–C9.
- **Fase 2 (E8–E13):** evidências → ativação → P → canonicalização e fusão (C) → síntese de rotas.
- **Fase 3 (E14):** modelo de custo calibrado (NNLS) nos dados brutos da V8-012.
- **Fase 4 (E15, E16):** composição por ILP (ótimos por carga, Pareto e k-best) e Hbench congelado.
- **Fase 5 (E17–E19):** coorte, materialização, corretude por oráculo e replay, e baselines.
- **Fase 6 (E20, E21):** benchmark na coorte numa janela contínua, intercalada, com protocolo congelado e guarda de isolamento.
- **Fase 7 (E22):** ablações dos critérios. Cada critério é neutralizado e o gerador é rodado de novo; os esquemas novos
  são medidos (D-V9-013).
- **Fase 8 (E23):** os 2 melhores de H em escala completa a frio, contra NoSE e QPG; depois a aceitação e o relatório.

Os resultados estão em `runs/v9_generation_001/reports/V9_REPORT.md` e `ACCEPTANCE_V9.json`. As decisões estão em
`DECISION_LOG.md` e as limitações em `LIMITATIONS.md`.
