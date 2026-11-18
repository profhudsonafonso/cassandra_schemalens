# SchemaLens V9 — relatório da execução `v9-generation-20261112-001`

Gerado em 2026-11-13T08:33:03-0300 por `schemalens_v9/report.py`. Todos os números são lidos dos arquivos de resultado dos estágios; o que não foi executado aparece como `NOT_EXECUTED`.

## 1. Estado dos estágios

| Estágio | Nome | Status | Duração (s) |
|---|---|---|---|
| INV | Inventário do repositório antigo | PASS | 0,2 |
| E1 | Preflight do ambiente | PASS | 11,0 |
| E2 | Fonte de dados rubis_full | PASS | 45,6 |
| E3 | Esquema conceitual S (EER) | PASS | 0,2 |
| E4 | Estatísticas observadas | PASS | 67,0 |
| E5 | Carga W (rubis.rb) | PASS | 0,0 |
| E6 | Contrato semântico das 36 instruções | PASS | 37,6 |
| E7 | Catálogo com contratos | PASS | 0,0 |
| E8 | Fragmentos e raízes | PASS | 0,0 |
| E9 | Matriz de evidências | PASS | 0,1 |
| E10 | Ativação A(q) | PASS | 0,1 |
| E11 | Especificações P | PASS | 0,1 |
| E12 | Canonicalização e fusão | PASS | 0,2 |
| E13 | Síntese de rotas | PASS | 3,6 |
| E14 | Modelo de custo calibrado | PASS | 55,9 |
| E15 | Composição H | PASS | 0,6 |
| E16 | Hbench congelado | PASS | 0,0 |
| E17 | Coorte e materialização | PASS | 33,2 |
| E18 | Corretude (oráculo + replay) | PASS | 737,4 |
| E19 | Baselines NoSE e QPG | PASS | 366,7 |
| E20 | Benchmark na coorte | PASS | 6.873,9 |
| E21 | Análise | PASS | 0,0 |
| E22 | Ablações dos critérios | PASS | 5.591,9 |
| E23 | Escala completa, aceitação e relatório | PASS | 42.624,5 |

## 2. O que mudou em relação à V8

- **Espaço de projeto.** V8: P = 86, C = 41, gerados por um módulo desligado de H. V9: P = 57, C = 24 (CCR = 0,5789), com linhagem até cada tabela de H.
- **Oráculo.** V8 calculava a resposta exata só das instruções 34 e 35. V9 calcula as 36 a partir de `rubis_full` e reproduz a referência da V8 em 397479 de 397479 avaliações.
- **Mecanismos.** access_profile: apenas metadados/especificações; relationships: apenas metadados/especificações; sharedness: composição H; support_sharing: estrutura física de C; write_exposure: estrutura física de C.
- **Rotas.** QueryPlans e WritePlans sintetizados por código (`schemalens_v9/routes.py`) e executados por um executor genérico; nenhuma rota escrita à mão para os esquemas de H.
- **Famílias como substituição.** O catálogo declara `supersedes`; a composição proíbe uma tabela de família junto com as tabelas que ela substitui.

## 3. Modelo de custo (E14)

Coeficientes (ms): leitura a = 0,4620, escrita b = 0,3887, linha lida c = 0,00751; constante por requisição d = 0,1485; R² ponderado = 0,9991. Calibrado em 12 esquemas da V8-012.

| Carga | τ (12 esquemas) | τ deixando um de fora | τ só competitivos |
|---|---|---|---|
| Browsing | 0,818 | 0,758 | 0,733 |
| Bidding | 0,879 | 0,788 | 0,822 |
| Update10x | 0,849 | 0,849 | 0,778 |
| Update100x | 0,818 | 0,818 | 0,733 |

## 4. Esquemas gerados (H)

| Esquema | Selecionado por | Tabelas | Famílias | Custo previsto Browsing | Bidding | Update10x | Update100x |
|---|---|---|---|---|---|---|---|
| H01 | fronteira de Pareto (leitura × escrita × espaço, mix bidding); ótimo da carga browsing; ótimo da carga update10x | 13 | C0, C1, C4, C5 | 1,053 | 1,561 | 2,016 | 2,434 |
| H02 | fronteira de Pareto (leitura × escrita × espaço, mix bidding); ótimo da carga bidding | 14 | C0, C1, C4, C5 | 1,053 | 1,555 | 2,039 | 2,484 |
| H03 | ótimo da carga update100x | 13 | C0, C1, C2, C4, C5 | 5,493 | 4,221 | 3,198 | 2,256 |
| H04 | fronteira de Pareto (leitura × escrita × espaço, mix bidding) | 13 | C0, C1, C2, C4, C5 | 1,758 | 2,273 | 2,391 | 2,499 |
| H05 | fronteira de Pareto (leitura × escrita × espaço, mix bidding) | 13 | C0, C1, C2, C4, C5 | 6,198 | 4,933 | 3,573 | 2,321 |

- Âncora `anchor_sl_norm`: 14 tabelas; coincide com opt_bidding.

- Âncora `anchor_c5_substitution`: 14 tabelas; coincide com opt_bidding.
- AUTO_H_norm da V8: 19 tabelas; âncora gerada: 14 tabelas.

Hbench congelado em 2026-11-12T11:53:08-0300 (hash `8446365297fc4918…`), |Hbench| = 5, DeploymentReduction = 0,00.

## 5. Corretude (E18, E19)

| Esquema | Concorda com o oráculo V9 | Concorda com a referência V8 |
|---|---|---|
| H01 | 79429/79429 | 79429/79429 |
| H02 | 79429/79429 | 79429/79429 |
| H03 | 79429/79429 | 79429/79429 |
| H04 | 79429/79429 | 79429/79429 |
| H05 | 79429/79429 | 79429/79429 |
| NoSE | 79429/79429 | 79429/79429 |
| QPG | 79429/79429 | 79429/79429 |
| V8_AUTO_H_norm | 79429/79429 | 79429/79429 |

## 6. Benchmark na coorte (E20, E21)

Latência média por requisição (ms, IC 95% t, n = 5), ordem intercalada, mesma tarde (ver janelas abaixo): H01, NoSE, H02, V8_AUTO_H_norm, H03, QPG, H04, H05.

| Esquema | Browsing | Bidding | Update10x | Update100x |
|---|---|---|---|---|
| H01 | 1,058 ± 0,050 | 1,484 ± 0,175 | 2,284 ± 0,177 | 2,991 ± 0,134 |
| NoSE | 1,244 ± 0,146 | 2,105 ± 0,243 | 3,070 ± 0,180 | 3,124 ± 0,408 |
| H02 | 1,066 ± 0,044 | 1,954 ± 0,727 | 3,027 ± 0,345 | 3,654 ± 0,208 |
| V8_AUTO_H_norm | 1,953 ± 0,200 | 2,907 ± 0,649 | 3,604 ± 0,307 | 3,377 ± 0,238 |
| H03 | 6,689 ± 0,507 | 5,098 ± 0,237 | 4,100 ± 0,277 | 2,739 ± 0,081 |
| QPG | 1,195 ± 0,063 | 1,703 ± 0,133 | 2,593 ± 0,149 | 2,659 ± 0,038 |
| H04 | 1,939 ± 0,320 | 2,334 ± 0,006 | 2,925 ± 0,097 | 3,178 ± 0,060 |
| H05 | 6,831 ± 0,124 | 5,345 ± 0,123 | 4,182 ± 0,170 | 2,967 ± 0,094 |

Contrastes pareados (esquema de H − referência, ms):

| Esquema | Contra | Browsing | Bidding | Update10x | Update100x |
|---|---|---|---|---|---|
| H01 | NoSE | H mais rápido (-0,186; -14,9%) | H mais rápido (-0,622; -29,5%) | H mais rápido (-0,786; -25,6%) | empate (-0,133; -4,3%) |
| H01 | QPG | H mais rápido (-0,137; -11,4%) | H mais rápido (-0,219; -12,9%) | H mais rápido (-0,310; -11,9%) | H mais lento (0,332; 12,5%) |
| H01 | V8_AUTO_H_norm | H mais rápido (-0,895; -45,8%) | H mais rápido (-1,423; -49,0%) | H mais rápido (-1,321; -36,6%) | H mais rápido (-0,386; -11,4%) |
| H02 | NoSE | H mais rápido (-0,179; -14,4%) | empate (-0,151; -7,2%) | empate (-0,043; -1,4%) | H mais lento (0,529; 16,9%) |
| H02 | QPG | H mais rápido (-0,129; -10,8%) | empate (0,251; 14,8%) | H mais lento (0,434; 16,7%) | H mais lento (0,995; 37,4%) |
| H02 | V8_AUTO_H_norm | H mais rápido (-0,888; -45,5%) | H mais rápido (-0,953; -32,8%) | H mais rápido (-0,577; -16,0%) | H mais lento (0,276; 8,2%) |
| H03 | NoSE | H mais lento (5,445; 437,5%) | H mais lento (2,992; 142,1%) | H mais lento (1,030; 33,5%) | H mais rápido (-0,386; -12,3%) |
| H03 | QPG | H mais lento (5,494; 459,8%) | H mais lento (3,395; 199,4%) | H mais lento (1,507; 58,1%) | H mais lento (0,080; 3,0%) |
| H03 | V8_AUTO_H_norm | H mais lento (4,736; 242,4%) | H mais lento (2,191; 75,4%) | H mais lento (0,496; 13,8%) | H mais rápido (-0,639; -18,9%) |
| H04 | NoSE | H mais lento (0,694; 55,8%) | empate (0,229; 10,9%) | H mais rápido (-0,145; -4,7%) | empate (0,053; 1,7%) |
| H04 | QPG | H mais lento (0,744; 62,2%) | H mais lento (0,631; 37,1%) | H mais lento (0,332; 12,8%) | H mais lento (0,519; 19,5%) |
| H04 | V8_AUTO_H_norm | empate (-0,015; -0,8%) | empate (-0,573; -19,7%) | H mais rápido (-0,679; -18,8%) | empate (-0,200; -5,9%) |
| H05 | NoSE | H mais lento (5,587; 449,0%) | H mais lento (3,240; 153,9%) | H mais lento (1,112; 36,2%) | empate (-0,158; -5,0%) |
| H05 | QPG | H mais lento (5,636; 471,6%) | H mais lento (3,642; 213,9%) | H mais lento (1,589; 61,3%) | H mais lento (0,308; 11,6%) |
| H05 | V8_AUTO_H_norm | H mais lento (4,878; 249,7%) | H mais lento (2,438; 83,9%) | H mais lento (0,578; 16,0%) | H mais rápido (-0,411; -12,2%) |

Previsto × medido (dentro de H):

| Carga | Ordem prevista | Ordem medida | τ | Top-1 preservado | Regret (ms) | A até 5% do melhor |
|---|---|---|---|---|---|---|
| Browsing | H01 < H02 < H04 < H03 < H05 | H01 < H02 < H04 < H03 < H05 | 0,900 | sim | 0,000 | H01, H02 |
| Bidding | H02 < H01 < H04 < H03 < H05 | H01 < H02 < H04 < H03 < H05 | 0,800 | não | 0,470 | H01 |
| Update10x | H01 < H02 < H04 < H03 < H05 | H01 < H04 < H02 < H03 < H05 | 0,800 | sim | 0,000 | H01 |
| Update100x | H03 < H05 < H01 < H02 < H04 | H03 < H05 < H01 < H04 < H02 | 0,800 | sim | 0,000 | H03 |

Htop (não dominados nas 4 cargas, só esquemas de H): H01, H03.

**Piso de ruído (pares de esquemas com trabalho idêntico na carga).** Mesmas leituras, escritas e linhas lidas por requisição; a diferença medida não pode ser efeito de esquema:

| Carga | Par | Leituras/req | Diferença medida (ms) |
|---|---|---|---|
| Browsing | H01 × H02 | 1,47 | -0,007 |

Maior diferença entre esquemas de trabalho idêntico: 0,007 ms. Contrastes menores que isso não devem ser lidos como efeito de esquema.

Janelas de medição (início–fim): H01 16:57–17:06; NoSE 17:06–17:16; H02 17:16–17:28; V8_AUTO_H_norm 17:28–17:41; H03 17:41–18:00; QPG 18:00–18:10; H04 18:10–18:21; H05 18:21–18:41. Janela contínua única: keyspaces rematerializados de `rubis_full` antes da janela; qualquer medição invalidada descarta a janela inteira (a medição anterior, em dois blocos, está arquivada em `E20_benchmark_run1_two_blocks`).

## 7. Ablações dos critérios (E22)

Cada critério foi neutralizado (D-V9-013) e o gerador foi rodado de novo (ativação → P → C → H).

| Critério | Neutralização | Ativações alteradas | ΔP | ΔC | Esquemas de H alterados | Resultado nulo |
|---|---|---|---|---|---|---|
| access_profile | expected_rows = rows_read_bound = 1, relaxed_ratio = ∞, nonroot_proj_bytes = 0 (perfil de acesso neutro) | 0 | 0 | 0 | — | sim |
| canonicalization | sem canonicalização nem fusão (cada especificação é uma tabela) | 0 | 0 | 33 | opt_update10x, pareto_1, pareto_6 | não |
| cost_selection | composição sem o modelo calibrado: cada leitura/escrita pesa 1, linhas pesam 0 | 0 | 0 | 0 | — | sim |
| relationships | reverse_hops = 0 (navegação reversa ignorada) | 8 | -2 | 0 | — | sim |
| sharedness | sharedness fixada em 1 (nenhuma consulta compartilha acesso) | 80 | -4 | 0 | pareto_1 | não |
| supersedes | restrições de substituição removidas (famílias viram acréscimo, como na V8) | 0 | 0 | -1 | pareto_1 | não |
| support_sharing | support_attrs e missing_maintenance_keys vazios (sem SupportQuery explícita) | 40 | -11 | -4 | — | sim |
| write_exposure | volatile_proj vazio (nenhum atributo é tratado como volátil) | 3 | -2 | -2 | kbest_1, kbest_2 | não |

Efeito físico na coorte: os esquemas distintos que mudaram foram materializados, conferidos pelo oráculo e medidos na mesma janela, intercalados com H02 (ordem A01, A02, H02, A03, A04, A05). Concordância com o oráculo: A01 PASS, A02 PASS, A03 PASS, A04 PASS, A05 PASS, H02 PASS; esquemas bloqueados pelo oráculo: nenhum.

| Esquema | Origem | Rótulo | Browsing | Bidding | Update10x | Update100x |
|---|---|---|---|---|---|---|
| A01 | sharedness, supersedes, canonicalization | pareto_1 | ablado mais rápido (-0,452 ms) | ablado mais rápido (-0,749 ms) | ablado mais rápido (-0,275 ms) | ablado mais lento (0,390 ms) |
| A02 | write_exposure | kbest_1 | ablado mais rápido (-0,412 ms) | ablado mais rápido (-0,597 ms) | empate (-0,075 ms) | ablado mais lento (0,336 ms) |
| A03 | write_exposure | kbest_2 | ablado mais rápido (-0,350 ms) | ablado mais rápido (-0,437 ms) | empate (-0,119 ms) | ablado mais lento (0,329 ms) |
| A04 | B2_all_addition | B2_all_addition | ablado mais rápido (-0,357 ms) | ablado mais rápido (-0,599 ms) | empate (0,062 ms) | ablado mais lento (0,639 ms) |
| A05 | B3_all_substitution_no_cost | B3_all_substitution_no_cost | ablado mais lento (4,418 ms) | ablado mais lento (2,414 ms) | ablado mais lento (1,193 ms) | ablado mais lento (0,324 ms) |

Diferença = esquema ablado − H02 (negativo: o ablado é mais rápido). IC 95% sobre as 5 repetições pareadas.

## 8. Escala completa a frio (E23)

Seleção: H01, H03. Regra: os 2 esquemas de H que mais vezes foram o melhor medido na coorte (E21, janela contínua); desempate pela posição média. Vitórias: {'H01': 3, 'H02': 0, 'H03': 1, 'H04': 0, 'H05': 0}.

Escopo: escala completa (2 M itens / 20 M lances), nó reiniciado e cache zerado antes de cada repetição, mesma janela, intercalado. Ordem: H01, NoSE, H03, QPG. Controle de cache: 84 ciclos, todos aprovados = True, cache máximo após a limpeza 213732 KiB. Concordância com as respostas da V8 em escala completa: H01 PASS, H03 PASS, NoSE PASS, QPG PASS.

Latência média (ms):

| Esquema | Browsing | Bidding | Update10x | Update100x |
|---|---|---|---|---|
| H01 | 1,747 | 2,256 | 2,871 | 3,175 |
| H03 | 6,228 | 4,739 | 3,858 | 2,929 |
| NoSE | 4,856 | 5,005 | 4,188 | 3,212 |
| QPG | 5,063 | 4,315 | 3,725 | 2,865 |

Contrastes pareados (H − baseline):

| Esquema | Contra | Browsing | Bidding | Update10x | Update100x |
|---|---|---|---|---|---|
| H01 | NoSE | H mais rápido (-3,110; -64,0%) | H mais rápido (-2,748; -54,9%) | H mais rápido (-1,317; -31,4%) | empate (-0,037; -1,2%) |
| H01 | QPG | H mais rápido (-3,317; -65,5%) | H mais rápido (-2,059; -47,7%) | H mais rápido (-0,854; -22,9%) | H mais lento (0,310; 10,8%) |
| H03 | NoSE | H mais lento (1,371; 28,2%) | H mais rápido (-0,265; -5,3%) | H mais rápido (-0,330; -7,9%) | H mais rápido (-0,282; -8,8%) |
| H03 | QPG | H mais lento (1,164; 23,0%) | H mais lento (0,424; 9,8%) | H mais lento (0,133; 3,6%) | empate (0,064; 2,2%) |

## 9. Aceitação (recalculada dos dados brutos)

Status: **PASS** (global_pass_hardcoded = False; avaliado em 2026-11-13T08:32:30-0300).

| Gate | Status |
|---|---|
| activation | PASS |
| cohort_benchmark | PASS |
| correctness | PASS |
| criteria_ablations | PASS |
| full_scale | PASS |
| generator_hash | PASS |
| mechanisms | PASS |
| supersedes | PASS |

Nível de efeito de cada mecanismo (D-V9-014): access_profile: apenas metadados/especificações; relationships: apenas metadados/especificações; sharedness: composição H; support_sharing: estrutura física de C; write_exposure: estrutura física de C.

Testes negativos: 11 de 11 rejeitados como esperado.

- esquema mantém tabela marcada como substituída → gate `supersedes`: rejeitado
- família ativada sem predicado verdadeiro → gate `activation`: rejeitado
- esquema com hash diferente da saída do gerador (edição manual) → gate `generator_hash`: rejeitado
- mecanismo declarado usado cujo desligamento não muda a saída → gate `mechanisms`: rejeitado
- hash de resposta alterado → gate `cohort_benchmark`: rejeitado
- observação apagada → gate `cohort_benchmark`: rejeitado
- erro injetado → gate `cohort_benchmark`: rejeitado
- carga concorrente → gate `cohort_benchmark`: rejeitado
- estado não restaurado → gate `cohort_benchmark`: rejeitado
- protocolo reduzido → gate `cohort_benchmark`: rejeitado
- divergência do oráculo escondida → gate `correctness`: rejeitado

## 10. Limitações

Ver `docs/LIMITATIONS.md`.
