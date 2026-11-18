## Resumo e discussão (escrita depois da medição; não realimenta o Stage 7/8)

### Quadro V2 × V3 (decisão: média; ruído pelo piso A/A)

| benchmark | método | HBench / espaço | DSR | Top-1 (ruído) | Top-3 | regret | retenção Pareto | τ (diag.) |
|---|---|---|---|---|---|---|---|---|
| IoT | V2 (relatado; espaço V2 = 6) | 3/6 | 0,50 | sim | sim | 0 | 1,00 | 0,87 |
| IoT | V2 no denominador V3 | 3/3 | 0 | sim | sim | 0 | 1,00 | 1,00 |
| IoT | **V3** | **1/3** | **0,67** | **sim** | sim | **0** | **1,00** | 1,00 |
| TPCx | V2 (= denominador V3) | 5/9 | 0,44 | não | sim | 0 | 0,83 | −0,06 |
| TPCx | **V3** | **2/9** | **0,78** | **não** | sim | **0,0017** | **0,33** | −0,34 |
| TSBS natural | V2 (campanha V2, espaço medido 10) | 7/10 | 0,30 | sim | sim | 0,0017 | 0,25 | −0,09 |
| TSBS natural | **V3** (campanha V3, 24 medidos) | **5/24** | **0,79** | **sim** | sim | **0,0167** | **0,33** | 0,35 |
| TSBS aligned | V2 | 7/10 | 0,30 | não | sim | 0 | 0,67 | 0,54 |
| TSBS aligned | **V3** | **5/24** | **0,79** | **sim** | sim | **0** | **0,57** | 0,36 |
| RUBiS 4.1 (dentro de H) | V2 (NNLS, H01–H05) | 5/— | — | 3 de 4 cargas | — | 0 a 0,19 ms | — | 0,2–1,0 |
| RUBiS 4.1 (dentro de H) | **V3** (V3H01–V3H05) | **5/256 viáveis** | **0,98** | **4 de 4 cargas** | — | 0 (3 cargas), 13,8% em update100x (empate pelo piso de 23%) | — | −0,6 a 0,8 |

Os denominadores são diferentes e estão declarados. No TSBS a V2 mediu 10 coberturas e a V3 mediu as 24 do espaço V3
(verificação completa). No RUBiS só o HBench e os baselines foram medidos.

### Qualidade de preservação (pergunta central)

* **IoT:** o único deployment retido é o melhor medido. Preservação total com redução de 3 para 1.
* **TPCx:** o HBench V3 (2 de 9) contém `h_3f1449e05468`, estatisticamente empatado com o melhor (regret de 0,17%).
  A preservação do melhor nível vale. Já a retenção da fronteira medida (leitura × armazenamento × escrita) cai para
  1/3, porque a fronteira estrutural V3 só tem 2 pontos e 6 deployments empatam na leitura.
* **TSBS:** na coorte natural, com piso A/A de 12,3%, 23 dos 24 deployments empatam com o melhor. O HBench (5 de 24)
  contém empatados (regret de 1,7%). Na coorte alinhada, onde as tabelas TS5/TS6 servem, o melhor medido está no HBench
  (regret 0). Isso preserva o melhor com 79% de redução.
* **RUBiS:** em 4.1.12 o melhor HBench V3 é mais rápido que NoSE e QPG em browsing, bidding e update10x
  (R1 = sim; razões de 0,71 a 0,77). Em update100x tudo empata pelo piso. Em 3.11.11 os vereditos são os mesmos
  (R2 = sim). Oráculo: 3 577 311 / 3 577 311 respostas iguais em cada versão. **Achado negativo:** `V3H05`, admitido
  pela regra de diversidade estrutural (tags C005/C008/C013/C015), é 1,2× a 2,9× mais lento que os baselines nas
  três primeiras cargas. A diversidade preservou variedade estrutural, não desempenho.

### Qualidade de ranking (diagnóstico)

O `balanced_structural_tiebreak` não é preditor. O τ vai de −0,34 (TPCx) a 1,0 (IoT) e fica em torno de 0,35 no TSBS.
No RUBiS 4.1 o τ é 0,8 em browsing, bidding e update10x e −0,6 em update100x. O NNLS legado,
**RETROSPECTIVE_BASELINE_ONLY**, aplicado depois da campanha ao HBench V3, tem τ semelhante (0,8–0,84; −0,2 em
update100x). Ele acerta o Top-1 em 3.11 bidding, mas erra o Top-1 em 3.11 update100x, onde o V3 acerta. Nenhum dos
dois domina o outro, e o V3 não usa coeficientes de latência.

### Desempenho contra baselines

* TSBS: o rank 1 V3 tem 0,548× a latência do esquema original do TSBS na coorte natural e 0,379× na alinhada.
* RUBiS: V3H01 = H01 tem 0,72–0,77× a latência de NoSE/QPG.
* IoT: TS2-U é o melhor medido (os baselines estão na V2).

### Custo de escrita e armazenamento

O vetor V3 penaliza mutações e armazenamento lógico na dominância. No TSBS, os deployments do HBench carregam de
88 mil a 111 mil mutações, contra 86,4 mil do TS0 sozinho, e ocupam 1,06–1,62 MB medidos. No TPCx, a projeção TS3
duplica as escritas (7 560 contra 3 780 por execução). O armazenamento medido acompanha o proxy lógico na ordem
dentro de cada benchmark.

### Reprodutibilidade entre sessões (RUBiS)

Os mesmos deployments medidos na V2 e na V3 (V3H01 = H01, V3H03 = H02, V3H04 = H04 e os 3 baselines) têm razão
V3/V2 entre 0,85 e 1,22. A ordem relativa H01 < NoSE/QPG se mantém. Os pisos A/A ficam entre 4,5% e 23% em 4.1, então
diferenças pequenas entre sessões não são interpretáveis.

### Conclusão em relação à hipótese da V3

Um Stage 7 único, com o mesmo vetor, a mesma dominância, a mesma viabilidade e a mesma diversidade nos 4
benchmarks, reduziu os espaços em 67–98% e manteve, em todos os casos, um deployment empatado com o melhor medido
dentro do denominador declarado, sem coeficientes de latência. As perdas foram duas: (i) a retenção da fronteira
medida de três objetivos é menor que a da V2 no TPCx, porque a V3 é mais seletiva; (ii) a diversidade estrutural pode
admitir deployments lentos (`V3H05`). Essas observações não alteram o HBench V3 congelado.
