# Correspondência com os algoritmos do SchemaLens (DKE)

O artigo DKE define uma arquitetura de duas fases: redução explicável do espaço de configurações seguida de seleção empírica. A extensão TS deve preservar essa fronteira.

| Artigo DKE | Pacote TPCx-IoT | Estado |
|---|---|---|
| Alg. 1, linhas 3–4: fragmento e classe | `workload()` / `analytical_record()` | IMPLEMENTADO, abstração EER |
| Alg. 2: raiz semântica/access-aware | `analytical_record()` | ADAPTER específico: semantic-first + fallback lexicográfico; ainda não geral para qualquer EER |
| Alg. 1, linha 6: matriz M | `01_analytical_matrix.csv`, `02_temporal_evidence.csv` | IMPLEMENTADO com evidências disponíveis e lacunas explícitas |
| Alg. 3: ativação | regras originais `TS0–TS8` e `M1–M5` | IMPLEMENTADO sem retuning |
| Instanciação `gamma_spec` | a construir | PENDENTE (Etapa 4) |
| Alg. 4: identidade, fusão, cobertura | a construir | PENDENTE (Etapas 5 e 8) |
| Alg. 5: compatible covers, HBench, Htop | a construir | PENDENTE (Etapas 7–9) |

## Cuidado com a avaliação de redução

Os templates TS0–TS8 são **famílias lógicas de representação**, não nove implementações garantidamente comparáveis em todo SGBD. A redução `1-|A(q)|/9` mede apenas ativação abstrata e **não** equivalência com um espaço físico deployável. Uma avaliação de preservação Top-K/regret deve estabelecer, antes de medir, um universo físico comum de alternativas válidas. Os templates incompatíveis com as capacidades do DB não contam como alternativas materializáveis sem um binding explícito.

## Dúvidas a resolver no kit oficial

1. Qual chave/predicado a query realmente usa (station + sensor ou somente nome de sensor)?
2. Como exatamente as janelas `TI1`/`TI2` são montadas em Java e quais limites são inclusivos?
3. Qual formato real do registro e nomes nos bindings, em vez da convenção dos nomes lógicos deste protótipo?
4. Como a simultaneidade, a validação, o índice e o nível de replicação são controlados no kit 2.1.2?
5. Se houver adaptação para MongoDB, qual política de índices, granularidade e série física respeita a mesma semântica sem promover um único vencedor?

## Diferença entre especificação de alto nível e consultas de literatura

A seção 2.2 do TPCx-IoT 2.1.2 descreve recuperação de leituras do sensor em duas janelas. O estudo acadêmico externo citado no README descreve as quatro operações MAX/MIN/AVG/COUNT, mas esse material não constitui validação dos arquivos Java do kit atual. Os dois perfis permanecem separados nos manifests e snapshots. Não calibrar a ativação com os resultados de nenhum dos dois.

## Limitações importantes da analogia com o artigo

- A seleção de raiz implementa a decisão semântica para as operações do TPC e calcula fallback centralidade quando necessário, mas não substitui um seletor geral sobre qualquer esquema EER.
- Sharedness é marcado como somente evidência de ownership EER, pois não há instâncias observadas para computar `shareobs` e `sharemaxobs`. Não classificá-lo como medição.
- Update exposure fica desconhecida quanto à frequência normalizada; a descrição do benchmark confirma ingestão, mas não um peso comparável ao mix de leituras no protótipo.
- O parser de SQL copiado do SmartRetail é uma ferramenta de extração controlada, não um parser geral SQL/CQL nem o Java do benchmark oficial.
