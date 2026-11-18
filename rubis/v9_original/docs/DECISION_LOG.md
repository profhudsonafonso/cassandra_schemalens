# Registro de decisões — V9

Decisões herdadas da V8 (citadas, não recriadas): **D-001** fonte `rubis_full` com semente 42, sem regenerar;
**D-002** C9 = `UNDEFINED_RESERVED`; D-003 a D-007 conforme `docs/DECISION_LOG.md` do repositório antigo.

## D-V9-001 — Repositório separado (2026-11-12)
A V9 roda em `~/Documents/schemalens_v9_cassandra`. O repositório V1–V8 é somente leitura: o código da V9
recusa gravar dentro dele (`common._guard`). Componentes reaproveitados são copiados para `reused/`, nunca importados do caminho antigo.

## D-V9-002 — Ambiente Python fixado (2026-11-12)
`.venv` próprio, criado a partir do CPython 3.12.16 usado pela V8; `cassandra-driver==3.30.1` (mesma versão da V8),
`PuLP==2.9.0` (composição por programação linear inteira, Fase 4), `PyYAML==6.0.2` (catálogo).

## D-V9-003 — Guarda de isolamento estendida (2026-11-12)
Mesma regra da V8-012 (amostra a cada 15 s, nó ocioso por 10 min, 0 compactações, medição contaminada =
`INVALID_CONCURRENT_LOAD`). Como a V9 roda de um terceiro diretório, os padrões passam a cobrir qualquer
interpretador do checkout irmão (`rubis3`) e do checkout da V8 (`rubis3_claude`), além dos carregadores da V9.
Implementação em `schemalens_v9/isolation.py`.

## D-V9-004 — Contagem de `rubis_full` por faixas de token (2026-11-12)
A E2 conta cada tabela com `count(*)` em 512 ou 2.048 faixas de token da chave de partição, só leitura.
Um `count(*)` único em 20 milhões de linhas excede o tempo-limite de leitura do nó.

## D-V9-005 — Contrato da instrução 15 (StoreBuyNow) (2026-11-12, aprovado pelo usuário)
O `rubis.rb` declara `UPDATE items SET quantity=?, nb_of_bids=?, end_date=?`. A V9 mantém a interpretação da V8
(rota e oráculo): a instrução 15 diminui `quantity` e não altera `nb_of_bids` nem `end_date`. Motivos: comparabilidade
com o oráculo e com as medições da V8, e evitar que `end_date` (chave de clustering em quatro tabelas) passe a ser
volátil por uma ambiguidade da carga. Consequência registrada no contrato (E6): `end_date` não é volátil na V9.

## D-V9-006 — Contrato semântico = semântica executada na V8, com oráculo completo (2026-11-12)
O "oráculo independente" da V8 só calculava a resposta exata das instruções 34 e 35; as outras 34 eram
checadas por formato e pela concordância entre os seis desenhos. A V9 escreve o contrato das 36 instruções
de forma declarativa (`schemalens_v9/contract.py`) e um oráculo genérico que o interpreta sobre um estado
lido de `rubis_full` (`schemalens_v9/oracle_v9.py`; a coorte da V8 fornece só as identidades). O contrato segue a
semântica que a V8 executou e que cinco desenhos independentes confirmaram, inclusive onde ela difere do texto
do NoSE (projeções reduzidas em 32–35, LIMIT 25 em 9/30/32/33/34, desempates explícitos, D-V9-005). Cada desvio
está em `deviation`. Verificação (E6): o oráculo reproduz a referência AUTO_H_norm da V8-011 em todas as
avaliações (ver `stages/E06_semantic_contract/ORACLE_VERIFICATION.json`). Durante a verificação, duas
entradas do contrato foram corrigidas (instrução 32: ordem crescente por data e LIMIT 25), antes de qualquer
geração de esquema.

## D-V9-007 — Parâmetros dos predicados de ativação (2026-11-12)
Os predicados (catálogo, E7) geram candidatos; quem escolhe entre eles é o modelo de custo (E14/E15). Os
limiares são, portanto, de geração, não de decisão:
- `lambda2_rows = 25` (C2): o LIMIT da carga; acima disso o índice + leituras pontuais não é limitado.
- `beta2_bytes = 64`, `beta8_bytes = 64` (C2, C8): projeção ou parte navegada com pelo menos 64 B estimados
  (tamanhos do modelo NoSE); abaixo disso uma cópia custa menos que uma leitura extra.
- `k3_copies = 2` (C3): o atributo volátil precisa estar em pelo menos duas tabelas para que isolá-lo poupe atualizações.
- `theta3_ratio = 0.05` (C3): as escritas do volátil precisam somar ao menos 5% das leituras da consulta no mix; abaixo
  disso (ex.: Browsing, sem escritas) a família não é candidata.
- `rho7_ratio = 4` (C7): relaxar a chave pode ler no máximo 4× as linhas necessárias (fan-out medido na E4).
Nenhum limiar foi ajustado depois de ver latências da V9.

## D-V9-008 — Fusão segura com ρmax = 0,5 (2026-11-12)
Duas especificações só são fundidas se tiverem o mesmo escopo (entidade das linhas, chave de partição e
clustering), se nenhum membro perder um atributo que a sua família removeu de propósito (C3/C8) e se a
redundância (bytes a mais por linha / bytes do membro) for ≤ 0,5. Identidade física idêntica sempre funde.

## D-V9-009 — Aceitação do modelo de custo: τ ≥ 0,6 (2026-11-12)
O modelo é aceito se o τ de Kendall entre custo previsto e latência medida for ≥ 0,6 em todas as cargas, com
os 12 esquemas da V8-012 e também deixando um esquema de fora. O τ só entre os 10 esquemas competitivos
(sem Normalizado e Especialista) é relatado à parte, porque é onde a escolha de H acontece.

## D-V9-010 — Cada entidade inserida mantém uma cópia completa (2026-11-12)
Na composição, toda entidade que a carga insere precisa de pelo menos uma tabela com todos os atributos
inseridos. Sem isso, o otimizador poderia descartar dados que nenhuma consulta da carga lê (ex.: `comments.item_id`),
o que seria perda de dados, não projeto de esquema.

## D-V9-011 — Desempate por espaço (2026-11-12)
O objetivo da composição soma 1e-4 × GB armazenado. Isso só desempata esquemas de mesmo custo (ex.: no mix
Browsing, sem escritas, tabelas extras não custariam nada), escolhendo o menor.

## D-V9-012 — Execuções vazias do runner irmão pausado (2026-11-12)
Três medições do E20 (H01, H03, QPG) foram invalidadas porque a guarda viu, em uma amostra cada, o runner
`sequential_baselines_011.py` do diretório irmão `rubis3`. Com o marcador `PAUSE_JOB` presente (desde 10/11),
esse runner retorna 75 antes de abrir qualquer sessão no Cassandra (lido no código do irmão, somente leitura).
A guarda da V9 passa a ter a mesma exceção que a V8-012 já tinha: runner nu, com o marcador presente e sem
carregador filho, é registrado em `ignored_paused_sibling_runner_noops` e não conta como carga concorrente.
As três medições invalidadas continuam descartadas (arquivos `*_INVALID_*.json`) e foram refeitas.

## D-V9-013 — Neutralização dos critérios nas ablações (2026-11-12)
Cada critério da E22 é desligado por um knob do gerador (descrição em `phase7.CRITERIA`), e o pipeline inteiro
(ativação → P → C → H) é rodado de novo. Nenhum esquema é editado à mão. Os baselines de decisão B1/B2/B3 são
montados a partir do mesmo C. Os esquemas novos são materializados, conferidos pelo oráculo e medidos na mesma
janela que o H02 (contraparte `opt_bidding`).

## D-V9-014 — Declaração de uso dos mecanismos em três níveis (2026-11-13)
Na primeira versão, o E13 declarava que um mecanismo "muda a saída" quando o hash de C mudava em qualquer
campo, inclusive em metadados. O gate `mechanisms` da aceitação exige que mude a estrutura física. Na primeira
aceitação, `access_profile`, `relationships` e `sharedness` reprovaram: tinham sido declarados como usados, mas
não mudam os `physical_id` de C.
Correção: o E13 passa a registrar `C_physical_changes`, `H_changes` e `C_content_changes`, mais um `level`.
`output_changes` passa a significar mudança física em C. O gate confere a declaração nos dois sentidos: uso
declarado sem efeito e efeito não declarado. Resultado:
- `support_sharing` e `write_exposure` mudam a estrutura física de C.
- `sharedness` muda só a composição H.
- `access_profile` e `relationships` mudam só os metadados e as especificações; não afetam nenhum esquema.
Só a declaração do E13 foi refeita. ROUTES_UNIVERSAL, CANONICAL_WITH_COVER e o hash do E13 continuam idênticos.
A versão anterior está arquivada em `MECHANISM_USAGE.v1_content_level.json` e `RESULT.v1.json`.
Também foi corrigido o teste negativo de `supersedes`, que procurava a tabela substituída só no primeiro esquema
de H e quebrava (StopIteration) quando ela não existia em C. Agora ele procura o primeiro par em que ela existe.
