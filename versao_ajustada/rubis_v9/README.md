# RUBiS (SchemaLens V9) — versão ajustada: E20 sob o protocolo comum v1

**Modo executado: neste servidor** (`run_server.sh`). O `rubis_full` da V9 existe só no Mac original e foi gerado
sem semente, então a fonte da coorte é **regenerada** (`rubis_v1/gen_data.py`): a estrutura vem das estatísticas do E04
e das convenções do código da V8, os valores seguem o contrato do `fake.js` e as datas são monótonas no id. Os mesmos
dados são carregados em **dois nós dedicados, Cassandra 4.1.12 e 3.11.11**, e o E20 é medido nos dois, intercalados em
ABBA, isolando o efeito da versão. A correção é conferida pelo **oráculo independente da V9** (`rubis_v1/oracle_ref.py`).
H01–H05, o Hbench e o código da V9 não são alterados.

```bash
cd versao_ajustada/rubis_v9
./run_server.sh gen      # fonte da coorte regenerada -> frozen/
./run_server.sh node     # nos dedicados 4.1.12 (9064) e 3.11.11 (9065)
./run_server.sh import   # mesma coorte nos dois nos; exige hashes identicos (funcoes da V9)
./run_server.sh oracle   # 397.479 respostas esperadas (oraculo V9)
./run_server.sh smoke    # 9 deployments x 2 versoes, contagens pequenas (todos PASS no oraculo)
./run_server.sh freeze   # PREREGISTRATION_RUBIS_V1.json
./run_server.sh measure  # ~4 h, desacoplado (setsid nohup)
./run_server.sh analyze && ./run_server.sh accept && ./run_server.sh report
```

`attempts/` guarda as tentativas descartadas antes da medição oficial (datas sorteadas; DDL do V8 com opções do 3.11).

---

## Modo alternativo: no Mac original (mesmos dados da V9)

O texto abaixo descreve o modo Mac (`run_mac.sh` + `transfer.py export/import`), que reproduziria o E20 sobre o
`rubis_full` original. Ele não foi executado.

## Como funciona

1. **Transferência exata** (`rubis_v1/transfer.py`). A V9 não lê os 70,4 M de linhas do `rubis_full`, e sim
   ≈44 mil: as linhas da coorte, o fecho de usuários, as categorias e regiões, e o domínio do benchmark. O `export`
   lê essas linhas do 3.11.11 (só `SELECT`); o `import` as recria no nó 4.1.12 com o mesmo DDL lógico. A cópia só
   é aceita se `load_raw_state` e `source_closure`, **as próprias funções da V9**, derem o mesmo hash canônico nos
   dois clusters.
2. **Código V9 sem alteração.** `materialize_*`, os adaptadores, as histórias semeadas e o `agreement()` contra a
   referência da V8 são os mesmos. O runner só redireciona a porta das conexões para o nó v1 e não escreve nada em
   `runs/v9_generation_001`.
3. **Protocolo v1.** Antes de cada uma das 5 repetições: `flush` + reinício a frio + gate de flush. Dentro da
   repetição, os 9 deployments (H01–H05, NoSE, V8_AUTO_H_norm, QPG e a réplica **A/A H01_AA**) rodam na ordem da
   V9 nas repetições ímpares e invertida nas pares. Cada (deployment, carga) tem 1.000 aquecimentos e 10.000
   requisições medidas (o protocolo de requisições da V9 não muda). A guarda combina o `BulkWriterWatch` da V9 com a
   guarda v1 para carga mista: compactação nos keyspaces medidos é registrada, mas não invalida a janela, porque as
   cargas de atualização escrevem.
4. **Análise.** Média (decisão), p50/p95/p99, IC bootstrap pareado, IC t, piso de ruído (A/A e o par de trabalho
   idêntico H01 × H02 em browsing), contrastes contra NoSE/QPG/V8, τ/Top-1/regret dentro de H, Htop. As latências
   brutas da V9 (3.11.11) são recalculadas com a **mesma** estatística, para comparar as versões.

## Passo a passo (no Mac, na raiz do repositório)

```bash
git fetch && git checkout versao-ajustada-tsdb && git pull
cd versao_ajustada/rubis_v9
./run_mac.sh setup      # venv versao_ajustada/.venv: cassandra-driver numpy matplotlib pyyaml
./run_mac.sh check      # container 3.11.11 no ar, repo V8 presente, sem escritores concorrentes, recursos do Docker
./run_mac.sh node       # cria o container dedicado sl_v1_rubis_cassandra (4.1.12, porta 9064, CPUs 0-3, 6 GB)
./run_mac.sh export     # le a coorte do 3.11.11 (somente leitura) -> frozen/
./run_mac.sh import     # recria no 4.1.12 e verifica IDENTICAL=true
./run_mac.sh smoke      # ~5 min: H01 + NoSE, 2 repeticoes, 200 requisicoes (nao conta como resultado)
./run_mac.sh freeze     # PREREGISTRATION_RUBIS_V1.json (exclusivo)
./run_mac.sh measure    # ~2,5 h em segundo plano (nohup); acompanhe com ./run_mac.sh status
./run_mac.sh analyze    # inclui o recalculo das latencias brutas da V9 (3.11.11)
./run_mac.sh accept     # 12 gates + 7 testes negativos
./run_mac.sh report     # tabelas + relatorio_overleaf/main.pdf
```

Depois, faça o commit de `versao_ajustada/rubis_v9/` (sem `outputs/*/raw`, que fica grande) e me avise: eu escrevo as
conclusões do relatório a partir de `outputs/v1_run1/analysis/`.

### Requisitos e cuidados

- **Docker Desktop**: no mínimo 4 CPUs e 8 GB (Settings › Resources); o `node` recusa se faltar.
- **Pare** o processo da V8 `methodology.tracker_012 --loop`, se estiver ativo: ele conta como escritor concorrente e
  invalida janelas.
- O container 3.11.11 é só **lido** (no `export`). Nada é apagado nele.
- O E23 (escala completa, ≈12 h na V9) não faz parte deste pacote e fica declarado como limitação: na escala completa,
  a evidência continua sendo a de 3.11.11.
