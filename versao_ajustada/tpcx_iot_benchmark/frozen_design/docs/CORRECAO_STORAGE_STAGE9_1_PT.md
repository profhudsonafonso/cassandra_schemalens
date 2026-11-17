# Etapa 9.1 — diagnóstico e correção da captura de armazenamento

## Problema observado

O runner original lançava `RuntimeError: Storage measurement was required but capture failed` sem informar se o erro ocorria durante o `nodetool flush`, `nodetool tablestats` ou na interpretação do campo `Space used (live)`.

**Não é possível identificar a causa exata usando apenas esse traceback.** A captura ocorre após a carga, a verificação das contagens e a auditoria. Como a exceção é anterior à gravação da linha de resultado, a execução interrompida não produz um relatório completo confiável.

## Correção

- Grava o comando executado, seu código de saída, saída completa do nodetool e stderr em `outputs/.../storage/repeatN`.
- Grava o resultado estruturado em `storage_diagnostic.json` mesmo em caso de falha.
- Lança exceção que indica status, tabela, mensagem de erro e caminho do relatório.
- Grava `79_stage91_storage_failure.json` na pasta do experimento para indicar a combinação afetada.
- Continua a exigir medida de armazenamento quando `--require-storage` é passado: nenhum zero, estimativa sintética ou dado inventado é substituído por medida ausente.
- Mantém os candidatos físicos e os cinco HBench **congelados**, sem alterar o protocolo de consulta ou de escrita.

## Diagnóstico não destrutivo antes da próxima execução

Execute com seu ambiente virtual ativo, na pasta do projeto atualizado:

```bash
# Comprovar container, JMX e keyspace
docker ps --format 'table {{.Names}}\t{{.Status}}'
docker exec schemalens_tpcx_cassandra nodetool status

# Testar leitura de métricas da tabela do primeiro HBench (sem flush)
python -m methodology.tpcx_iot.stage91.storage_diagnostic \
  --container schemalens_tpcx_cassandra \
  --keyspace sl_tpcx_h_119222012d4e \
  --table ts0_series_raw \
  --output outputs/stage91_storage_diagnostic
```

Se o keyspace não existir, consulte `docker exec schemalens_tpcx_cassandra cqlsh -e 'DESCRIBE KEYSPACES'` e/ou recrie os objetos com o teste. Não use `--reset` apenas para investigar o erro. Diagnóstico sem `--flush` mede SSTables existentes e pode reportar 0 se as inserções ainda estiverem em memtables.

Se a saída indicar `FLUSH_FAILED`, verifique o arquivo `nodetool_flush_*.txt`, estado JMX/memória e `docker logs --tail 100 schemalens_tpcx_cassandra`. Se indicar `TABLESTATS_FAILED`, examine a mensagem e confirme nome de keyspace/tabela. Se indicar `TABLESTATS_UNPARSEABLE`, anexe o arquivo `nodetool_<keyspace>_<table>.txt` para corrigirmos exatamente o formato produzido pela sua versão.

## Reexecução

Depois de confirmar que `nodetool tablestats` funciona, execute novamente o benchmark piloto mantendo todas as opções, inclusive `--require-storage --require-full-audit`. Use `--reset` somente numa instância de benchmark descartável, pois os keyspaces serão recriados.

Os resultados da tentativa interrompida não devem ser misturados com os novos resultados ou usados para comparar HBench. Continue tratando o experimento como adaptação acadêmica não oficial do TPCx-IoT.
