# SchemaLens TPCx-IoT — Stage 9.1: validação do alvo Cassandra

## Sintoma observado

`nodetool flush sl_tpcx_h_119222012d4e` falhou com `Unknown keyspace`, embora o container estivesse `healthy` e o `nodetool status` exibisse `UN`. O erro demonstra apenas que o keyspace não existe **no container consultado pelo nodetool**. Ele não demonstra que a ingestão deixou de ocorrer no Cassandra acessado pelo driver Python.

Como há três containers Cassandra simultaneamente ativos na estação do pesquisador, existe a hipótese de o driver e o `nodetool` atingirem instâncias distintas. Também é possível que o keyspace tenha sido removido ou nunca tenha sido criado no container consultado. O teste abaixo distingue essas possibilidades antes de reiniciar o benchmark.

## Diagnóstico seguro (não altera keyspaces)

A partir da raiz do projeto, com o ambiente virtual ativo:

```bash
# 1) Descobrir as portas reais publicadas pelos containers
docker ps --format 'table {{.Names}}\t{{.Ports}}'
docker port schemalens_tpcx_cassandra 9042/tcp

# 2) Confirmar os keyspaces existentes no container usado pelo nodetool
docker exec schemalens_tpcx_cassandra cqlsh 127.0.0.1 9042 -e 'DESCRIBE KEYSPACES'

# 3) Comparar a identidade do nó acessado pelo Python com a do container
python -m methodology.tpcx_iot.stage91.target_diagnostic \
  --hosts 127.0.0.1 \
  --port 9042 \
  --container schemalens_tpcx_cassandra \
  --output outputs/stage91_target_diagnostic.json
```

O comando do passo 3 não executa `DROP`, `CREATE`, `INSERT` ou `flush`. Ele consulta `system.local.host_id` pelo driver Python e pelo `cqlsh` do container; registra ambos em JSON.

### Interpretação

- `MATCH`: ambas as rotas alcançam o mesmo nó. Se o keyspace estiver ausente, pode ter sido removido ou não criado. Não use métricas da execução interrompida; repita o piloto limpo para recriar os objetos e medir armazenamento.
- `TARGET_MISMATCH`: o driver Python e o `nodetool` usam nós diferentes. Compare `docker ps ...Ports` e o valor de `--port` com `--nodetool-container`; ajuste o alvo, não os templates ou HBench.
- `DIAGNOSTIC_FAILED`: o `cqlsh` do container falhou ou não foi possível ler sua identidade. Consulte o JSON salvo para obter stdout/stderr. Não execute benchmark com armazenamento obrigatório até resolver.

## Proteção adicionada ao runner

Ao executar `benchmark` ou `verification` com `--nodetool-container`, o runner verifica o par driver/container antes de iniciar as repetições e antes da primeira operação destrutiva `--reset`. O arquivo `69_stage91_target_preflight.json` documenta a confirmação. Se houver divergência, o processo interrompe sem apagar keyspaces.

As mensagens de `FLUSH_FAILED` e `TABLESTATS_FAILED` agora incluem a saída de `stdout` quando `stderr` estiver vazio. Esse ajuste revela mensagens como `Unknown keyspace` no console, em vez do sufixo vazio anterior.

## Retomar o experimento

**Somente depois de obter `MATCH`** e confirmar que a instância é uma base de benchmark descartável, execute o mesmo piloto anterior:

```bash
python -m methodology.tpcx_iot.stage91.cli benchmark \
  --reset --stations 2 --sensors 3 \
  --seed-steps 130 --bench-steps 30 \
  --read-operations 12 --read-threads 2 --repetitions 1 \
  --nodetool-container schemalens_tpcx_cassandra \
  --require-storage --require-full-audit \
  --output outputs/stage91_pilot_target_verified
```

A coleta de armazenamento permanece obrigatória. Não são usados zeros, estimativas sintéticas ou dados inventados para suprir medidas ausentes. O conjunto HBench original, o baseline e todas as regras de ativação permanecem idênticos.

## Limites de escopo

Essa checagem identifica um único nó local. O benchmark de pesquisa atual usa Cassandra local de desenvolvimento com RF=1; não substitui um protocolo oficial multi-nó TPCx-IoT. Nenhuma medição real foi executada durante esta correção.
