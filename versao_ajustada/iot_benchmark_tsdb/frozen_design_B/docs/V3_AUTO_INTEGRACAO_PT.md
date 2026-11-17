# V3 automática — IoT Benchmark → SchemaLens → HBench (sem Cassandra)

## O que mudou

O guia manual `V3_OFFICIAL_BRIDGE_PT.md` continua disponível como documentação técnica, mas **não é mais necessário executar seus comandos um a um**. O executável `run_v3_auto.sh` automatiza:

1. Verificação das dependências Java 17+, Maven, Git e Python 3.
2. Clone de `thulab/iot-benchmark` e verificação do commit fixo `62e0dc100e56443978c8b4c66472b1d8a7ef0238` (sem atualizar para HEAD arbitrariamente).
3. Compilação **somente** do módulo Maven `core` e resolução de sua classpath. Não requer instalação ou execução de IoTDB ou Cassandra.
4. Configuração `generateDataMode` baseada no perfil existente do SchemaLens V3, sem modificar arquivos do upstream; geração de `schema.txt`, `info.txt` e `device/BigBatch_*.csv` usando as classes Java originais.
5. Compilação do arquivo V3 `SchemaLensQueryCapture.java` em um diretório externo à árvore Git do upstream; captura dos parâmetros Java em `native_queries.jsonl`.
6. Cópia limpa do projeto SchemaLens V3 para a execução, importação dos dados e objetos Java e execução automática dos Stages 1–8.
7. Validação dos planos e congelamento do HBench no Stage 8, registro de hashes e logs.

## Passo a passo mínimo (Ubuntu / VS Code)

1. Extraia o projeto novo `SchemaLens_IoT_Cassandra_V3_Automatico.zip` e abra a pasta no VS Code.
2. Na primeira utilização, instale as ferramentas do sistema (se ainda não tiver):

   ```bash
   sudo apt update
   sudo apt install -y git openjdk-17-jdk maven python3 unzip
   ```

3. Confira as dependências, sem baixar ou alterar arquivos:

   ```bash
   ./run_v3_auto.sh --check
   ```

4. Execute **um único comando**:

   ```bash
   ./run_v3_auto.sh
   ```

Na primeira vez o Maven precisa baixar dependências e o script precisa clonar o upstream, exigindo acesso à Internet. Nas execuções seguintes o clone e os artefatos locais podem ser reutilizados. O comando nunca liga Cassandra nem executa benchmark de desempenho. O processo pode levar alguns minutos na primeira execução.

## Onde ficam os resultados

A cada execução é criada uma pasta **nova**:

```
SchemaLens_IoT_Cassandra_V3_Automatico/
  native_auto_work/
    upstream_iot_benchmark/       # Git upstream; commit fixo
    build_cache/core_classpath.txt
    runs/v3_<timestamp>_pid<id>/
      java_config/config.properties
      native_data/schema.txt
      native_data/info.txt
      native_data/d_0/BigBatch_0.csv
      native_queries.jsonl
      logs/build_core.log
      logs/native_generation.log
      logs/compile_exporter.log
      logs/capture.log
      logs/prebenchmark.log
      schemalens/outputs/stage8/30_hbench_summary.csv
      schemalens/outputs/stage8/32_frozen_manifest.json
      AUTO_RUN_MANIFEST.json
      run_status.json
```

O script imprime os caminhos absolutos ao terminar. **Envie `AUTO_RUN_MANIFEST.json`, `30_hbench_summary.csv` e `32_frozen_manifest.json` para avaliarmos os resultados antes de executar o Stage 9 localmente.**

## Escala e execução repetível

O perfil inicial usa 3 dispositivos, 2 sensores DOUBLE, 60 loops e 5 observações por batch. São aproximadamente 1.800 células lógicas geradas (o total efetivo será registrado pelo importador), suficientemente pequeno para validar a integração. Ele mantém `QUERY_DEVICE_NUM=1`, `QUERY_SENSOR_NUM=2`, `IoTDB_DIALECT_MODE=table`, timestamps em milissegundos, alinhamento temporal e `NULL_RATIO=0`.

Para testar mais dados sem modificar as regras de ativação:

```bash
./run_v3_auto.sh --devices 8 --loops 100 --batch-size 20 --max-cells 500000
```

Para apenas inspecionar o que será feito:

```bash
./run_v3_auto.sh --dry-run
```

Para usar um clone já existente na revisão esperada:

```bash
./run_v3_auto.sh --repo-dir /caminho/para/iot-benchmark
```

Para compilar mais rapidamente a partir de artefatos Maven anteriores:

```bash
./run_v3_auto.sh --skip-java-build
```

Só use `--skip-java-build` após uma compilação prévia **bem-sucedida pelo próprio script**, pois depende do arquivo `build_cache/core_classpath.txt`.

## Segurança, metodologia e limites

- **Não executa Stage 9**, não acessa Cassandra, não altera Docker Compose e não elimina resultados anteriores.
- Não modifica nem reconfigura o repositório Git upstream depois do clone; o exportador é compilado fora dele.
- `generateDataMode` cria dados novos em um diretório exclusivo por execução; o script nunca apaga datasets antigos.
- As ativações continuam restritas a **TS0–TS8**, usando as regras e os modificadores originais do pacote.
- A seleção HBench do Stage 8 ocorre **antes de qualquer medição de desempenho**.
- A captura inclui **objetos dos parâmetros produzidos pelo gerador original**, mas representa apenas uma amostra por classe Q1–Q12. O pipeline ainda **não executa a sequência/frequência dos clientes Java**, não captura SQLs reais e não representa benchmark oficial reproduzido.
- As limitações de heterogeneidade de tipos, NULLs, Q8 e Q12 continuam explicitamente registradas no guia V3; este script automatiza a integração, não resolve todas as limitações científicas.
- Se a geração ou a compilação falhar, consulte o log indicado na mensagem `ERROR`. A pasta parcial é preservada para inspeção.

## Testes locais da automação

```bash
python3 -m unittest tests/test_v3_auto_orchestration.py -v
bash run_prebenchmark.sh
```

A suíte `tests/test_v3_auto_orchestration.py` só verifica a orquestração Python; a execução completa do gerador Java/Maven precisa ser confirmada na sua máquina com `./run_v3_auto.sh`.
