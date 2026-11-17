# Correção da Q12 — IoT Benchmark V3 (Fastjson `$ref`)

## Diagnóstico

O IoT Benchmark Java `GenerateQueryWorkLoad.getSetOpQuery()` reutiliza a lista de sensores na construção das subconsultas Q12. Com a serialização Fastjson padrão, sensores repetidos podem aparecer como objetos `{ "$ref": ... }`, em vez de `{"name":"s_0",...}`. O parser Python não conseguia recuperar `name` e interrompia A1 com `Q12 child: unknown sensor: None`.

O complemento força a serialização **por valor** da árvore de parâmetros Java (`SerializerFeature.DisableCircularReferenceDetect`) e recusa arquivos contendo objetos incompletos ou `$ref`. Não inventa nomes ou substitui consultas. Os CSVs nativos são reutilizados, assim como a revisão compilada do módulo `core`. O experimento antigo e seu JSONL original ficam intactos.

## Como usar

Estando na pasta original `SchemaLens_IoT_Cassandra_V3_Automatico`, copie o ZIP para lá e execute:

```bash
unzip -o SchemaLens_V3_Q12_Reference_Fix.zip
python3 -m unittest discover -s tests -p 'test_q12_reference_fix.py' -v
bash scripts/resume_v3_q12.sh native_auto_work/runs/v3_20261016T191603Z_pid2543627
```

Isso compila **apenas o exportador Java corrigido**, exporta novamente os objetos Q1–Q12, executa `run_native_prebenchmark.sh` em uma nova pasta isolada, congela o HBench e salva um manifesto de rastreabilidade. Não chama Docker/Cassandra nem Stage9 live.

### Se a etapa 4/4 falhar

Abra o último log da pasta criada sob `native_auto_work/runs/v3_20261016T191603Z_pid2543627/logs/q12fixed_*/prebenchmark.log`:

```bash
find native_auto_work/runs/v3_20261016T191603Z_pid2543627/logs \
  -path '*/q12fixed_*/prebenchmark.log' -print
```

Envie o **final do log**, especialmente o traceback, para investigar qualquer outra incompatibilidade de parâmetros. Não altere regras de seleção com base em latências do piloto.

### Para um experimento novo do zero

O mesmo ZIP substitui `java_bridge/.../SchemaLensQueryCapture.java` e o parser `phase_a1/tools/official_query_capture.py`. Então o script original `./run_v3_auto.sh --skip-java-build` passará a usar o exportador corrigido em novas gerações.

## Validação e escopo

A revisão Java foi inspecionada contra os DTOs e o `getSetOpQuery()` da revisão fixa `62e0dc1`. Os testes Python offline exercitam Q12 completo, `$ref` parcial, `$ref` de lista e rejeição de sensores desconhecidos. O projeto integrado passou em **52 testes após o recálculo do manifesto Stage8 no fixture**, sem executar Cassandra. A compilação e execução Java reais dependem do Ubuntu do usuário. As amostras ainda não constituem replay de SQL, frequência ou escalonamento do IoT Benchmark.


**Nota sobre hashes de reprodutibilidade:** modificar `official_query_capture.py` torna inválido o hash do HBench anterior na pasta principal. Isso é intencional: não reutilize a antiga seleção congelada como se o código não tivesse mudado. O comando de retomada cria um experimento isolado e congela um manifesto **novo**, mantendo todos os resultados históricos guardados.
