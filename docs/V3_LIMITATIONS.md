# V3 — Limitações

## Do método (Stage 7/8 V3)

1. **Trabalho estrutural, não tempo.** O PhysicalWorkVector conta requisições CQL, linhas e bytes lógicos, mutações,
   leituras de manutenção e armazenamento lógico. Não modela cache, compactação, custo por requisição do driver
   nem diferenças entre versões do Cassandra. Por isso a ordenação estrutural não é preditora de latência, e o τ é
   apenas diagnóstico.
2. **Regra de rota universal lexicográfica** (requisições → linhas → bytes). Ela foi declarada antes de qualquer
   medição V3 e não foi ajustada. Prioriza o número de requisições mesmo quando isso multiplica as linhas lidas (caso
   TSBS D-V3-008: `ts0` com 1 requisição e 6× mais linhas em vez de `ts5_max_by_host_hour` com 2 requisições).
3. **Logical byte proxy LBP-1.** As larguras de `text` são médias observadas nos dados da campanha, que são
   pré-benchmark. Não há overhead de linha, célula, tombstone ou compressão. Não são bytes de disco.
4. **Eixos de risco fora da dominância** (D-V3-006). Partições grandes só aparecem como diagnóstico e na
   diversidade. Um deployment com partição perigosa pode estar na fronteira.
5. **top_k = 5 fixo e diversidade gulosa.** Quando a fronteira passa de 5 pontos (TSBS 7, RUBiS 20), a escolha
   depende das tags estruturais e do desempate de pesos iguais. São escolhas declaradas, não ótimas.
6. **Regra estrutural sem tabela morta** (D-V3-007). Foi adotada depois de inspecionar a estrutura do espaço
   TSBS da V2 (não os resultados) e vale igualmente para todos os benchmarks.
7. **Frequências.** IoT e TSBS usam mix uniforme (não há frequência oficial); TPCx usa os parâmetros congelados
   do workload; RUBiS usa a média das 4 mixes E05 (D-V3-009). Outra agregação poderia mudar a fronteira do RUBiS.
8. **RUBiS: contagens esperadas da síntese de rotas da V9.** As linhas por rota vêm de `_expected_rows` (médias de
   fan-out e aproximação de independência) recalculadas na coorte e restritas ao domínio das histórias. Não são
   contagens exatas por requisição, ao contrário do TSBS (contagem exata na materialização de referência) e do
   IoT/TPCx (catálogo de planos / gerador determinístico).
9. **Firewall por padrão de caminho.** É heurístico: intercepta `open`/`read_text`/`read_bytes` e registra todos os
   arquivos abertos no manifest. Código-fonte `.py` é permitido (D-V3-010); dados com padrão proibido são bloqueados.

## Da medição

10. **IoT e TPCx não foram remedidos** (D-V3-001, instrução do usuário): as métricas V3 reanalisam as latências
    brutas da campanha V2, que mediu todo o espaço V3 com rotas idênticas. Não há uma sessão independente V3 para
    esses dois benchmarks.
11. **RUBiS: denominador = HBench V3 + baselines.** Os 256 deployments completos não foram medidos (é inviável
    sob o protocolo de 5 repetições × 4 cargas × 2 versões). A preservação do RUBiS só pode ser afirmada dentro
    do conjunto medido e contra os baselines, não contra o melhor do espaço completo.
12. **Escala pequena e nó único (RF 1).** Os efeitos de distribuição, replicação e partições muito grandes não
    aparecem. É a mesma limitação da V2.
13. **Desvio declarado do TSBS** (2 aquecimentos + 12 rodadas por repetição), herdado da V2 sem alteração.
14. **Contêineres Cassandra de outros projetos** permanecem ativos no servidor, como na V2. O isolamento é
    garantido por cpuset dedicado e pela guarda de compactação/isolamento; janelas violadas são descartadas e
    repetidas.

## Observadas depois da medição (não alteram o HBench congelado)

15. **Pisos A/A altos.** TSBS natural: 12,3%. RUBiS 4.1: 21,9% em browsing e 23,0% em update100x. Nessas
    condições muitas diferenças entre deployments ficam abaixo do ruído; o Top-1 dentro do ruído é uma afirmação
    fraca nesses casos.
16. **A diversidade estrutural pode admitir deployments lentos.** No RUBiS, `V3H05` (escolhido por novas tags
    estruturais) é 1,2–2,9× mais lento que os baselines. A preservação do melhor não foi afetada, mas um HBench menor
    teria o mesmo poder de preservação.
17. **Retenção da fronteira medida de 3 objetivos** (leitura × armazenamento × escrita) caiu no TPCx (0,83 → 0,33):
    a V3 é mais seletiva e os eixos estruturais não distinguem deployments que empatam na leitura.
18. **Variação entre sessões** no RUBiS: o mesmo deployment varia de 0,85× a 1,22× entre a sessão V2 e a V3.
