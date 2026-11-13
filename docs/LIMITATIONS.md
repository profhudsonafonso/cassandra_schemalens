# Limitações declaradas — V9

1. **Baselines reconstruídos.** NoSE e QPG são reconstruídos a partir dos artefatos históricos, não executados.
   A cópia local do QPG não tem metadados git; a fidelidade é verificada pelo hash dos arquivos.
2. **Domínio de parâmetros.** O domínio (30 usuários, 30 itens, 10 categorias, 10 regiões, sorteio uniforme com semente) é uma hipótese de pesquisa.
3. **Ambiente.** Nó único (RF = 1, consistência ONE, cliente sequencial), compartilhado com o diretório irmão `rubis3`.
   O Cassandra lê os dados por mmap, então em escala completa só o reinício do nó zera o cache.
4. **Coorte sem manifesto de IDs na V8.** A V9 vai exportar o manifesto a partir dos keyspaces da V8 (E17).
5. **Piso de ruído do benchmark na coorte.** H01 e H02 fazem trabalho idêntico na carga Browsing e ainda assim diferem 0,46 ms
   (`V9_REPORT.md`, seção 6). Contrastes desse tamanho não são evidência de efeito de esquema.
6. **Janela de medição em dois blocos.** As 8 medições do E20 ocorreram na mesma tarde, mas em dois blocos (13:22–14:41 e 15:44–16:23),
   por causa de reinícios (D-V9-012 e recriação do QPG). Não é uma janela contínua única.
7. **Rotas dos baselines.** NoSE, QPG e o SL norm da V8 usam as rotas escritas à mão na V8; algumas leem chaves de manutenção do
   fechamento em memória do cliente (sem SupportQuery), enquanto as rotas geradas da V9 fazem a leitura de apoio no Cassandra.
   Isso favorece levemente os baselines nas escritas.
8. **Escala.** O modelo de custo usa estatísticas de escala completa (E4) para escolher H, mas o benchmark da Fase 6 é feito
   na coorte. A escala completa (E23) mediu só os 2 esquemas selecionados (H01, H03), contra NoSE e QPG.
9. **Seleção sensível à janela.** Medida em dois blocos, a coorte apontava o H02 como o melhor na maioria das cargas. Na
   janela contínua, o melhor passou a ser o H01, em 3 de 4 cargas. H01 e H02 executam trabalho idêntico só em Browsing,
   onde diferem 0,007 ms. Em Bidding, o modelo previa o H02, mas o H01 foi medido como melhor: regret de 0,47 ms, com o IC
   do H02 largo (±0,73 ms).
10. **Mecanismos sem efeito físico.** `access_profile` e `relationships` mudam ativações e especificações, mas nenhum esquema
    (D-V9-014). `cost_selection` também não mudou H. Nesta carga, esses critérios não justificam ganho por si só.
11. **Ablações mais rápidas que o H02 nas leituras.** Na E22, os esquemas sem `sharedness`, `supersedes` ou `write_exposure`
    (A01 a A04) foram mais rápidos que o H02 em Browsing e Bidding, e mais lentos em Update100x. Nas leituras da coorte, esses
    critérios trocam velocidade de leitura por custo de escrita, e a vantagem só aparece com muitas escritas. O baseline B3
    (substituição sem custo) foi pior em todas as cargas.
12. **Update100x.** Em escala completa, o QPG continua 0,31 ms (11%) mais rápido que o H01.
