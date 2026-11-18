# Tentativa 1 (descartada antes de qualquer medição oficial)

Dados regenerados com datas sorteadas (`fake.js` `date.past`). O smoke mostrou que o NoSE divergia do oráculo
na instrução 32 (compras do usuário, ordenadas por data): a tabela do NoSE agrupa por id. Na V9 real, o NoSE
concordava em 100%, então no `rubis_full` original as datas crescem com o id. O gerador passou a produzir datas
monótonas no id (`gen_data.mono`). Esta pasta guarda os dados, o oráculo e o smoke da tentativa 1.
