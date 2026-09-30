# Case Databricks: tratamento de dados, do bronze ao ouro

[![Databricks](https://img.shields.io/badge/Databricks-FF3621?style=for-the-badge&logo=databricks&logoColor=white)](https://www.databricks.com/)
[![Apache Spark](https://img.shields.io/badge/Apache%20Spark-FDEE21?style=for-the-badge&logo=apachespark&logoColor=black)](https://spark.apache.org/)
[![Spark SQL](https://img.shields.io/badge/Spark%20SQL-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)](https://spark.apache.org/sql/)

Desafio (fonte: Emerson Costa) resolvido em Databricks SQL. A base tem 40 registros sujos, cada um quebrando uma ou mais das 13 regras de negócio, e tudo é tratado em uma única consulta, com auditoria coluna a coluna no final.

## Contexto e problema

A base traz sujeira de todos os tipos: idades negativas, emails sem `@`, telefones fora do formato, CEP e telefone com caracteres alfanuméricos, datas impossíveis (mês 15, dia 40), CPF com letras, salários e dívidas negativos, pontuação de crédito fora da faixa, gênero inválido, siglas de estado mal escrita e país gravado no formato errado.

O enunciado pede as 13 regras tratadas em **uma única consulta SQL**, sem uma query por regra. O resultado final é um `SELECT` com todos os dados limpos.

## Solução

Arquitetura medallion conceitual:
- **bronze**: `default.dados_brutos`, a tabela crua. Ela não é alterada.
- **ouro**: um único `SELECT` que lê o bronze e aplica todas as regras por coluna.

Estratégia por regra:
- Sentinelas: `NULL` para dado ausente, `00000-000` para CEP, `1900-00-00` para data inválida, `000.000.000-00` para CPF.
- Normalização: telefone `(XX) AAAA-BBBB`, CPF `000.000.000-00`, país padronizado como `Brasil` (com mapa de typos), estado expandido para o nome completo da UF.
- Colunas extras pedidas: `idade_real` (ano atual menos o de nascimento) e `estado_nome`.

## Auditoria

A validação do ouro usa derived table e `COUNT_IF` por coluna:

```
total=40 | idade=4 | email=1 | telefone=0 | cep=10 | contratação=6 | nascimento=10 |
cpf=10 | salario=8 | pontuação=8 | divida=11 | gênero=9 | estado_nome=19 | país=18 | idade_real=10
```

Um destaque: a auditoria corrigiu a própria referência. Eu esperava 18 `InvalidState`; os dados mostram 19.

## Decisões de engenharia

- Datas usam a sentinela `1900-00-00` em STRING, porque dia 00 não existe em DATE (`TRY_TO_DATE` retorna NULL). O trade-off de tipo fica registrado.
- Estado usa portão semântico (27 UFs + `ELSE NULL`) e CPF usa `^[0-9]{11}$`, em vez de só validar tamanho.
- Emails seguem o contrato do enunciado (conter `@`); os casos-limite ficam anotados como observação.
- O cluster 14.3 LTS e as 4 Spark configs são design de referência: a Free Edition atual é serverless-only.

## Stack

Databricks SQL, lógica de 3 valores (NULL), escalar vs agregação, `CASE`, `TRY_TO_DATE`, `SUBSTRING`/`CONCAT`, `REGEXP_LIKE` com âncoras, `TRIM`/`UPPER`, derived table, `COUNT_IF`, sentinelas, medallion.

## Estrutura

```
├── do-bronze-ao-ouro.py      # notebook Databricks (código-fonte)
├── do-bronze-ao-ouro.html    # notebook exportado (visual)
├── sql/
│   ├── 01_create_table.sql   # criação do bronze
│   ├── 02_insert.sql         # 40 registros sujos
│   ├── 03_ouro.sql           # a query única com as 13 regras
│   └── 04_auditoria.sql      # auditoria coluna a coluna
└── notas_candidato.md        # decisões e achados
```

## Como reproduzir

1. Rode os scripts na ordem (01, 02, 03, 04) no Databricks; o notebook `.py` já traz as células montadas.
2. Os scripts de setup (01 e 02) são idempotentes e recriam o bronze a cada execução.
3. `03` gera o ouro; `04` valida o resultado.

---
Autor: Renato Abreu · github.com/renatoapdl
