# Post sugerido para o LinkedIn

Rascunho para anunciar o case. Adaptado do registro em `POST_DIARIO.md`.

---

**O dia em que a auditoria corrigiu a minha própria referência**

Montei um case de tratamento de dados em Databricks SQL a partir de um desafio real de processo seletivo.

O problema: 40 registros cheios de sujeira de qualidade de dado.

- idades negativas, emails sem `@`, datas impossíveis (mês 15, dia 40)
- CPF com letras, salários e dívidas negativos
- estado `InvalidState` e país grafado errado

O desafio pedia **13 regras de negócio em uma única consulta SQL**.

O que eu entreguei:
- um `SELECT` com todas as regras aplicadas coluna a coluna (bronze preservado, ouro limpo)
- sentinelas por regra: `00000-000`, `000.000.000-00`, `1900-00-00` (decisão de tipo documentada)
- normalização de telefone `(XX) AAAA-BBBB`, país `Brasil` com mapa de typos e nome completo do estado (27 UFs)
- auditoria coluna a coluna com derived table

E a auditoria me corrigiu: eu esperava 18 estados inválidos, os dados diziam 19. Confiei nos dados, não na minha estimativa.

Dado sujo não entra. E quando entra, a gente descobre e documenta.

Notebook, SQL e notas de decisão: [github.com/renatoapdl/case-tratamento-input](https://github.com/renatoapdl/case-tratamento-input)

---

#EngenhariaDeDados #Databricks #SQL #DataQuality #DataEngineering

---

## Competências a associar a este projeto no LinkedIn

Databricks · Apache Spark · Spark SQL · SQL · Data Quality · Data Validation · Data Cleansing · ETL · Data Engineering