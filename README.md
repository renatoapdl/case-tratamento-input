# Case Databricks — Tratamento de dados (bonze → ouro)

> Engenharia de dados na prática: **13 regras de negócio · 40 linhas sujas · 1 única query SQL · auditoria coluna a coluna**.

Desafio de processo seletivo (fonte: Emerson Costa) resolvido e **publicado como estudo de caso** em Databricks SQL.

---

## 📋 Contexto & problema

Uma base de 40 registros com sujeira diversa de qualidade de dado: idades negativas, emails sem `@`, telefones fora do padrão, CEP/teléfone com caracteres alfanuméricos, datas impossíveis (mês 15, dia 40), CPF com letras, salários/dívidas negativos, pontuação de crédito fora da faixa, gênero inválido, siglas de estado contaminadas e país grafado errado.

O desafio pede **13 regras de negócio tratadas em UMA ÚNICA consulta SQL** — sem consultas separadas por regra — e o resultado final **um `SELECT` com todos os dados tratados**.

## 🎯 Solução

Arquitetura **medallion** (conceitual):
- **bronze**: `default.dados_brutos` — tabela crua, preservada, sem nenhuma mutação.
- **ouro**: um único `SELECT` que lê o bronze e aplica todas as regras por coluna.

Estratégia por regra:
- **Sentinelas por regra**: `NULL` (dado ausente), `00000-000` (CEP), `1900-00-00` (data inválida), `000.000.000-00` (CPF).
- **Normalização**: telefone `(XX) AAAA-BBBB`, CPF `000.000.000-00`, país → `Brasil` canônico (+ mapa de typos), estado → nome completo por UF.
- **Colunas extras pedidas**: `idade_real` (ano atual − ano de nascimento) e `estado_nome` (nome completo do estado).

## ✅ Auditoria

Validação do ouro por derived table (`COUNT_IF` por coluna):

```
total=40 | idade=4 | email=1 | telefone=0 | cep=10 | contratação=6 | nascimento=10 |
cpf=10 | salario=8 | pontuação=8 | divida=11 | gênero=9 | estado_nome=19 | país=18 | idade_real=10
```

**Destaque:** a auditoria corrigiu a própria referência — a expectativa era 18 `InvalidState`; os dados diziam **19**. Evidência > achismo.

## 🧠 Decisões de engenharia (documentadas)

- Datas: sentinela `1900-00-00` em **STRING** (dia 00 não é representável em DATE — `TRY_TO_DATE` confirma). Trade-off de tipo assumido e registrado.
- Gate semântico para estados (27 UFs + `ELSE NULL`) e para CPF (`^[0-9]{11}$`), não apenas estrutural/tamanho.
- Emails validados **pelo contrato do enunciado** (conter `@`); casos-limite registrados como observação.
- Adaptação: cluster 14.3 LTS + 4 Spark configs são **design de referência** (Free Edition agora é serverless-only).

## 🏗️ Stack

**Databricks SQL** · Spark SQL semantics (3-valor-lido `NULL`, escalar vs agregação) · `CASE` · `TRY_TO_DATE` · `SUBSTRING`/`CONCAT` · `REGEXP_LIKE` (âncoras + escape) · `TRIM`/`UPPER` · derived table · `COUNT_IF` · sentinelas · medallion.

## 📁 Estrutura

```
├── do-bronze-ao-ouro.py      # notebook Databricks (código-fonte)
├── do-bronze-ao-ouro.html    # notebook exportado (visual)
├── sql/
│   ├── 01_create_table.sql   # crio do bronze
│   ├── 02_insert.sql         # 40 registros sujos
│   ├── 03_ouro.sql           # a query única com as 13 regras
│   └── 04_auditoria.sql      # auditoria coluna a coluna
└── notas_candidato.md        # decisões e achados documentados
```

## ▶️ Como reproduzir

1. Abra os scripts em ordem (01 → 02 → 03 → 04) no Databricks (o notebook `.py` já tem as células montadas).
2. Rode os scripts de setup (`01`, `02`) — **idempotentes**.
3. `03` gera o ouro; `04` valida o resultado.

---
**Autor:** Renato Abreu · github.com/renatoapdl