# Notas do candidato — Case Databricks (tratamento de dados)

## Adaptação ao ambiente
- O enunciado previa cluster `cluster_desafio_seunome` com **Databricks Runtime 14.3 LTS** e **4 Spark configs** (`spark.databricks.delta.schema.autoMerge.enabled`, `spark.cleaner.periodicGC.interval`, `spark.databricks.delta.autoCompact.enabled`, `spark.databricks.delta.optimizeWrite.enabled`).
- A Community Edition evoluiu para **Free Edition (serverless)** — não oferece criação de cluster clássico. As configs ficam **documentadas como design de referência**; a execução foi validada no modelo serverless. Adaptação registrada de forma transparente.

## Decisões por regra (e por quê)
- **Email:** contrato literal do enunciado — só "conter `@`" → `joaosilva@`, `carlossantos@invalido` e `luciaalves@@exemplo.com` **passam**. Nota-se como observação no caso real (validação estrita seria um PLUS).
- **Telefone:** validação por **tamanho** (`LENGTH = 10`), conforme a regra. Todos os 40 tinham 10 chars → 0 NULLs (regra de proteção futura, não de correção atual).
- **CEP:** sentinela literal `00000-000` por regra. Regex com âncoras `^...$` valida 8 caracteres e números (rejeita `20000-ABC`, `01000-0A0`).
- **Datas (contratação/nascimento):** sentinela **`1900-00-00` em STRING**, decisão consciente. O enunciado pede "válidos → DATE", mas **dia 00 não é data representável** em DATE (`TRY_TO_DATE('1900-00-00') = NULL`). Troca assumida: coluna fica STRING, perde funções de data nativas; ganha sentinela visualmente inconfundível (mesma filosofia do CEP zerado).
- **CPF:** validado por **11 dígitos numéricos** (`^[0-9]{11}$` — pega letra/tamanho); válidos **normalizados** para `000.000.000-00`; inválidos → sentinela.
- **Estado:** `TRIM` na sigla + **coluna nova** `estado_nome` com as **27 UFs** e `ELSE NULL` (portão semântico — `InvalidState` cai no NULL). Gate semântico > gate estrutural (tamanho): um `XY` de 2 letras não passaria.
- **País:** `UPPER(TRIM(...))` + mapa de typos (`BRAZIL`, `BRAZIIL`, ...) → `Brasil` canônico; `InvalidCountry` → NULL. A regra literal ("tudo → Brasil") viraria lixo em dado — a observação dos dados separou "typo inequívoco" (corrige) de "sentinela de linha inválida" (NULL).
- **`idade_real`:** parse com `TRY_TO_DATE` + `IS NOT NULL`; inválidos → NULL (não computar idade de sentinela). `YEAR(CURRENT_DATE()) - YEAR(data)`.

## Achados da auditoria (evidências > achismo)
- Auditoria coluna-a-coluna via **derived table** (a query inteira + `COUNT_IF`).
- **A auditoria corrigiu a referência:** o esperado era 18 `InvalidState`; a verdade era **19**. A contagem que vale é a dos dados.
- Correlação entre sentinelas: **19 `InvalidState` vs 18 `InvalidCountry`** (overlap 18) — as linhas estruturalmente sujas quase sempre coincidem, mas não sempre: uma linha tem estado inválido com país válido.
- Campos **fora do escopo das regras** (`nome`, `cargo`, `cidade` em branco) permaneceram **verbatim**: não inventar regra onde o contrato não pediu.

## Resultado consolidado (40 linhas)

| Coluna | Tratamento | Contagem |
|---|---|---|
| total | — | 40 |
| idade | NULL se < 0 | 4 NULL |
| email | NULL sem `@` | 1 NULL |
| telefone | formatado `(XX) AAAA-BBBB` | 0 NULL |
| cep | sentinela `00000-000` | 10 |
| data_contratacao | sentinela `1900-00-00` | 6 |
| data_nascimento | sentinela `1900-00-00` | 10 |
| cpf | normalizado + sentinela | 10 |
| salario | NULL se < 0 | 8 NULL |
| pontuacao_credito | NULL fora de 300–850 (bordas válidas) | 8 NULL |
| divida | NULL se < 0 | 11 NULL |
| genero | NULL fora de M/F | 9 NULL |
| estado_nome | 27 UFs + NULL | 19 NULL |
| pais | `Brasil` canônico + NULL | 18 NULL |
| idade_real | calculada p/ data válida | 10 NULL |