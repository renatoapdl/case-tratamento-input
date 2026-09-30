# Notas do candidato: case Databricks

## Adaptação ao ambiente

O enunciado previa cluster `cluster_desafio_seunome` com Databricks Runtime 14.3 LTS e quatro Spark configs (`spark.databricks.delta.schema.autoMerge.enabled`, `spark.cleaner.periodicGC.interval`, `spark.databricks.delta.autoCompact.enabled`, `spark.databricks.delta.optimizeWrite.enabled`).

A Community Edition hoje é Free Edition (serverless) e não cria cluster clássico. As configs ficam registradas como design de referência; a execução foi validada no modelo serverless.

## Decisões por regra

- **Email.** O contrato literal pede só "conter `@`", então `joaosilva@`, `carlossantos@invalido` e `luciaalves@@exemplo.com` passam. Validação estrita seria um extra, não uma correção prevista.
- **Telefone.** Validação por tamanho (`LENGTH = 10`). Os 40 registros têm 10 caracteres, então a regra não descarta nada; vira proteção para dados futuros.
- **CEP.** Regex com âncoras `^[0-9]{5}-[0-9]{3}$` e sentinela literal `00000-000` (rejeita `20000-ABC`, `01000-0A0`).
- **Datas.** Sentinela `1900-00-00` em STRING. O enunciado pede DATE, mas dia 00 não existe em DATE (`TRY_TO_DATE('1900-00-00')` retorna NULL). A coluna perde o tipo DATE e ganha uma sentinela impossível de ignorar, no estilo do CEP zerado.
- **CPF.** Validação por 11 dígitos numéricos (`^[0-9]{11}$`, pega letra e tamanho); válidos formatados como `000.000.000-00`, inválidos recebem a sentinela.
- **Estado.** `TRIM` na sigla e coluna nova `estado_nome` com as 27 UFs e `ELSE NULL`. Portão semântico vale mais que o estrutural: `InvalidState` cai no NULL e um `XY` de dois caracteres não passaria.
- **País.** `UPPER(TRIM(...))` com mapa de typos (`BRAZIL`, `BRAZIIL`, `BRASIIL`, `BRRAZIL`, ...) que viram `Brasil`; `InvalidCountry` vira NULL. A regra literal faria tudo virar Brasil e jogaria lixo para dentro do dado; olhar os dados separou typo inequívoco (corrige) de sentinela de linha inválida (NULL).
- **`idade_real`.** Parse com `TRY_TO_DATE` e guarda `IS NOT NULL`; data inválida não gera idade. Cálculo: `YEAR(CURRENT_DATE()) - YEAR(data_nascimento)`.

## Achados da auditoria

A auditoria valida o ouro coluna a coluna com derived table e `COUNT_IF`.

Ela corrigiu a referência: eu esperava 18 `InvalidState`, o certo é 19. Vale a contagem dos dados, não a minha estimativa.

As sentinelas quase sempre coincidem: 19 estados inválidos contra 18 países inválidos, com 18 linhas em comum. Uma linha tem estado inválido e país válido.

Campos fora do escopo das regras (`nome`, `cargo`, `cidade` em branco) ficaram como estavam. Não se inventa regra onde o contrato não pediu.

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