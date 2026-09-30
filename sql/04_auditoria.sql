-- Databricks SQL — AUDITORIA: valida o ouro coluna a coluna
-- (derived table em volta do SELECT do ouro)
SELECT
    COUNT(*) AS total_rows,
    COUNT_IF(idade IS NULL) AS idade_null,
    COUNT_IF(email IS NULL) AS email_null,
    COUNT_IF(telefone IS NULL) AS telefone_null,
    COUNT_IF(cep = '00000-000') AS cep_sentinela,
    COUNT_IF(data_contratacao = '1900-00-00') AS contratacao_sentinela,
    COUNT_IF(data_nascimento = '1900-00-00') AS nascimento_sentinela,
    COUNT_IF(cpf = '000.000.000-00') AS cpf_sentinela,
    COUNT_IF(salario IS NULL) AS salario_null,
    COUNT_IF(pontuacao_credito IS NULL) AS pontuacao_null,
    COUNT_IF(divida IS NULL) AS divida_null,
    COUNT_IF(genero IS NULL) AS genero_null,
    COUNT_IF(estado_nome IS NULL) AS estado_nome_null,
    COUNT_IF(pais IS NULL) AS pais_null,
    COUNT_IF(idade_real IS NULL) AS idade_real_null
FROM (
    SELECT
        id,
        nome,
        CASE WHEN idade < 0 THEN NULL ELSE idade END AS idade,
        CASE WHEN NOT CONTAINS(email, '@') THEN NULL ELSE email END AS email,
        CASE
            WHEN LENGTH(telefone) != 10 THEN NULL
            ELSE CONCAT('(', SUBSTRING(telefone, 1, 2), ') ', SUBSTRING(telefone, 3, 4), '-', SUBSTRING(telefone, 7, 4))
        END AS telefone,
        endereco,
        cidade,
        TRIM(estado) AS estado,
        CASE WHEN TRIM(estado) = 'SP' THEN 'São Paulo' WHEN TRIM(estado) = 'RJ' THEN 'Rio de Janeiro' WHEN TRIM(estado) = 'MG' THEN 'Minas Gerais' WHEN TRIM(estado) = 'ES' THEN 'Espírito Santo' WHEN TRIM(estado) = 'AC' THEN 'Acre' WHEN TRIM(estado) = 'AL' THEN 'Alagoas' WHEN TRIM(estado) = 'AP' THEN 'Amapá' WHEN TRIM(estado) = 'AM' THEN 'Amazonas' WHEN TRIM(estado) = 'BA' THEN 'Bahia' WHEN TRIM(estado) = 'CE' THEN 'Ceará' WHEN TRIM(estado) = 'DF' THEN 'Distrito Federal' WHEN TRIM(estado) = 'GO' THEN 'Goiás' WHEN TRIM(estado) = 'MA' THEN 'Maranhão' WHEN TRIM(estado) = 'MT' THEN 'Mato Grosso' WHEN TRIM(estado) = 'MS' THEN 'Mato Grosso do Sul' WHEN TRIM(estado) = 'PA' THEN 'Pará' WHEN TRIM(estado) = 'PB' THEN 'Paraíba' WHEN TRIM(estado) = 'PE' THEN 'Pernambuco' WHEN TRIM(estado) = 'PI' THEN 'Piauí' WHEN TRIM(estado) = 'PR' THEN 'Paraná' WHEN TRIM(estado) = 'RN' THEN 'Rio Grande do Norte' WHEN TRIM(estado) = 'RO' THEN 'Rondônia' WHEN TRIM(estado) = 'RR' THEN 'Roraima' WHEN TRIM(estado) = 'RS' THEN 'Rio Grande do Sul' WHEN TRIM(estado) = 'SC' THEN 'Santa Catarina' WHEN TRIM(estado) = 'SE' THEN 'Sergipe' WHEN TRIM(estado) = 'TO' THEN 'Tocantins' ELSE NULL END AS estado_nome,
        CASE WHEN REGEXP_LIKE(cep, '^[0-9]{5}-[0-9]{3}$') THEN cep ELSE '00000-000' END AS cep,
        CASE WHEN UPPER(TRIM(pais)) IN ('BRASIL', 'BRAZIL', 'BRAZIIL', 'BRASIIL', 'BRRAZIL', 'BRASILL', 'BRAAZIL', 'BRASIIIL') THEN 'Brasil' ELSE NULL END AS pais,
        empresa,
        cargo,
        CASE WHEN salario < 0 THEN NULL ELSE salario END AS salario,
        CASE WHEN TRY_TO_DATE(data_contratacao) IS NOT NULL THEN data_contratacao ELSE '1900-00-00' END AS data_contratacao,
        CASE WHEN TRY_TO_DATE(data_nascimento) IS NOT NULL THEN data_nascimento ELSE '1900-00-00' END AS data_nascimento,
        CASE WHEN TRY_TO_DATE(data_nascimento) IS NOT NULL THEN YEAR(CURRENT_DATE()) - YEAR(TRY_TO_DATE(data_nascimento)) ELSE NULL END AS idade_real,
        CASE WHEN REGEXP_LIKE(cpf, '^[0-9]{11}$') THEN CONCAT(SUBSTRING(cpf, 1, 3), '.', SUBSTRING(cpf, 4, 3), '.', SUBSTRING(cpf, 7, 3), '-', SUBSTRING(cpf, 10, 2)) ELSE '000.000.000-00' END AS cpf,
        cartao_credito,
        CASE WHEN pontuacao_credito < 300 OR pontuacao_credito > 850 THEN NULL ELSE pontuacao_credito END AS pontuacao_credito,
        CASE WHEN divida < 0 THEN NULL ELSE divida END AS divida,
        CASE WHEN genero NOT IN ('M', 'F') THEN NULL ELSE genero END AS genero
    FROM default.dados_brutos
) AS ouro;