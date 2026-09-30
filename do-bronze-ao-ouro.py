# Databricks notebook source
# MAGIC %md
# MAGIC # Case Databricks — Tratamento de dados (bronze → ouro)
# MAGIC
# MAGIC **Fonte: (Desafio de tratamento por regras de negócio) - Emerson Costa**
# MAGIC
# MAGIC # As Regras:
# MAGIC
# MAGIC **Perguntas e Regras de Negócio:**
# MAGIC
# MAGIC **1. Regra de Negócio: Idade**
# MAGIC
# MAGIC ◦ Pergunta: Alguns registros contêm idades negativas. Crie uma lógica para substituir idades negativas por NULL.
# MAGIC ◦ Regra: idade < 0 deve ser substituída por NULL.
# MAGIC
# MAGIC **2. Regra de Negócio: Email**
# MAGIC
# MAGIC ◦ Pergunta: Alguns emails não possuem um formato válido. Crie uma lógica para definir como NULL os emails que não contêm o símbolo @.
# MAGIC ◦ Regra: Emails sem @ devem ser substituídos por NULL.
# MAGIC
# MAGIC **3. Regra de Negócio: Telefone**
# MAGIC
# MAGIC ◦ Pergunta: Verifique se todos os números de telefone possuem exatamente 10 dígitos. Se não, defina esses valores como NULL.
# MAGIC ◦ Regra: Telefones que não têm exatamente 10 dígitos devem ser substituídos por NULL.
# MAGIC ◦ Regra 2: Formate os telefones corretos para o layout (XX) AAAA-BBBB.
# MAGIC
# MAGIC **4. Regra de Negócio: CEP**
# MAGIC
# MAGIC ◦ Pergunta: Verifique se os CEPs estão no formato correto (00000-000) e se contêm apenas números. Se não, substitua-os por 00000-000.
# MAGIC ◦ Regra: CEPs que não estão no formato correto ou contêm caracteres não numéricos devem ser substituídos por 00000-000.
# MAGIC
# MAGIC **5. Regra de Negócio: Data de Contratação**
# MAGIC
# MAGIC ◦ Pergunta: Algumas datas de contratação não são válidas. Crie uma lógica para definir essas datas como 1900-00-00 e converta os valores válidos para o tipo DATE. O formato correto da data é AAAA-MM-DD.
# MAGIC ◦ Regra: Datas de contratação que não são válidas devem ser substituídas por 1900-00-00. O formato correto é AAAA-MM-DD.
# MAGIC
# MAGIC **6. Regra de Negócio: Data de Nascimento**
# MAGIC
# MAGIC ◦ Pergunta: Verifique se todas as datas de nascimento são válidas. Se não, defina essas datas como 1900-00-00 e converta os valores válidos para o tipo DATE. O formato correto da data é AAAA-MM-DD. Calcule a idade baseada na data de nascimento e a data atual.
# MAGIC ◦ Regra: Datas de nascimento que não são válidas devem ser substituídas por 1900-00-00. O formato correto é AAAA-MM-DD. A idade deve ser calculada com base na data de nascimento e a data atual.
# MAGIC
# MAGIC **7. Regra de Negócio: CPF**
# MAGIC
# MAGIC ◦ Pergunta: Verifique se os CPFs contêm exatamente 11 dígitos numéricos. Se não, substitua-os por 000.000.000-00. Formate os CPFs válidos para o formato 000.000.000-00.
# MAGIC ◦ Regra: CPFs que não contêm exatamente 11 dígitos numéricos ou contêm letras devem ser substituídos por 000.000.000-00. CPFs válidos devem ser formatados como 000.000.000-00.
# MAGIC **8. Regra de Negócio: Salário**
# MAGIC
# MAGIC ◦ Pergunta: Alguns registros possuem salários negativos. Crie uma lógica para definir esses salários como NULL.
# MAGIC ◦ Regra: Salários negativos devem ser substituídos por NULL.
# MAGIC
# MAGIC **9. Regra de Negócio: Pontuação de Crédito**
# MAGIC
# MAGIC ◦ Pergunta: Verifique se as pontuações de crédito estão dentro do intervalo aceitável (300 a 850). Se não, defina esses valores como NULL.
# MAGIC ◦ Regra: Pontuações de crédito fora do intervalo 300-850 devem ser substituídas por NULL.
# MAGIC
# MAGIC **10. Regra de Negócio: Dívida**
# MAGIC
# MAGIC ◦ Pergunta: Alguns registros possuem valores de dívida negativos. Crie uma lógica para definir essas dívidas como NULL.
# MAGIC ◦ Regra: Dívidas negativas devem ser substituídas por NULL.
# MAGIC
# MAGIC **11. Regra de Negócio: Gênero**
# MAGIC
# MAGIC ◦ Pergunta: Verifique se todos os gêneros são 'M' ou 'F'. Se não, defina esses valores como NULL.
# MAGIC ◦ Regra: Gêneros que não são 'M' ou 'F' devem ser substituídos por NULL.
# MAGIC
# MAGIC **12. Regra de Negócio: Estado**
# MAGIC
# MAGIC ◦ Pergunta: Remova espaços em branco antes e depois dos nomes dos estados e crie uma nova coluna com o nome completo do estado com base na sigla.
# MAGIC ◦ Regra: Espaços em branco devem ser removidos e uma nova coluna deve ser criada com o nome completo do estado com base na sigla.
# MAGIC
# MAGIC **13. Regra de Negócio: País**
# MAGIC
# MAGIC ◦ Pergunta: Verifique se todos os países são válidos e corrigidos para 'Brasil'. Se não, substitua-os por NULL.
# MAGIC ◦ Regra: Países que não são 'Brasil' ou estão incorretos devem ser substituídos ou corrigidos para 'Brasil'.
# MAGIC
# MAGIC **13 regras de negócio, 40 linhas sujas** (datas furadas, CPF grafado errado, InvalidState, typos de país), um único SELECT entregando o ouro limpo.
# MAGIC
# MAGIC **Auditoria coluna-a-coluna.** 
# MAGIC
# MAGIC **Stack:** Databricks SQL (serverless), Spark SQL semantics (3-valoervedade NULL, funções escalares vs agregação), CASE, TRY_TO_DATE/cast, SUBSTR/concat de formatação, regexp_like com âncoras+escape, TRIM/UPPER, derived table, COUNT_IF, medallion conceitual (bronze preservado → ouro), adoção de sentinela (00000-000, 1900-00-00, 000.000.000-00) com trade-off de tipo documentado.

# COMMAND ----------

# DBTITLE 1,CREATE TABLE
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE default.dados_brutos (
# MAGIC     id INT,
# MAGIC     nome VARCHAR(255),
# MAGIC     idade INT,
# MAGIC     email VARCHAR(255),
# MAGIC     telefone VARCHAR(50),
# MAGIC     endereco VARCHAR(255),
# MAGIC     cidade VARCHAR(100),
# MAGIC     estado VARCHAR(50),
# MAGIC     cep VARCHAR(20),
# MAGIC     pais VARCHAR(100),
# MAGIC     empresa VARCHAR(255),
# MAGIC     cargo VARCHAR(100),
# MAGIC     salario INT,
# MAGIC     data_contratacao STRING,
# MAGIC     data_nascimento STRING,
# MAGIC     cpf VARCHAR(20),
# MAGIC     cartao_credito VARCHAR(20),
# MAGIC     pontuacao_credito INT,
# MAGIC     divida INT,
# MAGIC     genero VARCHAR(10)
# MAGIC );

# COMMAND ----------

# DBTITLE 1,INSERT
# MAGIC %sql
# MAGIC INSERT INTO default.dados_brutos (id, nome, idade, email, telefone, endereco, cidade, estado, cep, pais, empresa, cargo, salario, data_contratacao, data_nascimento, cpf, cartao_credito, pontuacao_credito, divida, genero) VALUES
# MAGIC (1, 'João Silva', 29, 'joaosilva@', '1234567890', 'Rua Principal, 123', 'São Paulo', ' SP ', '12000-000', 'Brasil', 'Acme Corp', 'Engenheiro', 70000, '2021-15-01', '1992-22-03', '12345678901', '1234567890123456', 700, -5000, 'M'),
# MAGIC (2, 'Maria Oliveira', -25, 'mariaoliveira@exemplo.com', '5555555555', 'Rua Secundária, 456', 'Rio de Janeiro', ' RJ', '20001-000', 'Brasil', 'Beta Corp', 'Gerente', -90000, '2020-20-02', '1985-06-32', '98765432101', '9876543210987654', 650, 3000, 'F'),
# MAGIC (3, '', 35, 'alice@exemplo.com', '0000000000', 'Rua das Flores, 789', 'Curitiba', ' PR ', '80000-000', 'Brazil', 'Gamma Corp', '', 120000, '2019-03-40', '1988-15-08', '13579135791', '1357913579135791', 800, 10000, ''),
# MAGIC (4, 'Carlos Santos', 45, 'carlossantos@invalido', '6543216543', 'Avenida Paulista, 321', 'São Paulo', 'SP', '01310-000', 'Brasiil', 'Delta Corp', 'Analista', 50000, '2018-11-01', '1975-20-05', '24680246822', '2468024682246802', 600, -2000, 'M'),
# MAGIC (5, 'Ana Souza', 30, 'anasouza.com', '4444444444', 'Rua dos Pinheiros, 555', 'Belo Horizonte', 'MG', '30130-000', 'Brasill', 'Epsilon Corp', 'Consultora', 80000, '2020-05-50', '1990-10-10', '97531864239', '9753186423975318', 750, 5000, 'F'),
# MAGIC (6, 'Pedro Lima', -40, 'pedrolima@exemplo', '7777777777', 'Rua das Palmeiras, 666', 'Fortaleza', ' CE ', '60000-000', 'Brasil', 'Zeta Corp', 'Desenvolvedor', 0, '2022-03-14', '1980-25-07', '10293847561', '1029384756102938', 850, 0, ''),
# MAGIC (7, 'Lucia Alves', 50, 'luciaalves@@exemplo.com', '3333333333', 'Rua do Comércio, 777', 'Salvador', ' BA', '40000-000', 'Brasil', 'Theta Corp', 'Designer', -100000, '2017-09-30', '1970-15-01', '21354687952', '2135468795213546', 900, 20000, 'M'),
# MAGIC (8, 'Fernando Ribeiro', 28, 'fernandoribeiro@site', '2222222222', 'Rua das Acácias, 888', 'Porto Alegre', 'RS ', '90000-000', 'Brrazil', 'Iota Corp', 'Arquiteto', 65000, '2021-06-21', '1993-05-11', '32465798463', '3246579846324657', 500, -10000, 'F'),
# MAGIC (9, 'Gabriela Martins', 22, 'gabrielam@exemplo.com', '1111111111', 'Rua das Orquídeas, 999', 'Manaus', 'AM', '69000-000', 'Braziil', 'Kappa Corp', 'Enfermeira', 45000, '2019-12-11', '1998-02-28', '43576908714', '4357690871435769', 650, 0, ''),
# MAGIC (10, 'Hugo Ferreira', 33, 'hugoferreira@.com', '6666666666', 'Rua do Rosário, 123', 'Belém', ' InvalidState ', '66000-000', 'Braazil', 'Lambda Corp', 'Professor', -75000, '2016-08-08', '1987-17-09', '54687219095', '5468721909546872', 700, 15000, 'M'),
# MAGIC (11, 'Isabel Costa', 40, 'isabelc@exemplo.com', '0000000000', 'Rua do Carmo, 124', 'Recife', 'PE ', '50000-000', 'Brasiiil', 'Mu Corp', 'Contadora', 55000, '2021-01-12', '1981-04-05', '65798321106', '6579832110657983', 720, 500, 'F'),
# MAGIC (12, 'Joaquim Alves', 27, 'joaquima@dominio', '4444321234', 'Rua da Praia, 125', 'Vitória', ' ES', '29000-000', 'Brasil', 'Nu Corp', 'RH', 62000, '2020-07-23', '1994-06-12', '76809432197', '7680943219768094', 680, -3000, ''),
# MAGIC (13, 'Karina Rocha', 38, 'karinar@site.com', '3215555432', 'Rua das Laranjeiras, 126', 'Gotham', ' InvalidState ', '70000-000', 'InvalidCountry', 'Xi Corp', 'Marketing', -85000, '2018-04-11', '1983-30-05', '87920543218', '8792054321879205', 670, 7000, 'M'),
# MAGIC (14, 'Liam Brown', 24, 'liambrown@dominio.com', '1236666789', 'Rua dos Pinheiros, 127', 'Central City', ' InvalidState ', '90211-000', 'InvalidCountry', 'Omicron Corp', 'Vendas', 78000, '2019-06-19', '1997-22-03', '98031654329', '9803165432980316', 640, 300, 'F'),
# MAGIC (15, 'Mia Green', -32, 'miag@exemplo.com', '9871231234', 'Rua das Palmeiras, 128', 'Keystone', ' InvalidState ', '15002-000', 'InvalidCountry', 'Pi Corp', 'Suporte', 50000, '2021-08-29', '1989-05-12', '09142765430', '0914276543091427', 620, 8000, ''),
# MAGIC (16, 'Noah Blue', 26, 'noahblue@exemplo', '0000000000', 'Rua do Rosário, 129', 'Smallville', ' InvalidState ', '66003-000', 'InvalidCountry', 'Rho Corp', 'Engenheiro', 67000, '2020-14-02', '1995-11-11', '21053876542', '2105387654210538', 680, -1000, 'M'),
# MAGIC (17, 'Olivia Yellow', 31, 'oliviay@exemplo.com', '1115678111', 'Rua das Laranjeiras, 130', 'Star City', ' InvalidState ', '98102-000', 'InvalidCountry', 'Sigma Corp', 'Cientista', 90000, '2018-20-10', '1990-09-06', '32164987653', '3216498765321649', 650, 4000, 'F'),
# MAGIC (18, 'Paul Purple', 35, 'paulp@exemplo.com', '6663456543', 'Rua dos Pinheiros, 131', 'Coast City', ' InvalidState ', '33102-000', 'InvalidCountry', 'Tau Corp', 'Gerente', 56000, '2021-11-23', '1986-04-07', '43275098764', '4327509876432750', 600, -7000, ''),
# MAGIC (19, 'Quinn Violet', 42, 'quinnv@dominio', '3216789876', 'Rua do Comércio, 132', 'Gotham', ' InvalidState ', '70000-000', 'InvalidCountry', 'Upsilon Corp', 'Professor', -64000, '2020-05-13', '1979-08-14', '54386109875', '5438610987543861', 700, 2000, 'M'),
# MAGIC (20, 'Ryan Black', 37, 'ryanb@exemplo', '9874321234', 'Rua das Laranjeiras, 133', 'Metropolis', ' InvalidState ', '10004-000', 'InvalidCountry', 'Phi Corp', 'Desenvolvedor', 85000, '2019-12-30', '1984-10-03', '65497210986', '6549721098654972', 720, 500, 'F'),
# MAGIC (21, 'Sophia White', 29, 'sophiaw@exemplo.com', '5558765432', 'Rua das Palmeiras, 134', 'Central City', ' InvalidState ', '90212-000', 'InvalidCountry', 'Chi Corp', 'Designer', -53000, '2018-09-14', '1992-04-18', '76508321097', '7650832109765083', 650, 10000, ''),
# MAGIC (22, 'Tom Blue', 33, 'tomb@exemplo.com', '1119876543', 'Rua do Rosário, 135', 'Keystone', ' InvalidState ', '15003-000', 'InvalidCountry', 'Psi Corp', 'Analista', 47000, '2021-02-22', '1988-05-19', '87619432108', '8761943210876194', 690, -2000, 'M'),
# MAGIC (23, 'Uma Yellow', 26, 'umay@site.com', '6666543212', 'Rua das Laranjeiras, 136', 'Star City', ' InvalidState ', '98103-000', 'InvalidCountry', 'Omega Corp', 'RH', 53000, '2020-07-30', '1995-08-21', '98730543219', '9873054321987305', 710, 7000, 'F'),
# MAGIC (24, 'Vic Purple', 40, 'vicp@dominio.com', '4448765432', 'Rua dos Pinheiros, 137', 'Smallville', ' InvalidState ', '66004-000', 'InvalidCountry', 'Alpha Corp', 'Marketing', 65000, '2018-05-17', '1981-09-10', '09841765430', '0984176543098417', 620, -500, ''),
# MAGIC (25, 'Wendy Violet', 28, 'wendyv@exemplo.com', '3219876123', 'Rua do Comércio, 138', 'Coast City', ' InvalidState ', '33103-000', 'InvalidCountry', 'Beta Corp', 'Vendas', -72000, '2019-01-12', '1993-07-06', '10952876541', '1095287654109528', 680, 3000, 'M'),
# MAGIC (26, 'Xander Black', 31, 'xanderb@exemplo.com', '9876543333', 'Rua das Palmeiras, 139', 'Gotham', ' InvalidState ', '70005-000', 'InvalidCountry', 'Gamma Corp', 'Suporte', 58000, '2020-06-19', '1990-11-15', '21063987652', '2106398765210639', 700, -8000, 'F'),
# MAGIC (27, 'Yara White', -29, 'yaraw@dominio', '1238765432', 'Rua do Rosário, 140', 'Metropolis', ' InvalidState ', '10005-000', 'InvalidCountry', 'Delta Corp', 'Engenheira', 60000, '2021-04-21', '1992-30-03', '32174109873', '3217410987321741', 750, 500, ''),
# MAGIC (28, 'Zane Blue', 25, 'zaneb@site', '5556543333', 'Rua das Laranjeiras, 141', 'Central City', ' InvalidState ', '90213-000', 'InvalidCountry', 'Epsilon Corp', 'Enfermeiro', 49000, '2019-07-12', '1996-05-02', '43285210984', '4328521098432852', 680, -6000, 'M'),
# MAGIC (29, 'Anna Green', 39, 'annag@dominio.com', '4449876543', 'Rua dos Pinheiros, 142', 'Star City', ' InvalidState ', '98104-000', 'InvalidCountry', 'Zeta Corp', 'Cientista', -62000, '2020-08-18', '1982-12-29', '54396321095', '5439632109543963', 730, 1000, 'F'),
# MAGIC (30, 'Brian Yellow', 32, 'briany@site.com', '1116543212', 'Rua do Comércio, 143', 'Keystone', ' InvalidState ', '15004-000', 'InvalidCountry', 'Theta Corp', 'Gerente', 57000, '2021-03-25', '1989-07-11', '65407432106', '6540743210654074', 690, -300, 'M'),
# MAGIC (31, 'Lucas Branco', 25, 'lucas@dominio.com', '1111111111', 'Avenida Brasil, 200', 'Rio de Janeiro', ' RJ', '20000-ABC', 'Brasil', 'Omega Corp', 'Vendedor', 5000, '2022-10-10', '1997-08-12', '12345678A90', '1234567890123456', 600, 1000, 'M'),
# MAGIC (32, 'Eduardo Verde', 40, 'eduardo@dominio.com', '2222222222', 'Rua XV de Novembro, 300', 'São Paulo', 'SP ', '01000-0A0', 'Brazil', 'Alpha Corp', 'Engenheiro', 10000, '2020-05-20', '1982-12-20', '23456789B01', '2345678901234567', 700, 2000, 'M'),
# MAGIC (33, 'Clara Azul', 35, 'clara@dominio.com', '3333333333', 'Rua das Flores, 400', 'Curitiba', ' PR', '80000-0A0', 'Brasil', 'Beta Corp', 'Analista', 15000, '2019-03-15', '1986-04-01', '34567890C12', '3456789012345678', 800, 3000, 'F'),
# MAGIC (34, 'Beatriz Vermelho', 28, 'beatriz@dominio.com', '4444444444', 'Avenida Paulista, 500', 'São Paulo', ' SP ', '01310-0A0', 'Brasil', 'Gamma Corp', 'Consultor', 20000, '2018-11-11', '1993-07-17', '45678901D23', '4567890123456789', 900, 4000, 'F'),
# MAGIC (35, 'Fernando Amarelo', 42, 'fernando@dominio.com', '5555555555', 'Rua Augusta, 600', 'São Paulo', ' SP', '01400-0A0', 'Brazil', 'Delta Corp', 'Diretor', 25000, '2017-01-01', '1980-05-05', '56789012E34', '5678901234567890', 1000, 5000, 'M'),
# MAGIC (36, 'Carla Branco', 29, 'carla@dominio.com', '6666666666', 'Rua dos Pinheiros, 700', 'Belo Horizonte', ' MG ', '30130-0A0', 'Brasil', 'Epsilon Corp', 'Coordenador', 30000, '2021-05-05', '1992-10-10', '67890123F45', '6789012345678901', 1100, 6000, 'F'),
# MAGIC (37, 'Roberto Azul', 38, 'roberto@dominio.com', '7777777777', 'Rua das Palmeiras, 800', 'Fortaleza', ' CE ', '60000-0A0', 'Brasil', 'Zeta Corp', 'Gerente', 35000, '2022-03-03', '1984-07-07', '78901234G56', '7890123456789012', 1200, 7000, 'M'),
# MAGIC (38, 'Juliana Verde', 34, 'juliana@dominio.com', '8888888888', 'Rua do Comércio, 900', 'Salvador', 'BA ', '40000-0A0', 'Brazil', 'Theta Corp', 'Engenheiro', 40000, '2019-09-09', '1987-12-12', '89012345H67', '8901234567890123', 1300, 8000, 'F'),
# MAGIC (39, 'Felipe Vermelho', 27, 'felipe@dominio.com', '9999999999', 'Rua das Acácias, 1000', 'Porto Alegre', 'RS ', '90000-0A0', 'Brasil', 'Iota Corp', 'Desenvolvedor', 45000, '2020-12-12', '1995-02-02', '90123456I78', '9012345678901234', 1400, 9000, 'M'),
# MAGIC (40, 'Ana Rosa', 26, 'ana@dominio.com', '1111111111', 'Rua das Orquídeas, 1100', 'Manaus', ' AM ', '69000-0A0', 'Brazil', 'Kappa Corp', 'Analista', 50000, '2021-01-01', '1996-03-03', '01234567J89', '0123456789012345', 1500, 10000, 'F');
# MAGIC

# COMMAND ----------

# DBTITLE 1,BRONZE
# MAGIC %sql
# MAGIC SELECT * FROM default.dados_brutos;

# COMMAND ----------

# DBTITLE 1,OURO
# MAGIC %sql
# MAGIC SELECT
# MAGIC id,
# MAGIC nome,
# MAGIC CASE WHEN idade < 0 THEN NULL ELSE idade END AS idade,
# MAGIC CASE WHEN NOT CONTAINS(email, '@') THEN NULL ELSE email END AS email,
# MAGIC CASE
# MAGIC     WHEN length(telefone) != 10
# MAGIC         THEN NULL
# MAGIC     ELSE concat('(',substring(telefone,1,2),')',' ',substring(telefone,3,4),'-',substring(telefone,7, 4)) END AS telefone,
# MAGIC endereco,
# MAGIC cidade,
# MAGIC TRIM(estado) AS estado,
# MAGIC     CASE WHEN TRIM(estado) = 'SP' THEN 'São Paulo' WHEN TRIM(estado) = 'RJ' THEN 'Rio de Janeiro' WHEN TRIM(estado) = 'MG' THEN 'Minas Gerais' WHEN TRIM(estado) = 'ES' THEN 'Espírito Santo' WHEN TRIM(estado) = 'AC' THEN 'Acre' WHEN TRIM(estado) = 'AL' THEN 'Alagoas' WHEN TRIM(estado) = 'AP' THEN 'Amapá' WHEN TRIM(estado) = 'AM' THEN 'Amazonas' WHEN TRIM(estado) = 'BA' THEN 'Bahia' WHEN TRIM(estado) = 'CE' THEN 'Ceará' WHEN TRIM(estado) = 'DF' THEN 'Distrito Federal' WHEN TRIM(estado) = 'GO' THEN 'Goiás' WHEN TRIM(estado) = 'MA' THEN 'Maranhão' WHEN TRIM(estado) = 'MT' THEN 'Mato Grosso' WHEN TRIM(estado) = 'MS' THEN 'Mato Grosso do Sul' WHEN TRIM(estado) = 'PA' THEN 'Pará' WHEN TRIM(estado) = 'PB' THEN 'Paraíba' WHEN TRIM(estado) = 'PE' THEN 'Pernambuco' WHEN TRIM(estado) = 'PI' THEN 'Piauí' WHEN TRIM(estado) = 'PR' THEN 'Paraná' WHEN TRIM(estado) = 'RN' THEN 'Rio Grande do Norte' WHEN TRIM(estado) = 'RO' THEN 'Rondônia' WHEN TRIM(estado) = 'RR' THEN 'Roraima' WHEN TRIM(estado) = 'RS' THEN 'Rio Grande do Sul' WHEN TRIM(estado) = 'SC' THEN 'Santa Catarina' WHEN TRIM(estado) = 'SE' THEN 'Sergipe' WHEN TRIM(estado) = 'TO' THEN 'Tocantins'
# MAGIC         ELSE NULL END AS estado_nome,
# MAGIC CASE WHEN regexp_like(cep,'^[0-9]{5}-[0-9]{3}$') THEN cep ELSE '00000-000' END AS cep,
# MAGIC CASE
# MAGIC         WHEN UPPER(TRIM(pais)) IN ('BRASIL','BRAZIL','BRAZIIL','BRASIIL','BRRAZIL','BRASILL','BRAAZIL','BRASIIIL')
# MAGIC         THEN 'Brasil'
# MAGIC         ELSE NULL END AS pais,
# MAGIC empresa,
# MAGIC cargo,
# MAGIC CASE WHEN salario < 0 THEN NULL ELSE salario END AS salario,
# MAGIC case
# MAGIC     when try_to_date(data_contratacao) is not null
# MAGIC         then data_contratacao else '1900-00-00' end as data_contratacao,
# MAGIC case
# MAGIC     when try_to_date(data_nascimento) is not null then data_nascimento
# MAGIC     else '1900-00-00' end as data_nascimento,
# MAGIC CASE
# MAGIC     WHEN try_to_date(data_nascimento) IS NOT NULL
# MAGIC     THEN year(current_date()) - year(try_to_date(data_nascimento)) ELSE NULL
# MAGIC END AS idade_real,
# MAGIC CASE
# MAGIC     WHEN regexp_like(cpf,'^[0-9]{11}$')
# MAGIC         THEN concat(substring(cpf,1,3), '.', substring(cpf,4,3), '.', substring(cpf,7,3), '-', substring(cpf,10,2))
# MAGIC         ELSE '000.000.000-00'
# MAGIC END AS cpf,
# MAGIC cartao_credito,
# MAGIC case WHEN pontuacao_credito < 300 OR pontuacao_credito > 850 THEN NULL ELSE pontuacao_credito END AS pontuacao_credito,
# MAGIC case WHEN divida < 0 THEN NULL ELSE divida END AS divida,
# MAGIC case WHEN genero NOT IN ('M','F') THEN NULL ELSE genero END AS genero
# MAGIC FROM default.dados_brutos;

# COMMAND ----------

# DBTITLE 1,AUDITORIA
# MAGIC %sql
# MAGIC SELECT
# MAGIC   COUNT(*) AS total_rows,
# MAGIC   COUNT_IF(idade IS NULL) AS idade_null,
# MAGIC   COUNT_IF(email IS NULL) AS email_null,
# MAGIC   COUNT_IF(telefone IS NULL) AS telefone_null,
# MAGIC   COUNT_IF(cep = '00000-000') AS cep_sentinela,
# MAGIC   COUNT_IF(data_contratacao = '1900-00-00') AS contratacao_sentinela,
# MAGIC   COUNT_IF(data_nascimento = '1900-00-00') AS nascimento_sentinela,
# MAGIC   COUNT_IF(cpf = '000.000.000-00') AS cpf_sentinela,
# MAGIC   COUNT_IF(salario IS NULL) AS salario_null,
# MAGIC   COUNT_IF(pontuacao_credito IS NULL) AS pontuacao_null,
# MAGIC   COUNT_IF(divida IS NULL) AS divida_null,
# MAGIC   COUNT_IF(genero IS NULL) AS genero_null,
# MAGIC   COUNT_IF(estado_nome IS NULL) AS estado_nome_null,
# MAGIC   COUNT_IF(pais IS NULL) AS pais_null,
# MAGIC   COUNT_IF(idade_real IS NULL) AS idade_real_null
# MAGIC FROM (SELECT
# MAGIC id,
# MAGIC nome,
# MAGIC CASE WHEN idade < 0 THEN NULL ELSE idade END AS idade,
# MAGIC CASE WHEN NOT CONTAINS(email, '@') THEN NULL ELSE email END AS email,
# MAGIC CASE
# MAGIC     WHEN length(telefone) != 10
# MAGIC         THEN NULL
# MAGIC     ELSE concat('(',substring(telefone,1,2),')',' ',substring(telefone,3,4),'-',substring(telefone,7, 4)) END AS telefone,
# MAGIC endereco,
# MAGIC cidade,
# MAGIC TRIM(estado) AS estado,
# MAGIC     CASE WHEN TRIM(estado) = 'SP' THEN 'São Paulo' WHEN TRIM(estado) = 'RJ' THEN 'Rio de Janeiro' WHEN TRIM(estado) = 'MG' THEN 'Minas Gerais' WHEN TRIM(estado) = 'ES' THEN 'Espírito Santo' WHEN TRIM(estado) = 'AC' THEN 'Acre' WHEN TRIM(estado) = 'AL' THEN 'Alagoas' WHEN TRIM(estado) = 'AP' THEN 'Amapá' WHEN TRIM(estado) = 'AM' THEN 'Amazonas' WHEN TRIM(estado) = 'BA' THEN 'Bahia' WHEN TRIM(estado) = 'CE' THEN 'Ceará' WHEN TRIM(estado) = 'DF' THEN 'Distrito Federal' WHEN TRIM(estado) = 'GO' THEN 'Goiás' WHEN TRIM(estado) = 'MA' THEN 'Maranhão' WHEN TRIM(estado) = 'MT' THEN 'Mato Grosso' WHEN TRIM(estado) = 'MS' THEN 'Mato Grosso do Sul' WHEN TRIM(estado) = 'PA' THEN 'Pará' WHEN TRIM(estado) = 'PB' THEN 'Paraíba' WHEN TRIM(estado) = 'PE' THEN 'Pernambuco' WHEN TRIM(estado) = 'PI' THEN 'Piauí' WHEN TRIM(estado) = 'PR' THEN 'Paraná' WHEN TRIM(estado) = 'RN' THEN 'Rio Grande do Norte' WHEN TRIM(estado) = 'RO' THEN 'Rondônia' WHEN TRIM(estado) = 'RR' THEN 'Roraima' WHEN TRIM(estado) = 'RS' THEN 'Rio Grande do Sul' WHEN TRIM(estado) = 'SC' THEN 'Santa Catarina' WHEN TRIM(estado) = 'SE' THEN 'Sergipe' WHEN TRIM(estado) = 'TO' THEN 'Tocantins'
# MAGIC         ELSE NULL END AS estado_nome,
# MAGIC CASE WHEN regexp_like(cep,'^[0-9]{5}-[0-9]{3}$') THEN cep ELSE '00000-000' END AS cep,
# MAGIC CASE
# MAGIC         WHEN UPPER(TRIM(pais)) IN ('BRASIL','BRAZIL','BRAZIIL','BRASIIL','BRRAZIL','BRASILL','BRAAZIL','BRASIIIL')
# MAGIC         THEN 'Brasil'
# MAGIC         ELSE NULL END AS pais,
# MAGIC empresa,
# MAGIC cargo,
# MAGIC CASE WHEN salario < 0 THEN NULL ELSE salario END AS salario,
# MAGIC case
# MAGIC     when try_to_date(data_contratacao) is not null
# MAGIC         then data_contratacao else '1900-00-00' end as data_contratacao,
# MAGIC case
# MAGIC     when try_to_date(data_nascimento) is not null then data_nascimento
# MAGIC     else '1900-00-00' end as data_nascimento,
# MAGIC CASE
# MAGIC     WHEN try_to_date(data_nascimento) IS NOT NULL
# MAGIC     THEN year(current_date()) - year(try_to_date(data_nascimento)) ELSE NULL
# MAGIC END AS idade_real,
# MAGIC CASE
# MAGIC     WHEN regexp_like(cpf,'^[0-9]{11}$')
# MAGIC         THEN concat(substring(cpf,1,3), '.', substring(cpf,4,3), '.', substring(cpf,7,3), '-', substring(cpf,10,2))
# MAGIC         ELSE '000.000.000-00'
# MAGIC END AS cpf,
# MAGIC cartao_credito,
# MAGIC case WHEN pontuacao_credito < 300 OR pontuacao_credito > 850 THEN NULL ELSE pontuacao_credito END AS pontuacao_credito,
# MAGIC case WHEN divida < 0 THEN NULL ELSE divida END AS divida,
# MAGIC case WHEN genero NOT IN ('M','F') THEN NULL ELSE genero END AS genero
# MAGIC FROM default.dados_brutos) AS ouro;

# COMMAND ----------

# MAGIC %md
# MAGIC #NOTAS
# MAGIC
# MAGIC **Adaptação serverless** (cluster/14.3 LTS/4 spark configs documentadas como design de referência, não executáveis no plano grátis);
# MAGIC
# MAGIC **Decisões:** email por contrato | telefone por tamanho | sentinela de CEP literal | data 1900-00-00 em STRING (trade-off de tipo) | CPF normalizado + sentinela | estado 27 UFs + ELSE NULL | país typos→Brasil/InvalidCountry→NULL | idade_real com try_to_date+IS NULL;
# MAGIC
# MAGIC **Contagens da auditoria**
# MAGIC
# MAGIC **As linhas "fora de escopo":** (nome/cargo em branco) verbatim.