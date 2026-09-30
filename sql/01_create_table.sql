-- Databricks SQL — Criação da tabela bronze
CREATE OR REPLACE TABLE default.dados_brutos (
    id INT,
    nome VARCHAR(255),
    idade INT,
    email VARCHAR(255),
    telefone VARCHAR(50),
    endereco VARCHAR(255),
    cidade VARCHAR(100),
    estado VARCHAR(50),
    cep VARCHAR(20),
    pais VARCHAR(100),
    empresa VARCHAR(255),
    cargo VARCHAR(100),
    salario INT,
    data_contratacao STRING,
    data_nascimento STRING,
    cpf VARCHAR(20),
    cartao_credito VARCHAR(20),
    pontuacao_credito INT,
    divida INT,
    genero VARCHAR(10)
);