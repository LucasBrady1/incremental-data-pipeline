-- Tabela de Controle de Estado
CREATE TABLE pipeline_control (
    pipeline_name VARCHAR(100) PRIMARY KEY,
    last_watermark DATETIME2,
    status VARCHAR(100),
    last_run DATETIME2
);

-- Tabela Silver (Dados de Negócio)
CREATE TABLE vendas_silver (
    id_venda INT PRIMARY KEY,
    cliente VARCHAR(100),
    valor DECIMAL(10,2),
    status VARCHAR(50),
    updated_at DATETIME2
);

-- Inserir o registro inicial do pipeline
INSERT INTO pipeline_control (pipeline_name, last_watermark, status, last_run)
VALUES ('load_vendas', '2000-01-01 00:00:00', 'Success', GETDATE());