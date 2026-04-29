# Motivação Técnica (O "Porquê")
Nesta seção, explique o problema que você resolveu. Isso mostra senioridade.

Eficiência de Custos: Evita o reprocessamento de dados históricos (Full Load), processando apenas o delta (novos registros).

Confiabilidade: Implementação de Watermarking para saber exatamente onde a última carga parou.

Idempotência: O pipeline pode ser executado múltiplas vezes sem duplicar dados no destino, graças à lógica de UPSERT.

# Arquitetura e Componentes
Origem: Arquivos CSV simulando uma Landing Zone de um Data Lake.

Processamento: Python 3 + Pandas para extração e transformação.

Destino: SQLite (Schema Silver) com restrições de Primary Key.

Controle de Estado: Tabela técnica (pipeline_control) que armazena metadados de execução.

# Funcionalidades Chave
Deduplicação Estrutural: Limpeza de registros duplicados no lote antes da inserção.

Lógica de UPSERT: Se o ID da venda já existe, os dados são atualizados; caso contrário, são inseridos.

Atomicidade: O Watermark só é atualizado na tabela de controle se a carga no banco de destino for concluída com sucesso.

# Exemplo de Fluxo (The "Test of Fire")
Descreva as execuções que você testou:

Execução 1 (Histórica): Banco vazio -> Carga total do CSV inicial.

Execução 2 (Incremental): Adição de 2 novas linhas no CSV -> Apenas as 2 linhas são processadas.

Execução 3 (Update): Alteração de um status de venda antiga no CSV -> O registro é atualizado no banco sem gerar duplicatas.

# Tecnologias Utilizadas
Python 3.x

Pandas (Manipulação de dados)

SQLAlchemy / sqlite3 (Interface com banco de dados)

Logging (Para rastreabilidade do processo)

# Como Executar
Clone o repositório.

Instale as dependências: pip install -r requirements.txt.

Execute o script principal: python main.py.

Verifique os logs no terminal e o arquivo .db gerado.
