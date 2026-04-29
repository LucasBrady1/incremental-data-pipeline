# Pipeline de Dados Incremental com Python e SQLServer

Este projeto implementa um pipeline de dados focado em eficiência e escalabilidade. O objetivo principal é realizar a ingestão de dados de forma incremental, partindo de arquivos CSV (simulando um Data Lake) para um banco de dados SQLite, processando apenas os registros novos ou atualizados desde a última execução.

## Objetivo do Projeto

Resolver o problema da carga total (Full Load), que se torna lenta e custosa à medida que o volume de dados cresce. A solução utiliza a técnica de Watermarking para identificar o ponto exato onde o processamento anterior parou, garantindo que o sistema processe apenas o delta de dados.

## Arquitetura e Funcionamento

O fluxo de dados segue três etapas principais controladas por uma tabela técnica de estado:

1. **Consulta de Estado**: Antes de iniciar, o pipeline consulta a tabela `pipeline_control` para recuperar o último timestamp processado.
2. **Extração e Transformação**: O script lê a origem (CSV) e aplica um filtro temporal baseado no marcador recuperado. Os dados passam por uma limpeza para remover duplicidades dentro do lote atual.
3. **Carga com UPSERT**: A persistência no SQLite utiliza a lógica de "Update ou Insert". Se o ID do registro já existe, os dados são atualizados; caso contrário, são inseridos.

## Diferenciais Técnicos

- **Idempotência**: O pipeline pode ser executado diversas vezes com o mesmo arquivo sem gerar duplicidade de dados no destino.
- **Controle de Estado**: O uso de uma tabela de metadados permite gerenciar múltiplos pipelines de forma organizada dentro do banco de dados.
- **Atomicidade**: O marcador de progresso só é atualizado se a carga dos dados de negócio for concluída com sucesso, evitando perda de informação em caso de falhas.

## Validação do Sistema

O funcionamento foi validado através de três cenários de teste:
- **Carga Histórica**: Ingestão inicial completa com o banco vazio.
- **Ingestão Incremental**: Identificação e carga exclusiva de novas linhas adicionadas à origem.
- **Atualização de Registros**: Modificação de status em vendas antigas, onde o banco refletiu as mudanças sem duplicar os registros.

## Tecnologias Utilizadas

- **Python 3.x**
- **Pandas**: Para manipulação e filtragem dos dados.
- **SQLAlchemy / SQLite**: Para persistência e gerenciamento do Data Warehouse.
- **Logging**: Para rastreabilidade e monitoramento do processo.

## Como Executar

1. Clone o repositório.
2. Instale as dependências: `pip install -r requirements.txt`.
3. Execute o processo: `python main.py`.
4. Verifique os logs no terminal e consulte o arquivo `.db` gerado para validar os dados.
