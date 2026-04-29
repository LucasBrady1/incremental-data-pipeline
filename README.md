# Incremental Data Pipeline with Python & State Control

Este repositório apresenta uma implementação robusta de um pipeline de dados concebido para simular a ingestão incremental de dados de um Data Lake, estruturado em ficheiros CSV, para um Data Warehouse em SQLite. O projeto foca-se na resolução de desafios críticos da engenharia de dados moderna, utilizando o controlo de estado para garantir a máxima eficiência processual, a idempotência das operações e a integridade absoluta dos dados transformados ao longo do tempo.

## 🚀 Motivação Técnica

A principal dor resolvida por este projeto é a ineficiência de custos e de processamento computacional gerada por cargas totais (*Full Loads*) em bancos de dados que crescem continuamente. Ao implementar uma estratégia de **Watermarking**, o pipeline torna-se inteligente o suficiente para identificar o ponto exato onde a última extração foi interrompida, processando estritamente o "delta" (novos registos ou atualizações). Além da performance, o sistema foi desenhado para ser totalmente **idempotente**; através da lógica de **UPSERT**, garantimos que o pipeline possa ser executado múltiplas vezes sem que ocorra a duplicação de informação no destino, mantendo a "verdade única" dos dados sempre consistente.

## 🏗️ Arquitetura e Componentes

O fluxo de dados tem início numa camada de armazenamento que simula uma *Landing Zone*, local onde os ficheiros CSV são depositados pela origem. O motor de processamento, desenvolvido em **Python 3 com Pandas**, realiza a extração seletiva baseada em filtros temporais e executa as transformações necessárias. O destino final é um banco de dados **SQLite (Schema Silver)**, protegido por restrições de chaves primárias para evitar inconsistências. O diferencial estratégico desta arquitetura reside na tabela técnica de **Controlo de Estado (`pipeline_control`)**, que funciona como o cérebro da operação, armazenando metadados vitais como o último timestamp processado com sucesso e o status de cada execução.

## 🛠️ Funcionalidades Chave

Para assegurar a elevada qualidade dos dados, o pipeline executa uma **deduplicação estrutural** em memória, eliminando registos repetidos dentro do próprio lote antes de avançar para a fase de carga. A persistência no banco de dados utiliza a lógica de **UPSERT**: se o identificador da venda já existir no destino, o registo é atualizado com as informações mais recentes; caso contrário, é criada uma nova entrada. Todo este ecossistema é regido pelo princípio da **atomicidade**, o que significa que o marcador de progresso (*Watermark*) na tabela de controlo só é atualizado se a carga no destino for confirmada com sucesso, prevenindo lacunas de dados em caso de falhas parciais do sistema.

## 📊 Ciclo de Execução (O "Teste de Fogo")

A fiabilidade deste pipeline pode ser validada através de três cenários fundamentais de execução. No primeiro, a **Carga Histórica**, o sistema deteta o banco vazio e realiza a ingestão total dos dados iniciais. No segundo cenário, a **Ingestão Incremental**, novos registos são adicionados ao CSV e o pipeline carrega cirurgicamente apenas estas novas linhas. Por fim, no cenário de **Atualização (Update)**, caso o status de uma venda antiga seja alterado na origem, o sistema identifica a mudança e atualiza o registo correspondente no SQLite, sem gerar duplicados e mantendo o histórico de atualizações correto.

## 💻 Tecnologias Utilizadas

O projeto foi construído sobre o ecossistema **Python 3.x**, tirando partido da biblioteca **Pandas** para a manipulação eficiente de DataFrames de grandes dimensões. A interface com a camada de persistência é realizada via **SQLAlchemy / sqlite3**, assegurando uma comunicação performática e segura com o banco de dados. Adicionalmente, foi implementado um sistema de **Logging** estruturado que permite a total rastreabilidade de cada etapa, facilitando a observabilidade e o diagnóstico de erros em ambiente de produção.

## 📖 Como Executar

Para replicar este ambiente na sua máquina local, comece por clonar este repositório. De seguida, instale as dependências necessárias através do comando `pip install -r requirements.txt`. Com o ambiente configurado, execute o script principal com `python main.py`. Poderá acompanhar o progresso e a lógica de Watermarking diretamente através dos logs gerados no terminal, verificando posteriormente o ficheiro `.db` para inspecionar a consistência dos dados finais.
