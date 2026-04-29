import pandas as pd
from sqlalchemy import text

def get_delta_data(file_path, watermark_banco):
    """
    Lê o arquivo bruto, filtra apenas os novos registros baseados
    no watermark e remove duplicidade no lote.
    """

    # Carregando o CSV
    df = pd.read_csv(file_path)

    # Tipagem: Converter para datetime
    df['updated_at'] = pd.to_datetime(df['updated_at'], errors='coerce')

    # Filtra o Delta
    watermark_ts = pd.to_datetime(watermark_banco)
    df_delta = df[df['updated_at'] > watermark_ts].copy()

    if df_delta.empty:
        return df_delta
    
    # Deduplicação do Lote
    # Ordenando por data para garantir cronologia
    df_delta = df_delta.sort_values(by='updated_at', ascending=True)

    # Removendo duplicatas
    df_delta = df_delta.drop_duplicates(subset=['id_venda'], keep='last')

    return df_delta

def load_delta_data(df, engine):
    """
    Carrega o Dataframe para uma tabela temporária e executa o MERGE
    para garantir a idempotência no SQL Server.
    """

    if df.empty:
        print("Nenhum dado novo para carregar.")
        return
    
    # Usamos um bloco de conexão para garantir a transação
    with engine.begin() as conn:
        # Criar tabela temporária com a mesma estrutura do DataFrame
        df.to_sql('#staging_vendas', conn, if_exists='replace', index=False)

        # Comando MERGE
        merge_stmt = text("""
            MERGE INTO vendas_silver AS Target
                          USING #staging_vendas AS Source
                          ON Target.id_venda = Source.id_venda
                          WHEN MATCHED THEN
                            UPDATE SET
                                Target.cliente = Source.cliente,
                                Target.valor = Source.valor,
                                Target.status = Source.status,
                                Target.updated_at = Source.updated_at
                          WHEN NOT MATCHED THEN
                            INSERT (id_venda, cliente, valor, status, updated_at)
                            VALUES (Source.id_venda, Source.cliente, Source.valor, Source.status, Source.updated_at);
        """)

        conn.execute(merge_stmt)
        print(f"Carga de {len(df)} registros concluída com sucesso (Upsert)")

def update_pipeline_state(pipeline_name, df, engine):
    """
    Atualiza a tabela de controle com o novo watermark e status de sucesso.
    """

    if df.empty:
        return
    
    novo_watermark = df['updated_at'].max()

    with engine.begin() as conn:
        stmt = text("""
            UPDATE pipeline_control
            SET last_watermark = :nw,
                status = 'Success',
                last_run = GETDATE()
            WHERE pipeline_name = :pn
        """)

        conn.execute(stmt, {"nw": novo_watermark, "pn" : pipeline_name})
        print(f"Estado do pipeline '{pipeline_name}' atualizado para: {novo_watermark}")