import pandas as pd
from src.database import get_engine
from src.pipeline import get_delta_data, update_pipeline_state, load_delta_data


def run_pipeline():
    PIPELINE_NAME = 'load_vendas'
    CSV_PATH = 'data/vendas_raw.csv'

    print(f"--- Iniciando Pipeline: {PIPELINE_NAME}")

    try:
        # Inicializar Conexão
        engine = get_engine()

        # Recuperar o último estado (Watermark)
        df_control = pd.read_sql("""
            SELECT last_watermark 
                FROM pipeline_control 
            WHERE pipeline_name = '{PIPELINE_NAME}'
        """, engine)

        if df_control.empty:
            last_watermark = '2000-01-01 00:00:00'
        else:
            last_watermark = df_control.iloc[0, 0]

        print(f"Watermark atual: {last_watermark}")

        # identificar o delta
        df_delta = get_delta_data(CSV_PATH, last_watermark)

        if df_delta.empty:
            print("Nenhum dado novo encontrado. Encerrando execução")
            return
        
        print(f"Processando {len(df_delta)} novos registros...")

        # Executar Upsert
        load_delta_data(df_delta, engine)

        # Atualizar watermark
        update_pipeline_state(PIPELINE_NAME, df_delta, engine)

        print("--- Pipeline finalizado com sucesso! ---")

    except Exception as e:
        print(f"!!! Erro crítico no pipeline: {e}")

if __name__ == "__main__":
    run_pipeline()