# main.py

import os
import pandas as pd
import requests
from google.cloud import bigquery
from google.api_core.exceptions import Forbidden

def extrair_transformar_acoes(request):
    """
    Função principal acionada por HTTP para executar o pipeline ETL (Estilo 1ª Geração).
    O parâmetro 'request' é necessário para a assinatura da função, mas não será usado.
    """
    print("LOG: A função foi iniciada.")

    # --- CONFIGURAÇÕES ---
    # Busca a chave da API a partir das variáveis de ambiente da Cloud Function.
    api_key = os.environ.get('ALPHA_VANTAGE_API_KEY')
    if not api_key:
        print("ERRO CRÍTICO: A variável de ambiente ALPHA_VANTAGE_API_KEY não está configurada.")
        return "Erro de configuração no servidor: Chave de API não encontrada.", 500
    
    print("LOG: Chave de API lida com sucesso do ambiente.")

    simbolo_acao = 'AAPL'
    dataset_id = 'analise_acoes'
    table_id = 'historico_acoes_aapl'
    
    # O cliente do BigQuery deteta o ID do projeto automaticamente quando executado na GCP.
    client = bigquery.Client()

    # --- 1. EXTRAÇÃO (E) ---
    print(f"LOG: A buscar dados para o símbolo: {simbolo_acao}")
    url = f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={simbolo_acao}&apikey={api_key}'
    
    try:
        r = requests.get(url)
        r.raise_for_status()  # Lança um erro para status HTTP 4xx/5xx
        json_response = r.json()
        
        if "Time Series (Daily)" not in json_response:
            error_message = json_response.get("Information", "Resposta da API inválida ou limite de uso atingido.")
            print(f"ERRO na API: {error_message}")
            return f"Erro ao buscar dados da API: {error_message}", 500
        
        print("LOG: Extração da API concluída com sucesso.")

    except requests.exceptions.RequestException as e:
        print(f"ERRO de conexão com a API: {e}")
        return f"Erro de conexão com a API: {e}", 500

    # --- 2. TRANSFORMAÇÃO (T) ---
    print("LOG: A iniciar transformação com Pandas...")
    try:
        time_series = json_response['Time Series (Daily)']
        df = pd.DataFrame.from_dict(time_series, orient='index')
        
        df.rename(columns={
            '1. open': 'abertura', '2. high': 'alta',
            '3. low': 'baixa', '4. close': 'fechamento', '5. volume': 'volume'
        }, inplace=True)
        
        df.reset_index(inplace=True)
        df.rename(columns={'index': 'data'}, inplace=True)

        df['data'] = pd.to_datetime(df['data']).dt.date
        for col in ['abertura', 'alta', 'baixa', 'fechamento']:
            df[col] = df[col].astype(float)
        
        df['volume'] = df['volume'].astype(int)
        
        df['media_movel_7d'] = df['fechamento'].rolling(window=7, min_periods=1).mean().round(2)
        
        print("LOG: Transformação com Pandas concluída com sucesso.")

    except (KeyError, TypeError) as e:
        print(f"ERRO ao transformar os dados com Pandas: {e}")
        return f"Erro de processamento dos dados: {e}", 500

    # --- 3. CARREGAMENTO (L) ---
    print("LOG: A iniciar carregamento no BigQuery...")
    table_ref = client.dataset(dataset_id).table(table_id)
    
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",  # Apaga a tabela e insere os novos dados
    )
    
    try:
        job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        job.result()  # Aguarda a conclusão do job
        
        print(f"LOG: Dados carregados com sucesso na tabela {table_id}.")
        return 'Processo ETL concluído com sucesso!', 200
        
    except Forbidden as e:
        print(f"ERRO DE PERMISSÃO: A conta de serviço não tem permissão para escrever no BigQuery.")
        print(f"Detalhes do erro: {e}")
        return "Erro de permissão ao aceder ao BigQuery.", 500
    except Exception as e:
        print(f"ERRO inesperado ao carregar dados no BigQuery: {e}")
        return f"Erro ao carregar dados no BigQuery: {e}", 500