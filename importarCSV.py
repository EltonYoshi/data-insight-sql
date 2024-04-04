
import pandas as pd
from sqlalchemy import create_engine

# Função para conectar ao banco de dados MySQL usando SQLAlchemy
def conectar_bd():
    return create_engine('mysql+mysqlconnector://admin:meIjjT8QqlgVvEByAgTS@insights-ads.cjiqksugmowp.us-east-1.rds.amazonaws.com/insights')

# Função para inserir dados do CSV no banco de dados usando Pandas
def inserir_dados_csv_no_bd(nome_arquivo):
    try:
        # Ler o arquivo CSV em um DataFrame do Pandas
        df = pd.read_csv(nome_arquivo)

        # Conectar ao banco de dados usando SQLAlchemy e inserir os dados do DataFrame no MySQL
        conexao = conectar_bd()
        df.to_sql(name='insights_ads_2024', con=conexao, if_exists='append', index=False)

        print("Dados inseridos com sucesso!")

    except Exception as erro:
        print("Erro ao inserir dados no banco de dados:", erro)

# Chamada da função para inserir dados do CSV no banco de dados
inserir_dados_csv_no_bd("aws-mysql-python/insights_ads_2024.csv")
