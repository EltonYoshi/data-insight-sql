import pandas as pd
from sqlalchemy import create_engine, VARCHAR, INT, FLOAT, DATE
from sqlalchemy.sql import text



from datetime import datetime
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount

#Facebook API
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.adsinsights import AdsInsights


# Configurações de conexão com o banco de dados MySQL
db_user = 'admin'
db_password = 'meIjjT8QqlgVvEByAgTS'
db_host = 'insights-ads.cjiqksugmowp.us-east-1.rds.amazonaws.com'
db_name = 'insights'

engine = create_engine(f"mysql+pymysql://{db_user}:{db_password}@{db_host}/{db_name}")

# Nome da tabela
table_name = 'insights_ads_2024'

#Facebook Credentials
my_app_id = '1910236629370257'
my_app_secret = '34e61524a2997cecff31f4eb7c24860f'
my_access_token = 'EAAbJWZAZB7BZAEBOZBpoO6ZBMhI3ZCZCOzmmJVuCBDe60N20vDeuCVH0oGlZAZCEH12EQDYGXQcE6O8A0xDKTDitpKPrPTg54qOt4ECSesGpQLYdC49YehcdCRusdVh76vzEq0Pd2ZAbGjsCWZAfdF484VBmZBsqJOB9q7WFgYqTjT3pcEdJA5AcGyrMTh4vAodq3TLsGhYsd81fQ38pnHxZCFtfFBr2l'

graph = FacebookAdsApi.init(my_app_id, my_app_secret, my_access_token)

fields_dados_base = [
  AdsInsights.Field.campaign_id,
  AdsInsights.Field.adset_id,
  AdsInsights.Field.ad_id,
  AdsInsights.Field.impressions,
  AdsInsights.Field.reach,
  AdsInsights.Field.clicks,
  AdsInsights.Field.spend,
  AdsInsights.Field.actions,
  AdsInsights.Field.date_start
  
]

params_dados_base = {
  #'time_range': {'since': '2024-04-02', 'until': '2024-04-02'},
  'date_preset': 'yesterday',
  'level': 'ad',
  'breakdowns': ['age', 'gender']
}

colunas = ['CAMPANHA_ID', 'CONJUNTO_ID', 'AD_ID', 'IMPRESSOES', 'ALCANCE',
           'CLICKS', 'VALOR_USADO', 'CLICK_LINK', 'RESULTADOS', 'IDADE', 'GENERO', 'DATA']

my_account = AdAccount('act_577560246260074')
valores = ['link_click', 'onsite_conversion.messaging_conversation_started_7d']

def acoes_especificas(insight, valor):
  for acao in insight.get(AdsInsights.Field.actions, []):
    if(valor == acao['action_type'] ):
      valor_acao = acao['value']
      return valor_acao


def dados_insights(fields, params):
    insights = my_account.get_insights(fields=fields, params=params)

    dados = []

    for insight in insights:
      valores_especificos = []

      for valor in valores:
        valor_acao = acoes_especificas(insight, valor)
        valores_especificos.append(valor_acao)
        
      dados.append([insight[fields[0]],insight[fields[1]],insight[fields[2]], insight[fields[3]], insight[fields[4]], insight[fields[5]], insight[fields[6]], *valores_especificos, insight['age'], insight['gender'], insight[fields[8]]])

    return dados

base_dados = dados_insights(fields_dados_base, params_dados_base)
df = pd.DataFrame(base_dados, columns=colunas)

dtype_mapping = {
    'CAMPANHA_ID': VARCHAR(255),
    'CONJUNTO_ID': VARCHAR(255),
    'AD_ID': VARCHAR(255),
    'IMPRESSOES': INT(),
    'ALCANCE': INT(),
    'CLICKS': INT(),
    'VALOR_USADO': FLOAT(),
    'CLICK_LINK': INT(),
    'RESULTADOS': INT(),
    'IDADE': VARCHAR(50),
    'GENERO': VARCHAR(50),
    'DATA': DATE()
}
df['DATA'] = pd.to_datetime(df['DATA'])

data = df.at[0, 'DATA'].strftime('%Y-%m-%d')

conn = engine.connect()
data = df.at[0, 'DATA'].strftime('%Y-%m-%d')
query = text("SELECT COUNT(*) FROM insights_ads_2024 WHERE DATA = :data")
# Execute a consulta passando o parâmetro 'data'
result = conn.execute(query, {'data': data})

num_rows = result.fetchone()[0]


if num_rows == 0:
    df.to_sql(name=table_name, con=engine, if_exists='append', index=False, dtype=dtype_mapping)
    print("Dados importados com sucesso!")
else:
    print("Já existem dados com a mesma data na tabela. Os dados não foram importados.")

conn.close()

