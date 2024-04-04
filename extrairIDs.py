from math import e
from pickle import TRUE
from numpy import append, empty
import pandas as pd
from sqlalchemy import create_engine, VARCHAR, INT, FLOAT, DATE


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

fields_campanha = [
    AdsInsights.Field.campaign_id,
    AdsInsights.Field.campaign_name
]

fields_conjunto = [
    AdsInsights.Field.adset_id,
    AdsInsights.Field.adset_name
]

fields_ad = [
    AdsInsights.Field.ad_id,
    AdsInsights.Field.ad_name
]


params_campanha = {
  'date_preset': 'yesterday',
  'level': 'campaign'
}

params_conjunto = {
  'date_preset': 'yesterday',
  'level': 'adset'
}

params_ad = {
  'date_preset': 'yesterday',
  'level': 'ad'
}



my_account = AdAccount('act_577560246260074')
valores = ['link_click', 'onsite_conversion.messaging_conversation_started_7d']




def obter_ids_e_nomes(fields, params, coluna):

    insights = my_account.get_insights(fields=fields, params=params)
    dados = []

    for insight in insights:
        dados.append({f"{coluna}_ID": insight[fields[0]], f"{coluna}_NOME": insight[fields[1]]})

    return dados


def verificar_ids_existentes(ids, table, coluna):
    
  query = f"SELECT DISTINCT {coluna}_ID FROM {table}"
    
  existing_ids_df = pd.read_sql(query, con=engine)
    
  existing_ids = existing_ids_df[f"{coluna}_ID"].astype(str).tolist()
  
   

  ids_nao_existentes = [id for id in ids if id not in existing_ids]
  

  return ids_nao_existentes


def convertendo_para_df(fields, params, table, coluna):

    ids_e_nomes = obter_ids_e_nomes(fields, params, coluna)

    ids = [row[f"{coluna}_ID"] for row in ids_e_nomes]
    nomes = [row[f"{coluna}_NOME"] for row in ids_e_nomes]


    ids_nao_repetidos = verificar_ids_existentes(ids, table, coluna)
   
    if not ids_nao_repetidos:
      print("Não há dados para serem inseridos em", table)
      return
    else: 
      print("Dados inseridos com sucesso na tabela")
      df_nao_repetidos = pd.DataFrame({f"{coluna}_ID": ids_nao_repetidos, f"{coluna}_NOME": [nomes[i] for i in range(len(ids)) if ids[i] in ids_nao_repetidos]})
      df_nao_repetidos.to_sql(name=table, con=engine, if_exists='append', index=False)
      return df_nao_repetidos



convertendo_para_df(fields_campanha, params_campanha, "campanha_id", "CAMPANHA")

convertendo_para_df(fields_ad, params_ad, "anuncio_id", "AD")

convertendo_para_df(fields_conjunto, params_conjunto, "conjunto_id", "CONJUNTO")




