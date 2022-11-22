from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
import pandas as pd
import requests
import os

os.system('pip install fastparquet')

#Função para extrair dados:
def extrair_dados_2005_2021():
  url = "https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/arquivos/arquivos-producao-de-biocombustiveis/producao-biodiesel-m3-2005-2021.csv"
  x = requests.get(url) 
  open('dados_brutos/producao-biodiesel-m3-2005-2021.csv', 'wb').write(x.content) 
  
def extrair_dados_2022():
  url = "https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/arquivos/arquivos-producao-de-biocombustiveis/producao-biodiesel-m3-2022.csv"
  y = requests.get(url)
  open('dados_brutos/producao-biodiesel-m3-2022.csv', 'wb').write(y.content)

# Função para concatenar as duas tabelas
def concat_tb():
  df = pd.read_csv('dados_brutos/producao-biodiesel-m3-2005-2021.csv', sep=';')
  df1 = pd.read_csv('dados_brutos/producao-biodiesel-m3-2022.csv', sep=';')
  dfbio = pd.merge(df, df1, how = 'outer')
  dfbio.to_csv('dados_brutos/concat/prod_biodiesel.csv')
 
# Função de leitura e tratamento:
def tratamento():
  dfbio = pd.read_csv('dados_brutos/concat/prod_biodiesel.csv', sep=",")
  
  # Eliminação das colunas desnecessárias
  dfbio = dfbio.drop(['UNIDADE DA FEDERAÇÃO', 'PRODUTOR', 'PRODUTO'], axis=1)
  
  # Mudança dos Schemas
  dfbio.rename(columns = {
    'ANO': 'ano',
    'MÊS': 'mes',
    'GRANDE REGIÃO': 'regiao',
    'PRODUÇÃO': 'producao_metros_cubicos'}, inplace=True)

  # Normalização da coluna mes

  dfbio['mes'] = dfbio['mes'].replace(['ABR', 'AGO', 'Abril', 'DEZ', 'FEV', 'Fevereiro', 'JAN', 'JUL', 'JUN', 'Janeiro', 'Junho', 'MAI', 'MAR', 'Maio', 'Março', 'NOV', 'OUT', 'SET'], [ 'ABRIL', 'AGOSTO', 'ABRIL','DEZEMBRO', 'FEVEREIRO','FEVEREIRO','JANEIRO','JULHO','JUNHO','JANEIRO','JUNHO','MAIO','MARÇO','MAIO','MARÇO','NOVEMBRO','OUTUBRO','SETEMBRO'])
  
  # Eliminação da palavra "REGIÃO"

  dfbio['regiao'] = dfbio['regiao'].apply(lambda x: x.replace('REGIÃO', ''))

  # Replace de vírgula por ponto

  dfbio['producao_metros_cubicos'] = dfbio['producao_metros_cubicos'].apply(lambda x: x.replace(',', '.'))

  # Convertendo de string para tipo float

  dfbio['producao_metros_cubicos'] = dfbio['producao_metros_cubicos'].astype(float)

  # Padronizando para 3 casas decimais depois da vírgula

  dfbio.loc[:, "producao_metros_cubicos"] = dfbio["producao_metros_cubicos"].map('{:.3f}'.format)

  # Convertendo de string para tipo float

  dfbio['producao_metros_cubicos'] = dfbio['producao_metros_cubicos'].astype(float)

  return dfbio

# Função de exportação em formato csv e parquet

def exportacao_dados(**kwargs):
  ti=kwargs['ti']
  dfbio = ti.xcom_pull(task_ids='tratamento')
    
  # Conversão do dataset em arquivos csv e parquet
  dfbio.to_csv('dados_tratados/biocombustivel.csv',index=False)
  dfbio.to_parquet('dados_tratados/biocombustivel.parquet', index=False)

# Produção geral entre as regiões de acordo com o ano

def total_producao():
  dfparquet = pd.read_parquet('dados_tratados/biocombustivel.parquet', engine='fastparquet')
  dfparquet = dfparquet.groupby(['ano','regiao']) ['producao_metros_cubicos'].sum().reset_index() 
  
  # Armazenamento em formato parquet dentro da subpasta pesquisa
  dfparquet.to_parquet('dados_tratados/pesquisa/prod_regional_bioc.parquet',index=False)

# Função para inserir dados na tabela sql a partir do dataset tratado
def inserir_dados_sql():
  dfinal = pd.read_parquet('dados_tratados/pesquisa/prod_regional_bioc.parquet', engine = 'fastparquet')
  valores = []
  for i in range(len(dfinal)):    
    ano = dfinal.iloc[i,0]
    regiao = dfinal.iloc[i,1]
    producao_metros_cubicos = dfinal.iloc[i,2]
    valores.append("('%s', '%s', %s)" %(ano, regiao, producao_metros_cubicos))

  values = str(valores).strip('[]')
  values = values.replace('"', '')
  query = "INSERT INTO TB_BIODIESEL(ano, regiao, producao_metros_cubicos) VALUES %s;" %(values)
  return query

# Definindo alguns argumentos básicos
default_args = {
    'owner':'kkaori146',
    'start_date': datetime(2022,11,20),
    'email_on_failure':False,
    'email_on_retry':False,
    'retries':1,
    'retry_delay': timedelta(minutes=1)
}

# Instanciando a DAG:
with DAG(
    'prod_biodiesel',
    max_active_runs=2,
    schedule_interval="@daily",
    template_searchpath= '/opt/airflow/sql',
    catchup = True,
    default_args = default_args) as dag:
    

  extrair_dados_2005_2021 = PythonOperator(
      task_id = 'extrair_dados_2005_2021',
      python_callable = extrair_dados_2005_2021
  )

  extrair_dados_2022 = PythonOperator(
      task_id = 'extrair_dados_2022',
      python_callable = extrair_dados_2022
  )

  concat_tb = PythonOperator(
    task_id = 'concat_tb',
    python_callable = concat_tb
  )

  tratamento = PythonOperator(
    task_id = "tratamento",
    python_callable= tratamento
  )

  exportacao_dados = PythonOperator(
    task_id = 'exportacao_dados',
    provide_context = True,
    python_callable=exportacao_dados
  )

  total_producao = PythonOperator(
    task_id = 'total_producao',
    python_callable=total_producao
  )

  criar_tabela = PostgresOperator(
    task_id = 'criar_tabela',
    postgres_conn_id = 'postgres-producao-biodiesel',
    sql = 'criar_tabela.sql'
  )

  insere_dados_tabela = PostgresOperator(
    task_id = 'insere_dados_tabela',
    postgres_conn_id = 'postgres-producao-biodiesel',
    sql = inserir_dados_sql()
  )
  
[extrair_dados_2005_2021, extrair_dados_2022] >> concat_tb >> tratamento >> exportacao_dados >> total_producao >> criar_tabela >> insere_dados_tabela