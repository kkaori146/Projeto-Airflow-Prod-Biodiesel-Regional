# Projeto Imigrantes


- Projeto visa extrair dados de dois arquivos csv e posteriormente concatená-los
- Tratamento das inconsistências
- Análise preliminar sobre produção regional de biodiesel em metros cúbicos entre os anos de 2005 a 2022
- Exportação do resultado (arquivo parquet) da subpasta pesquisa para o PostgreSQL

## Fonte

- Portal Braasileiro de Dados Abertos

__https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/arquivos/arquivos-producao-de-biocombustiveis/producao-biodiesel-m3-2005-2021.csv__

<br>

__https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/arquivos/arquivos-producao-de-biocombustiveis/producao-biodiesel-m3-2022.csv__


## Ferramentas

<div align="center">
<p float="left">
  <img src="https://user-images.githubusercontent.com/83531935/202240019-106b54cb-b397-4bcc-a29a-e8ab55dcca85.png" width="180" />
  <img src="https://user-images.githubusercontent.com/83531935/202240028-cd1716fe-dfd5-4484-a9c1-9422da702468.png" width="380" /> 
  <img src="https://user-images.githubusercontent.com/83531935/202240030-59908174-d35d-4a4f-aeb8-9622420886f9.png" width="200" />
  <img src="https://user-images.githubusercontent.com/83531935/202240035-8d3d3582-b222-472d-baa8-1ed9551f2b0e.png" width="180" />
</p>
</div>


## Comandos

- Implementação das modificações no docker-compose.yaml (PostgreSQL)

  **__$\textcolor{darkgreen}{\text{docker-compose up -d --no-deps --build postgres}}$__**

- Solucionando problema no import do PostgresOperator dentro do vscode

  **__$\textcolor{darkgreen}{\text{pip install 'apache-airflow[postgres]}}$__**

- Inicialização rápida do airflow

  **__$\textcolor{darkgreen}{\text{docker-compose up airflow-init}}$__**

- Implementação das modificações na DAG ou docker-compose.yaml

  **__$\textcolor{darkgreen}{\text{docker-compose up}}$__**

## Etapas

1) Importação das bibliotecas e ferramentas necessárias
1) Extração dos dados
2) Concatenação dos datasets 
3) Leitura do dataset concatenado e tratamento das inconsistências
4) Armazenamento do dataset tratado em formato csv e parquet
5) Extração dos dados necessários e salvos em outra tabela em formato parquet
6) Criação da tabela no PostgreSQL
7) Inserção dos dados na tabela criada no item 6

## Resultados

- Dependências

<div align="center">
<img src="https://user-images.githubusercontent.com/83531935/203321696-6c8ee595-445a-47a7-b0de-d7db3d2af924.png" width=1000px > </div>


<br>

- PostgreSQL

<div align="center">
<img src="https://user-images.githubusercontent.com/83531935/203321214-fc603f59-393d-473a-a990-52f442f364c2.png" width=1000px > </div>

<br>
<br>
<hr/>

<div align="right"><p>Novembro, 2022</p></div>
