# Projeto Imigrantes


- Projeto que visa testar os conhecimentos em airflow
- Análise preliminar sobre produção regional de biodiesel em metros cúbicos entre os anos de 2005 a 2022
- Exportação do resultado (arquivo parquet) da subpasta pesquisa para o PostgreSQL

## Fonte

- Portal Braasileiro de Dados Abertos

__https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/arquivos/arquivos-producao-de-biocombustiveis/producao-biodiesel-m3-2005-2021.csv__

<br>

__https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/arquivos/arquivos-producao-de-biocombustiveis/producao-biodiesel-m3-2022.csv__

## Comandos

- Implementação das modificações no docker-compose.yaml (PostgreSQL)

**__$\textcolor{darkgreen}{\text{docker-compose up -d --no-deps --build postgres}}$__**

- Solucionando problema no import do PostgresOperator dentro do vscode

**__$\textcolor{darkgreen}{\text{pip install 'apache-airflow[postgres]}}$__**

- Inicialização rápida do airflow

**__$\textcolor{darkgreen}{\text{docker-compose up airflow-init}}$__**

- Implementação das modificações na DAG ou docker-compose.yaml

**__$\textcolor{darkgreen}{\text{docker-compose up}}$__**

## Resultados

- Dependências

<br>

- PostgreSQL

<br>
<br>
<hr/>

<div align="right"><p>Novembro, 2022</p></div>
