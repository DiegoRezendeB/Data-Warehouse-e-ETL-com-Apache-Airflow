AdventureWorks ETL & Data Warehouse 🚴‍♂️📊

📋 Sobre o Projeto

Este projeto implementa uma solução completa de Engenharia de Dados e Business Intelligence para a base de dados pública AdventureWorks. O objetivo foi construir um Data Warehouse (DW) robusto utilizando o modelo dimensional (Star Schema), orquestrado automaticamente para facilitar análises de vendas, produtos e performance regional.

A arquitetura foi desenvolvida em contêineres para garantir reprodutibilidade e isolamento, simulando um ambiente de produção real onde dados transacionais brutos são transformados em insights estratégicos.

⚙️ Arquitetura e Tecnologias

O pipeline de dados segue a arquitetura ELT/ETL moderna:

Ingestão (Staging): Script Python coleta dados brutos (CSV) e carrega no PostgreSQL.

Orquestração: Apache Airflow gerencia as dependências das tarefas (DAGs).

Processamento: Pandas e SQLAlchemy realizam a limpeza, tipagem e transformação.

Armazenamento (DW): PostgreSQL hospeda o Data Warehouse com tabelas Fato e Dimensões.

Modelagem: Criação de chaves artificiais (Surrogate Keys) e integridade referencial (Cascade).

🗂️ Modelagem de Dados (Star Schema)

O DW foi modelado para otimizar consultas analíticas, composto por:

Fato: fato_vendas (Métricas de vendas, descontos e quantidades).

Dimensões:

dim_produto: Detalhes do produto, custos e preços.

dim_cliente: Dados cadastrais unificados.

dim_territorio: Hierarquia geográfica (País, Região).

dim_tempo: Calendário expandido para análises temporais.

dim_status: Normalização dos status de pedidos.

🚀 Como Executar

Pré-requisitos

Docker e Docker Compose instalados.

Git.

Passo a Passo

Clone o repositório:

git clone [https://github.com/DiegoRezendeB/Data-Warehouse-e-ETL-com-Apache-Airflow.git)


Inicie o ambiente (Airflow + Postgres):

docker compose up -d --build


Prepare a área de Staging (Carga Inicial):
Execute o script localmente para baixar os dados brutos e popular o banco:

# Certifique-se de ter as libs instaladas (pandas, sqlalchemy, psycopg2)
python setup_dados.py


Acesse o Airflow:

URL: http://localhost:8080

Usuário/Senha: admin / admin

Execute a DAG:
Ative a DAG etl_adventureworks_completo e aguarde a conclusão.

📈 Indicadores (KPIs)

O projeto permite a extração de métricas estratégicas via SQL ou ferramentas de BI (Power BI), tais como:

💰 Receita Bruta e Líquida

🏷️ Ticket Médio por Pedido

🌍 Share de Vendas por País

📦 Curva ABC de Produtos

📉 Margem de Lucro

🛠️ Desafios Superados

Durante o desenvolvimento, foram solucionados problemas complexos de engenharia de dados:

Integridade Referencial: Implementação de DROP CASCADE e recriação de tabelas para garantir consistência entre Fato e Dimensões.

Tipagem de Dados: Tratamento de erros de casting (Text vs BigInt) provenientes de dados sujos no Staging.

Surrogate Keys: Geração automática de chaves primárias (SERIAL) para isolar o DW das chaves de negócio.

Locale/Idioma: Tratamento de datas em Português via código Python, independente do SO do contêiner.
