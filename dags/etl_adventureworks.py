from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
from sqlalchemy import create_engine, text

# --- CONFIGURAÇÕES ---
DB_CONN = "postgresql+psycopg2://airflow:airflow@postgres:5432/airflow"

default_args = {
    'owner': 'seu_nome',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# --- FUNÇÕES ETL ---

def get_engine():
    return create_engine(DB_CONN)

def etl_dim_territorio():
  
    print("Iniciando ETL Dim_Territorio...")
    engine = get_engine()
    
    df_raw = pd.read_sql("SELECT * FROM staging.territory", engine)
    
    df_dim = pd.DataFrame()
    df_dim['id_territorio_original'] = df_raw['territoryid'] 
    df_dim['nome_territorio'] = df_raw['name']
    df_dim['codigo_pais'] = df_raw['countryregioncode']
    df_dim['grupo'] = df_raw['group']
    
   
    with engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS dw.dim_territorio CASCADE;"))
        
   
    df_dim.to_sql('dim_territorio', engine, schema='dw', if_exists='replace', index=False)
    
   
    with engine.begin() as conn:
        conn.execute(text("ALTER TABLE dw.dim_territorio ADD COLUMN sk_territorio SERIAL PRIMARY KEY;"))
        
    print(f"Dim_Territorio carregada: {len(df_dim)} linhas.")


def etl_dim_status():
    
    print("Iniciando ETL Dim_Status...")
    engine = get_engine()
    
    data = {
        'id_status_original': [1, 2, 3, 4, 5],
        'nome_status': ['Em Processamento', 'Aprovado', 'Em Espera', 'Rejeitado', 'Enviado'],
        'flag_ativo': ['N', 'S', 'N', 'N', 'S']
    }
    df_dim = pd.DataFrame(data)
    
    with engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS dw.dim_status CASCADE;"))
        
    df_dim.to_sql('dim_status', engine, schema='dw', if_exists='replace', index=False)
    
  
    with engine.begin() as conn:
        conn.execute(text("ALTER TABLE dw.dim_status ADD COLUMN sk_status SERIAL PRIMARY KEY;"))
        
    print(f"Dim_Status carregada: {len(df_dim)} linhas.")


def etl_dim_tempo():
   
    print("Iniciando ETL Dim_Tempo...")
    engine = get_engine()
    
    data_inicial = '2010-01-01'
    data_final = datetime.now().strftime('%Y-%m-%d')
    dates = pd.date_range(start=data_inicial, end=data_final)
    
    mapa_meses = {
        1: 'Janeiro', 2: 'Fevereiro', 3: 'Março', 4: 'Abril',
        5: 'Maio', 6: 'Junho', 7: 'Julho', 8: 'Agosto',
        9: 'Setembro', 10: 'Outubro', 11: 'Novembro', 12: 'Dezembro'
    }
    mapa_dias = {
        0: 'Segunda-feira', 1: 'Terça-feira', 2: 'Quarta-feira', 3: 'Quinta-feira',
        4: 'Sexta-feira', 5: 'Sábado', 6: 'Domingo'
    }
    
    df = pd.DataFrame({'data_completa': dates})
    df['ano'] = df['data_completa'].dt.year
    df['mes'] = df['data_completa'].dt.month
    df['nome_mes'] = df['data_completa'].dt.month.map(mapa_meses)
    df['dia'] = df['data_completa'].dt.day
    df['trimestre'] = df['data_completa'].dt.quarter
    df['dia_da_semana'] = df['data_completa'].dt.dayofweek.map(mapa_dias)
    df['flag_fim_de_semana'] = df['data_completa'].dt.dayofweek.apply(lambda x: 'S' if x >= 5 else 'N')
    df['sk_tempo'] = df['data_completa'].dt.strftime('%Y%m%d').astype(int)
    
    with engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS dw.dim_tempo CASCADE;"))
    
    df.to_sql('dim_tempo', engine, schema='dw', if_exists='replace', index=False)
    print(f"Dim_Tempo carregada: {len(df)} linhas.")

def etl_dim_produto():
    
    print("Iniciando ETL Dim_Produto...")
    engine = get_engine()
    
    df_raw = pd.read_sql("SELECT * FROM staging.product", engine)
    
    df_dim = pd.DataFrame()
    df_dim['id_produto_original'] = df_raw['productid']
    df_dim['nome_produto'] = df_raw['name']
    df_dim['numero_produto'] = df_raw['productnumber']
    df_dim['cor'] = df_raw['color'].fillna('N/A')
    
    df_dim['custo_padrao'] = df_raw['standardcost']
    df_dim['preco_lista'] = df_raw['listprice']
    
    
    with engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS dw.dim_produto CASCADE;")) 
        
    
    df_dim.to_sql('dim_produto', engine, schema='dw', if_exists='replace', index=False)
    
   
    with engine.begin() as conn:
        conn.execute(text("ALTER TABLE dw.dim_produto ADD COLUMN sk_produto SERIAL PRIMARY KEY;"))
        
    print(f"Dim_Produto carregada: {len(df_dim)} linhas.")

def etl_dim_cliente():
    
    print("Iniciando ETL Dim_Cliente...")
    engine = get_engine()
    
    df_raw = pd.read_sql("SELECT * FROM staging.customer", engine)
    
    df_dim = pd.DataFrame()
    df_dim['id_cliente_original'] = df_raw['customerid']
    

    if 'firstname' in df_raw.columns and 'lastname' in df_raw.columns:
        df_dim['nome_completo'] = (
            df_raw['firstname'].fillna('').astype(str) + ' ' + 
            df_raw['lastname'].fillna('').astype(str)
        )
    else:
        df_dim['nome_completo'] = df_raw['companyname'].fillna('Consumidor Final').astype(str)

    with engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS dw.dim_cliente CASCADE;"))

    df_dim.to_sql('dim_cliente', engine, schema='dw', if_exists='replace', index=False)
    
    
    with engine.begin() as conn:
        conn.execute(text("ALTER TABLE dw.dim_cliente ADD COLUMN sk_cliente SERIAL PRIMARY KEY;"))
        
    print(f"Dim_Cliente carregada: {len(df_dim)} linhas.")

def etl_fato_vendas():
   
    print("Iniciando ETL Fato_Vendas...")
    engine = get_engine()
    
   
    query = """
    SELECT 
        CAST(d.salesorderid AS BIGINT) as salesorderid, 
        CAST(d.productid AS BIGINT) as productid, 
        CAST(h.customerid AS BIGINT) as customerid, 
        h.orderdate,
        CAST(h.territoryid AS BIGINT) as territoryid, 
        CAST(h.status AS BIGINT) as status,      
        d.orderqty, 
        d.unitprice, 
        d.unitpricediscount,
        d.linetotal
    FROM staging.salesorderdetail d
    JOIN staging.salesorderheader h 
      ON CAST(d.salesorderid AS BIGINT) = CAST(h.salesorderid AS BIGINT)
    """
    df_vendas = pd.read_sql(query, engine)
    
    df_prod = pd.read_sql("SELECT sk_produto, id_produto_original FROM dw.dim_produto", engine)
    df_merge = pd.merge(df_vendas, df_prod, left_on='productid', right_on='id_produto_original', how='left')
    
    df_cli = pd.read_sql("SELECT sk_cliente, id_cliente_original FROM dw.dim_cliente", engine)
    df_merge = pd.merge(df_merge, df_cli, left_on='customerid', right_on='id_cliente_original', how='left')

    df_terr = pd.read_sql("SELECT sk_territorio, id_territorio_original FROM dw.dim_territorio", engine)
    df_merge = pd.merge(df_merge, df_terr, left_on='territoryid', right_on='id_territorio_original', how='left')

    df_status = pd.read_sql("SELECT sk_status, id_status_original FROM dw.dim_status", engine)
    df_merge = pd.merge(df_merge, df_status, left_on='status', right_on='id_status_original', how='left')
    
    df_merge['orderdate'] = pd.to_datetime(df_merge['orderdate'])
    df_merge['sk_tempo'] = df_merge['orderdate'].dt.strftime('%Y%m%d').fillna(0).astype(int)
    
    df_fato = pd.DataFrame()
    df_fato['sk_produto'] = df_merge['sk_produto'].fillna(0).astype(int)
    df_fato['sk_cliente'] = df_merge['sk_cliente'].fillna(0).astype(int)
    df_fato['sk_tempo'] = df_merge['sk_tempo'].fillna(0).astype(int)
    df_fato['sk_territorio'] = df_merge['sk_territorio'].fillna(0).astype(int)
    df_fato['sk_status'] = df_merge['sk_status'].fillna(0).astype(int)
    
    df_fato['id_pedido_original'] = df_merge['salesorderid']
    df_fato['qtd_venda'] = df_merge['orderqty']
    df_fato['valor_unitario'] = df_merge['unitprice']
    df_fato['desconto_unitario'] = df_merge['unitpricediscount']
    df_fato['valor_total_linha'] = df_merge['linetotal']
    
    df_fato.to_sql('fato_vendas', engine, schema='dw', if_exists='append', index=False)
    print(f"Fato_Vendas carregada: {len(df_fato)} linhas.")


with DAG(
    'etl_adventureworks_completo',
    default_args=default_args,
    description='ETL Academico AdventureWorks DW',
    schedule_interval=None, 
    catchup=False
) as dag:
    
    t1 = PythonOperator(task_id='carga_dim_tempo', python_callable=etl_dim_tempo)
    t2 = PythonOperator(task_id='carga_dim_produto', python_callable=etl_dim_produto)
    t3 = PythonOperator(task_id='carga_dim_cliente', python_callable=etl_dim_cliente)
    t5 = PythonOperator(task_id='carga_dim_territorio', python_callable=etl_dim_territorio)
    t6 = PythonOperator(task_id='carga_dim_status', python_callable=etl_dim_status)

    t4 = PythonOperator(task_id='carga_fato_vendas', python_callable=etl_fato_vendas)

    [t1, t2, t3, t5, t6] >> t4