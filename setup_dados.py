import os
import pandas as pd
from sqlalchemy import create_engine, text
import requests

GITHUB_REPO = "https://raw.githubusercontent.com/datasets/adventure-works/main/data/"


DB_CONNECTION = "postgresql+psycopg2://airflow:airflow@localhost:5432/airflow"

FILES = {
    "salesorderheader": "SalesOrderHeader.csv", 
    "salesorderdetail": "SalesOrderDetail.csv",
    "product": "Product.csv",
    "customer": "Customer.csv",
    "territory": "SalesTerritory.csv", 
}

COLUMN_HEADERS = {
    "salesorderheader": ['SalesOrderID', 'RevisionNumber', 'OrderDate', 'DueDate', 'ShipDate', 'Status', 'OnlineOrderFlag', 'SalesOrderNumber', 'PurchaseOrderNumber', 'AccountNumber', 'CustomerID', 'SalesPersonID', 'TerritoryID', 'BillToAddressID', 'ShipToAddressID', 'ShipMethodID', 'CreditCardID', 'CreditCardApprovalCode', 'CurrencyRateID', 'SubTotal', 'TaxAmt', 'Freight', 'TotalDue', 'Comment', 'rowguid', 'ModifiedDate'],
    
    
    "salesorderdetail": ['SalesOrderID', 'SalesOrderDetailID', 'CarrierTrackingNumber', 'OrderQty', 'ProductID', 'SpecialOfferID', 'UnitPrice', 'UnitPriceDiscount', 'LineTotal', 'rowguid', 'ModifiedDate'],
    
    "product": ['ProductID', 'Name', 'ProductNumber', 'MakeFlag', 'FinishedGoodsFlag', 'Color', 'SafetyStockLevel', 'ReorderPoint', 'StandardCost', 'ListPrice', 'Size', 'SizeUnitMeasureCode', 'WeightUnitMeasureCode', 'Weight', 'DaysToManufacture', 'ProductLine', 'Class', 'Style', 'ProductSubcategoryID', 'ProductModelID', 'SellStartDate', 'SellEndDate', 'DiscontinuedDate', 'rowguid', 'ModifiedDate'],
    "customer": ['CustomerID', 'NameStyle', 'Title', 'FirstName', 'MiddleName', 'LastName', 'Suffix', 'CompanyName', 'SalesPerson', 'EmailAddress', 'Phone', 'PasswordHash', 'PasswordSalt', 'rowguid', 'ModifiedDate'],
    "territory": ['TerritoryID', 'Name', 'CountryRegionCode', 'Group', 'SalesYTD', 'SalesLastYear', 'CostYTD', 'CostLastYear', 'rowguid', 'ModifiedDate'],
}

def get_engine():
    return create_engine(DB_CONNECTION)

def download_file(filename):
    url = f"{GITHUB_REPO}{filename}"
    os.makedirs("data", exist_ok=True) 
    local_path = os.path.join("data", filename)
    
    print(f"⬇️ Baixando {filename}...")
    try:
        response = requests.get(url, timeout=30)
        if response.status_code == 200:
            with open(local_path, 'wb') as f:
                f.write(response.content)
            print(f"✅ {filename} salvo em /data")
            return local_path
        else:
            print(f"❌ Erro ao baixar {filename}: Status {response.status_code}")
            return None
    except Exception as e:
        print(f"❌ Erro de conexão ou timeout: {e}")
        return None

def load_to_postgres(file_path, table_name, engine):
    if not file_path or not os.path.exists(file_path):
        print(f"⚠️ Arquivo {file_path} não encontrado. Pulando carga.")
        return

    print(f"🚀 Carregando {table_name} no Postgres (Schema: staging)...")
    try:
        df = pd.read_csv(
            file_path,
            header=None,
            sep='\t',
            names=COLUMN_HEADERS[table_name],
            encoding='utf-8',
            on_bad_lines='skip'
        )
        
        df.columns = [col.lower() for col in df.columns]

        with engine.connect() as conn:
            conn.execute(text("CREATE SCHEMA IF NOT EXISTS staging;"))
            conn.commit()

        df.to_sql(table_name, engine, schema='staging', if_exists='replace', index=False)
        print(f"✅ Tabela staging.{table_name} carregada com {len(df)} linhas.")
    except Exception as e:
        print(f"❌ Erro ao carregar {table_name}: {e}")

def main():
    print("🔌 Conectando ao PostgreSQL...")
    engine = get_engine() 
    
    for table_key, csv_name in FILES.items():
        local_file = download_file(csv_name)
        if not local_file:
            local_file = os.path.join("data", csv_name)
        load_to_postgres(local_file, table_key, engine)

    print("\n🎉 Processo finalizado! Verifique seu banco de dados.")

if __name__ == "__main__":
    main()