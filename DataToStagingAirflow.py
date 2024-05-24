from datetime import datetime,timedelta
from sqlalchemy import create_engine
import urllib
import pandas as pd
from azure.storage.blob import generate_blob_sas, BlobSasPermissions
from tqdm import tqdm
import numpy as np
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.decorators import task

def getSQLengine():
    conn = 'Driver={ODBC Driver 18 for SQL Server};Server=tcp:azure-barq-sql-server.database.windows.net,1433;Database=Azure-SQL-Instance;Uid=azure-sql;Pwd=P@ssw0rd;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=-1;'
    params = urllib.parse.quote_plus(conn)
    conn_str = 'mssql+pyodbc:///?odbc_connect={}'.format(params)
    engine_azure = create_engine(conn_str,echo=False,fast_executemany = True)
    return engine_azure

def TruncateStagingTable(engine,tableName):
    con = engine.connect()
    try:
        con.execute(f'TRUNCATE TABLE {tableName}')
        return f"Done Truncate of {tableName}"
    except Exception as e:
        return ("ERROR IN TRUNCATE : " + e)


    

with DAG("Data To Staging",start_date=datetime(2024,5,24)
         ,schedule="@daily",description="Collecting data from CSV source to Staging Table"
         ,tags=["Amazon Sales","Staging"],catchup=False):
    
    @task
    def ReadCSVfromAureBlob(blobName):
        #Azure Credentials
        account_name="ezz0barq"
        account_key="5EoiGYry76XUlTCCxXGselpw9D3PVxPTZAJJG13hZIuV6Qg7OZDKuHvCJLKJj+qi3/PD+rbbeSSp+ASt5faqbA=="
        container_name="barq-container"
        required_blob_name = blobName
        sas = generate_blob_sas(account_name = account_name,
                                container_name = container_name,
                                blob_name = required_blob_name,
                                account_key=account_key,
                                permission=BlobSasPermissions(read=True),
                                expiry=datetime.utcnow() + timedelta(hours=1))
        sas_url = 'https://' + account_name+'.blob.core.windows.net/' + container_name + '/' + required_blob_name + '?' + sas
        df = pd.read_csv(sas_url,dtype=str)
        df.insert(0, 'order_key', range(0, len(df)))
        return df
    
    @task
    def GetNewRecords(engine,tableName,Dataframe):
        con = engine.connect()
        try:
            rows = con.execute(f'SELECT item_id FROM {tableName}')
            items = []
            for row in rows:
                items.append(row[0])
            if items == None:
                return Dataframe
            else:
                return Dataframe[ ~Dataframe['item_id'].isin(items)]
        except Exception:
            return Dataframe
        
    @task
    def InsertNewRecordsOnly(df,batch_size,engine,tableName):
        print(f"New Records to be Added : {len(df)}")
        batch_size =int(len(df) * 0.1)
        errors = []
        if len(df) > 0:
            batches = np.array_split(df,int(len(df)/batch_size))
            for batch in tqdm(batches):
                try:
                    batch.to_sql(tableName,engine,if_exists='append',index=False,method=None)
                except Exception as e:
                    errors.append(e)
                    continue
            print(f"Batch Size : {batch_size}")
            print(f"Added {len(batches) - len(errors)} Batches")
            print(f"Failed to add {len(errors)} Batches")
        else:
            print("No new records to be added")

    engine = getSQLengine()
    DF = GetNewRecords(engine,"AmazonSalesStaging",ReadCSVfromAureBlob("AmazonSalesFY2020-21.csv"))
    InsertNewRecordsOnly(DF,2500,engine,"AmazonSalesStaging")
    

