from datetime import datetime,timedelta
from sqlalchemy import create_engine
import urllib
import pandas as pd
from azure.storage.blob import generate_blob_sas, BlobSasPermissions
from tqdm import tqdm
import numpy as np
from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task

def getSQLengine():
    conn = Variable.get("connection_string")
    params = urllib.parse.quote_plus(conn)
    conn_str = 'mssql+pyodbc:///?odbc_connect={}'.format(params)
    engine_azure = create_engine(conn_str,echo=False,fast_executemany = True)
    return engine_azure




    

with DAG("Data_To_Staging",start_date=datetime(2024,5,24)
         ,schedule="@daily",description="Collecting data from CSV source to Staging Table"
         ,tags=["Amazon Sales","Staging"],catchup=False):
    
    @task
    def ReadCSVfromAureBlob(blobName):
        #Azure Credentials
        account_name=Variable.get("account_name")
        account_key=Variable.get("account_key")
        container_name=Variable.get("container_name")
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
    def TruncateStagingTable(engine,tableName):
        con = engine.connect()
        try:
            con.execute(f'TRUNCATE TABLE {tableName}').execution_options(autocommit=True)
            return f"Done Truncate of {tableName}"
        except Exception as e:
            return ("ERROR IN TRUNCATE : " + str(e))
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
            for error in errors:
                print(f'Error : {error}')
        else:
            print("No new records to be added")
    
    engine = getSQLengine()
    TruncateStagingTable(engine,"AmazonSalesStaging") >> InsertNewRecordsOnly(ReadCSVfromAureBlob("AmazonSalesFY2020-21.csv"),2500,engine,"AmazonSalesStaging")
    

