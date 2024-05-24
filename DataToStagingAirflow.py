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
    
    ReadCSVfromAureBlob("AmazonSalesFY2020-21.csv")
