# Importing necessary libraries and modules
from datetime import datetime, timedelta
from sqlalchemy import create_engine
import urllib
import pandas as pd
from azure.storage.blob import generate_blob_sas, BlobSasPermissions
from tqdm import tqdm
import numpy as np
from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

def getSQLengine():
    """
    Retrieves the database connection string from Airflow variables and creates an SQLAlchemy engine
    for connecting to an Azure SQL Database.
    
    Returns:
        engine_azure (sqlalchemy.engine.base.Engine): An SQLAlchemy engine for the Azure SQL Database.
    """
    conn = Variable.get("connection_string")
    params = urllib.parse.quote_plus(conn)
    conn_str = 'mssql+pyodbc:///?odbc_connect={}'.format(params)
    engine_azure = create_engine(conn_str, echo=False, fast_executemany=True)
    return engine_azure

# Define an Airflow DAG
with DAG("Data_To_Staging",
         start_date=datetime(2024, 5, 24),
         schedule="@daily",
         description="Collecting data from CSV source to Staging Table",
         tags=["Amazon Sales", "Staging"],
         catchup=False):

    @task
    def ReadCSVfromAureBlob(blobName):
        """
        Reads a CSV file from Azure Blob Storage using a SAS URL.
        
        Args:
            blobName (str): The name of the blob to read from Azure Blob Storage.
        
        Returns:
            df (pandas.DataFrame): DataFrame containing the data read from the CSV file.
        """
        account_name = Variable.get("account_name")
        account_key = Variable.get("account_key")
        container_name = Variable.get("container_name")
        required_blob_name = blobName
        sas = generate_blob_sas(account_name=account_name,
                                container_name=container_name,
                                blob_name=required_blob_name,
                                account_key=account_key,
                                permission=BlobSasPermissions(read=True),
                                expiry=datetime.utcnow() + timedelta(hours=1))
        sas_url = 'https://' + account_name + '.blob.core.windows.net/' + container_name + '/' + required_blob_name + '?' + sas
        df = pd.read_csv(sas_url, dtype=str)
        df.insert(0, 'order_key', range(0, len(df)))
        return df

    @task
    def TruncateStagingTable(engine, tableName):
        """
        Truncates the specified staging table in the SQL database to remove all existing data.
        
        Args:
            engine (sqlalchemy.engine.base.Engine): The SQLAlchemy engine for the database connection.
            tableName (str): The name of the table to truncate.
        
        Returns:
            str: Success message if truncation succeeds, error message if it fails.
        """
        con = engine.connect()
        try:
            con.execution_options(autocommit=True).execute(f'TRUNCATE TABLE {tableName}')
            return f"Done Truncate of {tableName}"
        except Exception as e:
            return "ERROR IN TRUNCATE: " + str(e)

    @task
    def InsertNewRecordsOnly(df, batch_size, engine, tableName):
        """
        Inserts new records from the DataFrame into the specified SQL table in batches.
        
        Args:
            df (pandas.DataFrame): DataFrame containing the new records to insert.
            batch_size (int): The size of each batch to insert.
            engine (sqlalchemy.engine.base.Engine): The SQLAlchemy engine for the database connection.
            tableName (str): The name of the table to insert records into.
        
        Returns:
            None
        """
        print(f"New Records to be Added: {len(df)}")
        batch_size = int(len(df) * 0.1)
        errors = []
        if len(df) > 0:
            batches = np.array_split(df, int(len(df) / batch_size))
            for batch in tqdm(batches):
                try:
                    batch.to_sql(tableName, engine, if_exists='append', index=False, method=None)
                except Exception as e:
                    errors.append(e)
                    continue
            print(f"Batch Size: {batch_size}")
            print(f"Added {len(batches) - len(errors)} Batches")
            print(f"Failed to add {len(errors)} Batches")
            for error in errors:
                print(f'Error: {error}')
        else:
            print("No new records to be added")

    # Task to trigger another DAG for further data processing
    triggerDWH = TriggerDagRunOperator(
        task_id='trigger_DWH_DAG',
        trigger_dag_id='Data_To_DWH'
    )

    # Create an SQL engine
    engine = getSQLengine()
    # Define the task dependencies
    TruncateStagingTable(engine, "AmazonSalesStaging") >> InsertNewRecordsOnly(ReadCSVfromAureBlob("AmazonSalesFY2020-21.csv"), 2500, engine, "AmazonSalesStaging") >> triggerDWH
