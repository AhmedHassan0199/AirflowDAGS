from datetime import datetime,timedelta
from sqlalchemy import create_engine
import urllib
import pandas as pd
from tqdm import tqdm
import numpy as np
from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from dateutil import parser
import math
from databaseClasses import Product,Date,Customer,OrderDetails,Order,Base

#Helping functions
def getSQLengine():
    conn = Variable.get("connection_string")
    params = urllib.parse.quote_plus(conn)
    conn_str = 'mssql+pyodbc:///?odbc_connect={}'.format(params)
    engine_azure = create_engine(conn_str,echo=False,fast_executemany = True)
    return engine_azure
####################

with DAG("Data_To_DWH",start_date=datetime(2024,5,24)
         ,schedule="@daily",description="Collecting data from Staging table to DWH Tables"
         ,tags=["Amazon Sales","Staging"],catchup=False):
    
    @task
    def GetMaxID(IdColumn,tableName,SQLengine):
        con = SQLengine.connect()
        try:
            rows = con.execute(f'SELECT MAX({IdColumn}) FROM {tableName}')
            for row in rows:
                return row[0]
        except Exception as e:
            print("No data found in table")
            return "NoData"
    
    @task
    def GetCurrentDataInTable(IdColumn,tableName,SQLengine):
        con = SQLengine.connect()
        values=list()
        try:
            rows = con.execute(f'SELECT {IdColumn} FROM {tableName}')
            for row in rows:
                values.append(row[0])
            return values
        except Exception as e:
            print("No data found in table")
            return values
        
    @task
    def GetStagingDataDimension(SQLengine,StagingTableName,columns,IDcolumn,DimClass,uniquValues):
        con = SQLengine.connect()
        columns_str = '],['.join(columns)
        newRecords = []
        if type(uniquValues) == list and len(uniquValues) > 0:
            query = f"SELECT [{columns_str}] FROM {StagingTableName}"
            rows = con.execute(query)
            for row in tqdm(rows):
                if row[IDcolumn] not in uniquValues:
                    newRecords.append(row[IDcolumn])
            print(len(newRecords))
        else:
            if uniquValues != "NoData" and uniquValues is not None and type(uniquValues) != list:
                if type(uniquValues) == datetime:
                    rows = con.execute(f"SELECT [{columns_str}] FROM {StagingTableName} WHERE CONVERT(DATETIME, {IDcolumn}, 103) > '{uniquValues}'")
                else:
                    rows = con.execute(f"SELECT [{columns_str}] FROM {StagingTableName} WHERE {IDcolumn} > '{uniquValues}'")
            else:
                rows = con.execute(f"SELECT [{columns_str}] FROM {StagingTableName}")
        Objects = list()
        dataTypes = DimClass.getDataTypes()
        
        for row in tqdm(rows):
            object_data  = {}
            if type(uniquValues) != list or len(newRecords) == 0 or row[IDcolumn] in newRecords:
                for key, value in zip(columns, row):
                    key = DimClass.map(key)
                    if key in dataTypes:
                        data_type = dataTypes[key]
                        if data_type == datetime.date:
                            value = parser.parse(value).date()
                        else:
                            value = data_type(value)
                        object_data [key] = value
                    else:
                        object_data[key] = value
                object = DimClass(**object_data )
                Objects.append(object)
        Objects = list(set(Objects))
        print("Number Of Records to be added :",len(Objects))
        DataFrame = pd.DataFrame.from_records([o.to_dict() for o in Objects])
        return DataFrame
    
    @task
    def SaveDataframeToSQLtable(DataFrame,tableName,SQLengine):
        if len(DataFrame) > 0:
            batch_size = int(math.ceil(len(DataFrame) * 0.1))
            batches = np.array_split(DataFrame,int(len(DataFrame)/batch_size))
            errors = []
            for batch in tqdm(batches):
                try:
                    batch.to_sql(tableName,SQLengine,if_exists='append',index=False,method=None)
                except Exception as e:
                    errors.append(e)
                    continue
            print(f"Batch Size : {batch_size}")
            print(f"Added {len(batches) - len(errors)} Batches")
            print(f"Failed to add {len(errors)} Batches")
            print(f"ERROR : {errors}")
        else:
            print("No new Data to be added")
    
    @task
    def FillDimension(IdColumn,DimName,SQLengine,StagingTableName,DimColumns,DimClass,UseMaxID):
        print(f"Filling : {DimName}")
        Base.metadata.create_all(bind=SQLengine)
        if UseMaxID:
            uniqueValues = GetMaxID(IdColumn,DimName,SQLengine)
        else:
            uniqueValues = GetCurrentDataInTable(IdColumn,DimName,SQLengine)
        productsDataFrame = GetStagingDataDimension(SQLengine,StagingTableName,DimColumns,IdColumn,DimClass,uniqueValues)
        SaveDataframeToSQLtable(productsDataFrame,DimName,SQLengine)

    @task
    def GetDimensionTablesInDictionary(SQLengine, DimTables):
        Dimensions = dict()
        con = SQLengine.connect()
        for table in tqdm(DimTables):
            DimName,DimID,NeededDimColumn,DimColumnDataType = table
            query = f"SELECT [{DimID}],[{NeededDimColumn}] FROM {DimName}"
            result = con.execute(query)
            Dimensions[DimName] = (dict(),NeededDimColumn,DimID,DimColumnDataType)
            for row in result:
                Dimensions[DimName][0][row[NeededDimColumn]] = row[DimID]
        return Dimensions
    
    @task
    def GetStagingDataFact(SQLengine,StagingTableName,NeededColumns):
        con = SQLengine.connect()
        columns_str = '],['.join(NeededColumns)
        query = f"SELECT [{columns_str}] FROM {StagingTableName}"
        rows = con.execute(query)
        return rows
    
    @task
    def GetFactDataFrame(rows,Dimensions,FactMainColumns,FactClass,FactData):
        factRecords = []
        for row in tqdm(rows):
            kwargs = {}
            validRow=True
            for Dimension in Dimensions:
                data,NeededDimColumn,DimID,DimColumnDataType = Dimensions[Dimension]
                if DimColumnDataType == datetime.date:
                    valueInStaging = parser.parse(row[NeededDimColumn]).replace(hour=0, minute=0, second=0, microsecond=0)
                else:
                    valueInStaging = DimColumnDataType(row[NeededDimColumn])
                if len(FactData[DimID]) > 0 and data[valueInStaging] in FactData[DimID]:
                    validRow=False
                    break
                kwargs[DimID] = data[valueInStaging]
            for column in FactMainColumns:
                kwargs[column] = row[column]
            if validRow:
                fact_instance = FactClass(**kwargs)
                factRecords.append(fact_instance)
        factRecords = list(set(factRecords))
        print("Number Of Records to be added :",len(factRecords))
        factRecords_df = pd.DataFrame.from_records([f.to_dict() for f in factRecords])
        return factRecords_df
    
    @task
    def GetCurrentFactRecordsInDictionary(SQLengine,FactName,FactIDs):
        FactData = dict()
        con = SQLengine.connect()
        columns_str = '],['.join(FactIDs)
        query = f"SELECT [{columns_str}] FROM {FactName}"
        result = con.execute(query)
        for ID in FactIDs:
            FactData[ID] = list()
        for row in result:
            for ID in FactIDs:
                FactData[ID].append(row[ID])
        return FactData
    
    @task
    def FillFact(FactName,SQLengine,StagingTableName,NeededColumns,FactClass,DimTables,FactMainColumns,AllFactDimIDs):
        print(f"Filling : {FactName}")
        Base.metadata.create_all(bind=SQLengine)
        
        rows = GetStagingDataFact(SQLengine,StagingTableName,NeededColumns)
        
        Dimensions = GetDimensionTablesInDictionary(SQLengine,DimTables)
        FactData = GetCurrentFactRecordsInDictionary(SQLengine,FactName,AllFactDimIDs)
        factRecords_df = GetFactDataFrame(rows,Dimensions,FactMainColumns,FactClass,FactData)
        
        SaveDataframeToSQLtable(factRecords_df, FactName, SQLengine)

    engine_azure = getSQLengine()
    FillDimension.partial(
        SQLengine=engine_azure,
        StagingTableName='AmazonSalesStaging'
        ).expand(
            IdColumn=['item_id','order_date','order_key','cust_id'],
            DimName=['DimProduct','DimDate','DimOrderDetails','DimCustomer'],
            DimColumns = [['item_id','price','sku','category'],['order_date','year','month'],['order_key', 'order_id', 'status', 'qty_ordered', 'value', 'discount_amount', 'payment_method', 'bi_st', 'ref_num', 'Discount_Percent'],['cust_id', 'Name Prefix', 'First Name', 'Middle Initial', 'Last Name', 'Gender', 'age', 'full_name', 'E Mail', 'Sign in date', 'Phone No.']],
            DimClass = [Product,Date,OrderDetails,Customer],
            UseMaxID = [True,True,True,False]
            )
    