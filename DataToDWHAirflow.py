from datetime import datetime, timedelta
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
from databaseClasses import Product, Date, Customer, OrderDetails, Order, Base
from airflow.operators.python import get_current_context

# Helping functions

def getSQLengine():
    """
    Creates and returns a SQLAlchemy engine connected to the Azure SQL database.

    Returns:
        engine (SQLAlchemy Engine): A SQLAlchemy engine object connected to the Azure SQL database.
    """
    conn = Variable.get("connection_string")
    params = urllib.parse.quote_plus(conn)
    conn_str = 'mssql+pyodbc:///?odbc_connect={}'.format(params)
    engine_azure = create_engine(conn_str, echo=False, fast_executemany=True, connect_args={'timeout': 3600})
    return engine_azure

####################

with DAG("Data_To_DWH", start_date=datetime(2024, 5, 24),
         schedule=None, description="Collecting data from Staging table to DWH Tables",
         tags=["Amazon Sales", "Data Warehousing"], catchup=False):

    def GetMaxID(IdColumn, tableName, SQLengine):
        """
        Retrieves the maximum ID from a specified table.

        Args:
            IdColumn (str): The name of the ID column.
            tableName (str): The name of the table.
            SQLengine (SQLAlchemy Engine): The SQLAlchemy engine object.

        Returns:
            max_id (int/str): The maximum ID value from the table, or "NoData" if no data found.
        """
        con = SQLengine.connect()
        try:
            rows = con.execute(f'SELECT MAX({IdColumn}) FROM {tableName}')
            for row in rows:
                return row[0]
        except Exception as e:
            print("No data found in table")
            return "NoData"

    def GetCurrentDataInTable(IdColumn, tableName, SQLengine):
        """
        Retrieves all unique IDs from a specified table.

        Args:
            IdColumn (str): The name of the ID column.
            tableName (str): The name of the table.
            SQLengine (SQLAlchemy Engine): The SQLAlchemy engine object.

        Returns:
            values (list): A list of unique ID values from the table.
        """
        con = SQLengine.connect()
        values = list()
        try:
            rows = con.execute(f'SELECT {IdColumn} FROM {tableName}')
            for row in rows:
                values.append(row[0])
            return values
        except Exception as e:
            print("No data found in table")
            return values

    def GetStagingDataDimension(SQLengine, StagingTableName, columns, IDcolumn, DimClass, uniquValues):
        """
        Retrieves data from the staging table for dimension tables and returns it as a DataFrame.

        Args:
            SQLengine (SQLAlchemy Engine): The SQLAlchemy engine object.
            StagingTableName (str): The name of the staging table.
            columns (list): A list of column names to retrieve.
            IDcolumn (str): The name of the ID column.
            DimClass (class): The dimension class.
            uniquValues (list/str): A list of unique values or a single unique value.

        Returns:
            DataFrame (pd.DataFrame): A pandas DataFrame containing the data.
        """
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
                    rows = con.execute(f"SELECT [{columns_str}] FROM {StagingTableName} WHERE {IDcolumn} > {uniquValues}")
            else:
                rows = con.execute(f"SELECT [{columns_str}] FROM {StagingTableName}")
        Objects = list()
        dataTypes = DimClass.getDataTypes()

        for row in tqdm(rows):
            object_data = {}
            if type(uniquValues) != list or len(newRecords) == 0 or row[IDcolumn] in newRecords:
                for key, value in zip(columns, row):
                    key = DimClass.map(key)
                    if key in dataTypes:
                        data_type = dataTypes[key]
                        if data_type == datetime.date:
                            value = parser.parse(value, dayfirst=True).date()
                        else:
                            value = data_type(value)
                        object_data[key] = value
                    else:
                        object_data[key] = value
                object = DimClass(**object_data)
                Objects.append(object)
        Objects = list(set(Objects))
        print("Number Of Records to be added:", len(Objects))
        DataFrame = pd.DataFrame.from_records([o.to_dict() for o in Objects])
        return DataFrame

    def SaveDataframeToSQLtable(DataFrame, tableName, SQLengine):
        """
        Saves a DataFrame to a SQL table.

        Args:
            DataFrame (pd.DataFrame): The pandas DataFrame to save.
            tableName (str): The name of the SQL table.
            SQLengine (SQLAlchemy Engine): The SQLAlchemy engine object.
        """
        if len(DataFrame) > 0:
            batch_size = int(math.ceil(len(DataFrame) * 0.1))
            batches = np.array_split(DataFrame, int(len(DataFrame) / batch_size))
            errors = []
            for batch in tqdm(batches):
                try:
                    batch.to_sql(tableName, SQLengine, if_exists='append', index=False, method=None)
                except Exception as e:
                    errors.append(e)
                    continue
            print(f"Batch Size: {batch_size}")
            print(f"Added {len(batches) - len(errors)} Batches")
            print(f"Failed to add {len(errors)} Batches")
            print(f"ERROR: {errors}")
        else:
            print("No new Data to be added")

    @task(map_index_template="{{ my_custom_map_index }}")
    def FillDimension(IdColumn, DimName, SQLengine, StagingTableName, DimColumns, DimClass, UseMaxID):
        """
        Fills the dimension table with data from the staging table.

        Args:
            IdColumn (str): The name of the ID column.
            DimName (str): The name of the dimension table.
            SQLengine (SQLAlchemy Engine): The SQLAlchemy engine object.
            StagingTableName (str): The name of the staging table.
            DimColumns (list): A list of dimension column names.
            DimClass (class): The dimension class.
            UseMaxID (bool): Whether to use the maximum ID or not.
        """
        context = get_current_context()
        context["my_custom_map_index"] = f"Filling: {DimName}"
        print(f"Filling: {DimName}")

        if UseMaxID:
            uniqueValues = GetMaxID(IdColumn, DimName, SQLengine)
        else:
            uniqueValues = GetCurrentDataInTable(IdColumn, DimName, SQLengine)
        productsDataFrame = GetStagingDataDimension(SQLengine, StagingTableName, DimColumns, IdColumn, DimClass, uniqueValues)
        SaveDataframeToSQLtable(productsDataFrame, DimName, SQLengine)

    def GetDimensionTablesInDictionary(SQLengine, DimTables):
        """
        Retrieves dimension tables data and returns it as a dictionary.

        Args:
            SQLengine (SQLAlchemy Engine): The SQLAlchemy engine object.
            DimTables (list): A list of dimension tables.

        Returns:
            Dimensions (dict): A dictionary containing dimension tables data.
        """
        Dimensions = dict()
        con = SQLengine.connect()
        for table in tqdm(DimTables):
            DimName, DimID, NeededDimColumn, DimColumnDataType = table
            query = f"SELECT [{DimID}], [{NeededDimColumn}] FROM {DimName}"
            result = con.execute(query)
            Dimensions[DimName] = (dict(), NeededDimColumn, DimID, DimColumnDataType)
            for row in result:
                Dimensions[DimName][0][row[NeededDimColumn]] = row[DimID]
        return Dimensions

    def GetStagingDataFact(SQLengine, StagingTableName, NeededColumns):
        """
        Retrieves data from the staging table for fact tables.

        Args:
            SQLengine (SQLAlchemy Engine): The SQLAlchemy engine object.
            StagingTableName (str): The name of the staging table.
            NeededColumns (list): A list of column names to retrieve.

        Returns:
            rows (SQLAlchemy ResultProxy): The result of the query execution.
        """
        con = SQLengine.connect()
        columns_str = '],['.join(NeededColumns)
        query = f"SELECT [{columns_str}] FROM {StagingTableName}"
        rows = con.execute(query)
        return rows

    def GetFactDataFrame(rows, Dimensions, FactMainColumns, FactClass, FactData):
        """
        Processes rows from the staging table and returns a DataFrame for the fact table.

        Args:
            rows (SQLAlchemy ResultProxy): The result of the query execution.
            Dimensions (dict): A dictionary containing dimension tables data.
            FactMainColumns (list): A list of main fact columns.
            FactClass (class): The fact class.
            FactData (dict): A dictionary containing current fact data.

        Returns:
            factRecords_df (pd.DataFrame): A pandas DataFrame containing the fact data.
        """
        factRecords = []
        for row in tqdm(rows):
            kwargs = {}
            validRow = True
            for Dimension in Dimensions:
                data, NeededDimColumn, DimID, DimColumnDataType = Dimensions[Dimension]
                if DimColumnDataType == datetime.date:
                    valueInStaging = parser.parse(row[NeededDimColumn], dayfirst=True).replace(hour=0, minute=0, second=0, microsecond=0)
                else:
                    valueInStaging = DimColumnDataType(row[NeededDimColumn])
                if len(FactData[DimID]) > 0 and data[valueInStaging] in FactData[DimID]:
                    validRow = False
                    break
                kwargs[DimID] = data[valueInStaging]
            for column in FactMainColumns:
                kwargs[column] = row[column]
            if validRow:
                fact_instance = FactClass(**kwargs)
                factRecords.append(fact_instance)
        factRecords = list(set(factRecords))
        print("Number Of Records to be added:", len(factRecords))
        factRecords_df = pd.DataFrame.from_records([f.to_dict() for f in factRecords])
        return factRecords_df

    def GetCurrentFactRecordsInDictionary(SQLengine, FactName, FactIDs):
        """
        Retrieves current fact records and returns them as a dictionary.

        Args:
            SQLengine (SQLAlchemy Engine): The SQLAlchemy engine object.
            FactName (str): The name of the fact table.
            FactIDs (list): A list of fact ID column names.

        Returns:
            FactData (dict): A dictionary containing current fact records.
        """
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
    def FillFact(FactName, SQLengine, StagingTableName, NeededColumns, FactClass, DimTables, FactMainColumns, AllFactDimIDs):
        """
        Fills the fact table with data from the staging table.

        Args:
            FactName (str): The name of the fact table.
            SQLengine (SQLAlchemy Engine): The SQLAlchemy engine object.
            StagingTableName (str): The name of the staging table.
            NeededColumns (list): A list of needed column names.
            FactClass (class): The fact class.
            DimTables (list): A list of dimension tables.
            FactMainColumns (list): A list of main fact columns.
            AllFactDimIDs (list): A list of all fact dimension IDs.
        """
        print(f"Filling: {FactName}")

        rows = GetStagingDataFact(SQLengine, StagingTableName, NeededColumns)

        Dimensions = GetDimensionTablesInDictionary(SQLengine, DimTables)
        FactData = GetCurrentFactRecordsInDictionary(SQLengine, FactName, AllFactDimIDs)
        factRecords_df = GetFactDataFrame(rows, Dimensions, FactMainColumns, FactClass, FactData)

        SaveDataframeToSQLtable(factRecords_df, FactName, SQLengine)

    engine_azure = getSQLengine()
    dim_params = [
        {
            'SQLengine': engine_azure,
            'StagingTableName': 'AmazonSalesStaging',
            'IdColumn': 'item_id',
            'DimName': 'DimProduct',
            'DimColumns': ['item_id', 'price', 'sku', 'category'],
            'DimClass': Product,
            'UseMaxID': True
        },
        {
            'SQLengine': engine_azure,
            'StagingTableName': 'AmazonSalesStaging',
            'IdColumn': 'order_date',
            'DimName': 'DimDate',
            'DimColumns': ['order_date', 'year', 'month'],
            'DimClass': Date,
            'UseMaxID': True
        },
        {
            'SQLengine': engine_azure,
            'StagingTableName': 'AmazonSalesStaging',
            'IdColumn': 'order_key',
            'DimName': 'DimOrderDetails',
            'DimColumns': ['order_key', 'order_id', 'status', 'qty_ordered', 'value', 'discount_amount', 'payment_method', 'bi_st', 'ref_num', 'Discount_Percent'],
            'DimClass': OrderDetails,
            'UseMaxID': True
        },
        {
            'SQLengine': engine_azure,
            'StagingTableName': 'AmazonSalesStaging',
            'IdColumn': 'cust_id',
            'DimName': 'DimCustomer',
            'DimColumns': ['cust_id', 'Name Prefix', 'First Name', 'Middle Initial', 'Last Name', 'Gender', 'age', 'full_name', 'E Mail', 'Sign in date', 'Phone No.', 'User Name'],
            'DimClass': Customer,
            'UseMaxID': True
        }
    ]
    DimTables = [
        ('DimProduct', 'product_id', 'item_id', str),
        ('DimDate', 'date_id', 'order_date', datetime.date),
        ('DimCustomer', 'customer_key', 'cust_id', int),
        ('DimOrderDetails', 'order_details_key', 'order_key', int)
    ]
    Base.metadata.create_all(bind=engine_azure)
    FillDimension.expand_kwargs(dim_params) >> FillFact('FactOrder', engine_azure, 'AmazonSalesStaging', ['item_id', 'order_date', 'cust_id', 'order_key', 'total'], Order, DimTables, ['total'], ['product_id', 'date_id', 'customer_key', 'order_details_key'])
