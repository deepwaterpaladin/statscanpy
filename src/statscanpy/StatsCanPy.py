from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
import pandas as pd
import io
import re
import requests
import zipfile

class StatsCanPy:
    '''
    Basic Wrapper for Querying StatsCan Data.
    Provides methods to search and retrieve datasets from the Statistics Canada website.
    '''
    def __init__(self, path: str=None, isSpark: bool=True):
        '''
        Initializes the StatsCanPy instance.

        Args:
            `path (str)`: Directory where downloads will be saved. Uses a temporary directory if None.
            `spark (bool)`: Use Spark for data processing if True; otherwise uses Pandas.

        Attributes:
            `path (str)`: Directory path for data storage.
            `spark (SparkSession)`: Spark session if Spark is enabled.
            `isSpark (bool)`: Indicates whether Spark is used.
            `base_url (str)`: Base URL for querying data.
            `patterns (list)`: Regular expressions for extracting data from HTML.
        '''
        self.path = "./temp" if path is None else path
        self.spark = SparkSession.builder.getOrCreate() if isSpark else None
        self.isSpark = isSpark
        self.base_url = "https://www150.statcan.gc.ca/n1/en/type/data?text="
        self.patterns = [r'<span>Table:</span>\s*(\d{2}-\d{2}-\d{4}-\d{2})', r'<div class="ndm-result-title">\s*<span>\d+\.\s*<a href="([^"]+)".*?>(.*?)</a>', r'<div class="ndm-result-title">\s*<span>\d+\.\s*<a[^>]*>(.*?)</a>']

    def get_table_id_from_name(self, table_name: str) -> str:
        '''
        Retrieves the table ID for a given table name.

        Args:
            `table_name (str)`: The name of the table to search for.

        Returns:
            `str`: The table ID if found, otherwise raises an exception.
        '''
        req = requests.get(self.base_url+table_name)
        match = re.search(self.patterns[0], req.text)
        name_match = re.search(self.patterns[2], req.text, re.DOTALL)
        table_url_match = re.search(self.patterns[1], req.text, re.DOTALL)
        if match:
            print(f"TOP MATCH:\n{name_match.group(1)}: {match.group(1)}\n\nAccessible at: {table_url_match.group(1)}")
            return match.group(1)
        else:
            print("No match found")
            raise Exception("No match found")
        
    def find_table_id_from_name(self, table_name: str, limit: int=10) -> list:
        '''
        Finds multiple table IDs from the given table name. Maximum return value is 50.

        Args:
            `table_name (str)`: The name of the table to search for.
            `limit (int)`: Maximum number of matches to return.

        Returns:
            `list (str)`: A list of table IDs and details if found, otherwise raises an exception.
        '''
        req = requests.get(self.base_url+table_name+"&count=50")
        matches = re.findall(self.patterns[0], req.text)
        name_matches = re.findall(self.patterns[2], req.text, re.DOTALL)
        table_url_matches = re.findall(self.patterns[1], req.text, re.DOTALL)
        if matches:
            try:
                lim = min(len(matches), limit)
                print(f"TOP {lim} MATCHES:\n")
                for i in range(lim):
                    table_name = name_matches[i]
                    table_id = matches[i]
                    table_url = table_url_matches[i][0]  # Extracting the URL part of the tuple
                    print(f"{i+1}. {table_name}: {table_id}\nAccessible at: {table_url}\n\n")
            except Exception as e:
                return Exception(f"Issue with match {i}: {str(e)}")
        else:
            return Exception("No match found.\nTry a different string.")
    
    def get_table_from_name(self, table_name: str):
        '''
        Retrieves a table as a DataFrame, given its name.

        Args:
            `table_name (str)`: The name of the table to retrieve.

        Returns:
            `DataFrame`: A Spark or Pandas DataFrame of the table data.
        '''
        table_id = self.get_table_id_from_name(table_name).replace("-", "")[0:-2]
        try:
            if self.isSpark:
                return self.__get_table_csv_as_spark(table_id)
            else:
                return self.__get_table_csv_as_pandas(table_id)
        except Exception as e:
            return Exception(f"Issue with {table_id}\n: {str(e)}")

    async def __download_data(self, table_id: str) -> str:
        '''
        Private method to download the data for a given table ID.

        Args:
            `table_id (str)`: The ID of the table to download.

        Returns:
            `str`: The path where the data is downloaded.
        '''
        base_endpoint = "https://www150.statcan.gc.ca/t1/wds/rest/getFullTableDownloadCSV/"
        response = requests.get(f"{base_endpoint}{table_id}/en")
        data = response.json()
        if data['status'] == 'SUCCESS':
            try:
                zip_file_url = data['object']
                zip_response = requests.get(zip_file_url)
                z = zipfile.ZipFile(io.BytesIO(zip_response.content))
                z.extractall(self.path)
                return self.path
            except Exception as e:
                raise e
    
    async def __get_table_csv_as_spark(self, table_id: str) -> DataFrame:
        '''
        Private method to retrieve a table as a Spark DataFrame.

        Args:
            `table_id (str)`: The ID of the table to retrieve.

        Returns:
            `DataFrame`: A Spark DataFrame of the table data.
        '''
        try:
            await self.__download_data(table_id)
            df = self.spark.read.csv(f"{self.path}/{table_id}.csv", header=True).withColumn("REF_DATE", F.col("REF_DATE").cast("date")).withColumn("UOM_ID", F.col("UOM_ID").cast("int")).withColumn("VALUE", F.col("VALUE").cast("float")).withColumn("DECIMALS", F.col("DECIMALS").cast("int")).orderBy(F.col("REF_DATE").desc())
            return df
        except Exception as e:
            raise e

    async def __get_table_csv_as_pandas(self, table_id: str) -> pd.DataFrame:
        '''
        Private method to retrieve a table as a Pandas DataFrame.

        Args:
            `table_id (str)`: The ID of the table to retrieve.

        Returns:
            `pd.DataFrame`: A Pandas DataFrame of the table data.
        '''
        try:
            await self.__download_data(table_id)
            df = pd.read_csv(f"{self.path}/{table_id}.csv", header=0)
            return df
        except Exception as e:
            raise e
