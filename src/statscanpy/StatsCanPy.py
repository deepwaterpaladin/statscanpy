from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import pandas as pd
import io
import json
import re
import requests
import zipfile

class StatsCanPy:
    '''Basic Wrapper for Querying StasCan Data'''
    def __init__(self, path:str, spark=True):
        '''
        Default usage is with Spark; otherwise uses Pandas.
        '''
        self.path = path
        self.spark = self.__init_spark(spark)
        self.isSpark = spark
        self.base_url = "https://www150.statcan.gc.ca/n1/en/type/data?text="
        self.patterns = [r'<span>Table:</span>\s*(\d{2}-\d{2}-\d{4}-\d{2})', r'<div class="ndm-result-title">\s*<span>\d+\.\s*<a href="([^"]+)".*?>(.*?)</a>', r'<div class="ndm-result-title">\s*<span>\d+\.\s*<a[^>]*>(.*?)</a>']

    def get_table_id_from_name(self, table_name:str) -> str:
        req = requests.get(self.base_url+table_name)
        match = re.search(self.patterns[0], req.text)
        name_match = re.search(self.patterns[2], req.text, re.DOTALL)
        table_url_match = re.search(self.patterns[1], req.text, re.DOTALL)
        if match:
            print(f"TOP MATCH:\n{name_match.group(1)}: {match.group(1)}\n\nAccessible at: {table_url_match.group(1)}")
            return match.group(1)
        else:
            print("No match found")
            return Exception("No match found")
        
    def find_table_id_from_name(self, table_name:str, limit:int=10) -> list:
        '''
        Implements RegEx search on HTML response from StatsCan.
        Does not make use of the StatsCan API.
        '''
        req = requests.get(self.base_url+table_name)
        match = re.search(self.patterns[0], req.text)
        name_match = re.search(self.patterns[2], req.text, re.DOTALL)
        table_url_match = re.search(self.patterns[1], req.text, re.DOTALL)
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
    
    def get_table_from_name(self, table_name:str):
        table_id = self.__clean_table_id(self.get_table_id_from_name(table_name))
        try:
            if self.isSpark:
                return self.__get_table_csv_as_spark(table_id)
            else:
                return self.__get_table_csv_as_pandas(table_id)
        except Exception as e:
            return Exception(f"Issue with {table_id}\n: {str(e)}")

    async def __download_data(self, table_id:str) -> str:
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
    
    async def __get_table_csv_as_spark(self, table_id:str) -> DataFrame:
        try:
            path = await self.__download_data(table_id)
            df = spark.read.csv(f"{self.path}/{table_id}.csv", header=True).withColumn("REF_DATE", F.col("REF_DATE").cast("date")).withColumn("UOM_ID", F.col("UOM_ID").cast("int")).withColumn("VALUE", F.col("VALUE").cast("float")).withColumn("DECIMALS", F.col("DECIMALS").cast("int")).orderBy(F.col("REF_DATE").desc())
            return df
        except Exception as e:
            raise e

    async def __get_table_csv_as_pandas(self, table_id:str) -> pd.DataFrame:
        try:
            path = await self.__download_data(table_id)
            df = pd.read_csv(f"{self.path}/{table_id}.csv", header=True)
            return df
        except Exception as e:
            raise e
    
    def __init_spark(self, isSpark):
        if isSpark:
            return spark.Builder().getOrCreate()
        
    def __clean_table_id(self, table_id:str) -> str:
        return table_id.replace("-", "")[0:-2]
