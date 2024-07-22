import io
import json
import re
import requests
import zipfile

class StatsCanPy:
    '''Basic Wrapper for Querying StasCan Data'''
    def __init__(self, spark=True):
        '''
        Default usage is with Spark; otherwise uses Pandas.
        '''
        self.spark = self.__init_spark(spark)
        self.isSpark = spark

    def get_table_id_from_name(self, table_name:str) -> str:
        # Get table id from name
        req = requests.get(f"https://www150.statcan.gc.ca/n1/en/type/data?text={table_name}")
        # use RegEx to parse the response & return the first 'table' id
        id_pattern = r'<span>Table:</span>\s*(\d{2}-\d{2}-\d{4}-\d{2})'
        table_url_pattern = r'<div class="ndm-result-title">\s*<span>\d+\.\s*<a href="([^"]+)".*?>(.*?)</a>'
        table_name_pattern = r'<div class="ndm-result-title">\s*<span>\d+\.\s*<a[^>]*>(.*?)</a>'
        match = re.search(id_pattern, req.text)
        name_match = re.search(table_name_pattern, req.text, re.DOTALL)
        table_url_match = re.search(table_url_pattern, req.text, re.DOTALL)
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
        req = requests.get(f"https://www150.statcan.gc.ca/n1/en/type/data?text={table_name}")
        id_pattern = r'<span>Table:</span>\s*(\d{2}-\d{2}-\d{4}-\d{2})'
        table_url_pattern = r'<div class="ndm-result-title">\s*<span>\d+\.\s*<a href="([^"]+)".*?>(.*?)</a>'
        table_name_pattern = r'<div class="ndm-result-title">\s*<span>\d+\.\s*<a[^>]*>(.*?)</a>'
        matches = re.findall(id_pattern, req.text)
        name_matches = re.findall(table_name_pattern, req.text, re.DOTALL)
        table_url_matches = re.findall(table_url_pattern, req.text, re.DOTALL)
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
    
    def __init_spark(self, isSpark):
        if isSpark:
          return spark.Builder().getOrCreate()
