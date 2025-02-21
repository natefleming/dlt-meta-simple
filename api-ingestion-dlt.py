# Databricks notebook source
from typing import Iterator, Tuple

from pyspark.sql import DataFrame
from pyspark.sql.datasource import DataSource, DataSourceReader, InputPartition

import pyspark.sql.types as T
import pyspark.sql.functions as F

class ApiDataSource(DataSource):
    """
    An example data source for batch query using the `faker` library.
    """

    @classmethod
    def name(cls) -> str:
        return "api"

    def schema(self) -> str:
        return "url string, content string, partition string"

    def reader(self, schema: T.StructType):
        return ApiDataSourceReader(schema, self.options)
      

class IndexPartition(InputPartition):
    def __init__(self, index: int):
        self.index = index

class ApiDataSourceReader(DataSourceReader):

    def __init__(self, schema, options):
        self.schema: T.StructType = schema
        self.options = options
        self.urls: list[str] = [url.strip() for url in self.options["urls"].split(",")]

    def partitions(self):
        return [IndexPartition(i) for i in range(len(self.urls))]  
      
    def read(self, partition: IndexPartition) -> Iterator[Tuple]:
        import requests
        import json
        print(f"partition: {partition}")
        url: str = self.urls[partition.index]

        response = requests.get(url)
        yield (url, json.loads(response.text), partition.index)



spark.dataSource.register(ApiDataSource)

# COMMAND ----------

import dlt


@dlt.table
def api_raw():
  urls: str = "https://dog.ceo/api/breeds/list/all/, https://world.openfoodfacts.org/api/v0/product/737628064502.json"
  return spark.read.format("api").option("urls", urls).load()
  

