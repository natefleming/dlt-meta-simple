# Databricks notebook source
import requests
import json
from pyspark.sql.functions import udf

class APIExtract():
  """
  Class used for transformations requiring an API call. 
  """

  def __init__(self):
    self.api_udf = udf(self.call_simple_rest_api)

  def call_simple_rest_api(self, url="https://dog.ceo/api/breeds/list/all/"):
    """ Example Rest API call to open API from Postman """
    # public REST API from PostMan: https://documenter.getpostman.com/view/8854915/Szf7znEe
    response = requests.get(url)
    return json.loads(response.text)

# COMMAND ----------

df = spark.createDataFrame(data)
display(df)

# COMMAND ----------

# Create a list of dictionaries with the URL values
request_params = [
    {"url": "https://dog.ceo/api/breeds/list/all/"},
    {"url": "https://world.openpetfoodfacts.org/api/v0/product/20106836.json"},
    {"url": "https://world.openfoodfacts.org/api/v0/product/737628064502.json"},
    {"url": "https://openlibrary.org/api/books?bibkeys=ISBN:0201558025,LCCN:93005405&format=json"},
]

# Create DataFrame from the list of dictionaries
request_df = spark.createDataFrame(request_params)
request_df.show()

# COMMAND ----------

from  pyspark.sql.functions import *
response_df = request_df.withColumn('response', api_extract_client.api_udf(col('url')))
display(response_df)

# COMMAND ----------

df = (spark.read
      .table('parallel_api_calls')
      .filter(col('url') == 'https://dog.ceo/api/breeds/list/all/')
      )

#df.write.saveAsTable('dog_breeds')