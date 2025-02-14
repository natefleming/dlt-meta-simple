# Databricks notebook source


spark.conf.set("iot.ingestion.eh.namespace", "dbrx-field-eng-mqtt")
spark.conf.set("iot.ingestion.eh.name", "dbrx-field-eng-mqtt-eh")
spark.conf.set("iot.ingestion.eh.accessKeyName", "RootManageSharedAccessKey")
spark.conf.set("iot.ingestion.eh.accessKey", "")
spark.conf.set("io.ingestion.eh.secretsScopeName", "fieldeng")

spark.conf.set("iot.ingestion.kafka.requestTimeout", "60000")
spark.conf.set("iot.ingestion.kafka.sessionTimeout", "120000")
spark.conf.set("iot.ingestion.spark.maxOffsetsPerTrigger", "5000")
spark.conf.set("iot.ingestion.spark.failOnDataLoss", "false")
spark.conf.set("iot.ingestion.spark.startingOffsets", "latest")

# COMMAND ----------


from typing import Dict

import dlt

import pyspark.sql.types as T
import pyspark.sql.functions as F



# Event Hubs configuration
EH_NAMESPACE: str = spark.conf.get("iot.ingestion.eh.namespace")
EH_NAME: str = spark.conf.get("iot.ingestion.eh.name")

EH_CONN_SHARED_ACCESS_KEY_NAME: str = spark.conf.get("iot.ingestion.eh.accessKeyName")
SECRET_SCOPE: str = spark.conf.get("io.ingestion.eh.secretsScopeName")
EH_CONN_SHARED_ACCESS_KEY_VALUE: str = spark.conf.get("iot.ingestion.eh.accessKey")

EH_CONN_STR: str = f"Endpoint=sb://{EH_NAMESPACE}.servicebus.windows.net/;SharedAccessKeyName={EH_CONN_SHARED_ACCESS_KEY_NAME};SharedAccessKey={EH_CONN_SHARED_ACCESS_KEY_VALUE};EntityPath={EH_NAME}"
# Kafka Consumer configuration

KAFKA_OPTIONS: Dict[str, str] = {
    "kafka.bootstrap.servers": f"{EH_NAMESPACE}.servicebus.windows.net:9093",
    "subscribe": EH_NAME,
    "kafka.sasl.mechanism": "PLAIN",
    "kafka.security.protocol": "SASL_SSL",
    "kafka.sasl.jaas.config": f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="$ConnectionString" password="{EH_CONN_STR}";',
    "kafka.request.timeout.ms": spark.conf.get("iot.ingestion.kafka.requestTimeout"),
    "kafka.session.timeout.ms": spark.conf.get("iot.ingestion.kafka.sessionTimeout"),
    "maxOffsetsPerTrigger": spark.conf.get("iot.ingestion.spark.maxOffsetsPerTrigger"),
    "failOnDataLoss": spark.conf.get("iot.ingestion.spark.failOnDataLoss"),
    "startingOffsets": spark.conf.get("iot.ingestion.spark.startingOffsets"),
}

# PAYLOAD SCHEMA
payload_ddl: str = """
STRUCT<payload: STRUCT<after: STRUCT<blnChangeThresh: BIGINT, blnDropNonPrint: BIGINT, blnEnabled: BIGINT, blnIgnoreBlank: BIGINT, blnTrim: BIGINT, intColor: BIGINT, intLastTime: BIGINT, intLineStyle: BIGINT, intPlottingMethod: BIGINT, intPointType: BIGINT, intTimeThresh: BIGINT, nvcLastWriteValue: STRING, reaLastWriteValue: DOUBLE, reaLimitHi: STRING, reaLimitLo: STRING, reaOffset: BIGINT, reaSlope: BIGINT, reaValThresh: BIGINT, sinAnalogID: BIGINT, sinAnalogOrder: BIGINT, sinDataType: BIGINT, sinLineID: BIGINT, sinMachID: BIGINT, txtComments: STRING, vchAddress: STRING, vchAnalogName: STRING, vchServer: STRING, vchTopic: STRING, vchUnits: STRING>, before: STRUCT<blnChangeThresh: BIGINT, blnDropNonPrint: BIGINT, blnEnabled: BIGINT, blnIgnoreBlank: BIGINT, blnTrim: BIGINT, intColor: BIGINT, intLastTime: BIGINT, intLineStyle: BIGINT, intPlottingMethod: BIGINT, intPointType: BIGINT, intTimeThresh: BIGINT, nvcLastWriteValue: STRING, reaLastWriteValue: DOUBLE, reaLimitHi: STRING, reaLimitLo: STRING, reaOffset: BIGINT, reaSlope: BIGINT, reaValThresh: BIGINT, sinAnalogID: BIGINT, sinAnalogOrder: BIGINT, sinDataType: BIGINT, sinLineID: BIGINT, sinMachID: BIGINT, txtComments: STRING, vchAddress: STRING, vchAnalogName: STRING, vchServer: STRING, vchTopic: STRING, vchUnits: STRING>, op: STRING, source: STRUCT<change_lsn: STRING, commit_lsn: STRING, connector: STRING, db: STRING, event_serial_no: BIGINT, name: STRING, schema: STRING, snapshot: STRING, table: STRING, ts_ms: BIGINT, version: STRING>, ts_ms: BIGINT>>
"""

payload_schema: T.StructType = T._parse_datatype_string(payload_ddl)

# COMMAND ----------

from pyspark.sql import DataFrame

import pyspark.sql.types as T
import pyspark.sql.functions as F


def create_text_stream() -> DataFrame:
  catalog: str = "nfleming"
  database: str = "rich_products"
  volume: str = "data"
  df: DataFrame = spark.readStream.text(f"/Volumes/{catalog}/{database}/{volume}/*.jsonl").withColumn("key", F.expr("uuid()"))
  return df
    

# COMMAND ----------

# MAGIC %md Implement this function to return your configure Kafka stream

# COMMAND ----------

from pyspark.sql import DataFrame

import pyspark.sql.types as T
import pyspark.sql.functions as F

# Implement this!
def create_kafka_stream() -> DataFrame:
  df: DataFrame = (
    spark.readStream
    .format("kafka")
    .options(**KAFKA_OPTIONS)
  )
  return df

# COMMAND ----------

from typing import Callable

from pyspark.sql import DataFrame


create_stream: Callable[[], DataFrame] = create_text_stream

# Uncomment this for kafka
# create_stream: Callable[[], DataFrame] = create_kafka_stream



# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC The bronze layer store the incoming payload in its original state. It adds a processed_ts and processed_dt

# COMMAND ----------

from pyspark.sql import DataFrame

import pyspark.sql.types as T
import pyspark.sql.functions as F

def parse(df: DataFrame) -> DataFrame:
  payload_df: DataFrame = (
    df.withColumn("records", F.from_json(F.col("value"), payload_schema))
    .select("records.payload.*")
    .withColumns({
      "processed_ts": F.current_timestamp(),
      "processed_dt": F.current_date()
    })
  )
  return payload_df
  

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC The bronze table partitions by processed_dt. If the table Volume is less than 1 TB then you can remove partitioning.

# COMMAND ----------


from typing import Callable

from pyspark.sql import DataFrame

import pyspark.sql.types as T
import pyspark.sql.functions as F


@dlt.create_table(
  name="bronze",
  comment="Streaming Bronze Table",
  table_properties={
    "quality": "bronze",
    "pipelines.reset.allowed": "true" # preserves the data in the delta table if you do full refresh
  },
  partition_cols=["processed_dt"]
)
def _():
  df: DataFrame = create_stream()
  df = parse(df)
  return df

# COMMAND ----------

import dlt

from pyspark.sql import DataFrame

import pyspark.sql.types as T
import pyspark.sql.functions as F


def transform(df: DataFrame) -> DataFrame:
  transformed_df: DataFrame = (
    df.select("ts_ms", "source.name", "source.db", "source.schema", "source.table", "after.*", "processed_ts")
    .withColumn("updated_ts", F.to_timestamp(F.col("ts_ms") / 1000))
    .drop("ts_ms")
  )
  return transformed_df

# COMMAND ----------

import dlt

from pyspark.sql import DataFrame

import pyspark.sql.types as T
import pyspark.sql.functions as F


@dlt.create_view(
  name="transform",
  comment="Transformation View",
)
def _():
  df: DataFrame = dlt.readStream("bronze")
  df = transform(df)
  return df

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC May want to consider partitioning on the silver table if its Append Only or SCD 2. If its SCD 1 then its probably not needed.

# COMMAND ----------

import dlt

from pyspark.sql import DataFrame

import pyspark.sql.types as T
import pyspark.sql.functions as F


@dlt.create_table(
  name="silver",
  comment="Streaming Silver Table",
  table_properties={
    "quality": "bronze",
    "pipelines.reset.allowed": "true" # preserves the data in the delta table if you do full refresh
  }
)
def _():
  return dlt.readStream("transform")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC SCD 1

# COMMAND ----------

# import dlt

# from pyspark.sql import DataFrame

# import pyspark.sql.types as T
# import pyspark.sql.functions as F

# dlt.create_streaming_table("silver")

# dlt.apply_changes(
#   target = "silver",
#   source = "transform",
#   keys = ["sinMachID"],
#   sequence_by = F.col("updated_ts"),
#   stored_as_scd_type = 1
# )