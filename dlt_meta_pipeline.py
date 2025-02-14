# Databricks notebook source
# MAGIC %pip install --upgrade --quiet dlt-meta

# COMMAND ----------

from typing import Optional
from src.dataflow_pipeline import DataflowPipeline


layer: Optional[str] = spark.conf.get("layer", None)
DataflowPipeline.invoke_dlt_pipeline(spark, layer)