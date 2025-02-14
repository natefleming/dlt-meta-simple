# Databricks notebook source
# MAGIC %pip install --upgrade --quiet databricks-sdk dlt-meta

# COMMAND ----------

from typing import List

catalog_name: str = "nfleming"
schema_name: str = "sgws"
volume_name: str = "data"

users: List[str] = []

# COMMAND ----------

from pathlib import Path

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import (
  CatalogInfo, 
  SchemaInfo, 
  VolumeInfo, 
  VolumeType,
  SecurableType,
  PermissionsChange,
  Privilege
)


def _volume_as_path(self: VolumeInfo) -> Path:
  return Path(f"/Volumes/{self.catalog_name}/{self.schema_name}/{self.name}")

# monkey patch
VolumeInfo.as_path = _volume_as_path

w: WorkspaceClient = WorkspaceClient()

catalog: CatalogInfo 
try:
  catalog = w.catalogs.get(catalog_name)
except Exception as e: 
  catalog = w.catalogs.create(catalog_name)

schema: SchemaInfo
try:
  schema = w.schemas.get(f"{catalog.full_name}.{schema_name}")
except Exception as e:
  schema = w.schemas.create(schema_name, catalog.full_name)
  
volume: VolumeInfo
try:
  volume = w.volumes.read(f"{catalog.full_name}.{schema_name}.{volume_name}")
except Exception as e:
  volume = w.volumes.create(catalog.full_name, schema.name, volume_name, VolumeType.MANAGED)

for user in users:
  user: str
  w.grants.update(
    full_name=catalog.full_name,
    securable_type=SecurableType.CATALOG,
    changes=[
      PermissionsChange(add=[Privilege.ALL_PRIVILEGES], principal=user)
    ])
  
spark.sql(f"USE {schema.full_name}")

current_user: str = w.current_user.me().user_name
print(current_user)

# COMMAND ----------

incoming_path: Path = volume.as_path() / "incoming"
w.files.create_directory(incoming_path)

config_path: Path = volume.as_path() / "config"
w.files.create_directory(config_path)

data_quality_path: Path = config_path / "dqe"
w.files.create_directory(data_quality_path)

ddl_path: Path = config_path / "ddl"
w.files.create_directory(ddl_path)

transformations_path: Path = config_path / "transformations"
w.files.create_directory(transformations_path)

# COMMAND ----------

import urllib.request

download_url: str = "https://gis-calema.opendata.arcgis.com/datasets/527151eb840d46f3aa5dcfa5ac13d561_0.csv?outSR=%7B%22latestWkid%22%3A3857%2C%22wkid%22%3A102100%7D"

csv_path: Path = incoming_path / "flood_data.csv"

urllib.request.urlretrieve(download_url, csv_path)

# COMMAND ----------

from pyspark.sql import DataFrame

df: DataFrame = spark.read.csv(incoming_path.as_posix(), header=True, inferSchema=True)
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Optionally define schema for source. 
# MAGIC

# COMMAND ----------

bronze_ddl_config_path: Path = ddl_path / "flood_data.ddl"
print(bronze_ddl_config_path)

# COMMAND ----------

ddl: List[str] = []
for c in df.schema:
  ddl.append(f"{c.name}: {c.dataType.simpleString()}")

ddl = ", ".join(ddl)
print(ddl)


with open(bronze_ddl_config_path, 'w') as fout:
    fout.write(ddl)

# COMMAND ----------

bronze_expectations_config_path: Path = data_quality_path / "bronze_data_quality_expectations.json"
print(bronze_expectations_config_path)

silver_expectations_config_path: Path = data_quality_path / "silver_data_quality_expectations.json"
print(silver_expectations_config_path)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Optionally define expectations for bronze and silver layers

# COMMAND ----------

import json

bronze_expections = {

}

silver_expections = {
    "expect_or_drop": {
        "valid_object_id": "OBJECTID IS NOT NULL"
    }
}

with open(bronze_expectations_config_path, 'w') as fout:
    json.dump(bronze_expections, fout, indent=4)


with open(silver_expectations_config_path, 'w') as fout:
    json.dump(silver_expections, fout, indent=4)

# COMMAND ----------

silver_transformations_config_path: Path = transformations_path / "silver_transformations.json"
print(silver_transformations_config_path)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Optionally define transformations for silver layer

# COMMAND ----------

import json

# In this example select_exp('column_name') is ultimately a no-op
silver_transformations = [
    {
    "target_table": "flood_data_silver",
    "select_exp": [
      'OBJECTID',
      'DFIRM_ID',
      'VERSION_ID',
      'FLD_AR_ID',
      'STUDY_TYP',
      'FLD_ZONE',
      'ZONE_SUBTY',
      'SFHA_TF',
      'STATIC_BFE',
      'V_DATUM',
      'DEPTH',
      'LEN_UNIT',
      'VELOCITY',
      'VEL_UNIT',
      'DUAL_ZONE',
      'SOURCE_CIT',
      'GFID',
      'esri_symbology',
      'GlobalID',
      'SHAPE_Length',
      'SHAPE_Area'
    ]
  },
]

with open(silver_transformations_config_path, 'w') as fout:
    json.dump(silver_transformations, fout, indent=4)

# COMMAND ----------

onboarding_config_path: Path = config_path / "onboarding_config.json"
print(onboarding_config_path)

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ## Required - Write onboarding config file

# COMMAND ----------

onboarding_config = [
   {
      "data_flow_id": "100",
      "data_flow_group": "A1",
      "source_system": "mysql",
      "source_format": "cloudFiles",
      "source_details": {
         "source_path_dev": incoming_path.as_posix()
      },
      "bronze_database_dev": schema.full_name,
      "bronze_table": "bronze_flood_data",
      "bronze_reader_options": {
         "cloudFiles.format": "csv",
         "cloudFiles.rescuedDataColumn": "_rescued_data",
         "header": "true"
      },
      "silver_database_dev": f"{schema.full_name}",
      "silver_table": "flood_data_silver",
      "silver_transformation_json_dev": silver_transformations_config_path.as_posix(),
      "silver_data_quality_expectations_json_dev": silver_expectations_config_path.as_posix(),
      "silver_cluster_by":["OBJECTID"],
  }
]

with open(onboarding_config_path, 'w') as fout:
    json.dump(onboarding_config, fout, indent=4)

# COMMAND ----------

# MAGIC %sh cat /Volumes/nfleming/sgws/data/config/onboarding_config.json
# MAGIC

# COMMAND ----------

from src.onboard_dataflowspec import OnboardDataflowspec

onboarding_params_map = {
	"database": schema.full_name,
	"onboarding_file_path": onboarding_config_path.as_posix(),
	"bronze_dataflowspec_table": "bronze_dataflowspec_table", 
	"silver_dataflowspec_table": "silver_dataflowspec_table",
	"overwrite": "True",
	"env": "dev",
	"version": "v1",
	"import_author": current_user
}

OnboardDataflowspec(spark, onboarding_params_map, uc_enabled=True).onboard_dataflow_specs()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Sanity Checks
# MAGIC
# MAGIC These are a couple of internal methods used by dlt-meta at runtime to extract data flows

# COMMAND ----------

from src.dataflow_spec import BronzeDataflowSpec, SilverDataflowSpec, DataflowSpecUtils
    
spark.conf.set("layer", "bronze")
spark.conf.set("bronze.dataflowspecTable", "nfleming.sgws.bronze_dataflowspec_table")
spark.conf.set("bronze.group", "A1")

DataflowSpecUtils.get_bronze_dataflow_spec(spark)

# COMMAND ----------

from src.dataflow_spec import BronzeDataflowSpec, SilverDataflowSpec, DataflowSpecUtils
    
spark.conf.set("layer", "silver")
spark.conf.set("silver.dataflowspecTable", "nfleming.sgws.silver_dataflowspec_table")
spark.conf.set("silver.group", "A1")

DataflowSpecUtils.get_silver_dataflow_spec(spark)