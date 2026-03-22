import os
import sys
from pyspark.sql import DataFrame
from pyspark.sql.functions import col
import pandas as pd
from pathlib import Path
sys.path.insert(1, 'src/utils')
from spark import getSpark
from log import log_error
import json

def get_seasons_from_raw(spark, path):
    df = spark.read.format("parquet").load(path)
    df = df.withColumnRenamed("_corrupt_record", "seasons")
    return [row["seasons"] for row in df.select("seasons").collect()]

def get_races_from_raw(spark, path):
    df = spark.read.format("parquet").load(path)
    return [row["id"] for row in df.select("id").collect()]



def read_raw(path: str) -> DataFrame:
    spark = getSpark()
    df = spark.read.parquet(path)
    return df


