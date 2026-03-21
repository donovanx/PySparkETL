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



def read_raw(path: str) -> DataFrame:
    spark = getSpark()
    df = spark.read.parquet(path)
    return df
