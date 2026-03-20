from pyspark.sql import SparkSession
import json
from pathlib import Path
from log import log_info, log_error

with open(r"/Users/donovan/Documents/PySparkETL/configs/job_config.json") as f:
    data = json.load(f)


def getSpark():
    try:
        spark = SparkSession.builder \
            .appName(data["job"]["app_name"]) \
            .master(data["job"]["master"]) \
            .config("spark.executor.memory", data["job"]["spark.executor.memory"]) \
            .config("spark.driver.memory", data["job"]["spark.driver.memory"]) \
            .config("spark.sql.shuffle.partitions",  data["job"]["spark.sql.shuffle.partitions"]) \
            .getOrCreate()

        log_info("SparkSession created.")
        return spark

    except Exception as e:
        log_error(f"Error starting Spark session: {e}")



getSpark()