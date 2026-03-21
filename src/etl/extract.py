import requests
from dotenv import load_dotenv
import os
import sys
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col
from pyspark.sql.types import StructType
import pandas as pd
from pathlib import Path
sys.path.insert(1, 'src/utils')
from spark import getSpark
from log import log_error
import json
from datetime import datetime

class ETL_Raw():

    def __init__(self, api: str, url: str, spark: SparkSession, table_name: str):
        self.api_key = api
        self.url = url
        self.spark = spark
        self.table_name = table_name


    def fetch_api_data(self) -> list:
        try:
            """Fetches API data

            Args:
                url (str): url of the api
                api_key (str): api key needed to make requests

            Returns:
                list: list of data, so list of jsons [{},{}]
            """
            source = self.url
            payload={}
            headers = {
            'x-apisports-key': self.api_key,
            }

            response = requests.request("GET", source, headers=headers, data=payload)

            return response.json()["response"]

        except:
            log_error("Error fetching api data.")



    def first_extract_from_api(
        self,
        schema: StructType = None
    ) -> DataFrame:
        """
        Creates a Spark DataFrame from an API response list.

        Args:
            api_response: List of JSON objects from the API response
            spark: Active SparkSession
            schema: Optional StructType schema — recommended for consistency

        Returns:
            DataFrame with raw API data

        Raises:
            ValueError: If api_response is empty or not a list
        """

        api_response = self.fetch_api_data()

        if not isinstance(api_response, list):
            raise ValueError(f"api_response must be a list, got {type(api_response)}")

        if not api_response:
            raise ValueError("api_response is empty")

        try:
            if schema:
                df = self.spark.createDataFrame(api_response, schema=schema)
            else:
                json_rdd = self.spark.sparkContext.parallelize(
                    [json.dumps(row) for row in api_response]
                )
                df = self.spark.read.json(json_rdd)

            return df

        except Exception as e:
            raise RuntimeError(f"Failed to create DataFrame from API response: {e}")

    def write_to_raw(self, df: DataFrame, base_path: str = None) -> None:
        """
        Write a DataFrame to the raw layer in parquet format.

        Args:
            df: PySpark DataFrame to write
            table_name: Name of the table/entity
            base_path: Base path for raw storage (defaults to cwd/data/raw)
        """

        base_path = base_path or Path.cwd() / "data" / "raw"
        timestamp = datetime.now().strftime("%Y/%m/%d")
        output_path = f"{base_path}/{self.table_name}_raw"

        (df.write
            .format("parquet")
            .mode("append")
            .option("compression", "snappy")
            .save(output_path)
        )

        df.cache()

        print(f"Written {df.count()} rows to {output_path}")




