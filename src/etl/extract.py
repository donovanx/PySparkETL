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


    def fetch_api_data(self, param_name: str = None, parameters: int = None) -> list:
        """Fetches API data
        Args:
            param_name (str): name of the parameter to pass to the api
            parameters (list): list of parameter values e.g. [2011, 2012, 2013]
        Returns:
            list: list of data, so list of jsons [{},{}]
        """
        try:
            headers = {'x-apisports-key': self.api_key}

            # if parameters and param_name:
            #     params = [(param_name, p) for p in parameters]
            # else:
            #     params = None

            response = requests.get(self.url, headers=headers, params={param_name: parameters})
            response.raise_for_status()
            return response.json()["response"]

        except requests.exceptions.HTTPError as e:
            log_error(f"HTTP error fetching data from {self.url}: {e}")
        except requests.exceptions.RequestException as e:
            log_error(f"Request error fetching data from {self.url}: {e}")
        except KeyError:
            log_error(f"Unexpected response format from {self.url}")

        return []



    def first_extract_from_api(
        self,
        api_response: list,
        schema: StructType = None,
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
        #self.fetch_api_data(param_name=param_name, parameters=parameters)
        api_response = api_response

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

    def flatten_json(self, spark: SparkSession, array: list) -> DataFrame:
        """Flatten an array of data

        Args:
            spark (SparkSession): Spark session
            array (list): array of data in the following format : [[{}], [{}]]

        Returns:
            DataFrame: Returns Spark DataFrame.
        """
        try:
            cleaned_array = [x for x in array if x != []]
            flatten_array = [row for sub in cleaned_array for row in sub]

            return flatten_array
        except:
            log_error("Error creating dataframe from array.")

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




