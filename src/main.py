from utils.spark import getSpark
from etl.extract import ETL_Raw
from utils.log import log_error
from etl.load import get_races_from_raw, get_seasons_from_raw
from functools import reduce
from pyspark.sql import DataFrame, functions as F
from config import (
    API_URL_COMPETITIONS,
    API_URL_STATUS,
    API_URL_TIMEZONE,
    API_URL_SEASONS,
    API_URL_CIRCUITS,
    API_URL_TEAMS,
    API_URL_DRIVERS,
    API_URL_RACES,
    API_URL_RANKINGS_TEAMS,
    API_URL_RANKINGS_DRIVERS,
    API_URL_RANKINGS_RACES,
    API_URL_RANKINGS_FASTLAPS,
    API_URL_RANKINGS_STARTGRID,
    API_URL_PITSTOPS,
    API_KEY,
)


def union_dataframe(dfs: DataFrame, names: str) -> DataFrame:
    all_dfs = [df for name, df in dfs.items() if name.startswith(names)]
    df = reduce(DataFrame.union, all_dfs)

    df.write.csv("test.csv")

    return df

def extract_flatten_and_write_json(df: list, dependent_list: list) -> None:

    main_dict = {}
    for param in df:
        for table_name, endpoint in dependent_list:
            try:
                etl_class = ETL_Raw(API_KEY, endpoint, spark, table_name.lower())
                api_response = etl_class.fetch_api_data(param_name="season", parameters=param)
                df = etl_class.first_extract_from_api(etl_class.flatten_json(spark, api_response))
                main_dict[table_name+"_"+param] = df
            except:
                continue

    for key in dict(dependent_list).keys():
        df = union_dataframe(main_dict, key)
        etl_class.write_to_raw(df, table_name.lower())





if __name__ == "__main__":
    spark = getSpark()
    try:
        independent_endpoints = [
            ("COMPETITIONS", API_URL_COMPETITIONS, None),
            ("TIMEZONE", API_URL_TIMEZONE, None),
            ("SEASONS", API_URL_SEASONS, None),
            ("CIRCUITS", API_URL_CIRCUITS, None),
            ("TEAMS", API_URL_TEAMS, None),
        ]

        for table_name, endpoint, params in independent_endpoints:
            etl_class = ETL_Raw(API_KEY, endpoint, spark, table_name.lower())
            df = etl_class.first_extract_from_api(etl_class.fetch_api_data())
            etl_class.write_to_raw(df)

        seasons = get_seasons_from_raw(spark, "./data/raw/seasons_raw")
        #['2012', '2015', '2016', '2021', '2022', '2019', '2020', '2017', '2018', '2023', '2024', '2013', '2014', '2025', '2026']

        season_dependent = [
            ("RACES", API_URL_RACES),
            ("RANKINGS_TEAMS", API_URL_RANKINGS_TEAMS),
            ("RANKINGS_DRIVERS", API_URL_RANKINGS_DRIVERS),
        ]
        extract_flatten_and_write_json(seasons, season_dependent)


        races = get_races_from_raw(spark, "./data/raw/races_raw")  # e.g. [101, 102, 103]
        race_dependent = [
            ("RANKINGS_RACES", API_URL_RANKINGS_RACES),
            ("RANKINGS_FASTLAPS", API_URL_RANKINGS_FASTLAPS),
            ("RANKINGS_STARTGRID", API_URL_RANKINGS_STARTGRID),
            ("PITSTOPS", API_URL_PITSTOPS),
            ("DRIVERS", API_URL_DRIVERS),
        ]
        extract_flatten_and_write_json(races, race_dependent)


    except ValueError as e:
        log_error(e)
    finally:
        spark.stop()






