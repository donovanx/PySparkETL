from utils.spark import getSpark
from etl.extract import ETL_Raw
from utils.log import log_error
from etl.load import get_races_from_raw, get_seasons_from_raw
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
            df = etl_class.first_extract_from_api()
            etl_class.write_to_raw(df)

        seasons = get_seasons_from_raw(spark, "./data/raw/seasons_raw")  # e.g. [2011, 2012, 2013]
        season_params = [('season', s) for s in seasons]

        season_dependent = [
            ("RACES", API_URL_RACES),
            ("RANKINGS_TEAMS", API_URL_RANKINGS_TEAMS),
            ("RANKINGS_DRIVERS", API_URL_RANKINGS_DRIVERS),
        ]

        for table_name, endpoint in season_dependent:
            etl_class = ETL_Raw(API_KEY, endpoint, spark, table_name.lower(), season_params)
            df = etl_class.first_extract_from_api(param_name="season", parameters=season_params)
            etl_class.write_to_raw(df)

        races = get_races_from_raw(spark, "./data/raw/races_raw")  # e.g. [101, 102, 103]
        race_params = [('race', r) for r in races]

        race_dependent = [
            ("RANKINGS_RACES", API_URL_RANKINGS_RACES),
            ("RANKINGS_FASTLAPS", API_URL_RANKINGS_FASTLAPS),
            ("RANKINGS_STARTGRID", API_URL_RANKINGS_STARTGRID),
            ("PITSTOPS", API_URL_PITSTOPS),
            ("DRIVERS", API_URL_DRIVERS),
        ]

        for table_name, endpoint in race_dependent:
            etl_class = ETL_Raw(API_KEY, endpoint, spark, table_name.lower(), race_params)
            df = etl_class.first_extract_from_api(param_name="races", parameters=race_params)
            etl_class.write_to_raw(df)

    except ValueError as e:
        log_error(e)
    finally:
        spark.stop()






