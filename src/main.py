from utils.spark import getSpark
from etl.extract import ETL_Raw
from utils.log import log_error
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

# ("COMPETITIONS", API_URL_COMPETITIONS),
#         ("TIMEZONE", API_URL_TIMEZONE),
#         ("SEASONS", API_URL_SEASONS),
#         ("CIRCUITS", API_URL_CIRCUITS),
#         ("TEAMS", API_URL_TEAMS),


if __name__ == "__main__":
    api_endpoints = [
        ("DRIVERS", API_URL_DRIVERS),
        ("RACES", API_URL_RACES),
        ("RANKINGS_TEAMS", API_URL_RANKINGS_TEAMS),
        ("RANKINGS_DRIVERS", API_URL_RANKINGS_DRIVERS),
        ("RANKINGS_RACES", API_URL_RANKINGS_RACES),
        ("RANKINGS_FASTLAPS", API_URL_RANKINGS_FASTLAPS),
        ("RANKINGS_STARTGRID", API_URL_RANKINGS_STARTGRID),
        ("PITSTOPS", API_URL_PITSTOPS)
    ]
    spark = getSpark()
    try:
        for table_name, endpoint in api_endpoints:
            etl_class = ETL_Raw(API_KEY, endpoint, spark, table_name.lower())
            df_competition = etl_class.first_extract_from_api()
            etl_class.write_to_raw(df_competition)

    except ValueError as e:
        log_error(e)

    finally:
        spark.stop()






