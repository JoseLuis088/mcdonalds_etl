import polars as pl
import dagster as dg

from adlfs import AzureBlobFileSystem
from datetime import date, timedelta
from dotenv import dotenv_values

config = dotenv_values(".env")

@dg.asset(
    group_name = "Ingestion"
)
def sensor_reads() -> pl.DataFrame:
    """
        Raw data of all of the reads
    """

    yesterday = date.today() - timedelta(days = 1)

    fs = AzureBlobFileSystem(account_name = config["ACCOUNT"], account_key = config["KEY"])

    all_files = fs.glob(f"{config['CONTAINER']}/reads/{yesterday.strftime('%Y/%-m/%-d')}/*.parquet")

    df = pl.read_parquet(f"abfs://{all_files[0]}", storage_options = {"account_name": config["ACCOUNT"], "account_key": config["KEY"]})

    for file in all_files[1:]:
        df_2 = pl.read_parquet(f"abfs://{file}", storage_options = {"account_name": config["ACCOUNT"], "account_key": config["KEY"]})
        df = df.extend(df_2)

    df = df.sort(by = pl.col("LocalTimeSpan"))

    df = df.select(
        pl.col("SensorId").str.to_uppercase(),
        pl.col("Value"),
        pl.col("LocalTimeSpan")
    )

    return df