import polars as pl

def average_temp(df) -> pl.DataFrame:
    df = df.group_by(["Date", "Hour", "LocationId"], maintain_order = True).agg(pl.col("Value").mean().alias("Temperatura Promedio"))

    return df