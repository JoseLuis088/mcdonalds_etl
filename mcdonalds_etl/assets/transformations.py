import polars as pl
import dagster as dg

from mcdonalds_etl.assets import constants

@dg.asset(
    group_name = "Transformation",
    ins = {
        "sensor_reads": dg.AssetIn(key = ["sensor_reads"]),
        "sensor_catalog": dg.AssetIn(key = ["sensor_catalog"])
    }
)
def reads_per_sensor(sensor_reads: pl.DataFrame, sensor_catalog: pl.DataFrame) -> pl.DataFrame:
    """
        The sensors values joined with the sensors catalog
    """

    all_data = sensor_reads.join(sensor_catalog, on = "SensorId", how = "inner")
    all_data = all_data.filter(pl.col("SensorName").is_in(["Temperatura", "Fase 1", "Fase 2", "Fase 3", "Presión agua carbonatada", "Presión Agua Carbonatada", "Temperatura congelación 1", "Temperatura conservacion", "Corriente resistencia deshielo", "Resistencia deshielo"]))

    all_data = all_data.select(
        pl.col("SensorId"),
        pl.col("SensorTyId"),
        pl.col("LocalTimeSpan"),
        pl.col("LocalTimeSpan").cast(pl.Date).alias("Date"),
        pl.col("LocalTimeSpan").dt.truncate("1h").cast(pl.Time).alias("Hour"),
        pl.col("LocationId"),
        pl.col("SubLocationId"),
        pl.col("DeviceId"),
        pl.col("DeviceTyId"),
        pl.col("DeviceName"),
        pl.col("SensorName"),
        pl.col("Value")
    )

    all_data.write_csv("data/all_data.csv")

    return all_data

@dg.asset(
    deps = [reads_per_sensor],
    group_name = "Transformation"
)
def soda(reads_per_sensor: pl.DataFrame) -> pl.DataFrame:
    """
        Reads for the soda machine
    """

    soda = reads_per_sensor.filter(pl.col("DeviceTyId") == "AF0132F0-3EB7-4F52-593F-08DC84AC4EC2")

    soda = soda.with_columns(
        pl.when(
            pl.col("SensorName").is_in(["Fase 1", "Fase 2", "Fase 3"]),
            pl.col("Value").is_between(2, 11, closed = "both")
        )
        .then(1)
        .when(
            pl.col("SensorName").is_in(["Presión agua carbonatada", "Presión Agua Carbonatada"]),
            pl.col("Value").is_between(20, 40, closed = "both")
        )
        .then(1)
        .otherwise(0)
        .alias("Operacion")
    )

    soda = soda.group_by(["Date", "Hour", "LocationId", "Operacion"], maintain_order = True).count()

    soda = soda.pivot(on = "Operacion", index = ["Date", "Hour", "LocationId"], values = "count", aggregate_function = "first")

    soda = soda.fill_null(0)

    print(soda)

    soda = soda.select(
        pl.col("Date").alias("Fecha"),
        pl.col("Hour").alias("Hora"),
        pl.col("LocationId"),
        pl.col("1").alias("En Rango"),
        pl.col("0").alias("Fuera De Rango"),
        (pl.col("1") / (pl.col("1") + pl.col("0"))).alias("Funcionamiento")
    )

    print(soda)

    soda.write_csv("data/soda.csv")

    return soda

@dg.asset(
    deps = [reads_per_sensor],
    group_name = "Transformation"
)
def ice_cream(reads_per_sensor: pl.DataFrame) -> pl.DataFrame:
    """
        Read fot the ice cream machine
    """

    ice_cream = reads_per_sensor.filter(pl.col("DeviceTyId") == "8CF77DA4-E964-4719-82DA-08DC4394787A")

    ice_cream = ice_cream.with_columns(
        pl.when(
            pl.col("Value") > 3.5
        )
        .then(1)
        .otherwise(0)
        .alias("Operacion")
    )

    ice_cream = ice_cream.group_by(["Date", "Hour", "LocationId", "Operacion"], maintain_order = True).count()

    ice_cream = ice_cream.pivot(on = "Operacion", index = ["Date", "Hour", "LocationId"], values = "count", aggregate_function = "first")

    ice_cream = ice_cream.fill_null(0)

    ice_cream = ice_cream.select(
        pl.col("Date").alias("Fecha"), 
        pl.col("Hour").alias("Hora"),
        pl.col("LocationId"),
        pl.col("1").alias("En Rango"),
        pl.col("0").alias("Fuera De Rango"),
        (pl.col("1") / (pl.col("1") + pl.col("0"))).alias("Funcionamiento")
    )

    ice_cream.write_csv("data/ice_cream.csv")

    return ice_cream

@dg.asset(
    deps = [reads_per_sensor],
    group_name = "Transformation"
)
def freezing(reads_per_sensor: pl.DataFrame) -> pl.DataFrame:
    """
        Reads of the freezing with his percentage of functionality
    """

    freezing = reads_per_sensor.filter(
        pl.col("DeviceTyId").is_in(["C48A1D4F-664C-4863-2555-08DD462F6DF2", "38308792-2617-400D-8914-08DC484D6E84"]),
        pl.col("SensorName") != "Resistencia deshielo"
    )
    freezing = constants.average_temp(freezing)

    freezing = freezing.select(
        pl.col("Date").alias("Fecha"),
        pl.col("Hour").alias("Hora"),
        pl.col("LocationId"),
        pl.col("Temperatura Promedio")
    )

    freezing.write_csv("data/freezing.csv")

    return freezing

@dg.asset(
    deps = [reads_per_sensor],
    group_name = "Transformation"
)
def conservation(reads_per_sensor: pl.DataFrame) -> pl.DataFrame:
    """
        Reads of the conservation with his percentage of funcionality
    """

    conservation = reads_per_sensor.filter(pl.col("DeviceTyId").is_in(["9EB73A3F-2385-41AB-2554-08DD462F6DF2", "F2259CBC-7268-4388-8913-08DC484D6E84"]))
    conservation = constants.average_temp(conservation)

    conservation = conservation.select(
        pl.col("Date").alias("Fecha"),
        pl.col("Hour").alias("Hora"),
        pl.col("LocationId"),
        pl.col("Temperatura Promedio")
    )

    conservation.write_csv("data/conservation.csv")

    return conservation

@dg.asset(
    deps = [reads_per_sensor],
    group_name = "Transformation"
)
def defrost_resistance(reads_per_sensor: pl.DataFrame) -> pl.DataFrame:
    """
        Reads of the defrost resistance
    """

    defrost_resistance = reads_per_sensor.filter(
        pl.col("DeviceName").is_in(["Resistencia de deshielo 1", "Cuarto de congelación"]),
        pl.col("SensorName").is_in(["Resistencia deshielo", "Corriente resistencia deshielo"])
    )

    defrost_resistance = defrost_resistance.select(
        pl.col("Date").alias("Fecha"),
        pl.col("Hour").alias("Hora"),
        pl.col("LocationId"),
        pl.col("DeviceName"),
        pl.col("SensorName"),
        pl.col("Value")
    )

    defrost_resistance.write_csv("data/defrost_resistance.csv")

    return defrost_resistance