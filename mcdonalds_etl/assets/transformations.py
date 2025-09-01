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
        Hace el join entre el DataFrame de las lecturas de Azure y el DataFrame con el catalogo de sensores, solamente se usan los siguientes SensorName:

        - Temperatura
        - Fase 1
        - Fase 2
        - Fase 3
        - Presion agua carbonatada (con todas sus variantes, con acentos, etc. En la linea 35 esta la lista)
        - Temperatura congelacion 1
        - Temperatura conservacion
        - Corriente Resistencia Deshielo
        - Resistencia Deshielo
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

    return all_data

@dg.asset(
    deps = [reads_per_sensor],
    group_name = "Transformation"
)
def soda(reads_per_sensor: pl.DataFrame) -> pl.DataFrame:
    """
        Lecturas para la maquina de soda en un horario de 7 de la manana a 23 horas, agregando columnas para ver si los valores se encuentran en rango de operacion.

        Para Fase 1, Fase 2 y Fase 3 los valores en rango son de 2 a 11 y para Presion agua carbonatada es de 20 a 40

        Al final este cuenta el numero de horas que la maquina estuvo funcionando correctamente 
    """
    #df = df.filter(pl.col("LocalTimeSpan").cast(pl.Time).is_between(pl.time(7, 0, 0, 0), pl.time(23, 0, 0, 0), closed = "both"))
    soda = reads_per_sensor.filter(
        pl.col("DeviceTyId") == "AF0132F0-3EB7-4F52-593F-08DC84AC4EC2",
        pl.col("LocalTimeSpan").cast(pl.Time).is_between(pl.time(7, 0, 0, 0), pl.time(23, 0, 0, 0), closed = "both")
    )

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

    return soda

@dg.asset(
    deps = [reads_per_sensor],
    group_name = "Transformation"
)
def ice_cream(reads_per_sensor: pl.DataFrame) -> pl.DataFrame:
    """
        Lecturas para la maquina de nieve, igualmente en un horario de 7 a 23 horas.

        Los valores operativos son mayores a 3.5.

        Regresa el numero de horas que estuvo funcionando en rango y el porcentaje de funcionamiento.
    """

    ice_cream = reads_per_sensor.filter(
        pl.col("DeviceTyId") == "8CF77DA4-E964-4719-82DA-08DC4394787A",
        pl.col("LocalTimeSpan").cast(pl.Time).is_between(pl.time(7, 0, 0, 0), pl.time(23, 0, 0, 0), closed = "both")
    )

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

    return ice_cream

@dg.asset(
    deps = [reads_per_sensor],
    group_name = "Transformation"
)
def freezing(reads_per_sensor: pl.DataFrame) -> pl.DataFrame:
    """
       Este asset retorna un DataFrame que contiene la temperatura promedio, para hacer esta transformacion usa una funcion que se encuentra en el archivo constants.py, esa misma funcion
       ya se encuentra documentada. 
    """

    freezing = reads_per_sensor.filter(
        pl.col("DeviceTyId").is_in(["C48A1D4F-664C-4863-2555-08DD462F6DF2", "38308792-2617-400D-8914-08DC484D6E84"]),
        pl.col("SensorName") != "Resistencia deshielo"
    )
    freezing = constants.average_temp(freezing)

    freezing = freezing.select(
        pl.col("Date"),
        pl.col("Hour"),
        pl.col("LocationId"),
        pl.col("Temperatura Promedio")
    )

    return freezing

@dg.asset(
    deps = [reads_per_sensor],
    group_name = "Transformation"
)
def conservation(reads_per_sensor: pl.DataFrame) -> pl.DataFrame:
    """
        Este asset retorna un DataFrame que contiene la temperatura promedio, para hacer esta transformacion usa una funcion que se encuentra en el archivo constants.py, esa misma funcion
        ya se encuentra documentada.
    """

    conservation = reads_per_sensor.filter(pl.col("DeviceTyId").is_in(["9EB73A3F-2385-41AB-2554-08DD462F6DF2", "F2259CBC-7268-4388-8913-08DC484D6E84"]))
    conservation = constants.average_temp(conservation)

    conservation = conservation.select(
        pl.col("Date"),
        pl.col("Hour"),
        pl.col("LocationId"),
        pl.col("Temperatura Promedio")
    )

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
        pl.col("LocalTimeSpan"),
        pl.col("Date").alias("Fecha"),
        pl.col("Hour").alias("Hora"),
        pl.col("LocationId"),
        pl.col("DeviceName"),
        pl.col("SensorName"),
        pl.col("Value")
    )

    return defrost_resistance