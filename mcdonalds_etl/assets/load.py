import polars as pl
import dagster as dg

from mcdonalds_etl.resources import SqlConnection

@dg.asset(
    group_name = "Load",
    ins = {
        "soda": dg.AssetIn(key = ["soda"])
    }
)
def soda_sql(soda: pl.DataFrame, database: SqlConnection) -> None:
    """
        Soda Dataframe loaded to SQL Server
    """

    print(soda.schema)
    soda.write_database("factOperacionSoda", database.connection_string(), if_table_exists = "append")

@dg.asset(
    group_name = "Load",
    ins = {
        "ice_cream": dg.AssetIn(key = ["ice_cream"])
    }
)
def ice_cream_sql(ice_cream: pl.DataFrame, database: SqlConnection) -> None:
    """
        Ice cream Dataframe loaded to SQL Server
    """

    print(ice_cream.schema)
    ice_cream.write_database("factOperacionNieve", database.connection_string(), if_table_exists = "append")

@dg.asset(
    group_name = "Load",
    ins = {
        "freezing": dg.AssetIn(key = ["freezing"])
    }
)
def freezing_sql(freezing: pl.DataFrame, database: SqlConnection) -> None:
    """
        Freezing Dataframe loaded to SQL Server
    """

    print(freezing.schema)
    freezing.write_database("factTempCongelacion", database.connection_string(), if_table_exists = "append")

@dg.asset(
    group_name = "Load",
    ins = {
        "conservation": dg.AssetIn(key = ["conservation"])
    }
)
def conservation_sql(conservation: pl.DataFrame, database: SqlConnection) -> None:
    """
        Conservation Dataframe loaded to SQL Server
    """

    print(conservation.schema)
    conservation.write_database("factTempConservacion", database.connection_string(), if_table_exists = "append")

@dg.asset(
    group_name = "Load",
    ins = {
        "defrost_resistance": dg.AssetIn(key = ["defrost_resistance"])
    }
)
def defrost_resistance_sql(defrost_resistance: pl.DataFrame, database: SqlConnection) -> None:
    """
        Defrost Resistance Dataframe loaded to SQL Server
    """

    print(defrost_resistance.schema)
    defrost_resistance.write_database("factResistencia", database.connection_string(), if_table_exists = "append")