import pyodbc
import polars as pl
import dagster as dg

from dotenv import dotenv_values

config = dotenv_values(".env")

PYODBC = f"DRIVER={config['DRIVER']};SERVER={config['SERVER']};DATABASE={config['DATABASE']};UID={config['USERNAME']};PWD={config['PASSWORD']}"

@dg.asset(
    group_name = "Ingestion"
)
def sensor_catalog() -> pl.DataFrame:
    """
        Catalog of all of the Sensors in SQL Server
    """

    with pyodbc.connect(PYODBC) as connection:
        query = """
            SELECT
                a.LocationId AS LocationId,
                b.SubLocationId AS SubLocationId,
                c.DeviceId AS DeviceId,
                c.DeviceTyId AS DeviceTyId,
                c.Name AS DeviceName,
                d.SensorId AS SensorId,
                d.SensorTyId AS SensorTyId,
                d.Name AS SensorName
            FROM Locations AS a
            LEFT JOIN SubLocations AS b ON a.LocationId = b.LocationId
            LEFT JOIN Devices AS c ON b.SubLocationId = c.SubLocationId
            LEFT JOIN Sensors AS d
                ON c.DeviceId = d.DeviceId
                AND (
                    LOWER(c.Name) NOT LIKE 'cuarto%'
                    OR LOWER(d.Name) = 'temperatura'
                    OR LOWER(d.Name) LIKE 'Resistencia%'
                )
            WHERE
                c.DeviceTyId IN (
                    'c48a1d4f-664c-4863-2555-08dd462f6df2',
                    '8cf77da4-e964-4719-82da-08dc4394787a',
                    'af0132f0-3eb7-4f52-593f-08dc84ac4ec2',
                    '9eb73a3f-2385-41ab-2554-08dd462f6df2',
                    '38308792-2617-400D-8914-08DC484D6E84',
                    'F2259CBC-7268-4388-8913-08DC484D6E84',
                    '334afc4e-08a7-491e-8915-08dc484d6e84'
                )
            ORDER BY a.Street ASC;
        """

    information = pl.read_database(query, connection)
    return information