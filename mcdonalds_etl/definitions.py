import dagster as dg

from mcdonalds_etl.assets import azure, sql_server, transformations, load
from mcdonalds_etl.resources import database_resource
from mcdonalds_etl.jobs import mcdonalds_update_job
from mcdonalds_etl.schedules import mcdonalds_update_schedule

azure_assets = dg.load_assets_from_modules([azure])
sql_assets = dg.load_assets_from_modules([sql_server])
transformation_assets = dg.load_assets_from_modules([transformations])
load_assets = dg.load_assets_from_modules([load])

all_jobs = [mcdonalds_update_job]
all_schedules = [mcdonalds_update_schedule]

defs = dg.Definitions(
    assets = [*azure_assets, *sql_assets, *transformation_assets, *load_assets],
    resources = {
        "database": database_resource
    },
    jobs = all_jobs,
    schedules = all_schedules
)