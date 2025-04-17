import dagster as dg

from mcdonalds_etl.jobs import mcdonalds_update_job

mcdonalds_update_schedule = dg.ScheduleDefinition(
    job = mcdonalds_update_job,
    cron_schedule = "30 12 * * *"
)