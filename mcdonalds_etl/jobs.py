import dagster as dg

mcdonalds_update_job = dg.define_asset_job(
    name = "mcdonalds_update_job",
    selection = dg.AssetSelection.all()
)