import dagster as dg


taxi_pipeline_job = dg.define_asset_job(
    name="taxi_pipeline_daily",
    selection=dg.AssetSelection.all(),
    description="Full refresh: ingest taxi data, build staging view, build marts table",
)


daily_schedule = dg.ScheduleDefinition(
    name="taxi_pipeline_daily_schedule",
    job=taxi_pipeline_job,
    cron_schedule="0 6 * * *",
)