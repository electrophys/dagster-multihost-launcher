from dagster_multihost_launcher import build_admin_definitions

defs = build_admin_definitions(
    cron_schedule="*/5 * * * *",
    cleanup_max_age_hours=1.0 / 60,  # 1 minute
)
