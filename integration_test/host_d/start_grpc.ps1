$env:DAGSTER_HOME = "C:\dagster-test"
$env:DAGSTER_POSTGRES_USER = "dagster"
$env:DAGSTER_POSTGRES_PASSWORD = "dagster_password"
$env:DAGSTER_POSTGRES_HOST = "<HOST_A_IP>"
$env:DAGSTER_POSTGRES_DB = "dagster"
$env:DAGSTER_POSTGRES_PORT = "5433"
Set-Location C:\dagster-test
uv run dagster code-server start -h 0.0.0.0 -p 4000 -m bare_process_test_location *> C:\dagster-test\grpc.log
