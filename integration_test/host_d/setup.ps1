# setup.ps1 — Host D (bare-process host) setup script
#
# Run this on the bare-process host to set up the dagster code location.
# Prerequisites: uv installed, network access to Host A

$ErrorActionPreference = "Stop"

$DAGSTER_TEST_DIR = "C:\dagster-test"

# Create project directory
if (-not (Test-Path $DAGSTER_TEST_DIR)) {
    New-Item -ItemType Directory -Path $DAGSTER_TEST_DIR -Force
}
Set-Location $DAGSTER_TEST_DIR

# Initialize uv project if not already done
if (-not (Test-Path "$DAGSTER_TEST_DIR\pyproject.toml")) {
    uv init --no-readme .
    uv add dagster==1.12.14 dagster-postgres==0.28.14
}

# Ensure bare_process_test_location exists
if (-not (Test-Path "$DAGSTER_TEST_DIR\bare_process_test_location\__init__.py")) {
    Write-Error "bare_process_test_location/__init__.py not found. Copy it from Host A first."
    exit 1
}

# Ensure dagster.yaml exists
if (-not (Test-Path "$DAGSTER_TEST_DIR\dagster.yaml")) {
    Write-Error "dagster.yaml not found. Copy it from Host A first."
    exit 1
}

# Open firewall port 4000 if not already open
$rule = Get-NetFirewallRule -DisplayName "Dagster gRPC" -ErrorAction SilentlyContinue
if (-not $rule) {
    Write-Host "Adding firewall rule for port 4000..."
    New-NetFirewallRule -DisplayName "Dagster gRPC" -Direction Inbound -Protocol TCP -LocalPort 4000 -Action Allow
}

# Set DAGSTER_HOME and start the gRPC server
$env:DAGSTER_HOME = $DAGSTER_TEST_DIR
Write-Host "Starting Dagster gRPC server on 0.0.0.0:4000..."
uv run dagster code-server start -h 0.0.0.0 -p 4000 -m bare_process_test_location
