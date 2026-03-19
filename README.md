# CF Scripts

Utilities for collecting Cloud Foundry and Ops Manager inventory data, orchestrating remote collection across foundations, and running a few focused networking or RabbitMQ checks.

## What is in this repo

### Foundation inventory

- `get_foundation_data.sh`
  Collects normalized foundation data from the CF API. In multi-table mode it produces:
  - `app_data.csv`
  - `service_data.csv`
  - `developer_space_data.csv`
  - `java_runtime_data.csv`
  - `audit_events.csv`
  - `service_bindings.csv`

- `get_foundation_data_gitbash.sh`
  Older Windows/Git Bash friendly collector. It emits a single CSV to stdout with app metadata, buildpack/runtime details, routes, services, and developers.

- `get_foundation_opsman_data.sh`
  Collects Ops Manager and BOSH inventory for a foundation. It writes `opsman_inventory.csv` and also downloads a per-foundation diagnostic report JSON.

### Focused CF reports

- `get-app-bindings-by-broker.sh`
  Exports service bindings for a specific service broker. Includes app bindings, service keys, and unbound service instances.

- `ls_c2c_apps2csv.sh`
  Lists container-to-container network policies as CSV.

- `ls_singleton_apps2csv.sh`
  Lists started apps with exactly one instance as CSV.

### Orchestration / remote execution

- `Run-EnvOrchestrator.ps1`
  PowerShell 7 orchestrator for uploading a collector to remote hosts, running it detached, polling status, downloading outputs, and cleaning up. It is designed to drive the Bash collectors above across multiple environments/foundations.

- `psscripts/sshshortcut.ps1`
  PowerShell helpers for defining reusable SSH/SCP shortcuts in-session.

### RabbitMQ connectivity checks

- `rabbithandshake.ps1`
  Minimal TCP/AMQP handshake check to a RabbitMQ host and port.

- `testrabbit.py`
  Low-level AMQP test script that attempts authentication, opens a channel, and passively checks queue existence.

## Prerequisites

### Common

- `cf` CLI
- `jq`

### Bash collectors

- `bash`
- `xargs`
- `flock`

### Ops Manager collector

- `om` CLI

### PowerShell tooling

- PowerShell 7+
- OpenSSH client (`ssh`, `scp`) on `PATH`

### Python script

- Python 3

## Required environment and auth

### CF-based scripts run in one of two modes

#### Interactive/local mode

These scripts expect an existing CF CLI session:

- `get_foundation_data.sh`
- `get_foundation_data_gitbash.sh`
- `get-app-bindings-by-broker.sh`
- `ls_c2c_apps2csv.sh`
- `ls_singleton_apps2csv.sh`

Typical local setup:

```bash
export CF_API=https://api.sys.example.com
cf login -a "$CF_API" -u "$CF_USERNAME" -p "$CF_PASSWORD"
```

Required in practice:

- `CF_API`
  Not strictly required by every script, but it should be set. `get_foundation_data.sh` uses it to derive a foundation slug when `CF_FOUNDATION` or `CF_ORCH_PLATFORM` is not provided.
- Active `cf login` session
  Required for all CF API calls made by the scripts above.

Optional but commonly useful:

- `CF_FOUNDATION`
- `CF_ORCH_PLATFORM`
- `ENV_LOCATION`
- `ENV_TYPE`
- `ENV_DATACENTER`

#### Orchestrated/remote mode

`Run-EnvOrchestrator.ps1` can perform the remote `cf login` for the collector if these are exported in the generated remote environment:

- `CF_API`
- `CF_USERNAME`
- `CF_PASSWORD`

Optional for remote login target selection:

- `CF_ORG`
  Defaults to `system`.
- `CF_SPACE`
  Defaults to `system`.

The orchestrator also exports:

- `BATCH_ID`
- `CF_ORCH_PLATFORM`
- `CF_ORCH_RUN_DIR`
- `CF_ORCH_OUT_DIR`
- `CF_ORCH_CACHE_ROOT`
- `CF_ORCH_DATA_OUT`

### Ops Manager collector requirements

`get_foundation_opsman_data.sh` requires:

- `OM_TARGET`
- Either `OM_USERNAME` and `OM_PASSWORD`
- Or `OM_CLIENT_ID` and `OM_CLIENT_SECRET`

Optional but recommended:

- `CF_ORCH_PLATFORM` or `CF_FOUNDATION`
- `CF_ORCH_DATA_MODE`
- `CF_ORCH_DATA_OUT`

## Typical usage

### 1. Collect CF foundation data locally

Log in with the CF CLI first, then run:

```bash
export CF_API=https://api.sys.example.com
cf login -a "$CF_API" -u "$CF_USERNAME" -p "$CF_PASSWORD"
./get_foundation_data.sh
```

Useful environment variables:

- `CF_API`
  Strongly recommended. Used to derive the foundation name when explicit foundation variables are not set.
- `CF_ORCH_DATA_MODE`
  `multi` by default. In `multi` mode, `CF_ORCH_DATA_OUT` is treated as a directory. In `single` mode, it is treated as a CSV file path.
- `CF_ORCH_DATA_OUT`
  Output base path. Default is `./foundation_data`.
- `CF_FOUNDATION`, `CF_ORCH_PLATFORM`
  Foundation name/slug used in output records.
- `ENV_LOCATION`, `ENV_TYPE`, `ENV_DATACENTER`
  Optional environment metadata added to the output.
- `KPI_FILTER_ENABLED`, `ORG_FILTER_ENABLED`
  Control audit-event filtering.
- `WORKERS`, `SPACE_DEV_WORKERS`, `BROKER_WORKERS`
  Concurrency tuning.

Example:

```bash
export CF_API=https://api.system.fd-prod-chd.example.com
export CF_ORCH_DATA_MODE=multi
export CF_ORCH_DATA_OUT=./out/fd-prod-chd
export CF_ORCH_PLATFORM=fd-prod-chd
export ENV_LOCATION=chicago
export ENV_TYPE=prod
./get_foundation_data.sh
```

### 2. Collect Ops Manager inventory

Authenticate either with username/password or client credentials:

```bash
export OM_TARGET=https://opsman.example.com
export OM_USERNAME=admin
export OM_PASSWORD='...'
export CF_ORCH_DATA_MODE=multi
export CF_ORCH_DATA_OUT=./out/fd-prod-chd
export CF_ORCH_PLATFORM=fd-prod-chd
./get_foundation_opsman_data.sh
```

This writes:

- `./out/fd-prod-chd/opsman_inventory.csv`
- `./out/fd-prod-chd/fd-prod-chd_diagnostic_report.json`

### 3. Export bindings for one broker

Requires an active `cf login` session:

```bash
export CF_API=https://api.sys.example.com
cf login -a "$CF_API" -u "$CF_USERNAME" -p "$CF_PASSWORD"
./get-app-bindings-by-broker.sh p.rabbitmq
./get-app-bindings-by-broker.sh p.mysql mysql
```

By default it writes CSV to stdout. When `CF_ORCH_DATA_OUT` is set, it appends to that file under a file lock.

### 4. Run the legacy Git Bash collector

Requires an active `cf login` session:

```bash
export CF_API=https://api.sys.example.com
cf login -a "$CF_API" -u "$CF_USERNAME" -p "$CF_PASSWORD"
./get_foundation_data_gitbash.sh > foundation_data.csv
```

Use this if you need a simpler single-CSV collector in Git Bash on Windows. It is more sequential and less feature-rich than `get_foundation_data.sh`.

### 5. Run point-in-time reports

```bash
export CF_API=https://api.sys.example.com
cf login -a "$CF_API" -u "$CF_USERNAME" -p "$CF_PASSWORD"
./ls_c2c_apps2csv.sh > c2c_policies.csv
./ls_singleton_apps2csv.sh > singleton_apps.csv
```

### 6. Orchestrate collection remotely

`Run-EnvOrchestrator.ps1` expects a config file named `connect_env_config.ps1` by default:

```powershell
pwsh ./Run-EnvOrchestrator.ps1 -BatchSize 3 -QuickChecks 2 -QuickDelaySec 5 -PollIntervalSec 15
pwsh ./Run-EnvOrchestrator.ps1 -RunTag 20251016-003800 -Resume
pwsh ./Run-EnvOrchestrator.ps1 -NoRemoteCleanup
```

The orchestrator:

- uploads the collector and helper binaries to a remote host
- creates a per-run working directory
- exports variables such as `BATCH_ID`, `CF_ORCH_PLATFORM`, `CF_ORCH_OUT_DIR`, `CF_ORCH_CACHE_ROOT`, and `CF_ORCH_DATA_OUT`
- performs `cf login` on the remote side when `CF_API`, `CF_USERNAME`, and `CF_PASSWORD` are present
- launches the script detached with `nohup`
- polls status, downloads outputs, and optionally removes the remote workspace

Note: `connect_env_config.ps1` is not included in this repository, so you will need to provide it.

## SSH shortcut helpers

Load the helper file in PowerShell:

```powershell
. ./psscripts/sshshortcut.ps1
```

Then define and use shortcuts:

```powershell
New-SshShortcut -Name ABC -SSHHost host.example.com -User svc_cf -KeyPath ~/.ssh/id_rsa
ABC ls -la
ABC_scp C:\temp\file.txt :/tmp/file.txt
ezcmd
```

The helper creates paired SSH and SCP convenience commands for the session.

## RabbitMQ test scripts

### `rabbithandshake.ps1`

Opens a TCP connection and sends the AMQP protocol header. Use it for a quick reachability/protocol check:

```powershell
pwsh ./rabbithandshake.ps1
```

The script currently contains a hard-coded host and port.

### `testrabbit.py`

Edit the placeholders at the top of the file before use:

```python
HOST = "10.31.220.52"
PORT = 5672
USER = "<user>"
PASS = "<pass>"
VHOST = "<vhost>"
QUEUE = "<queue-name>"
```

Then run:

```bash
python3 ./testrabbit.py
```

This script performs a raw AMQP handshake and a passive `queue.declare` check.

## Notes and caveats

- Most scripts assume you are already authenticated with the correct CF or Ops Manager target unless credentials are supplied via environment variables.
- Several scripts print CSV directly to stdout. Redirect output to a file if you want to keep it.
- `get_foundation_data.sh` is the primary collector in this repo. The Git Bash variant appears to be a compatibility fallback, not the main path.
- `rabbithandshake.ps1` and `testrabbit.py` contain hard-coded connection values and should be treated as ad hoc diagnostics.
