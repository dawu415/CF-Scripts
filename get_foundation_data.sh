#!/usr/bin/env bash
#
# get_foundation_data.sh
#
# Unified data collector for a single Tanzu / CF foundation.
#
# Outputs (in multi-table mode):
#   app_data.csv              - core app inventory + env metadata + buildpack/runtime
#   service_data.csv          - app ⇄ service-instance relationships (normalized)
#   developer_space_data.csv  - Org/Space/Developer (normalized)
#   java_runtime_data.csv     - JRE version per Java app (from JRE lookup table)
#   audit_events.csv          - audit events per app (v3, with v2 fallback)
#   service_bindings.csv      - all brokers, app bindings, service keys, unbound instances
#
# Designed to work with Run-EnvOrchestrator.ps1. When CF_ORCH_DATA_MODE=multi,
# CF_ORCH_DATA_OUT is treated as a *base path/directory* and multiple CSVs
# are created beneath it.

set -Eeuo pipefail
[ -n "${BASH_VERSION:-}" ] || exec /usr/bin/env bash "$0" "$@"

# Non-interactive behavior; avoid profile noise & tty warnings
export CF_COLOR=false CF_TRACE=false TERM=dumb
stty -g >/dev/null 2>&1 || true
IFS=$' \t\n'

ORIGINAL_ERR_TRAP='ec=$?; set +u;
      ts=$(date "+%F %T" 2>/dev/null || printf N/A);
      src="${BASH_SOURCE[0]:-$0}"; fn="${FUNCNAME[0]:-main}";
      echo "[$ts] ERROR ${ec:-1} at ${src}:${LINENO}: ${fn}: ${BASH_COMMAND:-?}" >&2;
      exit "${ec:-1}"'

command -v cf    >/dev/null 2>&1 || { echo "cf CLI not found in PATH" >&2; exit 6; }
command -v jq    >/dev/null 2>&1 || { echo "jq not found in PATH" >&2; exit 6; }
command -v xargs >/dev/null 2>&1 || { echo "xargs not found in PATH" >&2; exit 6; }
command -v flock >/dev/null 2>&1 || { echo "flock not found in PATH" >&2; exit 6; }

# ----------------------------- Configuration -----------------------------
# Output mode: "single" (legacy: single CSV to stdout/file) or "multi" (recommended)
OUTPUT_MODE="${CF_ORCH_DATA_MODE:-multi}"

# Base output path:
#   - In multi mode: treated as directory (created if needed)
#   - In single mode: treated as CSV file
BASE_OUTPUT="${CF_ORCH_DATA_OUT:-./foundation_data}"

# Foundation key used for env mapping (can be set from orchestrator)
FOUNDATION_KEY="${CF_FOUNDATION:-${CF_ORCH_PLATFORM}}"

# Batch tracking (can be provided externally)
BATCH_ID="${BATCH_ID:-$(date +%Y%m%d_%H%M%S)}"

# Environment metadata - strongly recommended to set these from orchestrator
ENV_LOCATION="${ENV_LOCATION:-unknown}"
ENV_TYPE="${ENV_TYPE:-unknown}"
ENV_DATACENTER="${ENV_DATACENTER:-unknown}"
# When an orchestrator provides CF_ORCH_PLATFORM, prefer that value as the
# foundation slug.  Otherwise, fall back to any pre-set FOUNDATION_SLUG and
# ultimately the FOUNDATION_KEY derived from the API host.  This avoids
# defaulting to the system slug when a platform identifier is available.
FOUNDATION_SLUG="${CF_ORCH_PLATFORM:-${FOUNDATION_SLUG:-$FOUNDATION_KEY}}"

###############################################################################
# Audit Event Filtering Configuration
#
# IGNORE_ORGS: List of organisation names for which audit event collection will
#              be skipped entirely. Apps belonging to these orgs will not
#              trigger any calls to the /v3/audit_events endpoint.
#
# KPI_EVENT_TYPES: List of v3 audit-event types that are considered relevant
#                  for KPI analysis. Only events with a type in this list will
#                  be processed and written to the audit_events.csv. All other
#                  event types will be discarded.
###############################################################################

# Organisations to exclude from audit event collection
IGNORE_ORGS=("system" "p-spring-cloud-services" "Platform-Operations" "splunk-nozzle-org")
# Audit event types to retain for KPI reporting
KPI_EVENT_TYPES=(
  "audit.app.crash"          # v3 crash event
  "audit.app.process.crash"  # v3 process‑level crash
  "audit.app.stop"           # app stopped via cf stop
  "audit.app.start" 
  "audit.app.restart"        # app restarted
  "audit.app.update"
  "audit.app.scale"
  "audit.app.process.scale"
  "app.crash"                # v2 crash event for completeness
)

KPI_EVENT_TYPES+=(
  # Application deployment
  "audit.app.apply_manifest"
  "audit.app.build.create"
  "audit.app.copy-bits"

  # Application lifecycle
  "audit.app.process.create"
  "audit.app.process.delete"
  "audit.app.process.update"
  "audit.app.process.terminate_instance"
  "audit.app.process.ready"

  # Route lifecycle
  "audit.route.create" "audit.route.delete-request" "audit.route.update"
  "audit.route.share" "audit.route.unshare" "audit.route.transfer-owner"

  # Service lifecycle
  "audit.service.create" "audit.service.delete" "audit.service.update"

  # Service binding lifecycle
  "audit.service_binding.create" "audit.service_binding.delete"
  "audit.service_binding.start_create" "audit.service_binding.start_delete"
  "audit.service_binding.update" "audit.service_binding.show"

  # Service instance lifecycle
  "audit.service_instance.bind_route" "audit.service_instance.create"
  "audit.service_instance.delete" "audit.service_instance.share"
  "audit.service_instance.unbind_route" "audit.service_instance.unshare"
  "audit.service_instance.update" "audit.service_instance.show"
  "audit.service_instance.start_create" "audit.service_instance.start_update"
  "audit.service_instance.start_delete" "audit.service_instance.purge"

  # Service key lifecycle
  "audit.service_key.create" "audit.service_key.delete"
  "audit.service_key.start_create" "audit.service_key.start_delete"
  "audit.service_key.update" "audit.service_key.show"

  # Space lifecycle
  "audit.space.create" "audit.space.delete-request" "audit.space.update"

  # User lifecycle (role assignments)
  "audit.user.organization_auditor_add"    "audit.user.organization_auditor_remove"
  "audit.user.organization_billing_manager_add" "audit.user.organization_billing_manager_remove"
  "audit.user.organization_manager_add"    "audit.user.organization_manager_remove"
  "audit.user.organization_user_add"       "audit.user.organization_user_remove"
  "audit.user.space_auditor_add"           "audit.user.space_auditor_remove"
  "audit.user.space_supporter_add"         "audit.user.space_supporter_remove"
  "audit.user.space_developer_add"         "audit.user.space_developer_remove"
  "audit.user.space_manager_add"           "audit.user.space_manager_remove"
)

# Toggling parameters for audit event filtering
#
# KPI_FILTER_ENABLED: When set to "1" (default), the script will filter audit
# events to only include types listed in KPI_EVENT_TYPES.  When set to "0",
# all audit events returned by the API will be processed and written to
# audit_events.csv.
KPI_FILTER_ENABLED="${KPI_FILTER_ENABLED:-1}"
# ORG_FILTER_ENABLED: When set to "1" (default), audit event collection will
# be skipped for applications belonging to organisations listed in IGNORE_ORGS.
# When set to "0", the IGNORE_ORGS list is ignored and audit events will
# be collected for all orgs.
ORG_FILTER_ENABLED="${ORG_FILTER_ENABLED:-1}"

echo "Foundation key: $FOUNDATION_KEY" >&2
echo "Environment:   $ENV_LOCATION / $ENV_TYPE / $ENV_DATACENTER" >&2
echo "Batch Id:      $BATCH_ID" >&2

# ----------------------------- JRE Version Mapping -----------------------------
# Ported from JRE.js (jreMap + getJRE) so we can compute JRE versions wholly
# on the bash side. If you update JRE.js, mirror the changes here.

declare -A JRE_MAP

# v4.0-4.15 (Java 8 only)
for v in 4.0 4.1 4.2;        do JRE_MAP["$v:8"]="1.8.0_131"; done
for v in 4.3 4.4 4.5;        do JRE_MAP["$v:8"]="1.8.0_141"; done
for v in 4.5.1 4.5.2;        do JRE_MAP["$v:8"]="1.8.0_144"; done
for v in 4.6 4.7 4.7.1;      do JRE_MAP["$v:8"]="1.8.0_152"; done
for v in 4.8 4.9 4.10;       do JRE_MAP["$v:8"]="1.8.0_162"; done
for v in 4.11 4.12 4.13;     do JRE_MAP["$v:8"]="1.8.0_172"; done
JRE_MAP["4.13.1:8"]="1.8.0_181"
for v in 4.14 4.15;          do JRE_MAP["$v:8"]="1.8.0_181"; done

# 4.16+
JRE_MAP["4.16:8"]="1.8.0_181"; JRE_MAP["4.16:11"]="11.0.0_28"
JRE_MAP["4.16.1:8"]="1.8.0_192"; JRE_MAP["4.16.1:11"]="11.0.1_13"
JRE_MAP["4.17:8"]="1.8.0_192"; JRE_MAP["4.17:11"]="11.0.1_13"
JRE_MAP["4.17.1:8"]="1.8.0_202"; JRE_MAP["4.17.1:11"]="11.0.2_07"
JRE_MAP["4.17.2:8"]="1.8.0_202"; JRE_MAP["4.17.2:11"]="11.0.2_09"
JRE_MAP["4.18:8"]="1.8.0_202"; JRE_MAP["4.18:11"]="11.0.2_09"
JRE_MAP["4.19:8"]="1.8.0_202"; JRE_MAP["4.19:11"]="11.0.2_09"; JRE_MAP["4.19:12"]="12.0.0_33"
JRE_MAP["4.19.1:8"]="1.8.0_212"; JRE_MAP["4.19.1:11"]="11.0.3_07"; JRE_MAP["4.19.1:12"]="12.0.1_12"
JRE_MAP["4.20:8"]="1.8.0_222"; JRE_MAP["4.20:11"]="11.0.4_11"; JRE_MAP["4.20:12"]="12.0.1_12"
JRE_MAP["4.21:8"]="1.8.0_222"; JRE_MAP["4.21:11"]="11.0.4_11"; JRE_MAP["4.21:12"]="12.0.2_10"
JRE_MAP["4.22:8"]="1.8.0_222"; JRE_MAP["4.22:11"]="11.0.4_11"; JRE_MAP["4.22:12"]="12.0.2_10"
JRE_MAP["4.23:8"]="1.8.0_222"; JRE_MAP["4.23:11"]="11.0.4_11"; JRE_MAP["4.23:12"]="12.0.2_10"
JRE_MAP["4.24:8"]="1.8.0_232"; JRE_MAP["4.24:11"]="11.0.5_10"; JRE_MAP["4.24:13"]="13.0.0_33"
JRE_MAP["4.25:8"]="1.8.0_232"; JRE_MAP["4.25:11"]="11.0.5_10"; JRE_MAP["4.25:13"]="13.0.1_09"
JRE_MAP["4.26:8"]="1.8.0_232"; JRE_MAP["4.26:11"]="11.0.5_10"; JRE_MAP["4.26:13"]="13.0.1_09"
JRE_MAP["4.27:8"]="1.8.0_232"; JRE_MAP["4.27:11"]="11.0.5_10"; JRE_MAP["4.27:13"]="13.0.1_09"
JRE_MAP["4.28:8"]="1.8.0_242"; JRE_MAP["4.28:11"]="11.0.6_10"; JRE_MAP["4.28:13"]="13.0.2_08"
JRE_MAP["4.29:8"]="1.8.0_242"; JRE_MAP["4.29:11"]="11.0.6_10"; JRE_MAP["4.29:14"]="14.0.0_36"
JRE_MAP["4.29.1:8"]="1.8.0_242"; JRE_MAP["4.29.1:11"]="11.0.6_10"; JRE_MAP["4.29.1:14"]="14.0.0_36"
JRE_MAP["4.30:8"]="1.8.0_252"; JRE_MAP["4.30:11"]="11.0.7_10"; JRE_MAP["4.30:14"]="14.0.1_8"
JRE_MAP["4.31:8"]="1.8.0_252"; JRE_MAP["4.31:11"]="11.0.7_10"; JRE_MAP["4.31:14"]="14.0.1_8"
JRE_MAP["4.31.1:8"]="1.8.0_252"; JRE_MAP["4.31.1:11"]="11.0.7_10"; JRE_MAP["4.31.1:14"]="14.0.1_8"
JRE_MAP["4.32:8"]="1.8.0_262"; JRE_MAP["4.32:11"]="11.0.8_10"; JRE_MAP["4.32:14"]="14.0.2_13"
JRE_MAP["4.32.1:8"]="1.8.0_265"; JRE_MAP["4.32.1:11"]="11.0.8_10"; JRE_MAP["4.32.1:14"]="14.0.2_13"

# Recent v4.58+
for v in 4.58 4.59.0 4.60.0; do
  # Versions prior to 4.64.0 only support Java 8, 11 and 17.  They do not
  # include a Java 21 entry in the JRE lookup table.  See JRE.js for
  # reference.
  JRE_MAP["$v:8"]="1.8.0_372"
  JRE_MAP["$v:11"]="11.0.19_7"
  JRE_MAP["$v:17"]="17.0.7_7"
done

for v in 4.61.0 4.61.1 4.62.0; do
  # Versions 4.61.x only support Java 8, 11 and 17.  There is no Java 21
  # entry for these buildpacks.
  JRE_MAP["$v:8"]="1.8.0_382"
  JRE_MAP["$v:11"]="11.0.20_8"
  JRE_MAP["$v:17"]="17.0.8_7"
done

for v in 4.63.0 4.63.1; do
  # Versions 4.63.x only support Java 8, 11 and 17.  There is no Java 21
  # entry for these buildpacks.
  JRE_MAP["$v:8"]="1.8.0_392"
  JRE_MAP["$v:11"]="11.0.21_10"
  JRE_MAP["$v:17"]="17.0.9_11"
done

for v in 4.64.0 4.65.0; do
  JRE_MAP["$v:8"]="1.8.0_392"
  JRE_MAP["$v:11"]="11.0.21_10"
  JRE_MAP["$v:17"]="17.0.9_11"
  JRE_MAP["$v:21"]="21.0.1_12"
done

for v in 4.66.0 4.67.0 4.67.1 4.68.0; do
  JRE_MAP["$v:8"]="1.8.0_402"
  JRE_MAP["$v:11"]="11.0.22_12"
  JRE_MAP["$v:17"]="17.0.10_13"
  JRE_MAP["$v:21"]="21.0.2_14"
done

for v in 4.69.0 4.70.0 4.71.0; do
  JRE_MAP["$v:8"]="1.8.0_412"
  JRE_MAP["$v:11"]="11.0.23_10"
  JRE_MAP["$v:17"]="17.0.11_10"
  JRE_MAP["$v:21"]="21.0.3_10"
done

for v in 4.72.0 4.73.0; do
  JRE_MAP["$v:8"]="1.8.0_422"
  JRE_MAP["$v:11"]="11.0.24_9"
  JRE_MAP["$v:17"]="17.0.12_10"
  JRE_MAP["$v:21"]="21.0.4_9"
done

for v in 4.74.0 4.75.0; do
  JRE_MAP["$v:8"]="1.8.0_432"
  JRE_MAP["$v:11"]="11.0.25_11"
  JRE_MAP["$v:17"]="17.0.13_12"
  JRE_MAP["$v:21"]="21.0.5_11"
done

# v4.76.0, 4.77.0, 4.78.0 were never released or had critical bugs.
for v in 4.79.0 4.80.0 4.81.0; do
  JRE_MAP["$v:8"]="1.8.0_442"
  JRE_MAP["$v:11"]="11.0.26_9"
  JRE_MAP["$v:17"]="17.0.14_10"
  JRE_MAP["$v:21"]="21.0.6_10"
done

for v in 4.82.0 4.83.0; do
  JRE_MAP["$v:8"]="1.8.0_452"
  JRE_MAP["$v:11"]="11.0.27_9"
  JRE_MAP["$v:17"]="17.0.15_10"
  JRE_MAP["$v:21"]="21.0.7_9"
done

get_jre_version() {
  local bp_version="$1"
  local runtime_pref="$2"

  [[ -z "$bp_version" || "$bp_version" == "null" ]] && { echo "Unknown"; return; }

  local normalized="$bp_version"
  if [[ "$bp_version" =~ ^([0-9]+\.[0-9]+)\.0$ ]]; then
    normalized="${BASH_REMATCH[1]}"
  fi

  local major=""
  if [[ -n "$runtime_pref" && "$runtime_pref" != "null" ]]; then
    # If the preference is in the form 1.x (e.g. 1.8) then the effective
    # major version is x.  Otherwise extract the leading numeric portion.
    if [[ "$runtime_pref" =~ ^1\.([0-9]+) ]]; then
      major="${BASH_REMATCH[1]}"
    elif [[ "$runtime_pref" =~ ([0-9]+) ]]; then
      major="${BASH_REMATCH[1]}"
    fi
  fi

  # When no valid major is extracted from the runtime preference, default to Java 8.
  # This aligns with the platform behaviour where Java 8 is chosen when the
  # developer does not specify a runtime or specifies an invalid one.
  if [[ -z "$major" ]]; then
    major="8"
  fi

  # Attempt to look up an exact mapping for the normalized or full buildpack version
  # combined with the chosen major.  Prefer the normalized version (strip trailing .0)
  local lookup_key
  lookup_key="${normalized}:${major}"
  if [[ -n "${JRE_MAP[$lookup_key]:-}" ]]; then
    echo "${JRE_MAP[$lookup_key]}"
    return
  fi
  lookup_key="${bp_version}:${major}"
  if [[ -n "${JRE_MAP[$lookup_key]:-}" ]]; then
    echo "${JRE_MAP[$lookup_key]}"
    return
  fi

  # If no direct mapping exists (very unlikely), fall back to the highest
  # defined major for this buildpack version.
  local highest_major=0
  local highest_jre=""
  local key
  for key in "${!JRE_MAP[@]}"; do
    if [[ "$key" == "${normalized}:"* || "$key" == "${bp_version}:"* ]]; then
      local curr="${key##*:}"
      if [[ "$curr" -gt "$highest_major" ]]; then
        highest_major="$curr"
        highest_jre="${JRE_MAP[$key]}"
      fi
    fi
  done

  [[ -n "$highest_jre" ]] && echo "$highest_jre" || echo "Unknown"
}

###############################################################################
# Credential Redaction
#
# By default, the script redacts all service credential URIs and JSON payloads
# when producing the service_bindings.csv. This behaviour is controlled by the
# environment variable CF_ORCH_REDACT_CREDENTIALS. When set to any value other
# than "0", redaction is enabled and any non-empty credential field will be
# replaced with the literal string "[REDACTED]". To disable redaction and
# include the full credential values, set CF_ORCH_REDACT_CREDENTIALS=0 before
# running the script.
#
###############################################################################

redact_credentials() {
  local value="$1"
  # If CF_ORCH_REDACT_CREDENTIALS is explicitly set to "0", return the original value.
  if [[ "${CF_ORCH_REDACT_CREDENTIALS:-1}" == "0" ]]; then
    printf '%s' "$value"
  else
    # When redacting, preserve empties; otherwise emit a constant marker.
    if [[ -z "$value" ]]; then
      printf '%s' ""
    else
      printf '[REDACTED]'
    fi
  fi
}
export -f redact_credentials

###############################################################################
# CSV Helpers
#
# The helper functions below provide simple CSV cell and row formatting, as well
# as atomic write operations with file locking. These are used throughout the
# script to reliably construct the output CSV files.
###############################################################################

csv_cell() {
  local s="${1//$'\r'/ }"
  s="${s//$'\n'/ }"
  s="${s//$'\t'/ }"
  case "$s" in
    *[\",]*|*" "*)
      s="${s//\"/\"\"}"
      printf '"%s"' "$s"
      ;;
    *)
      printf '%s' "$s"
      ;;
  esac
}

csv_row() {
  local line="" sep="" a
  for a in "$@"; do
    line+="$sep$(csv_cell "$a")"
    sep=",";
  done
  printf '%s\n' "$line"
}

csv_write_header() {
  local file="$1"; shift
  [[ -z "$file" ]] && return
  exec 200>>"$file"
  flock -x 200
  if [[ ! -s "$file" || "${CF_ORCH_FORCE_HEADER:-0}" == 1 ]]; then
    csv_row "$@" >&200
  fi
  flock -u 200
  exec 200>&-
}

csv_write_row() {
  local file="$1"; shift
  if [[ -n "$file" ]]; then
    exec 200>>"$file"
    flock -x 200
    csv_row "$@" >&200
    flock -u 200
    exec 200>&-
  else
    csv_row "$@"
  fi
}

export -f csv_cell csv_row csv_write_header csv_write_row

###############################################################################
# Buildpack Helpers
#
# A collection of helper functions to simplify buildpack names, extract version
# information from buildpack filenames, and perform other lookup operations.
###############################################################################

simplify_buildpack_name() {
  local bp="$1"
  [[ -z "$bp" || "$bp" == "null" ]] && { echo "Unknown"; return; }
  local lower; lower=$(echo "$bp" | tr '[:upper:]' '[:lower:]')
  case "$lower" in
    *java*offline*)  echo "Java (Offline)" ;;
    *java*)          echo "Java (Online)" ;;
    *nodejs*|*node*) echo "Node.js" ;;
    *python*)        echo "Python" ;;
    *dotnet*)        echo ".NET Core" ;;
    *go*)            echo "Go" ;;
    *static*)        echo "Static" ;;
    *ruby*)          echo "Ruby" ;;
    *hwc*)           echo ".NET Framework" ;;
    *binary*)        echo "Binary" ;;
    *nginx*)         echo "NGINX" ;;
    *)               echo "$bp" ;;
  esac
}

extract_full_version() {
  local filename="$1"
  [[ -z "$filename" || "$filename" == "null" ]] && { echo ""; return; }
  # Extract the semantic version portion of a buildpack filename following the JavaScript logic in Utilities.js.
  # It looks for '-vX.Y' with an optional third component and optional pre-release/build suffixes.
  # Example matches: java-buildpack-v4.70.0.zip -> 4.70.0
  #                  java-buildpack-v4.9.zip    -> 4.9
  #                  java-buildpack-v4.64.0-alpha -> 4.64.0-alpha
  #                  java-buildpack-v4.66.0+build -> 4.66.0+build
  # The plus sign must be escaped in bash regex to match a literal plus.  Without escaping, it becomes a quantifier and causes the entire pattern to fail.
  if [[ "$filename" =~ -v([0-9]+\.[0-9]+(?:\.[0-9]+)?(?:-[[:alnum:]._-]+)?(?:\+[[:alnum:]._-]+)?) ]]; then
    echo "${BASH_REMATCH[1]}"
  else
    # Fallback: search for the first dotted numeric pattern like 4.70.0 or 4.58 in the filename
    local fallback
    fallback=$(echo "$filename" | grep -oE '[0-9]+\.[0-9]+(\.[0-9]+)?' | head -n1 || true)
    echo "$fallback"
  fi
}

###############################################################################
# Pagination Helpers
#
# Helper functions to fetch paginated results from the CF API across both v2 and
# v3 endpoints. These utilities ensure that all pages of data are retrieved
# in a single call.
###############################################################################

fetch_all_pages_v2() {
  local base_path="$1"
  local url="$base_path"
  if [[ "$url" == *"?"* ]]; then url="${url}&results-per-page=100"; else url="${url}?results-per-page=100"; fi
  local acc='{"resources":[]}' resp next_url
  while [[ -n "$url" ]]; do
    resp=$(cf curl "$url" 2>/dev/null || echo '{}')
    if ! printf '%s' "$resp" | jq -e . >/dev/null 2>&1; then
      resp='{"resources":[],"next_url":null}'
    fi
    acc=$(printf '%s\n%s\n' "$acc" "$resp" | jq -cs '.[0].resources += (.[1].resources // []) | .[0]')
    next_url=$(printf '%s' "$resp" | jq -r '.next_url // empty')
    url="${next_url:-}"
  done
  printf '%s\n' "$acc"
}

fetch_all_pages_v3() {
  local base_path="$1"
  local url="$base_path"
  if [[ "$url" == *"?"* ]]; then
    url="${url}&per_page=5000"
  else
    url="${url}?per_page=5000"
  fi
  {
    while [[ -n "$url" ]]; do
      local resp
      resp="$(cf curl "$url" 2>/dev/null || echo '{}')"
      jq -rc '.resources[]' <<<"$resp" 2>/dev/null || true
      local next
      next="$(jq -r '.pagination.next.href // ""' <<<"$resp")"
      if [[ -n "$next" ]]; then
        if [[ "$next" == /* ]]; then
          url="$next"
        else
          url="${next#*//*/}"
        fi
      else
        url=""
      fi
    done
  } | jq -s '[.[]]'
}

###############################################################################
# Cache Setup
#
# Prepare on-disk caches for expensive API calls such as buildpacks, spaces,
# organizations and stacks. These caches are keyed by the foundation and avoid
# repeated API hits when the script is executed multiple times.
###############################################################################

ORCH_OUT_DIR="${CF_ORCH_OUT_DIR:-$PWD/outputs}"
if [[ -n "${CF_ORCH_CACHE_ROOT:-}" ]]; then
  CACHE_ROOT="$CF_ORCH_CACHE_ROOT"
else
  CACHE_ROOT="$ORCH_OUT_DIR/cache/$FOUNDATION_KEY"
fi
mkdir -p "$CACHE_ROOT"
echo "Using cache root: $CACHE_ROOT" >&2

WORK_DIR="$(mktemp -d "${CACHE_ROOT%/}/work.XXXXXX")"
cleanup() { rm -rf "$WORK_DIR"; }
trap cleanup EXIT

BUILDPACKS_JSON_FILE="$CACHE_ROOT/buildpacks.json"
SPACES_JSON_FILE="$CACHE_ROOT/spaces.json"
ORGS_JSON_FILE="$CACHE_ROOT/orgs.json"
STACKS_JSON_FILE="$CACHE_ROOT/stacks.json"

export ORCH_OUT_DIR CACHE_ROOT WORK_DIR \
       BUILDPACKS_JSON_FILE SPACES_JSON_FILE ORGS_JSON_FILE STACKS_JSON_FILE

echo "Preloading foundation metadata..." >&2

tmp_bp="$(mktemp "${CACHE_ROOT%/}/.buildpacks.json.tmp.XXXXXX")"
cf curl "/v2/buildpacks?results-per-page=100" 2>/dev/null >"$tmp_bp" || echo '{}' >"$tmp_bp"
mv -f "$tmp_bp" "$BUILDPACKS_JSON_FILE"

tmp_spaces="$(mktemp "${CACHE_ROOT%/}/.spaces.json.tmp.XXXXXX")"
fetch_all_pages_v2 "/v2/spaces" >"$tmp_spaces" || echo '{"resources":[]}' >"$tmp_spaces"
mv -f "$tmp_spaces" "$SPACES_JSON_FILE"

tmp_orgs="$(mktemp "${CACHE_ROOT%/}/.orgs.json.tmp.XXXXXX")"
fetch_all_pages_v2 "/v2/organizations" >"$tmp_orgs" || echo '{"resources":[]}' >"$tmp_orgs"
mv -f "$tmp_orgs" "$ORGS_JSON_FILE"

tmp_stacks="$(mktemp "${CACHE_ROOT%/}/.stacks.json.tmp.XXXXXX")"
fetch_all_pages_v2 "/v2/stacks" >"$tmp_stacks" || echo '{"resources":[]}' >"$tmp_stacks"
mv -f "$tmp_stacks" "$STACKS_JSON_FILE"

###############################################################################
# Lookups
#
# A set of helper functions that perform lookups against cached data or the CF
# API to determine buildpack filenames, version information, stack names,
# organization/space names and more. These functions abstract away the details
# of the underlying API structure and caching mechanics.
###############################################################################

get_buildpack_filename() {
  local bp_key="$1" stack_name="${2:-}" out=""
  if [[ -s "$BUILDPACKS_JSON_FILE" ]]; then
    if [[ "$bp_key" =~ ^[0-9a-fA-F-]{36}$ ]]; then
      out=$(jq -r --arg g "$bp_key" '.resources[]? | select(.metadata.guid == $g) | .entity.filename // empty' "$BUILDPACKS_JSON_FILE" | head -n1)
    else
      if [[ -z "$stack_name" || "$stack_name" == "null" ]]; then
        out=$(jq -r --arg n "$bp_key" '.resources[]? | select(.entity.name == $n) | .entity.filename // empty' "$BUILDPACKS_JSON_FILE" | head -n1)
      else
        out=$(jq -r --arg n "$bp_key" --arg s "$stack_name" \
          '.resources[]? | select(.entity.name == $n and ((.entity.stack // "") == $s)) | .entity.filename // empty' \
          "$BUILDPACKS_JSON_FILE" | head -n1)
        [[ -z "$out" ]] && out=$(jq -r --arg n "$bp_key" \
          '.resources[]? | select(.entity.name == $n and ((.entity.stack // "") == "")) | .entity.filename // empty' \
          "$BUILDPACKS_JSON_FILE" | head -n1)
      fi
    fi
  fi
  if [[ -z "$out" && "$bp_key" =~ ^[0-9a-fA-F-]{36}$ ]]; then
    out=$(cf curl "/v2/buildpacks/$bp_key" 2>/dev/null | jq -r '.entity.filename // empty')
  fi

  # If still not found and bp_key is a name rather than a GUID, attempt a remote lookup by name.
  # Some buildpacks may not be present in the cached buildpacks.json; query the API directly.
  if [[ -z "$out" && -n "$bp_key" && ! "$bp_key" =~ ^[0-9a-fA-F-]{36}$ ]]; then
    local resp
    resp=$(cf curl "/v2/buildpacks?q=name:${bp_key}" 2>/dev/null || echo '{}')
    out=$(printf '%s' "$resp" | jq -r '.resources[0].entity.filename // empty')
  fi
  printf '%s' "$out"
}

get_version_info() {
  local app_guid="$1" detected_buildpack="$2" buildpack_filename="${3:-}"
  local env_data buildpack_version runtime_version
  runtime_version=""

  env_data=$(cf curl "/v2/apps/${app_guid}/env" 2>/dev/null || echo '{}')
  buildpack_version=$(jq -r '.staging_env_json.BUILDPACK_VERSION // empty' <<<"$env_data")

  # Determine runtime_version based on language.  For Java, look at both
  # staging_env_json and environment_json JBP_CONFIG_OPEN_JDK_JRE.  Fall back
  # to JAVA_VERSION if present.  For other languages, use their respective
  # environment variables.  We intentionally preserve the original runtime
  # string (e.g. "17.+") rather than collapsing to just the digits here.  The
  # get_jre_version() function will extract the major version as needed.
  if [[ "$detected_buildpack" =~ [Jj]ava ]]; then
    # Try to find a JRE config in staging or environment JSON
    local jre_cfg=""
    jre_cfg=$(jq -r '.environment_json.JBP_CONFIG_OPEN_JDK_JRE // .staging_env_json.JBP_CONFIG_OPEN_JDK_JRE // empty' <<<"$env_data")
    if [[ -z "$jre_cfg" || "$jre_cfg" == "null" ]]; then
      # Fall back to JAVA_VERSION or similar
      jre_cfg=$(jq -r '.environment_json.JAVA_VERSION // .staging_env_json.JAVA_VERSION // empty' <<<"$env_data")
    fi
    # Extract just the version string if the config appears to be a JSON object
    # e.g. '{ "jre": { "version": "17.+" }}' -> '17.+'
    if [[ "$jre_cfg" =~ \{ ]]; then
      # Attempt to parse as JSON and extract .jre.version; if that fails, leave empty
      local extracted=""
      extracted=$(printf '%s' "$jre_cfg" | sed 's/\([a-zA-Z_][a-zA-Z0-9_]*\):/"\1":/g' | sed 's/: \([0-9][^,} ]*\)/: "\1"/g' | jq -r 'try .jre.version // empty' 2>/dev/null || true)
      if [[ -n "$extracted" ]]; then
        runtime_version="$extracted"
      else
        # Not a standard JSON; fallback to first numeric pattern like '17.+' or '17'
        runtime_version=$(echo "$jre_cfg" | grep -oE '[0-9]+(\.[0-9]+)?\+?' | head -n1 || true)
      fi
    else
      runtime_version="$jre_cfg"
    fi
  elif [[ "$detected_buildpack" =~ [Nn]ode ]]; then
    runtime_version=$(jq -r '.environment_json.NODE_VERSION // empty' <<<"$env_data")
  elif [[ "$detected_buildpack" =~ [Pp]ython ]]; then
    runtime_version=$(jq -r '.environment_json.PYTHON_VERSION // empty' <<<"$env_data")
  else
    runtime_version=""
  fi

  [[ -z "$buildpack_version" || "$buildpack_version" == "null" ]] && buildpack_version=""
  [[ -z "$runtime_version"   || "$runtime_version"   == "null" ]] && runtime_version=""
  echo "${buildpack_version}|${runtime_version}"
}

get_stack_name_safe() {
  local stack_url="$1" stack_guid="${stack_url##*/}"
  if [[ -s "$STACKS_JSON_FILE" && -n "$stack_guid" && "$stack_guid" != "null" ]]; then
    jq -r --arg gid "$stack_guid" '.resources[]? | select(.metadata.guid == $gid) | .entity.name // empty' "$STACKS_JSON_FILE"
  else
    cf curl "$stack_url" 2>/dev/null | jq -r '.entity.name // empty'
  fi
}

get_space_org_names_safe() {
  local space_url="$1" space_guid="${space_url##*/}" space_name="" org_guid="" org_name=""
  if [[ -s "$SPACES_JSON_FILE" && -n "$space_guid" && "$space_guid" != "null" ]]; then
    space_name=$(jq -r --arg gid "$space_guid" '.resources[]? | select(.metadata.guid == $gid) | .entity.name // empty' "$SPACES_JSON_FILE")
    org_guid=$(jq -r  --arg gid "$space_guid" '.resources[]? | select(.metadata.guid == $gid) | .entity.organization_guid // empty' "$SPACES_JSON_FILE")
    if [[ -s "$ORGS_JSON_FILE" && -n "$org_guid" && "$org_guid" != "null" ]]; then
      org_name=$(jq -r --arg gid "$org_guid" '.resources[]? | select(.metadata.guid == $gid) | .entity.name // empty' "$ORGS_JSON_FILE")
    else
      org_name=$(cf curl "/v2/organizations/$org_guid" 2>/dev/null | jq -r '.entity.name // empty')
    fi
  else
    local space_json; space_json=$(cf curl "$space_url" 2>/dev/null || echo '{}')
    space_name=$(jq -r '.entity.name // empty' <<<"$space_json")
    local org_url; org_url=$(jq -r '.entity.organization_url // empty' <<<"$space_json")
    org_name=$(cf curl "$org_url" 2>/dev/null | jq -r '.entity.name // empty')
  fi
  printf '%s|%s\n' "$space_name" "$org_name"
}

###############################################################################
# Output Setup
#
# Prepare output paths and write CSV headers based on the selected output mode.
###############################################################################

if [[ "$OUTPUT_MODE" == "multi" ]]; then
  OUTPUT_DIR="${BASE_OUTPUT%.csv}"
  mkdir -p "$OUTPUT_DIR"
  APP_DATA_OUT="$OUTPUT_DIR/app_data.csv"
  SERVICE_DATA_OUT="$OUTPUT_DIR/service_data.csv"
  DEVELOPER_DATA_OUT="$OUTPUT_DIR/developer_space_data.csv"
  JAVA_RUNTIME_OUT="$OUTPUT_DIR/java_runtime_data.csv"
  AUDIT_EVENTS_OUT="$OUTPUT_DIR/audit_events.csv"
  SERVICE_BINDINGS_OUT="$OUTPUT_DIR/service_bindings.csv"
else
  APP_DATA_OUT="${BASE_OUTPUT%.csv}.csv"
  SERVICE_DATA_OUT=""
  DEVELOPER_DATA_OUT=""
  JAVA_RUNTIME_OUT=""
  AUDIT_EVENTS_OUT=""
  SERVICE_BINDINGS_OUT=""
fi

###############################################################################
# Initialize CSV Files
#
# Define column headers for each CSV and write them out if the file is empty or
# if CF_ORCH_FORCE_HEADER is set.
###############################################################################

echo "Initializing output files..." >&2

APP_HEADER=(
  "Org_Name" "Space_Name" "Created_At" "Updated_At" "App_Name" "App_GUID"
  "Instances" "Memory_MB" "Disk_Quota_MB"
  "Requested_Buildpack" "Detected_Buildpack" "Detected_Buildpack_GUID"
  "Buildpack_Filename" "Buildpack_Version" "Runtime_Version"
  "DropletSizeBytes" "PackagesSizeBytes" "HealthCheckType" "App_State" "Stack_Name"
  "Services" "Routes" "Developers" "Detected_Start_Command"
  "Stable_Key" "Foundation_Slug" "Env_Location" "Env_Type" "Env_Datacenter"
  "Buildpack_AutoDetectState" "Grouped_Buildpack"
  "Estimated_Size_Bytes" "Developer_Count" "Service_Count" "Batch_Id"
)

SERVICE_HEADER=(
  "Stable_Key" "Foundation_Slug" "App_GUID" "Org_Name" "Space_Name"
  "Env_Location" "Env_Type" "Env_Datacenter"
  "Service_GUID" "Service_Type" "Service_Plan" "Service_Name" "Batch_Id"
)

DEVELOPER_HEADER=(
  "Stable_Key" "Foundation_Slug" "Org_Name" "Space_Name"
  "Env_Location" "Env_Type" "Env_Datacenter"
  "Developer" "Batch_Id"
)

JAVA_HEADER=(
  "App_GUID" "App_Name" "Org_Name" "Space_Name"
  "Buildpack_Version" "Runtime_Version" "JRE_Version"
  "Foundation_Slug" "Batch_Id"
)

EVENTS_HEADER=(
  "App_GUID" "App_Name" "Event_Type" "Actor" "Actor_Type" "Actor_Name"
  "Timestamp" "Metadata" "Foundation_Slug" "Batch_Id"
)

BINDINGS_HEADER=(
  "broker_name" "binding_type" "service_offering_name" "service_plan_name"
  "service_instance_name" "service_instance_guid" "service_binding_guid"
  "binding_name" "app_name" "app_guid" "space_name" "space_guid"
  "org_name" "org_guid" "credential_uri" "credentials_json"
  "Foundation_Slug" "Batch_Id"
)

csv_write_header "$APP_DATA_OUT"        "${APP_HEADER[@]}"
[[ -n "$SERVICE_DATA_OUT" ]]    && csv_write_header "$SERVICE_DATA_OUT"    "${SERVICE_HEADER[@]}"
[[ -n "$DEVELOPER_DATA_OUT" ]]  && csv_write_header "$DEVELOPER_DATA_OUT"  "${DEVELOPER_HEADER[@]}"
[[ -n "$JAVA_RUNTIME_OUT" ]]    && csv_write_header "$JAVA_RUNTIME_OUT"    "${JAVA_HEADER[@]}"
[[ -n "$AUDIT_EVENTS_OUT" ]]    && csv_write_header "$AUDIT_EVENTS_OUT"    "${EVENTS_HEADER[@]}"
[[ -n "$SERVICE_BINDINGS_OUT" ]] && csv_write_header "$SERVICE_BINDINGS_OUT" "${BINDINGS_HEADER[@]}"

###############################################################################
# Process App Data
#
# The process_app function handles per-app logic to generate rows for the app
# inventory as well as service data, developer data, Java runtime data, and
# audit events. It is designed to be called concurrently to speed up
# processing across a large number of applications.
###############################################################################

declare -A SPACE_DEV_SEEN

process_app() {
  local app="$1"

  local name app_guid instances memory disk_quota buildpack detected_buildpack detected_buildpack_guid
  name=$(jq -r '.entity.name // empty' <<<"$app")
  app_guid=$(jq -r '.metadata.guid' <<<"$app")
  instances=$(jq -r '.entity.instances // 0' <<<"$app")
  memory=$(jq -r '.entity.memory // 0' <<<"$app")
  disk_quota=$(jq -r '.entity.disk_quota // 0' <<<"$app")
  buildpack=$(jq -r '.entity.buildpack // empty' <<<"$app")
  detected_buildpack=$(jq -r '.entity.detected_buildpack // empty' <<<"$app")
  detected_buildpack_guid=$(jq -r '.entity.detected_buildpack_guid // empty' <<<"$app")

  local dl pl droplet_size_bytes="" packages_size_bytes=""
  dl=$(cf curl "/v3/apps/${app_guid}/droplets" 2>/dev/null | jq -r '.resources // [] | .[0].links.download.href // empty' | sed 's|http[s]*://[^/]*||')
  pl=$(cf curl "/v3/apps/${app_guid}/packages" 2>/dev/null | jq -r '.resources // [] | .[0].links.download.href // empty' | sed 's|http[s]*://[^/]*||')
  [[ -n "$dl" ]] && droplet_size_bytes=$( (
    cf curl -X HEAD -v "$dl" 2>&1 \
      | awk -F': ' '/Content-Length:/ {gsub(/\r/,"",$2); print $2; exit}' \
    || true
    ))

  [[ -n "$pl" ]] && packages_size_bytes=$( (
    cf curl -X HEAD -v "$pl" 2>&1 \
      | awk -F': ' '/Content-Length:/ {gsub(/\r/,"",$2); print $2; exit}' \
    || true
    ))

  local health_check app_state
  health_check=$(jq -r '.entity.health_check_type // empty' <<<"$app")
  app_state=$(jq -r '.entity.state // empty' <<<"$app")

  local stack_url stack_name
  stack_url=$(jq -r '.entity.stack_url // empty' <<<"$app")
  stack_name=$(get_stack_name_safe "$stack_url")

  local space_url space_name org_name
  space_url=$(jq -r '.entity.space_url // empty' <<<"$app")
  IFS='|' read -r space_name org_name < <(get_space_org_names_safe "$space_url")

  local created_at updated_at detected_start_command
  created_at=$(jq -r '.metadata.created_at // empty' <<<"$app")
  updated_at=$(jq -r '.metadata.updated_at // empty' <<<"$app")
  detected_start_command=$(jq -r '.entity.detected_start_command // empty' <<<"$app")

  local stable_key="${FOUNDATION_SLUG}:${org_name}:${space_name}"

  local buildpack_filename input
  if [[ "$detected_buildpack_guid" != "null" && -n "$detected_buildpack_guid" ]]; then
    buildpack_filename=$(get_buildpack_filename "$detected_buildpack_guid")
  else
    input="${detected_buildpack:-$buildpack}"
    buildpack_filename=$(get_buildpack_filename "$input" "$stack_name")
  fi

  local buildpack_version runtime_version
  # Retrieve any explicit buildpack and runtime versions from the app's staging/env metadata.
  # get_version_info echoes two fields separated by a pipe: BUILD PACK VERSION and runtime preference.
  IFS='|' read -r buildpack_version runtime_version <<<"$(get_version_info "$app_guid" "$detected_buildpack" "$buildpack_filename")"

  local auto_state="NotFound"
  local reqVal="$buildpack"
  local detVal="$detected_buildpack"

  if [[ -z "$reqVal" || "$reqVal" == "null" ]]; then
    if [[ -n "$detVal" && "$detVal" != "null" ]]; then
      auto_state="AutoDetected"
    else
      auto_state="NotFound"
    fi
  else
    auto_state="ManuallyRequested"
  fi

  local lookup_val="$buildpack_filename"
  if [[ -z "$lookup_val" || "$lookup_val" == "null" ]]; then
    if [[ "$auto_state" == "ManuallyRequested" ]]; then
      lookup_val="$reqVal"
      if [[ "$lookup_val" == *http* && -n "$detVal" && "$detVal" != "null" ]]; then
        lookup_val="$detVal"
      fi
    elif [[ "$auto_state" == "AutoDetected" ]]; then
      lookup_val="$detVal"
    fi
  fi

  local grouped_buildpack
  grouped_buildpack=$(simplify_buildpack_name "$lookup_val")

  local extracted_version=""
  if [[ -n "$buildpack_filename" && "$buildpack_filename" != "null" ]]; then
    extracted_version=$(extract_full_version "$buildpack_filename")
  fi
  [[ -z "$extracted_version" && -n "$buildpack_version" ]] && extracted_version="$buildpack_version"
  # Fallback: extract version directly from buildpack or detected buildpack names if still empty
  if [[ -z "$extracted_version" ]]; then
    if [[ "$buildpack" =~ ([0-9]+\.[0-9]+(\.[0-9]+)?) ]]; then
      extracted_version="${BASH_REMATCH[1]}"
    elif [[ "$detected_buildpack" =~ ([0-9]+\.[0-9]+(\.[0-9]+)?) ]]; then
      extracted_version="${BASH_REMATCH[1]}"
    fi
  fi

  # ---------------------------------------------------------------------
  # Prefer the extracted version from the buildpack filename or name over
  # the value obtained from BUILDPACK_VERSION in the app's staging env.
  # The JSON environment value is often a bare number (e.g. 4.20 becomes
  # 4.2) which causes truncation of trailing zeros.  To avoid this
  # ambiguity, we always overwrite buildpack_version with the more
  # accurate extracted_version when it is available.
  if [[ -n "$extracted_version" ]]; then
    buildpack_version="$extracted_version"
  fi

  # ---------------------------------------------------------------
  # At this point, extracted_version holds the buildpack version, and
  # runtime_version holds the version preference (e.g. "17.+").  In
  # some environments these may remain empty.  To ensure the
  # java_runtime_data.csv always contains a non-empty Buildpack_Version
  # and Runtime_Version field, set them to "Unknown" if missing.  Use
  # temporary variables for output so as not to interfere with
  # subsequent logic (e.g. computing jre_version).
  local bp_version_field="$extracted_version"
  if [[ -z "$bp_version_field" ]]; then
    bp_version_field="Unknown"
  fi
  # Preserve the original runtime version (which may be blank) for the
  # java_runtime_data.csv output.  Do not replace a blank runtime
  # preference with "Unknown" here; leaving it blank indicates the
  # developer did not specify a runtime version.  We still keep
  # runtime_version for computing the JRE lookup.
  local runtime_version_field="$runtime_version"

  local jre_version=""
  # Compute JRE version for Java apps.  We consider any grouped buildpack containing
  # the substring "java" (case-insensitive) to be a Java application.  If a
  # runtime version is provided, it will be used to select the corresponding JRE
  # version; otherwise the highest available JRE for the buildpack will be returned.
  if [[ "$grouped_buildpack" =~ [Jj]ava ]]; then
    jre_version=$(get_jre_version "$extracted_version" "$runtime_version")
  fi

  local total_bytes=0
  [[ -n "$droplet_size_bytes" && "$droplet_size_bytes" =~ ^[0-9]+$ ]] && ((total_bytes += droplet_size_bytes))
  [[ -n "$packages_size_bytes" && "$packages_size_bytes" =~ ^[0-9]+$ ]] && ((total_bytes += packages_size_bytes))
  # We report total package/droplet size in raw bytes rather than converting
  # to megabytes.  The user prefers byte units.
  local est_bytes=""
  [[ $total_bytes -gt 0 ]] && est_bytes="$total_bytes"

  local summary_json routes
  summary_json=$(cf curl "/v2/apps/${app_guid}/summary" 2>/dev/null || echo '{}')
  routes=$(jq -r '.routes // [] | .[] | (.host + "." + .domain.name)' <<<"$summary_json" | paste -sd ':' -)

  # ------------------------ services & service_data ------------------------
  local -a services_list=()
  local svc service_guid service_type service_string label plan svc_name
  while IFS= read -r svc; do
    service_guid=$(jq -r '.guid // empty' <<<"$svc")
    [[ -z "$service_guid" || "$service_guid" == "null" ]] && continue
    service_type=$(jq -r '.type // empty' <<<"$svc")
    # Capture the developer-assigned service instance name for all service types
    svc_name=$(jq -r '.name // ""' <<<"$svc")
    if [[ "$service_type" != "user_provided_service_instance" ]]; then
      label=$(jq -r '.service_plan.service.label // ""' <<<"$svc")
      plan=$(jq -r  '.service_plan.name // ""' <<<"$svc")
      service_string="$label ($plan)-($service_guid)"
    else
      # For user provided services, the plan is the service name and the label is fixed
      label="User Provided Service"
      plan="$svc_name"
      service_string="$svc_name (user provided service)-($service_guid)"
    fi
    services_list+=("$service_string")

    if [[ -n "$SERVICE_DATA_OUT" ]]; then
      csv_write_row "$SERVICE_DATA_OUT" \
        "$stable_key" "$FOUNDATION_SLUG" "$app_guid" "$org_name" "$space_name" \
        "$ENV_LOCATION" "$ENV_TYPE" "$ENV_DATACENTER" \
        "$service_guid" "$label" "$plan" "$svc_name" "$BATCH_ID"
    fi
  done < <(jq -c '.services // [] | .[]' <<<"$summary_json")

  local services=""; local service_count=0
  if ((${#services_list[@]} > 0)); then
    services=$(printf "%s:" "${services_list[@]}"); services="${services%:}"
    service_count=${#services_list[@]}
  fi

  # ------------------------ developers & developer_space_data ------------------------
  local dev_usernames
  dev_usernames=$(cf curl "${space_url}/developers" 2>/dev/null \
    | jq -r '.resources // [] | .[] | .entity | select(.username != null) | .username' \
    | paste -sd ':' -)

  if [[ -n "$DEVELOPER_DATA_OUT" && -n "$dev_usernames" ]]; then
    IFS=':' read -ra devs <<<"$dev_usernames"
    local dev
    for dev in "${devs[@]}"; do
      dev="${dev,,}"
      [[ -z "$dev" ]] && continue
      local key="${stable_key}||${BATCH_ID}||${dev}"
      if [[ -n "${SPACE_DEV_SEEN[$key]:-}" ]]; then
        continue
      fi
      SPACE_DEV_SEEN["$key"]=1
      csv_write_row "$DEVELOPER_DATA_OUT" \
        "$stable_key" "$FOUNDATION_SLUG" "$org_name" "$space_name" \
        "$ENV_LOCATION" "$ENV_TYPE" "$ENV_DATACENTER" \
        "$dev" "$BATCH_ID"
    done
  fi

  # ------------------------ java_runtime_data ------------------------
  # Always write a Java runtime data row for apps identified as using a Java buildpack.
  # Use fallback values of "Unknown" for missing buildpack or runtime version info.
  if [[ -n "$JAVA_RUNTIME_OUT" && "$grouped_buildpack" =~ [Jj]ava ]]; then
    local bp_ver_out="$bp_version_field"
    [[ -z "$bp_ver_out" ]] && bp_ver_out="Unknown"
    # For the runtime version column, leave it blank if the developer did not
    # specify one.  We do not fill in "Unknown" here.
    local rt_ver_out="$runtime_version_field"
    local jr_ver_out="$jre_version"
    [[ -z "$jr_ver_out" ]] && jr_ver_out="Unknown"
    csv_write_row "$JAVA_RUNTIME_OUT" \
      "$app_guid" "$name" "$org_name" "$space_name" \
      "$bp_ver_out" "$rt_ver_out" "$jr_ver_out" \
      "$FOUNDATION_SLUG" "$BATCH_ID"
  fi

  # ------------------------ audit events (paginated) ------------------------
  local events=""
  local events_json="[]"

  # Determine whether to collect audit events based on the org.  When
  # ORG_FILTER_ENABLED=1 (default), skip collection if the app belongs to an
  # organisation in the IGNORE_ORGS list.  When ORG_FILTER_ENABLED=0, no
  # org-based skipping is applied.
  local skip_audit="false"
  if [[ "$ORG_FILTER_ENABLED" == "1" ]]; then
    for ignore_org in "${IGNORE_ORGS[@]}"; do
      if [[ "$org_name" == "$ignore_org" ]]; then
        skip_audit="true"
        break
      fi
    done
  fi

  if [[ "$skip_audit" == "false" && -n "$AUDIT_EVENTS_OUT" ]]; then
    # Fetch all audit events for this app
    events_json="$(fetch_all_pages_v3 "/v3/audit_events?target_guids=${app_guid}")"
    # Optionally filter events down to KPI-relevant types when KPI_FILTER_ENABLED=1
    if [[ "$KPI_FILTER_ENABLED" == "1" && "$(jq 'length' <<<"$events_json")" -gt 0 ]]; then
      # Build a jq boolean expression from KPI_EVENT_TYPES
      local kpi_expr=""
      local _evt
      for _evt in "${KPI_EVENT_TYPES[@]}"; do
        kpi_expr+=".type == \"${_evt}\" or "
      done
      # Trim the trailing ' or '
      kpi_expr="${kpi_expr% or }"
      events_json="$(jq -c "[.[] | select(${kpi_expr})]" <<<"$events_json")"
    fi
  fi

  # If no events remain or collection was skipped, ensure events_json is an empty array
  [[ -z "$events_json" || "$events_json" == "null" ]] && events_json="[]"

  # Write filtered audit events (if any)
  if [[ "$(jq 'length' <<<"$events_json")" -gt 0 ]]; then
    if [[ -n "$AUDIT_EVENTS_OUT" ]]; then
      while IFS= read -r event; do
        local event_type actor actor_type actor_name ts meta
        event_type="$(jq -r '.type // ""'           <<<"$event")"
        actor="$(jq -r      '.actor.guid // ""'     <<<"$event")"
        actor_type="$(jq -r '.actor.type // ""'     <<<"$event")"
        actor_name="$(jq -r '.actor.name // ""'     <<<"$event")"
        ts="$(jq -r         '.created_at // ""'     <<<"$event")"
        meta="$(jq -c       '.data // {}'           <<<"$event")"

        csv_write_row "$AUDIT_EVENTS_OUT" \
          "$app_guid" "$name" "$event_type" "$actor" "$actor_type" "$actor_name" \
          "$ts" "$meta" "$FOUNDATION_SLUG" "$BATCH_ID"
      done < <(jq -c '.[]' <<<"$events_json")
    fi
    events="$(jq -cr '.[]?' <<<"$events_json" | paste -sd ':#:' -)"
  fi

  # ------------------------ developer count ------------------------
  local dev_count=0
  [[ -n "$dev_usernames" ]] && dev_count=$(echo "$dev_usernames" | tr ':' '\n' | grep -v '^$' | wc -l | tr -d ' ')

  # ------------------------ final app_data row ------------------------
  csv_write_row "$APP_DATA_OUT" \
    "$org_name" "$space_name" "$created_at" "$updated_at" "$name" "$app_guid" \
    "$instances" "$memory" "$disk_quota" \
    "$buildpack" "$detected_buildpack" "$detected_buildpack_guid" \
    "$buildpack_filename" "$buildpack_version" "$runtime_version" \
    "$droplet_size_bytes" "$packages_size_bytes" "$health_check" "$app_state" "$stack_name" \
    "$services" "$routes" "$dev_usernames" "$detected_start_command" \
    "$stable_key" "$FOUNDATION_SLUG" "$ENV_LOCATION" "$ENV_TYPE" "$ENV_DATACENTER" \
    "$auto_state" "$grouped_buildpack" \
    "$est_bytes" "$dev_count" "$service_count" "$BATCH_ID"
}

export -f process_app fetch_all_pages_v2 fetch_all_pages_v3 \
  get_buildpack_filename get_version_info get_stack_name_safe \
  get_space_org_names_safe simplify_buildpack_name extract_full_version \
  csv_write_row csv_row csv_cell get_jre_version
export FOUNDATION_SLUG ENV_LOCATION ENV_TYPE ENV_DATACENTER BATCH_ID \
       APP_DATA_OUT SERVICE_DATA_OUT DEVELOPER_DATA_OUT JAVA_RUNTIME_OUT \
       AUDIT_EVENTS_OUT SERVICE_BINDINGS_OUT

APPS_JSON_FILE="$WORK_DIR/apps.json"
fetch_all_pages_v2 "/v2/apps" >"$APPS_JSON_FILE"

if ! jq -e . >/dev/null 2>&1 <"$APPS_JSON_FILE"; then
  echo "cf curl /v2/apps did not return valid JSON. Are you logged in?" >&2
  exit 5
fi

# Global flag we can use to know if any app failed
overall_status=0

process_app_wrapper() {
  local app_json="$1"

  # Extract some ID info for logging
  local app_name app_guid
  app_name=$(jq -r '.entity.name // "UNKNOWN_APP"'    <<<"$app_json" 2>/dev/null)
  app_guid=$(jq -r '.metadata.guid // "UNKNOWN_GUID"' <<<"$app_json" 2>/dev/null)

  # Run process_app in a subshell with its own strict mode + trap
  (
    set -Eeuo pipefail

    err_local() {
      local ec=$?
      echo "ERROR: process_app failed for app ${app_name} (${app_guid}) on foundation ${FOUNDATION_SLUG} (exit ${ec})" >&2
      # exit non-zero so the parent wrapper can see that it failed
      exit "$ec"
    }

    trap "$ORIGINAL_ERR_TRAP" ERR

    process_app "$app_json"
  )
  local ec=$?

  # In the parent shell: record that something failed
  if [[ $ec -ne 0 ]]; then
    overall_status=1
  fi

  return 0  # we don't propagate failure up; we just log & mark overall_status
}

workers="${WORKERS:-6}"

# Ensure workers is a sane positive integer
if ! [[ "$workers" =~ ^[1-9][0-9]*$ ]]; then
  workers=1
fi

echo "Processing apps (workers: $workers)..." >&2

# Temporarily disable global ERR trap and -e while we process apps
trap - ERR
set +e

if (( workers > 1 )); then
  current=0
  declare -a pids=()
  
  while IFS= read -r app; do
    process_app_wrapper "$app" &
    pids+=($!)
    ((++current))
    
    if (( current >= workers )); then
      wait -n
      ((--current))
    fi
  done < <(jq -c '.resources // [] | .[]' "$APPS_JSON_FILE")
  
  # Critical: Wait for ALL jobs to complete
  for pid in "${pids[@]}"; do
    wait "$pid" 2>/dev/null || true
  done
else
  while IFS= read -r app; do
    process_app_wrapper "$app"
  done < <(jq -c '.resources // [] | .[]' "$APPS_JSON_FILE")
fi

# Restore strict mode and global ERR trap
set -e
trap "$ORIGINAL_ERR_TRAP" ERR

if (( overall_status != 0 )); then
  echo "WARNING: One or more apps failed to process on foundation ${FOUNDATION_SLUG}. See ERROR lines above for details." >&2
fi

echo "Note: Developer_Count in app_data is per-app; developer_space_data provides normalized per-space records." >&2
###############################################################################
# Service bindings (all brokers)
#
# Collect service binding information across all brokers. This section fetches
# all service binding information across all brokers and produces rows for:
#   - app bindings (binding_type=app)
#   - key bindings (binding_type=key)
#   - unbound instances (binding_type=none)
#
# Fixes:
#   - Preload /v3/service_credential_bindings once and filter client‑side to
#     avoid server‑side filter issues (which previously made everything look
#     unbound).
#   - Fix jq quoting in parallel_get_binding_details_map so the filter
#     {($g): .} is parsed correctly.
#   - Always populate Org/Space for unbound instances using v2 caches via
#     get_space_org_names_safe.
###############################################################################

if [[ -n "$SERVICE_BINDINGS_OUT" ]]; then
  echo "Collecting service binding data (all brokers)..." >&2

  # Preload all service credential bindings once for the foundation.  The
  # previous implementation relied on /v3/service_credential_bindings
  # filters per broker, which returned no rows on some foundations and caused
  # every binding to be classified as "none".
  SERVICE_CRED_BINDINGS_FILE="$WORK_DIR/service_credential_bindings.json"
  fetch_all_pages_v3 "/v3/service_credential_bindings" \
    >"$SERVICE_CRED_BINDINGS_FILE" || echo '[]' >"$SERVICE_CRED_BINDINGS_FILE"

  get_all() {
    local path="$1"
    {
      while [[ -n "$path" ]]; do
        if [[ "$path" == *"?"* ]]; then
          path="${path}&per_page=5000"
        else
          path="${path}?per_page=5000"
        fi
        local resp
        resp="$(cf curl "$path" 2>/dev/null || echo '{}')"
        # Output each resource; if .resources is null, substitute an empty array
        jq -rc '.resources // [] | .[]' <<<"$resp"
        local next
        next="$(jq -r '.pagination.next.href // ""' <<<"$resp")"
        if [[ -n "$next" ]]; then
          if [[ "$next" == /* ]]; then
            path="$next"
          else
            path="${next#*//*/}"
          fi
        else
          path=""
        fi
      done
    } | jq -s '[.[]]'
  }

  fetch_with_param_chunks() {
    local base="$1" param="$2" size="$3" extra_q="$4"
    shift 4
    extra_q="${extra_q#?}"; extra_q="${extra_q#&}"
    local -a items=( "$@" )
    {
      local -a chunk=(); local g
      for g in "${items[@]}"; do
        [[ -n "$g" ]] && chunk+=( "$g" )
        if ((${#chunk[@]} >= size)); then
          local q; printf -v q '%s,' "${chunk[@]}"; q="${q%,}"
          local url="$base"
          if [[ -n "$extra_q" ]]; then
            url+="?${extra_q}&${param}=${q}"
          else
            url+="?${param}=${q}"
          fi
          get_all "$url" | jq -rc '.[]'
          chunk=()
        fi
      done
      if ((${#chunk[@]})); then
        local q; printf -v q '%s,' "${chunk[@]}"; q="${q%,}"
        local url="$base"
        if [[ -n "$extra_q" ]]; then
          url+="?${extra_q}&${param}=${q}"
        else
          url+="?${param}=${q}"
        fi
        get_all "$url" | jq -rc '.[]'
      fi
    } | jq -s '[.[]]'
  }

  parallel_get_objs() {
    local base="$1"
    # Fetch each object in parallel using its GUID appended to the base path.
    env BASE="$base" xargs -I{} -P 16 bash -c 'cf curl "$BASE/{}" 2>/dev/null' \
      | jq -s '[.[] | select(type=="object" and .guid != null)]'
  }

  parallel_get_binding_details_map() {
    # Build a JSON object mapping each GUID to its details.  We must prevent
    # the shell from expanding $g inside the jq program; use "\$g" so jq sees
    # the variable and not an empty string.
    xargs -I{} -P 16 bash -c '
      guid="{}"
      cf curl "/v3/service_credential_bindings/$guid/details" 2>/dev/null |
      jq -c --arg g "$guid" "{ (\$g): . }"
    ' | jq -s 'add // {}'
  }

  # app bindings
  app_jq_script=$(cat <<'JQAPP'
def safe_name(m; k): (m[0][k]? | objects | .name?) // "N/A";
def safe_val(m; k; f): (m[0][k]? | objects | .[f]?) // null;
.[]
| . as $b
| ($b.guid // "N/A") as $bid
| "app" as $btype
| ($b.name // "") as $bname
| ($b.relationships.service_instance.data.guid? // null) as $si
| ($b.relationships.app.data.guid?              // null) as $appg
| (safe_val($INST; $si; "plan_guid"))            as $plan_guid
| (safe_val($PLAN; $plan_guid; "offering_guid")) as $off_guid
| ($DET[0][$bid].credentials? // {}) as $creds
| ($creds.url // $creds.uri // $creds.connection // $creds.jdbcUrl // $creds.jdbc_url // "") as $uri
| [
    $BROKER,
    $btype,
    safe_name($OFFER; $off_guid),
    safe_name($PLAN;  $plan_guid),
    safe_name($INST;  $si),
    ($si // ""),
    $bid,
    $bname,
    safe_name($APP;   $appg),
    ($appg // ""),
    safe_name($SPACE; (safe_val($APP; $appg; "space_guid"))),
    (safe_val($APP; $appg; "space_guid") // ""),
    safe_name($ORG;   (safe_val($SPACE; (safe_val($APP; $appg; "space_guid")); "org_guid"))),
    (safe_val($SPACE; (safe_val($APP; $appg; "space_guid")); "org_guid") // ""),
    ($uri // ""),
    (if $creds=={} then "" else ($creds|tojson) end)
  ] | @tsv
JQAPP
)

  # key bindings
  key_jq_script=$(cat <<'JQKEY'
def safe_name(m; k): (m[0][k]? | objects | .name?) // "N/A";
def safe_val(m; k; f): (m[0][k]? | objects | .[f]?) // null;
.[]
| . as $b
| ($b.guid // "N/A") as $bid
| "key" as $btype
| ($b.name // "") as $bname
| ($b.relationships.service_instance.data.guid? // null) as $si
| (safe_val($INST; $si; "plan_guid"))            as $plan_guid
| (safe_val($PLAN; $plan_guid; "offering_guid")) as $off_guid
| ($DET[0][$bid].credentials? // {}) as $creds
| ($creds.url // $creds.uri // $creds.connection // $creds.jdbcUrl // $creds.jdbc_url // "") as $uri
| [
    $BROKER,
    $btype,
    safe_name($OFFER; $off_guid),
    safe_name($PLAN;  $plan_guid),
    safe_name($INST;  $si),
    ($si // ""),
    $bid,
    $bname,
    "",
    "",
    "",
    "",
    "",
    "",
    ($uri // ""),
    (if $creds=={} then "" else ($creds|tojson) end)
  ] | @tsv
JQKEY
)

  # unbound instances
  unbound_jq_script=$(cat <<'JQUNB'
def safe_name(m; k): (m[0][k]? | objects | .name?) // "N/A";
def safe_val(m; k; f): (m[0][k]? | objects | .[f]?) // null;
def bound_si_guids:
  (( $APPB[0] // [] ) + ( $KEYB[0] // [] ))
  | map(.relationships.service_instance.data.guid // empty)
  | unique;

(bound_si_guids) as $bound
| .[]
| . as $i
| ($i.guid // "N/A") as $si
| select( $bound | index($si) | not )
| "none" as $btype
| "" as $bname
| (safe_val($INST; $si; "plan_guid"))            as $plan_guid
| (safe_val($PLAN; $plan_guid; "offering_guid")) as $off_guid
| (safe_val($INST; $si; "space_guid"))           as $space_guid
| [
    $BROKER,
    $btype,
    safe_name($OFFER; $off_guid),
    safe_name($PLAN;  $plan_guid),
    safe_name($INST;  $si),
    ($si // ""),
    "",
    $bname,
    "",
    "",
    safe_name($SPACE; $space_guid),
    ($space_guid // ""),
    safe_name($ORG;   (safe_val($SPACE; $space_guid; "org_guid"))),
    (safe_val($SPACE; $space_guid; "org_guid") // ""),
    "",
    ""
  ] | @tsv
JQUNB
)

  brokers_json=$(fetch_all_pages_v3 "/v3/service_brokers")

  while IFS= read -r broker; do
    [[ -z "$broker" || "$broker" == "null" ]] && continue

    broker_name=$(jq -r '.name' <<<"$broker")
    broker_guid=$(jq -r '.guid' <<<"$broker")
    echo "  → Broker: $broker_name" >&2

    offerings="$(get_all "/v3/service_offerings?service_broker_guids=${broker_guid}")"
    if [[ "$(jq 'length' <<<"$offerings")" -eq 0 ]]; then
      continue
    fi
    offer_guids_csv="$(jq -r '.[].guid' <<<"$offerings" | paste -sd, -)"

    plans="$(get_all "/v3/service_plans?service_offering_guids=${offer_guids_csv}")"
    mapfile -t PLAN_GUIDS < <(jq -r '.[].guid' <<<"$plans")
    if ((${#PLAN_GUIDS[@]} == 0)); then
      continue
    fi

    instances="$(fetch_with_param_chunks "/v3/service_instances" "service_plan_guids" 50 "" "${PLAN_GUIDS[@]}")"
    if [[ "$(jq 'length' <<<"$instances")" -eq 0 ]]; then
      continue
    fi
    mapfile -t INSTANCE_GUIDS < <(jq -r '.[].guid' <<<"$instances")

    # Filter the global bindings down to instances belonging to this broker.
    if ((${#INSTANCE_GUIDS[@]})); then
      inst_filter_json=$(printf '%s\n' "${INSTANCE_GUIDS[@]}" \
        | jq -R -s -c 'split("\n") | map(select(length>0))')
    else
      inst_filter_json='[]'
    fi

    app_bindings="$(jq -c --argjson insts "$inst_filter_json" '
        .[]
        | select(.relationships.service_instance.data.guid as $g | $insts | index($g))
        | select(.type == "app")
      ' "$SERVICE_CRED_BINDINGS_FILE" | jq -s '[.[]]')"

    key_bindings="$(jq -c --argjson insts "$inst_filter_json" '
        .[]
        | select(.relationships.service_instance.data.guid as $g | $insts | index($g))
        | select(.type == "key")
      ' "$SERVICE_CRED_BINDINGS_FILE" | jq -s '[.[]]')"

    mapfile -t APP_GUIDS < <(jq -r '.[].relationships.app.data.guid' <<<"$app_bindings" | sort -u)
    apps='[]'
    if ((${#APP_GUIDS[@]})); then
      apps="$(printf '%s\n' "${APP_GUIDS[@]}" | parallel_get_objs "/v3/apps")"
    fi

    mapfile -t SPACE_GUIDS < <(
      {
        jq -r '.[].relationships.space.data.guid' <<<"$apps"
        jq -r '.[].relationships.space.data.guid' <<<"$instances"
      } | sort -u
    )
    spaces='[]'
    if ((${#SPACE_GUIDS[@]})); then
      spaces="$(printf '%s\n' "${SPACE_GUIDS[@]}" | parallel_get_objs "/v3/spaces")"
    fi

    mapfile -t ORG_GUIDS < <(jq -r '.[].relationships.organization.data.guid' <<<"$spaces" | sort -u)
    orgs='[]'
    if ((${#ORG_GUIDS[@]})); then
      orgs="$(printf '%s\n' "${ORG_GUIDS[@]}" | parallel_get_objs "/v3/organizations")"
    fi

    mapfile -t APP_BINDING_GUIDS < <(jq -r '.[].guid' <<<"$app_bindings")
    mapfile -t KEY_BINDING_GUIDS < <(jq -r '.[].guid' <<<"$key_bindings")

    app_detail_map='{}'
    if ((${#APP_BINDING_GUIDS[@]})); then
      app_detail_map="$(printf '%s\n' "${APP_BINDING_GUIDS[@]}" | parallel_get_binding_details_map)"
    fi
    key_detail_map='{}'
    if ((${#KEY_BINDING_GUIDS[@]})); then
      key_detail_map="$(printf '%s\n' "${KEY_BINDING_GUIDS[@]}" | parallel_get_binding_details_map)"
    fi

    offer_map="$(jq -c 'map({key:.guid, value:{name:.name}}) | from_entries' <<<"$offerings")"
    plan_map="$(jq -c 'map({key:.guid, value:{name:.name, offering_guid:.relationships.service_offering.data.guid}}) | from_entries' <<<"$plans")"
    inst_map="$(jq -c 'map({key:.guid, value:{name:.name, plan_guid:.relationships.service_plan.data.guid, space_guid:.relationships.space.data.guid}}) | from_entries' <<<"$instances")"
    app_map="$(jq -c 'map({key:.guid, value:{name:.name, space_guid:.relationships.space.data.guid}}) | from_entries' <<<"$apps")"
    space_map="$(jq -c 'map({key:.guid, value:{name:.name, org_guid:.relationships.organization.data.guid}}) | from_entries' <<<"$spaces")"
    org_map="$(jq -c 'map({key:.guid, value:{name:.name}}) | from_entries' <<<"$orgs")"

    tmpdir="$(mktemp -d)"
    offer_file="$tmpdir/offer.json"
    plan_file="$tmpdir/plan.json"
    inst_file="$tmpdir/inst.json"
    app_file="$tmpdir/app.json"
    space_file="$tmpdir/space.json"
    org_file="$tmpdir/org.json"
    det_app_file="$tmpdir/app_details.json"
    det_key_file="$tmpdir/key_details.json"
    app_bind_file="$tmpdir/app_bindings.json"
    key_bind_file="$tmpdir/key_bindings.json"

    printf '%s' "$offer_map"      >"$offer_file"
    printf '%s' "$plan_map"       >"$plan_file"
    printf '%s' "$inst_map"       >"$inst_file"
    printf '%s' "$app_map"        >"$app_file"
    printf '%s' "$space_map"      >"$space_file"
    printf '%s' "$org_map"        >"$org_file"
    printf '%s' "$app_detail_map" >"$det_app_file"
    printf '%s' "$key_detail_map" >"$det_key_file"
    printf '%s' "$app_bindings"   >"$app_bind_file"
    printf '%s' "$key_bindings"   >"$key_bind_file"

    # app bindings
    jq -r \
      --arg BROKER "$broker_name" \
      --slurpfile OFFER "$offer_file" \
      --slurpfile PLAN  "$plan_file" \
      --slurpfile INST  "$inst_file" \
      --slurpfile APP   "$app_file" \
      --slurpfile SPACE "$space_file" \
      --slurpfile ORG   "$org_file" \
      --slurpfile DET   "$det_app_file" \
      "$app_jq_script" <"$app_bind_file" \
    | while IFS=$'\t' read -r broker_name bt offer_name plan_name si_name si_guid binding_guid binding_name app_name app_guid space_name space_guid org_name org_guid cred_uri creds_json; do
        # Fill in missing space/org from v2 caches if jq could not resolve them.
        if { [[ -z "$space_name" || "$space_name" == "N/A" ]] || [[ -z "$org_name" || "$org_name" == "N/A" ]]; } && [[ -n "$space_guid" ]]; then
          so="$(get_space_org_names_safe "/v2/spaces/$space_guid")"
          IFS='|' read -r resolved_space resolved_org <<<"$so"
          [[ -n "$resolved_space" ]] && space_name="$resolved_space"
          [[ -n "$resolved_org"   ]] && org_name="$resolved_org"
        fi

        redacted_uri=$(redact_credentials "$cred_uri")
        redacted_creds=$(redact_credentials "$creds_json")
        csv_write_row "$SERVICE_BINDINGS_OUT" \
          "$broker_name" "$bt" "$offer_name" "$plan_name" \
          "$si_name" "$si_guid" "$binding_guid" "$binding_name" \
          "$app_name" "$app_guid" "$space_name" "$space_guid" \
          "$org_name" "$org_guid" "$redacted_uri" "$redacted_creds" \
          "$FOUNDATION_SLUG" "$BATCH_ID"
      done

    # key bindings
    jq -r \
      --arg BROKER "$broker_name" \
      --slurpfile OFFER "$offer_file" \
      --slurpfile PLAN  "$plan_file" \
      --slurpfile INST  "$inst_file" \
      --slurpfile DET   "$det_key_file" \
      "$key_jq_script" <"$key_bind_file" \
    | while IFS=$'\t' read -r broker_name bt offer_name plan_name si_name si_guid binding_guid binding_name app_name app_guid space_name space_guid org_name org_guid cred_uri creds_json; do
        if { [[ -z "$space_name" || "$space_name" == "N/A" ]] || [[ -z "$org_name" || "$org_name" == "N/A" ]]; } && [[ -n "$space_guid" ]]; then
          so="$(get_space_org_names_safe "/v2/spaces/$space_guid")"
          IFS='|' read -r resolved_space resolved_org <<<"$so"
          [[ -n "$resolved_space" ]] && space_name="$resolved_space"
          [[ -n "$resolved_org"   ]] && org_name="$resolved_org"
        fi

        redacted_uri=$(redact_credentials "$cred_uri")
        redacted_creds=$(redact_credentials "$creds_json")
        csv_write_row "$SERVICE_BINDINGS_OUT" \
          "$broker_name" "$bt" "$offer_name" "$plan_name" \
          "$si_name" "$si_guid" "$binding_guid" "$binding_name" \
          "$app_name" "$app_guid" "$space_name" "$space_guid" \
          "$org_name" "$org_guid" "$redacted_uri" "$redacted_creds" \
          "$FOUNDATION_SLUG" "$BATCH_ID"
      done

    # unbound instances
    jq -r \
      --arg BROKER "$broker_name" \
      --slurpfile OFFER "$offer_file" \
      --slurpfile PLAN  "$plan_file" \
      --slurpfile INST  "$inst_file" \
      --slurpfile SPACE "$space_file" \
      --slurpfile ORG   "$org_file" \
      --slurpfile APPB  "$app_bind_file" \
      --slurpfile KEYB  "$key_bind_file" \
      "$unbound_jq_script" <<<"$instances" \
    | while IFS=$'\t' read -r broker_name bt offer_name plan_name si_name si_guid binding_guid binding_name app_name app_guid space_name space_guid org_name org_guid cred_uri creds_json; do
        # For truly unbound instances we always have an originating space; resolve
        # human‑readable names from the v2 caches if jq did not find them.
        if { [[ -z "$space_name" || "$space_name" == "N/A" ]] || [[ -z "$org_name" || "$org_name" == "N/A" ]]; } && [[ -n "$space_guid" ]]; then
          so="$(get_space_org_names_safe "/v2/spaces/$space_guid")"
          IFS='|' read -r resolved_space resolved_org <<<"$so"
          [[ -n "$resolved_space" ]] && space_name="$resolved_space"
          [[ -n "$resolved_org"   ]] && org_name="$resolved_org"
        fi

        redacted_uri=$(redact_credentials "$cred_uri")
        redacted_creds=$(redact_credentials "$creds_json")
        csv_write_row "$SERVICE_BINDINGS_OUT" \
          "$broker_name" "$bt" "$offer_name" "$plan_name" \
          "$si_name" "$si_guid" "$binding_guid" "$binding_name" \
          "$app_name" "$app_guid" "$space_name" "$space_guid" \
          "$org_name" "$org_guid" "$redacted_uri" "$redacted_creds" \
          "$FOUNDATION_SLUG" "$BATCH_ID"
      done

    rm -rf "$tmpdir"
  done < <(jq -c '.[]' <<<"$brokers_json")
fi

echo "Data collection complete." >&2
echo "Output:" >&2
echo "  app_data:             $APP_DATA_OUT" >&2
[[ -n "$SERVICE_DATA_OUT" ]]     && echo "  service_data:         $SERVICE_DATA_OUT" >&2
[[ -n "$DEVELOPER_DATA_OUT" ]]   && echo "  developer_space_data: $DEVELOPER_DATA_OUT" >&2
[[ -n "$JAVA_RUNTIME_OUT" ]]     && echo "  java_runtime_data:    $JAVA_RUNTIME_OUT" >&2
[[ -n "$AUDIT_EVENTS_OUT" ]]     && echo "  audit_events:         $AUDIT_EVENTS_OUT" >&2
[[ -n "$SERVICE_BINDINGS_OUT" ]] && echo "  service_bindings:     $SERVICE_BINDINGS_OUT" >&2