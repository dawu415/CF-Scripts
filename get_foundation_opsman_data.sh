#!/usr/bin/env bash
#
# get_opsman_tiles_stemcells_bosh.sh
#
# Collect per-foundation Ops Manager + Director + tile + stemcell inventory.
# Designed to work with Run-EnvOrchestrator.ps1 ("Porchini") and the
# get_foundation_data.sh conventions:
#
#   - If CF_ORCH_DATA_MODE=multi, CF_ORCH_DATA_OUT is a *base directory*.
#   - Otherwise, CF_ORCH_DATA_OUT is the CSV file path.
#
# Outputs:
#   <base>/opsman_inventory.csv                - one row per deployed product
#   <base>/<foundation-slug>_diagnostic_report.json - OpsMan diagnostic report
#
set -Eeuo pipefail
[ -n "${BASH_VERSION:-}" ] || exec /usr/bin/env bash "$0" "$@"

export TERM=dumb
IFS=$' \t\n'
trap 'ec=$?; set +u; ts=$(date "+%F %T" 2>/dev/null || printf N/A); echo "[$ts] ERROR ${ec:-1} at ${BASH_SOURCE[0]}:${LINENO}: ${BASH_COMMAND:-?}" >&2; exit "${ec:-1}"' ERR

# ---- Requirements ----
command -v om   >/dev/null 2>&1 || { echo "om CLI not found in PATH" >&2; exit 6; }
command -v jq   >/dev/null 2>&1 || { echo "jq not found in PATH" >&2; exit 6; }
command -v flock>/dev/null 2>&1 || { echo "flock not found in PATH" >&2; exit 6; }

# ---- Inputs via env (Porchini / orchestrator) ----
: "${OM_TARGET:?Set OM_TARGET (e.g., https://opsman.example.com)}"

# Auth: either OM_USERNAME/OM_PASSWORD OR OM_CLIENT_ID/OM_CLIENT_SECRET
if [[ -z "${OM_USERNAME:-}" || -z "${OM_PASSWORD:-}" ]]; then
  if [[ -z "${OM_CLIENT_ID:-}" || -z "${OM_CLIENT_SECRET:-}" ]]; then
    echo "Provide OM_USERNAME/OM_PASSWORD or OM_CLIENT_ID/OM_CLIENT_SECRET" >&2
    exit 7
  fi
fi

FOUNDATION="${CF_ORCH_PLATFORM:-${CF_FOUNDATION:-unknown}}"
ORCH_MODE="${CF_ORCH_DATA_MODE:-multi}"
BASE_OUT="${CF_ORCH_DATA_OUT:-./opsman_inventory.csv}"

# ---- Resolve CSV path based on CF_ORCH_DATA_MODE semantics ----
if [[ "$ORCH_MODE" == "multi" ]]; then
  # In multi-table mode, CF_ORCH_DATA_OUT is a *directory* base path.
  base_dir="${BASE_OUT%/}"
  mkdir -p "$base_dir"
  OPS_CSV_PATH="${base_dir}/opsman_inventory.csv"
else
  # Single-table mode: CF_ORCH_DATA_OUT is the CSV file path.
  OPS_CSV_PATH="$BASE_OUT"
  mkdir -p "$(dirname -- "$OPS_CSV_PATH")"
fi

# Foundation-safe slug for filenames (for diagnostic report)
foundation_slug="$(printf '%s' "$FOUNDATION" | tr '[:upper:] ' '[:lower:]-' | tr -cd 'a-z0-9._-')"
diag_dir="$(dirname -- "$OPS_CSV_PATH")"
diagnostic_path="${diag_dir}/${foundation_slug}_diagnostic_report.json"

# ---- CSV helpers (aligned with get_foundation_data style) ----
CSV_HEADER=(foundation opsman_version director_tile_version director_bosh_release_version product_type product_version stemcell_os stemcell_version)

csv_cell() {
  local s="${1//$'\r'/ }"; s="${s//$'\n'/ }"; s="${s//$'\t'/ }"
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
  local line="" sep=""
  for a in "$@"; do
    line+="$sep$(csv_cell "$a")"
    sep=","
  done
  printf '%s\n' "$line"
}

# File handle 200 for flock'ed append
csv_open() {
  exec 200>>"$OPS_CSV_PATH"
  flock -x 200
  if [[ ! -s "$OPS_CSV_PATH" || "${CF_ORCH_FORCE_HEADER:-0}" == 1 ]]; then
    csv_row "${CSV_HEADER[@]}" >&200
  fi
  flock -u 200
}

csv_emit() {
  { : >&200; } 2>/dev/null || csv_open
  flock -x 200
  csv_row "$@" >&200
  flock -u 200
}

# ---- Collect: Ops Manager version ----
INFO_JSON="$(om curl -s -p /api/v0/info | jq -rS .)"
OPSMAN_VERSION="$(printf '%s' "$INFO_JSON" | jq -r '.info.version // .version // empty')"
OPSMAN_VERSION="${OPSMAN_VERSION:-N/A}"

# ---- Collect: Deployed products (tiles) ----
DEPLOYED_JSON="$(om curl -s -p /api/v0/deployed/products | jq -rS .)"

# ---- Collect: Director tile version (p-bosh product_version) ----
DIRECTOR_TILE_VERSION="$(
  jq -r '[.[] | select(.type=="p-bosh") | .product_version][0] // empty' <<<"$DEPLOYED_JSON"
)"
DIRECTOR_TILE_VERSION="${DIRECTOR_TILE_VERSION:-N/A}"

# ---- Collect: Director BOSH release version (from director manifest) ----
DIRECTOR_MANIFEST="$(om curl -s -p /api/v0/deployed/director/manifest || true)"
DIRECTOR_BOSH_RELEASE_VERSION="N/A"
if [[ -n "$DIRECTOR_MANIFEST" ]]; then
  DIRECTOR_BOSH_RELEASE_VERSION="$(
    awk '
      BEGIN{inrel=0; inbosh=0}
      $1=="releases:" {inrel=1; next}
      inrel && $0 ~ /- *name: *bosh/ {inbosh=1; next}
      inrel && inbosh && $0 ~ /version:/ {
        for(i=1;i<=NF;i++){
          if ($i ~ /^version:/){
            ver=$(i+1); gsub(/["'\'']/, "", ver); print ver; exit
          }
        }
      }
    ' <<<"$DIRECTOR_MANIFEST"
  )"
  DIRECTOR_BOSH_RELEASE_VERSION="${DIRECTOR_BOSH_RELEASE_VERSION:-N/A}"
fi

# ---- Collect: Stemcell assignments per product (by GUID) ----
# /api/v0/stemcell_assignments => { products: [ {guid, deployed_stemcell:{os,version}} ] }
STEM_ASSIGN_JSON="$(om curl -s -p /api/v0/stemcell_assignments | jq -rS .)"
STEM_MAP="$(
  printf '%s' "$STEM_ASSIGN_JSON" \
  | jq -r '((.products // []) |
            map({key:.guid,
                 value:{os:(.deployed_stemcell.os // "N/A"),
                        ver:(.deployed_stemcell.version // "N/A")}})) | from_entries'
)"

# ---- Emit CSV rows (one per deployed product) ----
if [[ "$(jq 'length' <<<"$DEPLOYED_JSON")" -gt 0 ]]; then
  jq -c '.[] | {guid,type,product_version}' <<<"$DEPLOYED_JSON" | while read -r row; do
    guid="$(jq -r '.guid // empty' <<<"$row")"
    t="$(jq -r '.type // "N/A"' <<<"$row")"
    v="$(jq -r '.product_version // "N/A"' <<<"$row")"
    os="$(jq -r --arg g "$guid" '.[$g].os  // "N/A"' <<<"$STEM_MAP")"
    sv="$(jq -r --arg g "$guid" '.[$g].ver // "N/A"' <<<"$STEM_MAP")"
    csv_emit "$FOUNDATION" "$OPSMAN_VERSION" "$DIRECTOR_TILE_VERSION" "$DIRECTOR_BOSH_RELEASE_VERSION" "$t" "$v" "$os" "$sv"
  done
else
  # No products — still emit a single row so foundation info is captured
  csv_emit "$FOUNDATION" "$OPSMAN_VERSION" "$DIRECTOR_TILE_VERSION" "$DIRECTOR_BOSH_RELEASE_VERSION" "N/A" "N/A" "N/A" "N/A"
fi

# ---- Save diagnostic report JSON next to the CSV (for Porchini to pull) ----
tmp_diag="$(mktemp "${diagnostic_path}.XXXX" 2>/dev/null || mktemp)"
if om curl -s -p /api/v0/diagnostic_report >"$tmp_diag"; then
  mv -f "$tmp_diag" "$diagnostic_path"
  echo "Saved diagnostic report: ${diagnostic_path}"
else
  echo "WARN: failed to download diagnostic report; leaving no file" >&2
  rm -f "$tmp_diag" || true
fi
