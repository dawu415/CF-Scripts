#!/usr/bin/env bash
set -Eeuo pipefail
[ -n "${BASH_VERSION:-}" ] || exec /usr/bin/env bash "$0" "$@"

export TERM=dumb
IFS=$' \t\n'
trap 'ec=$?; set +u; ts=$(date "+%F %T" 2>/dev/null || printf N/A); echo "[$ts] ERROR ${ec:-1} at ${BASH_SOURCE[0]}:${LINENO}: ${BASH_COMMAND:-?}" >&2; exit "${ec:-1}"' ERR

# ---- Requirements ----
command -v om >/dev/null 2>&1 || { echo "om CLI not found in PATH" >&2; exit 6; }
command -v jq >/dev/null 2>&1 || { echo "jq not found in PATH" >&2; exit 6; }
command -v flock >/dev/null 2>&1 || { echo "flock not found in PATH" >&2; exit 6; }

# ---- Inputs via env (OM_* are required; read directly by `om`) ----
: "${OM_TARGET:?Set OM_TARGET (e.g., https://opsman.example.com)}"
# Must supply one auth mode:
if [[ -z "${OM_USERNAME:-}" || -z "${OM_PASSWORD:-}" ]]; then
  if [[ -z "${OM_CLIENT_ID:-}" || -z "${OM_CLIENT_SECRET:-}" ]]; then
    echo "Provide OM_USERNAME/OM_PASSWORD or OM_CLIENT_ID/OM_CLIENT_SECRET" >&2
    exit 7
  fi
fi
FOUNDATION="${FOUNDATION:-${CF_ORCH_ENV:-unknown}}"

# ---- CSV helpers ----
CSV_HEADER=(foundation opsman_version director_tile_version director_bosh_release_version product_type product_version stemcell_os stemcell_version)
csv_cell() {
  local s="${1//$'\r'/ }"; s="${s//$'\n'/ }"; s="${s//$'\t'/ }"
  case "$s" in *[\",]*|*" "*) s="${s//\"/\"\"}"; printf '"%s"' "$s" ;; *) printf '%s' "$s" ;; esac
}
csv_row(){ local line="" sep=""; for a in "$@"; do line+="$sep$(csv_cell "$a")"; sep=","; done; printf '%s\n' "$line"; }
csv_open() {
  [[ -n "${CF_ORCH_DATA_OUT:-}" ]] || return 0
  exec 200>>"$CF_ORCH_DATA_OUT"
  flock -x 200
  if [[ ! -s "$CF_ORCH_DATA_OUT" || "${CF_ORCH_FORCE_HEADER:-0}" == 1 ]]; then
    csv_row "${CSV_HEADER[@]}" >&200
  fi
  flock -u 200
}
csv_emit() {
  if [[ -n "${CF_ORCH_DATA_OUT:-}" ]]; then
    { : >&200; } 2>/dev/null || csv_open
    flock -x 200; csv_row "$@" >&200; flock -u 200
  else
    csv_row "$@"
  fi
}

# ---- Ops Manager version ----
INFO_JSON="$(om curl -s -p /api/v0/info | jq -rS .)"
OPSMAN_VERSION="$(printf '%s' "$INFO_JSON" | jq -r '.info.version // .version // empty')"
OPSMAN_VERSION="${OPSMAN_VERSION:-N/A}"

# ---- Deployed products (tiles) ----
DEPLOYED_JSON="$(om curl -s -p /api/v0/deployed/products | jq -rS .)"

# ---- Director tile version (p-bosh product_version) ----
DIRECTOR_TILE_VERSION="$(jq -r '[.[] | select(.type=="p-bosh") | .product_version][0] // empty' <<<"$DEPLOYED_JSON")"
DIRECTOR_TILE_VERSION="${DIRECTOR_TILE_VERSION:-N/A}"

# ---- Director bosh release version (from deployed director manifest YAML) ----
# Parse the version of the "bosh" release from the manifest's 'releases' list.
# No extra deps (yq/ruby) required; use a small awk that scans the releases block.
DIRECTOR_MANIFEST="$(om curl -s -p /api/v0/deployed/director/manifest || true)"
DIRECTOR_BOSH_RELEASE_VERSION="N/A"
if [[ -n "$DIRECTOR_MANIFEST" ]]; then
  DIRECTOR_BOSH_RELEASE_VERSION="$(
    awk '
      BEGIN{inrel=0; inbosh=0}
      $1=="releases:" {inrel=1; next}
      inrel && $0 ~ /- *name: *bosh/ {inbosh=1; next}
      inrel && inbosh && $0 ~ /version:/ {
        # field after "version:"; strip quotes
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

# ---- Stemcell assignments per product (by GUID) ----
# Structure typically: { "products": [ { "guid": "...", "identifier": "...",
#   "deployed_stemcell": { "os": "ubuntu-jammy", "version": "1.628" } }, ... ] }
STEM_ASSIGN_JSON="$(om curl -s -p /api/v0/stemcell_assignments | jq -rS .)"
STEM_MAP="$(printf '%s' "$STEM_ASSIGN_JSON" \
  | jq -r '((.products // []) | map({key:.guid, value:{os:(.deployed_stemcell.os // "N/A"), ver:(.deployed_stemcell.version // "N/A")}})) | from_entries'
)"

# ---- Emit CSV ----
csv_open

# If there are deployed products, emit per-product rows with stemcells.
if [[ "$(jq 'length' <<<"$DEPLOYED_JSON")" -gt 0 ]]; then
  jq -c '.[] | {guid,type,product_version}' <<<"$DEPLOYED_JSON" | while read -r row; do
    guid="$(jq -r '.guid // empty' <<<"$row")"
    t="$(jq -r '.type // "N/A"' <<<"$row")"
    v="$(jq -r '.product_version // "N/A"' <<<"$row")"
    os="$(jq -r --arg g "$guid" '.[$g].os // "N/A"' <<<"$STEM_MAP")"
    sv="$(jq -r --arg g "$guid" '.[$g].ver // "N/A"' <<<"$STEM_MAP")"
    csv_emit "$FOUNDATION" "$OPSMAN_VERSION" "$DIRECTOR_TILE_VERSION" "$DIRECTOR_BOSH_RELEASE_VERSION" "$t" "$v" "$os" "$sv"
  done
else
  # No products—still output a single row to record foundation-level versions.
  csv_emit "$FOUNDATION" "$OPSMAN_VERSION" "$DIRECTOR_TILE_VERSION" "$DIRECTOR_BOSH_RELEASE_VERSION" "N/A" "N/A" "N/A" "N/A"
fi

