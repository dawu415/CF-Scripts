#!/usr/bin/env bash
set -Eeuo pipefail
# Ensure we are running under bash (not sh) even if invoked incorrectly
[ -n "${BASH_VERSION:-}" ] || exec /usr/bin/env bash "$0" "$@"

# ---------- robust error trap ----------
on_error() {
  local ec=$?
  # Don't let missing vars or failing 'date' tear down the trap itself
  set +u
  local ts
  ts=$(date "+%F %T" 2>/dev/null) || ts="N/A"

  local src="$0"
  if [ -n "${BASH_SOURCE-}" ] && [ "${#BASH_SOURCE[@]}" -gt 0 ]; then
    src="${BASH_SOURCE[0]}"
  fi
  local fn="main"
  if [ -n "${FUNCNAME-}" ] && [ "${#FUNCNAME[@]}" -gt 0 ]; then
    fn="${FUNCNAME[0]}"
  fi

  local cmd="${BASH_COMMAND:-N/A}"
  echo "[$ts] ERROR ${ec:-1} at ${src}:${LINENO}: ${fn}: ${cmd}" >&2
  exit "${ec:-1}"
}
trap on_error ERR

# ---------- temp workspace for caches (prevents huge environments) ----------
TMP_ROOT="${TMPDIR:-/tmp}"
WORK_DIR="$(mktemp -d "${TMP_ROOT%/}/cf_collect.XXXXXX")"
cleanup() { rm -rf "$WORK_DIR"; }
trap cleanup EXIT

# -----------------------------------------------------------------------------
# PERFORMANCE NOTE
#
# - Large JSONs (spaces/orgs/stacks/buildpacks) are cached on disk; only file
#   paths are exported, preventing E2BIG (Argument list too long).
# - v2 and v3 pagination helpers accumulate .resources across pages.
# - Droplet/package sizes are collected (required for your analysis).
# -----------------------------------------------------------------------------

# ---------- helpers: pagination ----------
fetch_all_pages_v2() {
  # Walk /v2 pages by following .next_url; cap 100 per page
  local base_path="$1"
  local url
  if [[ "$base_path" == *"?"* ]]; then
    url="${base_path}&results-per-page=100"
  else
    url="${base_path}?results-per-page=100"
  fi
  local acc='{ "resources": [] }'
  local resp next_url
  while [[ -n "$url" ]]; do
    resp=$(cf curl "$url" 2>/dev/null || echo '{}')
    acc=$(jq --argjson res "$resp" '.resources += ($res.resources // [])' <<<"$acc")
    next_url=$(echo "$resp" | jq -r '.next_url // empty')
    url="$next_url"
  done
  echo "$acc"
}

fetch_all_pages_v3() {
  # Walk /v3 pages by following .pagination.next.href; caller sets per_page
  local url="$1"
  local acc='{ "resources": [] }'
  local resp next_url
  while [[ -n "$url" ]]; do
    resp=$(cf curl "$url" 2>/dev/null || echo '{}')
    acc=$(jq --argjson res "$resp" '.resources += ($res.resources // [])' <<<"$acc")
    next_url=$(echo "$resp" | jq -r '.pagination.next.href // empty')
    url="$next_url"
  done
  echo "$acc"
}

export -f fetch_all_pages_v2
export -f fetch_all_pages_v3

# ---------- buildpack cache (file, not env) ----------
BUILDPACKS_JSON_FILE="$WORK_DIR/buildpacks.json"
cf curl "/v2/buildpacks?results-per-page=100" 2>/dev/null >"$BUILDPACKS_JSON_FILE" || echo '{}' >"$BUILDPACKS_JSON_FILE"

get_buildpack_filename() {
  # Args: $1 GUID or name, $2 optional stack name (case-insensitive)
  local bp_key="$1"
  local stack_name="${2:-}"
  if [[ "$bp_key" =~ ^[0-9a-fA-F-]{36}$ ]]; then
    jq -r --arg guid "$bp_key" \
      '.resources[]? | select(.metadata.guid == $guid) | .entity.filename // empty' \
      "$BUILDPACKS_JSON_FILE" | head -n 1
  else
    if [[ -z "$stack_name" || "$stack_name" == "null" ]]; then
      jq -r --arg name "$bp_key" \
        '.resources[]? | select(.entity.name == $name) | .entity.filename // empty' \
        "$BUILDPACKS_JSON_FILE" | head -n 1
    else
      jq -r --arg name "$bp_key" \
        '.resources[]? | select(.entity.name == $name) | .entity.filename // empty' \
        "$BUILDPACKS_JSON_FILE" | grep -i "$stack_name" | head -n 1
    fi
  fi
}
export -f get_buildpack_filename

# ---------- spaces / orgs / stacks caches (files, not env) ----------
SPACES_JSON_FILE="$WORK_DIR/spaces.json"
ORGS_JSON_FILE="$WORK_DIR/orgs.json"
STACKS_JSON_FILE="$WORK_DIR/stacks.json"
fetch_all_pages_v2 "/v2/spaces"        >"$SPACES_JSON_FILE"
fetch_all_pages_v2 "/v2/organizations" >"$ORGS_JSON_FILE"
fetch_all_pages_v2 "/v2/stacks"        >"$STACKS_JSON_FILE"
export SPACES_JSON_FILE ORGS_JSON_FILE STACKS_JSON_FILE

# ---------- version/runtime detection ----------
get_version_info() {
  local app_guid=$1
  local detected_buildpack=$2
  local buildpack_filename=$3

  local env_data
  env_data=$(cf curl "/v2/apps/${app_guid}/env" 2>/dev/null || echo "{}")

  # Extract X.Y[.Z] from ...-vX.Y.Z.zip using bash regex (portable)
  local buildpack_version=""
  if [[ -n "$buildpack_filename" && "$buildpack_filename" =~ -v([0-9]+(\.[0-9]+)*) ]]; then
    buildpack_version="${BASH_REMATCH[1]}"
  fi

  local runtime_version=""
  if [[ "$detected_buildpack" == *"java"* || "$detected_buildpack" == *"Java"* ]]; then
    runtime_version=$(echo "$env_data" | jq -r '.environment_json.JAVA_VERSION // empty' 2>/dev/null)
    if [[ -z "$runtime_version" || "$runtime_version" == "null" ]]; then
      local jbp_config
      jbp_config=$(echo "$env_data" | jq -r '.environment_json.JBP_CONFIG_OPEN_JDK_JRE // empty' 2>/dev/null)
      if [[ -n "$jbp_config" && "$jbp_config" != "null" ]]; then
        # Keep your original grep -P for this one (works well here)
        runtime_version=$(echo "$jbp_config" | grep -oP 'version:\s*["\047]?\K[^"\047,}]+' | head -1)
      fi
    fi
  elif [[ "$detected_buildpack" == *"node"* || "$detected_buildpack" == *"Node"* ]]; then
    runtime_version=$(echo "$env_data" | jq -r '.environment_json.NODE_VERSION // empty' 2>/dev/null)
  elif [[ "$detected_buildpack" == *"python"* || "$detected_buildpack" == *"Python"* ]]; then
    runtime_version=$(echo "$env_data" | jq -r '.environment_json.PYTHON_VERSION // empty' 2>/dev/null)
  fi

  [[ -z "$buildpack_version" || "$buildpack_version" == "null" ]] && buildpack_version=""
  [[ -z "$runtime_version"   || "$runtime_version"   == "null" ]] && runtime_version=""

  echo "${buildpack_version}|${runtime_version}"
}
export -f get_version_info

# ---------- CSV header ----------
echo "Org_Name,Space_Name,Created_At,Updated_At,Name,GUID,Instances,Memory,Disk_Quota,Requested_Buildpack,Detected_Buildpack,Detected_Buildpack_GUID,Buildpack_Filename,Buildpack_Version,Runtime_Version,DropletSizeBytes,PackagesSizeBytes,HealthCheckType,App_State,Stack_Name,Services,Routes,Developers,Detected_Start_Command"

# ---------- per-app processing ----------
process_app() {
  local app="$1"

  # Basic metadata
  local name app_guid instances memory disk_quota buildpack detected_buildpack detected_buildpack_guid
  name=$(echo "$app" | jq -r '.entity.name // empty')
  app_guid=$(echo "$app" | jq -r '.metadata.guid')
  instances=$(echo "$app" | jq -r '.entity.instances // 0')
  memory=$(echo "$app" | jq -r '.entity.memory // 0')
  disk_quota=$(echo "$app" | jq -r '.entity.disk_quota // 0')
  buildpack=$(echo "$app" | jq -r '.entity.buildpack // empty')
  detected_buildpack=$(echo "$app" | jq -r '.entity.detected_buildpack // empty')
  detected_buildpack_guid=$(echo "$app" | jq -r '.entity.detected_buildpack_guid // empty')

  # Droplet & package sizes (HEAD only)
  local droplet_size_bytes="" packages_size_bytes=""
  local droplet_download_link packages_download_link
  droplet_download_link=$(cf curl "/v3/apps/${app_guid}/droplets" | jq -r '.resources // [] | .[0].links.download.href // empty' | sed 's|http[s]*://[^/]*||')
  packages_download_link=$(cf curl "/v3/apps/${app_guid}/packages" | jq -r '.resources // [] | .[0].links.download.href // empty' | sed 's|http[s]*://[^/]*||')
  if [[ -n "$droplet_download_link" ]]; then
    droplet_size_bytes=$(cf curl -X HEAD -v "$droplet_download_link" 2>&1 | grep -i 'Content-Length:' | awk '{print $2}' | tr -d '\r' || true)
  fi
  if [[ -n "$packages_download_link" ]]; then
    packages_size_bytes=$(cf curl -X HEAD -v "$packages_download_link" 2>&1 | grep -i 'Content-Length:' | awk '{print $2}' | tr -d '\r' || true)
  fi

  # Health and state
  local health_check app_state
  health_check=$(echo "$app" | jq -r '.entity.health_check_type // empty')
  app_state=$(echo "$app" | jq -r '.entity.state // empty')

  # Stack (from cached file)
  local stack_url stack_guid stack_name
  stack_url=$(echo "$app" | jq -r '.entity.stack_url // empty')
  stack_guid="${stack_url##*/}"
  stack_name=$(jq -r --arg gid "$stack_guid" \
                 '.resources[]? | select(.metadata.guid == $gid) | .entity.name // empty' \
                 "$STACKS_JSON_FILE")

  # Space & org names (from cached files)
  local space_url space_guid space_name org_guid org_name
  space_url=$(echo "$app" | jq -r '.entity.space_url // empty')
  space_guid="${space_url##*/}"
  space_name=$(jq -r --arg gid "$space_guid" \
                 '.resources[]? | select(.metadata.guid == $gid) | .entity.name // empty' \
                 "$SPACES_JSON_FILE")
  org_guid=$(jq -r --arg gid "$space_guid" \
               '.resources[]? | select(.metadata.guid == $gid) | .entity.organization_guid // empty' \
               "$SPACES_JSON_FILE")
  org_name=$(jq -r --arg gid "$org_guid" \
               '.resources[]? | select(.metadata.guid == $gid) | .entity.name // empty' \
               "$ORGS_JSON_FILE")

  local created_at updated_at detected_start_command
  created_at=$(echo "$app" | jq -r '.metadata.created_at // empty')
  updated_at=$(echo "$app" | jq -r '.metadata.updated_at // empty')
  detected_start_command=$(echo "$app" | jq -r '.entity.detected_start_command // empty')

  # Buildpack filename from cached file
  local buildpack_filename input
  if [[ "$detected_buildpack_guid" != "null" && -n "$detected_buildpack_guid" ]]; then
    buildpack_filename=$(get_buildpack_filename "$detected_buildpack_guid")
  else
    input="${detected_buildpack:-$buildpack}"
    buildpack_filename=$(get_buildpack_filename "$input" "$stack_name")
  fi

  local version_info buildpack_version runtime_version
  version_info=$(get_version_info "$app_guid" "$detected_buildpack" "$buildpack_filename")
  IFS='|' read -r buildpack_version runtime_version <<< "$version_info"

  # Routes & services from summary
  local summary_json routes services
  summary_json=$(cf curl "/v2/apps/${app_guid}/summary" 2>/dev/null || echo '{}')
  routes=$(echo "$summary_json" | jq -r '.routes // [] | .[] | "\(.host).\(.domain.name)"' | paste -sd ':' -)

  # Memoize managed services across apps; UPS per space only
  declare -gA MANAGED_SERVICE_CACHE
  declare -gA UPS_SERVICE_CACHE
  local -a services_list=()
  mapfile -t __services_data < <(echo "$summary_json" | jq -c \
    '.services // [] | .[] | {guid: .guid, type: .type, label: (.service_plan.service.label // ""), plan: (.service_plan.name // ""), name: .name}')

  local svc service_guid service_type service_string label plan name up_key
  for svc in "${__services_data[@]:-}"; do
    service_guid=$(echo "$svc" | jq -r '.guid')
    service_type=$(echo "$svc" | jq -r '.type')
    if [[ "$service_type" != "user_provided_service_instance" ]]; then
      if [[ -n "${MANAGED_SERVICE_CACHE[$service_guid]+set}" ]]; then
        service_string="${MANAGED_SERVICE_CACHE[$service_guid]}"
      else
        label=$(echo "$svc" | jq -r '.label')
        plan=$(echo "$svc" | jq -r '.plan')
        service_string="$label ($plan)-($service_guid)"
        MANAGED_SERVICE_CACHE[$service_guid]="$service_string"
      fi
    else
      up_key="${space_guid}:${service_guid}"
      if [[ -n "${UPS_SERVICE_CACHE[$up_key]+set}" ]]; then
        service_string="${UPS_SERVICE_CACHE[$up_key]}"
      else
        name=$(echo "$svc" | jq -r '.name')
        service_string="$name (user provided service)-($service_guid)"
        UPS_SERVICE_CACHE[$up_key]="$service_string"
      fi
    fi
    services_list+=("$service_string")
  done
  if (( ${#services_list[@]} > 0 )); then
    services=$(printf "%s:" "${services_list[@]}"); services="${services%:}"
  else
    services=""
  fi
  routes=${routes:-""}; services=${services:-""}

  # Developers on the space
  local dev_usernames
  dev_usernames=$(cf curl "${space_url}/developers" | jq -r \
                     '.resources // [] | .[] | .entity | select(.username != null) | .username' \
                   | paste -sd ':' -)


  echo "$org_name,$space_name,$created_at,$updated_at,$name,$app_guid,$instances,$memory,$disk_quota,$buildpack,$detected_buildpack,$detected_buildpack_guid,$buildpack_filename,$buildpack_version,$runtime_version,$droplet_size_bytes,$packages_size_bytes,$health_check,$app_state,$stack_name,$services,$routes,$dev_usernames,$detected_start_command"
}
export -f process_app

# ---------- drive the listing ----------
total_pages=$(cf curl "/v2/apps?results-per-page=100" | jq '.total_pages // 1')

for i in $(seq 1 "$total_pages"); do
  if command -v parallel >/dev/null 2>&1; then
    cf curl "/v2/apps?page=$i&results-per-page=100" \
      | jq -c '.resources // [] | .[]' \
      | parallel -j 10 process_app
  else
    cf curl "/v2/apps?page=$i&results-per-page=100" \
      | jq -c '.resources // [] | .[]' \
      | while IFS= read -r app; do printf '%s\0' "$app"; done \
      | xargs -0 -P 6 -I {} bash -lc 'process_app "$@"' _ {}
  fi
done
