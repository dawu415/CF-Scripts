#!/usr/bin/env bash
set -Eeuo pipefail
[ -n "${BASH_VERSION:-}" ] || exec /usr/bin/env bash "$0" "$@"

# Non-interactive behavior; avoid profile noise & tty warnings
export CF_COLOR=false CF_TRACE=false TERM=dumb
stty -g >/dev/null 2>&1 || true
IFS=$' \t\n'

trap 'ec=$?; set +u;
      ts=$(date "+%F %T" 2>/dev/null || printf N/A);
      src="${BASH_SOURCE[0]:-$0}"; fn="${FUNCNAME[0]:-main}";
      echo "[$ts] ERROR ${ec:-1} at ${src}:${LINENO}: ${fn}: ${BASH_COMMAND:-?}" >&2;
      exit "${ec:-1}"' ERR

# ----------------------------- helpers -----------------------------

fetch_all_pages_v2() {
  local base="$1" url acc resp next_url
  if [[ "$base" == *"?"* ]]; then url="${base}&results-per-page=100"; else url="${base}?results-per-page=100"; fi
  acc='{ "resources": [] }'
  while [[ -n "$url" ]]; do
    resp=$(cf curl "$url" 2>/dev/null || echo '{}')
    acc=$(jq --argjson res "$resp" '.resources += ($res.resources // [])' <<<"$acc")
    next_url=$(jq -r '.next_url // empty' <<<"$resp")
    url="$next_url"
  done
  echo "$acc"
}

fetch_all_pages_v3() {
  local url="$1" acc resp next_url
  acc='{ "resources": [] }'
  while [[ -n "$url" ]]; do
    resp=$(cf curl "$url" 2>/dev/null || echo '{}')
    acc=$(jq --argjson res "$resp" '.resources += ($res.resources // [])' <<<"$acc")
    next_url=$(jq -r '.pagination.next.href // empty' <<<"$resp")
    url="$next_url"
  done
  echo "$acc"
}

# Cache heavy catalogs to files so every worker can read them safely
TMP_ROOT="${TMPDIR:-/tmp}"
WORK_DIR="$(mktemp -d "${TMP_ROOT%/}/cf_collect.XXXXXX")"
cleanup(){ rm -rf "$WORK_DIR"; }
trap cleanup EXIT

BUILDPACKS_JSON_FILE="$WORK_DIR/buildpacks.json"
SPACES_JSON_FILE="$WORK_DIR/spaces.json"
ORGS_JSON_FILE="$WORK_DIR/orgs.json"
STACKS_JSON_FILE="$WORK_DIR/stacks.json"

cf curl "/v2/buildpacks?results-per-page=100" 2>/dev/null >"$BUILDPACKS_JSON_FILE" || echo '{}' >"$BUILDPACKS_JSON_FILE"
fetch_all_pages_v2 "/v2/spaces"        >"$SPACES_JSON_FILE"
fetch_all_pages_v2 "/v2/organizations" >"$ORGS_JSON_FILE"
fetch_all_pages_v2 "/v2/stacks"        >"$STACKS_JSON_FILE"

get_buildpack_filename() {
  local bp_key="$1" stack_name="${2:-}"
  [[ -s "$BUILDPACKS_JSON_FILE" ]] || { echo ""; return; }
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

get_version_info() {
  local app_guid=$1 detected_buildpack=$2 buildpack_filename=$3
  local env_data; env_data=$(cf curl "/v2/apps/${app_guid}/env" 2>/dev/null || echo "{}")

  local buildpack_version=""
  if [[ -n "$buildpack_filename" && "$buildpack_filename" =~ -v([0-9]+(\.[0-9]+)*) ]]; then
    buildpack_version="${BASH_REMATCH[1]}"
  fi

  local runtime_version=""
  if [[ "$detected_buildpack" == *"java"* || "$detected_buildpack" == *"Java"* ]]; then
    runtime_version=$(jq -r '.environment_json.JAVA_VERSION // empty' <<<"$env_data")
    if [[ -z "$runtime_version" || "$runtime_version" == "null" ]]; then
      local jbp_config; jbp_config=$(jq -r '.environment_json.JBP_CONFIG_OPEN_JDK_JRE // empty' <<<"$env_data")
      if [[ -n "$jbp_config" && "$jbp_config" != "null" ]]; then
        runtime_version=$(grep -oE 'version:[[:space:]]*["'\'']?[^"'\'' ,}]+' <<<"$jbp_config" | head -1 | sed -E 's/^version:[[:space:]]*["'\'']?//')
      fi
    fi
  elif [[ "$detected_buildpack" == *"node"* || "$detected_buildpack" == *"Node"* ]]; then
    runtime_version=$(jq -r '.environment_json.NODE_VERSION // empty' <<<"$env_data")
  elif [[ "$detected_buildpack" == *"python"* || "$detected_buildpack" == *"Python"* ]]; then
    runtime_version=$(jq -r '.environment_json.PYTHON_VERSION // empty' <<<"$env_data")
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
    cf curl "$stack_url" | jq -r '.entity.name // empty'
  fi
}

get_space_org_names_safe() {
  local space_url="$1" space_guid="${space_url##*/}"
  local space_name="" org_guid="" org_name=""
  if [[ -s "$SPACES_JSON_FILE" && -n "$space_guid" && "$space_guid" != "null" ]]; then
    space_name=$(jq -r --arg gid "$space_guid" '.resources[]? | select(.metadata.guid == $gid) | .entity.name // empty' "$SPACES_JSON_FILE")
    org_guid=$(jq -r  --arg gid "$space_guid" '.resources[]? | select(.metadata.guid == $gid) | .entity.organization_guid // empty' "$SPACES_JSON_FILE")
    if [[ -s "$ORGS_JSON_FILE" && -n "$org_guid" && "$org_guid" != "null" ]]; then
      org_name=$(jq -r --arg gid "$org_guid" '.resources[]? | select(.metadata.guid == $gid) | .entity.name // empty' "$ORGS_JSON_FILE")
    else
      org_name=$(cf curl "/v2/organizations/$org_guid" | jq -r '.entity.name // empty')
    fi
  else
    local space_json; space_json=$(cf curl "$space_url")
    space_name=$(jq -r '.entity.name // empty' <<<"$space_json")
    local org_url; org_url=$(jq -r '.entity.organization_url // empty' <<<"$space_json")
    org_name=$(cf curl "$org_url" | jq -r '.entity.name // empty')
  fi
  printf '%s|%s\n' "$space_name" "$org_name"
}

# ---------------------------- header ------------------------------
echo "Org_Name,Space_Name,Created_At,Updated_At,Name,GUID,Instances,Memory,Disk_Quota,Requested_Buildpack,Detected_Buildpack,Detected_Buildpack_GUID,Buildpack_Filename,Buildpack_Version,Runtime_Version,DropletSizeBytes,PackagesSizeBytes,HealthCheckType,App_State,Stack_Name,Services,Routes,Developers,Detected_Start_Command,Events"

# ---------------------------- per app -----------------------------
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
  dl=$(cf curl "/v3/apps/${app_guid}/droplets"  | jq -r '.resources // [] | .[0].links.download.href // empty' | sed 's|http[s]*://[^/]*||')
  pl=$(cf curl "/v3/apps/${app_guid}/packages"  | jq -r '.resources // [] | .[0].links.download.href  // empty' | sed 's|http[s]*://[^/]*||')
  [[ -n "$dl" ]] && droplet_size_bytes=$(cf curl -X HEAD -v "$dl" 2>&1 | awk -F': ' '/Content-Length:/ {gsub(/\r/,"",$2); print $2; exit}')
  [[ -n "$pl" ]] && packages_size_bytes=$(cf curl -X HEAD -v "$pl" 2>&1 | awk -F': ' '/Content-Length:/ {gsub(/\r/,"",$2); print $2; exit}')

  local health_check app_state
  health_check=$(jq -r '.entity.health_check_type // empty' <<<"$app")
  app_state=$(jq -r '.entity.state // empty' <<<"$app")

  local stack_url stack_name; stack_url=$(jq -r '.entity.stack_url // empty' <<<"$app")
  stack_name=$(get_stack_name_safe "$stack_url")

  local space_url space_name org_name
  space_url=$(jq -r '.entity.space_url // empty' <<<"$app")
  IFS='|' read -r space_name org_name < <(get_space_org_names_safe "$space_url")

  local created_at updated_at detected_start_command
  created_at=$(jq -r '.metadata.created_at // empty' <<<"$app")
  updated_at=$(jq -r '.metadata.updated_at // empty' <<<"$app")
  detected_start_command=$(jq -r '.entity.detected_start_command // empty' <<<"$app")

  local buildpack_filename input
  if [[ "$detected_buildpack_guid" != "null" && -n "$detected_buildpack_guid" ]]; then
    buildpack_filename=$(get_buildpack_filename "$detected_buildpack_guid")
  else
    input="${detected_buildpack:-$buildpack}"
    buildpack_filename=$(get_buildpack_filename "$input" "$stack_name")
  fi

  local buildpack_version runtime_version
  IFS='|' read -r buildpack_version runtime_version <<<"$(get_version_info "$app_guid" "$detected_buildpack" "$buildpack_filename")"

  local summary_json routes
  summary_json=$(cf curl "/v2/apps/${app_guid}/summary" 2>/dev/null || echo '{}')
  routes=$(jq -r '.routes // [] | .[] | (.host + "." + .domain.name)' <<<"$summary_json" | paste -sd ':' -)

  # Per-app caches (guard against empty GUIDs)
  declare -A MANAGED_SERVICE_CACHE=()
  declare -A UPS_SERVICE_CACHE=()

  local -a services_list=()
  local svc service_guid service_type service_string label plan name up_key
  while IFS= read -r svc; do
    service_guid=$(jq -r '.guid // empty' <<<"$svc")
    [[ -z "$service_guid" || "$service_guid" == "null" ]] && continue
    service_type=$(jq -r '.type // empty' <<<"$svc")
    if [[ "$service_type" != "user_provided_service_instance" ]]; then
      if [[ ${MANAGED_SERVICE_CACHE["$service_guid"]+_} ]]; then
        service_string="${MANAGED_SERVICE_CACHE[$service_guid]}"
      else
        label=$(jq -r '.service_plan.service.label // ""' <<<"$svc")
        plan=$(jq -r  '.service_plan.name // ""'         <<<"$svc")
        service_string="$label ($plan)-($service_guid)"
        MANAGED_SERVICE_CACHE["$service_guid"]="$service_string"
      fi
    else
      up_key="$(basename "$space_url"):${service_guid}"
      if [[ ${UPS_SERVICE_CACHE["$up_key"]+_} ]]; then
        service_string="${UPS_SERVICE_CACHE[$up_key]}"
      else
        name=$(jq -r '.name // ""' <<<"$svc")
        service_string="$name (user provided service)-($service_guid)"
        UPS_SERVICE_CACHE["$up_key"]="$service_string"
      fi
    fi
    services_list+=("$service_string")
  done < <(jq -c '.services // [] | .[]' <<<"$summary_json")

  local services=""
  if (( ${#services_list[@]} > 0 )); then
    services=$(printf "%s:" "${services_list[@]}"); services="${services%:}"
  fi

  routes=${routes:-""}; services=${services:-""}

  local dev_usernames
  dev_usernames=$(cf curl "${space_url}/developers" | jq -r '.resources // [] | .[] | .entity | select(.username != null) | .username' | paste -sd ':' -)

  # v2 events (keeps your column shape)
  local events="" events_url events_json
  events_url=$(jq -r '.entity.events_url // empty' <<<"$app")
  if [[ -n "$events_url" ]]; then
    events_json=$(fetch_all_pages_v2 "$events_url")
    events=$(jq -cr '.resources[]?' <<<"$events_json" | paste -sd ':#:' -)
  fi

  echo "$org_name,$space_name,$created_at,$updated_at,$name,$app_guid,$instances,$memory,$disk_quota,$buildpack,$detected_buildpack,$detected_buildpack_guid,$buildpack_filename,$buildpack_version,$runtime_version,$droplet_size_bytes,$packages_size_bytes,$health_check,$app_state,$stack_name,$services,$routes,$dev_usernames,$detected_start_command,$events"
}

export -f process_app fetch_all_pages_v2 fetch_all_pages_v3 get_buildpack_filename get_version_info get_stack_name_safe get_space_org_names_safe

# ---------------------------- driver ------------------------------
total_pages=$(cf curl "/v2/apps?results-per-page=100" | jq '.total_pages // 1')
workers="${WORKERS:-6}"

for i in $(seq 1 "$total_pages"); do
  cf curl "/v2/apps?page=$i&results-per-page=100" \
    | jq -c '.resources // [] | .[]' \
    | while IFS= read -r app; do
        printf '%s\0' "$app"
      done \
    | xargs -0 -P "$workers" -n 1  bash -c 'process_app "$1"' _
done