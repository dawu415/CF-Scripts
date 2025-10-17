#!/bin/bash
set -Eeuo pipefail
trap 'ec=$?;
      ts=$(date "+%F %T");
      echo "[$ts] ERROR ${ec:-1} at ${BASH_SOURCE[0]}:${LINENO}: ${FUNCNAME[0]:-main}: ${BASH_COMMAND}" >&2;
      exit "${ec:-1}"' ERR

# -----------------------------------------------------------------------------
# PERFORMANCE NOTE
#
# This script collects detailed information about every Cloud Foundry application
# in an org.  Early versions of this script made many API requests per app to
# assemble routes, services and buildpack filenames.  The code below has been
# refactored to minimise the number of API calls:
#
#   * Buildpack metadata is fetched once at startup and cached in the
#     environment variable BUILD_PACKS_JSON.  A helper function
#     `get_buildpack_filename` looks up filenames from this cache instead of
#     calling `/v2/buildpacks` repeatedly.
#
#   * The Cloud Foundry "summary" endpoint (/v2/apps/<guid>/summary) returns
#     most of the information needed about routes and bound services in a
#     single API call.  Using this endpoint replaces separate loops that
#     queried domains, services and plans individually for every app【171883346609456†L396-L426】.
#
# See the README or script header comments for more details.
# -----------------------------------------------------------------------------

get_version_info() {
    local app_guid=$1
    local detected_buildpack=$2
    local buildpack_filename=$3

    env_data=$(cf curl "/v2/apps/${app_guid}/env" 2>/dev/null || echo "{}")

    # Extract buildpack version from the filename using a portable bash regex.
    # Files are typically of the form <name>-vX.Y.Z.zip.  We avoid grep -P for
    # portability: bash regex is used instead.  If the filename is empty or
    # does not contain a version, the variable remains empty and we later
    # fall back to parsing the detected_buildpack string.
    buildpack_version=""
    if [[ -n "$buildpack_filename" && "$buildpack_filename" =~ -v([0-9]+(\.[0-9]+)*) ]]; then
        buildpack_version="${BASH_REMATCH[1]}"
    fi
    runtime_version=""

    if [[ "$detected_buildpack" == *"java"* || "$detected_buildpack" == *"Java"* ]]; then
        runtime_version=$(echo "$env_data" | jq -r '.environment_json.JAVA_VERSION // empty' 2>/dev/null)
        if [[ -z "$runtime_version" || "$runtime_version" == "null" ]]; then
            jbp_config=$(echo "$env_data" | jq -r '.environment_json.JBP_CONFIG_OPEN_JDK_JRE // empty' 2>/dev/null)
            if [[ -n "$jbp_config" && "$jbp_config" != "null" ]]; then
                runtime_version=$(echo "$jbp_config" | grep -oP 'version:\s*["\047]?\K[^"\047,}]+' | head -1)
            fi
        fi

    elif [[ "$detected_buildpack" == *"node"* || "$detected_buildpack" == *"Node"* ]]; then
        runtime_version=$(echo "$env_data" | jq -r '.environment_json.NODE_VERSION // empty' 2>/dev/null)

    elif [[ "$detected_buildpack" == *"python"* || "$detected_buildpack" == *"Python"* ]]; then
        runtime_version=$(echo "$env_data" | jq -r '.environment_json.PYTHON_VERSION // empty' 2>/dev/null)
    fi

    # Normalise null/empty values
    [[ "$buildpack_version" == "null" || -z "$buildpack_version" ]] && buildpack_version=""
    [[ "$runtime_version" == "null" || -z "$runtime_version" ]] && runtime_version=""

    echo "${buildpack_version}|${runtime_version}"
}

echo "Org_Name,Space_Name,Created_At,Updated_At,Name,GUID,Instances,Memory,Disk_Quota,Requested_Buildpack,Detected_Buildpack,Detected_Buildpack_GUID,Buildpack_Filename,Buildpack_Version,Runtime_Version,DropletSizeBytes,PackagesSizeBytes,HealthCheckType,App_State,Stack_Name,Services,Routes,Developers,Detected_Start_Command, Events"

# ----------------------------------------------------------------------------
# Prefetch buildpack metadata
#
# The Cloud Foundry buildpack catalogue rarely changes during a run.  Fetch
# all buildpacks once and store the raw JSON in the BUILD_PACKS_JSON
# environment variable.  The helper `get_buildpack_filename` will parse
# this cache to retrieve the filename for a given buildpack name/stack or
# GUID.  This avoids calling `/v2/buildpacks` for every application.
#
BUILD_PACKS_JSON=$(cf curl "/v2/buildpacks?results-per-page=100" 2>/dev/null || echo '{}')
export BUILD_PACKS_JSON


get_buildpack_filename() {
    # Look up a buildpack filename from the cached BUILD_PACKS_JSON.
    # Arguments:
    #   $1 - buildpack GUID or name
    #   $2 - optional stack name (case-insensitive)
    local bp_key="$1"
    local stack_name="${2:-}"
    # Use jq to filter the cached JSON.  If bp_key looks like a GUID, match
    # against metadata.guid; otherwise match entity.name.  If stack_name is
    # provided and non-empty, filter filenames whose stack matches (case
    # insensitive); otherwise return the first matching filename.
    if [[ "$bp_key" =~ ^[0-9a-fA-F-]{36}$ ]]; then
        # GUID lookup
        echo "$BUILD_PACKS_JSON" | jq -r --arg guid "$bp_key" '
            .resources[]? | select(.metadata.guid == $guid) | .entity.filename // empty' | head -n 1
    else
        # Name lookup (optionally filter by stack)
        if [[ -z "$stack_name" || "$stack_name" == "null" ]]; then
            echo "$BUILD_PACKS_JSON" | jq -r --arg name "$bp_key" '
                .resources[]? | select(.entity.name == $name) | .entity.filename // empty' | head -n 1
        else
            echo "$BUILD_PACKS_JSON" | jq -r --arg name "$bp_key" --arg stack "$stack_name" '
                .resources[]? | select(.entity.name == $name) | .entity.filename // empty' | grep -i "$stack_name" | head -n 1
        fi
    fi
}
export -f get_buildpack_filename


##########################################################################
# Prefetch spaces, organisations and stacks metadata with pagination
#
# The v2 API paginates results with a maximum of 100 items per page. Some
# foundations contain more than 100 spaces or organisations, so requesting a
# large `results-per-page` value can cause the API to return an error. To
# fetch all resources, define a helper that walks through each page until
# the `next_url` field is empty and concatenates all `resources` arrays. The
# resulting JSON has the same top-level structure as a single call but
# contains all resources. This helper uses `jq` to merge arrays and is
# exported so that it is available in worker processes spawned by `parallel`
# or `xargs`.

fetch_all_pages_v2() {
    local base_path="$1"
    local url="${base_path}?results-per-page=100"
    local acc='{ "resources": [] }'
    local resp next_url
    while [[ -n "$url" ]]; do
        resp=$(cf curl "$url" 2>/dev/null || echo '{}')
        # Merge the resources from this page into our accumulator.  Using
        # the "+" operator to produce a new object avoids jq's complaint
        # about assigning to a variable property.  See
        # https://stackoverflow.com/q/41439484 for details.
        acc=$(jq -n --argjson acc "$acc" --argjson res "$resp" \
            '$acc + {resources: ($acc.resources + ($res.resources // []))}')
        # Determine the next URL.  The API returns a relative path; cf curl
        # accepts relative paths directly.  Empty string means no more pages.
        next_url=$(echo "$resp" | jq -r '.next_url // empty')
        if [[ -n "$next_url" ]]; then
            url="$next_url"
        else
            url=""
        fi
    done
    echo "$acc"
}
export -f fetch_all_pages_v2

# Fetch all spaces, organisations and stacks once at startup.  These JSON
# blobs are exported so they are available to each worker process spawned by
# GNU parallel or xargs.  See fetch_all_pages_v2 above for details.
SPACES_JSON=$(fetch_all_pages_v2 "/v2/spaces")
ORGS_JSON=$(fetch_all_pages_v2 "/v2/organizations")
STACKS_JSON=$(fetch_all_pages_v2 "/v2/stacks")
export SPACES_JSON ORGS_JSON STACKS_JSON

process_app() {
    local app="$1"

    # Basic metadata
    name=$(echo "$app" | jq -r '.entity.name // empty')
    app_guid=$(echo "$app" | jq -r '.metadata.guid')
    instances=$(echo "$app" | jq -r '.entity.instances // 0')
    memory=$(echo "$app" | jq -r '.entity.memory // 0')
    disk_quota=$(echo "$app" | jq -r '.entity.disk_quota // 0')
    buildpack=$(echo "$app" | jq -r '.entity.buildpack // empty')
    detected_buildpack=$(echo "$app" | jq -r '.entity.detected_buildpack // empty')
    detected_buildpack_guid=$(echo "$app" | jq -r '.entity.detected_buildpack_guid // empty')

    # Droplet and package sizes
    # We fetch the droplet and package download
    # links once and then issue HEAD requests to obtain the Content‑Length (in bytes).
    droplet_size_bytes=""
    packages_size_bytes=""
    # Obtain download links for droplet and package.  These endpoints return lists of
    # resources; use the first entry (index 0).  Remove protocol/host for cf curl.
    droplet_download_link=$(cf curl "/v3/apps/${app_guid}/droplets" | jq -r '.resources // [] | .[0].links.download.href // empty' | sed 's|http[s]*://[^/]*||')
    packages_download_link=$(cf curl "/v3/apps/${app_guid}/packages" | jq -r '.resources // [] | .[0].links.download.href // empty' | sed 's|http[s]*://[^/]*||')
    # Always collect droplet size. Use HEAD request to avoid downloading content; parse
    # Content‑Length from the headers.  On failure, leave blank.
    if [[ -n "$droplet_download_link" ]]; then
        droplet_size_bytes=$(cf curl -X HEAD -v "$droplet_download_link" 2>&1 | grep -i 'Content-Length:' | awk '{print $2}' | tr -d '\r' || true)
    fi

    if [[ -n "$packages_download_link" ]]; then
        packages_size_bytes=$(cf curl -X HEAD -v "$packages_download_link" 2>&1 | grep -i 'Content-Length:' | awk '{print $2}' | tr -d '\r' || true)
    fi
    
    # Health and state
    health_check=$(echo "$app" | jq -r '.entity.health_check_type // empty')
    app_state=$(echo "$app" | jq -r '.entity.state // empty')

    # Stack: derive stack GUID from stack_url and look up name in the cached
    # STACKS_JSON.  Fall back to empty if not found.
    stack_url=$(echo "$app" | jq -r '.entity.stack_url // empty')
    stack_guid="${stack_url##*/}"
    stack_name=$(echo "$STACKS_JSON" | jq -r --arg gid "$stack_guid" \
        '.resources[]? | select(.metadata.guid == $gid) | .entity.name // empty')

    # Space and organisation names: derive space GUID from space_url and look up
    # names from the cached SPACES_JSON and ORGS_JSON.  This avoids
    # per-application cf curl calls for spaces and organisations.
    space_url=$(echo "$app" | jq -r '.entity.space_url // empty')
    space_guid="${space_url##*/}"
    space_name=$(echo "$SPACES_JSON" | jq -r --arg gid "$space_guid" \
        '.resources[]? | select(.metadata.guid == $gid) | .entity.name // empty')
    org_guid=$(echo "$SPACES_JSON" | jq -r --arg gid "$space_guid" \
        '.resources[]? | select(.metadata.guid == $gid) | .entity.organization_guid // empty')
    org_name=$(echo "$ORGS_JSON" | jq -r --arg gid "$org_guid" \
        '.resources[]? | select(.metadata.guid == $gid) | .entity.name // empty')

    created_at=$(echo "$app" | jq -r '.metadata.created_at // empty')
    updated_at=$(echo "$app" | jq -r '.metadata.updated_at // empty')
    detected_start_command=$(echo "$app" | jq -r '.entity.detected_start_command // empty')

    # Determine buildpack filename using cached buildpacks
    buildpack_filename=""
    if [[ "$detected_buildpack_guid" != "null" && -n "$detected_buildpack_guid" ]]; then
        buildpack_filename=$(get_buildpack_filename "$detected_buildpack_guid")
    else
        # fall back to buildpack name and stack
        input="${detected_buildpack:-$buildpack}"
        buildpack_filename=$(get_buildpack_filename "$input" "$stack_name")
    fi

    version_info=$(get_version_info "$app_guid" "$detected_buildpack" "$buildpack_filename")
    IFS='|' read -r buildpack_version runtime_version <<< "$version_info"

    # Use the summary endpoint for routes and services to minimise API calls【171883346609456†L396-L426】
    summary_json=$(cf curl "/v2/apps/${app_guid}/summary" 2>/dev/null || echo '{}')
    routes=$(echo "$summary_json" | jq -r '.routes // [] | .[] | "\(.host).\(.domain.name)"' | paste -sd ':' -)
    # Extract services from the summary.  Use memoisation for managed service
    # instances: managed services often appear across multiple apps, so cache
    # the formatted string by GUID.  User‑provided services are not cached
    # across apps (unless they appear within the same space) because their
    # representation is based on the instance name.  The `.type` field
    # distinguishes `user_provided_service_instance` from managed services.
    # Build an associative cache on first use.
    declare -gA MANAGED_SERVICE_CACHE
    declare -gA UPS_SERVICE_CACHE
    services_list=()
    # Parse services into an array for processing.  Each element is a compact
    # JSON object containing guid, type, label, plan and name.  We use
    # mapfile to avoid subshell issues so that services_list persists.
    mapfile -t __services_data < <(
        echo "$summary_json" | jq -c '.services // [] | .[] | {guid: .guid, type: .type, label: (.service_plan.service.label // ""), plan: (.service_plan.name // ""), name: .name}'
    )
    for svc in "${__services_data[@]}"; do
        service_guid=$(echo "$svc" | jq -r '.guid')
        service_type=$(echo "$svc" | jq -r '.type')
        if [[ "$service_type" != "user_provided_service_instance" ]]; then
            # Managed service: cache globally by GUID since many apps share the same instance.
            if [[ -n "${MANAGED_SERVICE_CACHE[$service_guid]+set}" ]]; then
                service_string="${MANAGED_SERVICE_CACHE[$service_guid]}"
            else
                label=$(echo "$svc" | jq -r '.label')
                plan=$(echo "$svc" | jq -r '.plan')
                service_string="$label ($plan)-($service_guid)"
                MANAGED_SERVICE_CACHE[$service_guid]="$service_string"
            fi
        else
            # User‑provided services should only be memoised within the same space.  Use
            # a composite key of space GUID and service GUID to avoid sharing
            # across different spaces.
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
    # Join service strings with a colon.  If no services found, leave empty.
    if (( ${#services_list[@]} > 0 )); then
        services=$(printf "%s:" "${services_list[@]}")
        services="${services%:}"
    else
        services=""
    fi

    # Fallback to empty strings if routes or services are empty
    routes=${routes:-""}
    services=${services:-""}

    # List developers on the space
    dev_usernames=$(cf curl "${space_url}/developers" | jq -r '.resources // [] | .[] | .entity | select(.username != null) | .username' | paste -sd ':' -)

    # Collect events for this app using the events_url from the app entity.  Use the
    # pagination helper to fetch all pages of events.  Each event is output as a
    # compact JSON string and events are joined with :#: to produce a single
    # field.  If there are no events, the field will be empty.
    events=""
    events_url=$(echo "$app" | jq -r '.entity.events_url // empty')
    if [[ -n "$events_url" ]]; then
        events_json=$(fetch_all_pages_v2 "$events_url")
        echo $events_json
        # Join compact event JSON objects separated by :#:
        events=$(echo "$events_json" | jq -cr '.resources[]?' | paste -sd ':#:' -)
        events=${events:-""}
    fi

    echo "$org_name,$space_name,$created_at,$updated_at,$name,$app_guid,$instances,$memory,$disk_quota,$buildpack,$detected_buildpack,$detected_buildpack_guid,$buildpack_filename,$buildpack_version,$runtime_version,$droplet_size_bytes,$packages_size_bytes,$health_check,$app_state,$stack_name,$services,$routes,$dev_usernames,$detected_start_command,$events"

}

export -f process_app
export -f get_version_info

total_pages=$(cf curl "/v2/apps?results-per-page=100" | jq '.total_pages // 1')

for i in $(seq 1 $total_pages); do
    if command -v parallel &> /dev/null; then
        cf curl "/v2/apps?page=$i&results-per-page=100" | jq -c '.resources // [] | .[]' | parallel -j 10 process_app
    else
        cf curl "/v2/apps?page=$i&results-per-page=100" | jq -c '.resources // [] | .[]' | \
        while IFS= read -r app; do
            printf '%s\0' "$app"
        done | xargs -0 -P 6 -I {} bash -c 'process_app "$@"' _ {}
    fi
done