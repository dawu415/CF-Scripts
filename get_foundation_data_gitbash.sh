#!/usr/bin/env bash
set -euo pipefail

# Use Windows-compatible executables
CF=$(command -v cf.exe || command -v cf)
JQ=$(command -v jq.exe || command -v jq)

if [[ -z "$CF" || -z "$JQ" ]]; then
  echo "❌ Missing cf.exe or jq.exe in PATH. Please add them."
  exit 1
fi

get_version_info() {
    local app_guid=$1
    local detected_buildpack=$2
    local buildpack_filename=$3

    env_data=$($CF curl "/v2/apps/${app_guid}/env" 2>/dev/null || echo "{}")

    buildpack_version=$(echo "$buildpack_filename" | grep -oP 'v\K[\d.]+' | sed 's/[-.]$//' || echo "")
    runtime_version=""

    if [[ "$detected_buildpack" == *"java"* || "$detected_buildpack" == *"Java"* ]]; then
        runtime_version=$(echo "$env_data" | $JQ -r '.environment_json.JAVA_VERSION // empty' 2>/dev/null)

        if [[ -z "$runtime_version" || "$runtime_version" == "null" ]]; then
            jbp_config=$(echo "$env_data" | $JQ -r '.environment_json.JBP_CONFIG_OPEN_JDK_JRE // empty' 2>/dev/null)
            if [[ -n "$jbp_config" && "$jbp_config" != "null" ]]; then
                runtime_version=$(echo "$jbp_config" | grep -oP 'version:\s*["\047]?\K[^"\047,}]+' | head -1)
            fi
        fi

        buildpack_version=$(echo "$detected_buildpack" | grep -oP 'v\K[\d.]+' || echo "")

    elif [[ "$detected_buildpack" == *"node"* || "$detected_buildpack" == *"Node"* ]]; then
        runtime_version=$(echo "$env_data" | $JQ -r '.environment_json.NODE_VERSION // empty' 2>/dev/null)

    elif [[ "$detected_buildpack" == *"python"* || "$detected_buildpack" == *"Python"* ]]; then
        runtime_version=$(echo "$env_data" | $JQ -r '.environment_json.PYTHON_VERSION // empty' 2>/dev/null)
    fi

    [[ "$buildpack_version" == "null" || -z "$buildpack_version" ]] && buildpack_version=""
    [[ "$runtime_version" == "null" || -z "$runtime_version" ]] && runtime_version=""

    echo "${buildpack_version}|${runtime_version}"
}

process_app() {
    local app="$1"

    name=$(echo "$app" | $JQ -r '.entity.name')
    app_guid=$(echo "$app" | $JQ -r '.metadata.guid')
    instances=$(echo "$app" | $JQ -r '.entity.instances')
    memory=$(echo "$app" | $JQ -r '.entity.memory')
    disk_quota=$(echo "$app" | $JQ -r '.entity.disk_quota')
    buildpack=$(echo "$app" | $JQ -r '.entity.buildpack')
    detected_buildpack=$(echo "$app" | $JQ -r '.entity.detected_buildpack')
    detected_buildpack_guid=$(echo "$app" | $JQ -r '.entity.detected_buildpack_guid')

    droplet_download_link=$($CF curl "/v3/apps/${app_guid}/droplets" | $JQ -r '.resources[0].links.download.href' | sed 's|http[s]*://[^/]*||')
    packages_download_link=$($CF curl "/v3/apps/${app_guid}/packages" | $JQ -r '.resources[0].links.download.href' | sed 's|http[s]*://[^/]*||')

    droplet_size_bytes=$($CF curl -X HEAD -v "$droplet_download_link" 2>&1 | grep -i 'Content-Length:' | awk '{print $2}' | tr -d '\r')
    packages_size_bytes=$($CF curl -X HEAD -v "$packages_download_link" 2>&1 | grep -i 'Content-Length:' | awk '{print $2}' | tr -d '\r')

    health_check=$($JQ -r '.entity.health_check_type' <<< "$app")
    app_state=$($JQ -r '.entity.state' <<< "$app")
    stack_name=$($CF curl "$($JQ -r '.entity.stack_url' <<< "$app")" | $JQ -r '.entity.name')

    space_url=$($JQ -r '.entity.space_url' <<< "$app")
    space_json=$($CF curl "$space_url")
    space_name=$(echo "$space_json" | $JQ -r '.entity.name')
    org_url=$(echo "$space_json" | $JQ -r '.entity.organization_url')
    org_name=$($CF curl "$org_url" | $JQ -r '.entity.name')

    created_at=$($JQ -r '.metadata.created_at' <<< "$app")
    updated_at=$($JQ -r '.metadata.updated_at' <<< "$app")

    routes_url=$($JQ -r '.entity.routes_url' <<< "$app")

    buildpack_filename=""
    if [[ "$detected_buildpack_guid" == "null" ]]; then
        input="${detected_buildpack:-$buildpack}"

        if [[ -z "$stack_name" || "$stack_name" == "null" ]]; then
            buildpack_filename=$($CF curl "/v2/buildpacks?results-per-page=100" | $JQ -r --arg buildpack "$input" '.resources[] | select(.entity.name == $buildpack) | .entity.filename' | head -n 1)
        else
            buildpack_filename=$($CF curl "/v2/buildpacks?results-per-page=100" | $JQ -r --arg buildpack "$input" '.resources[] | select(.entity.name == $buildpack) | .entity.filename' | grep -i "$stack_name" | head -n 1)
        fi
    else
        buildpack_filename=$($CF curl "/v2/buildpacks/${detected_buildpack_guid}" | $JQ -r '.entity.filename')
    fi

    version_info=$(get_version_info "$app_guid" "$detected_buildpack" "$buildpack_filename")
    IFS='|' read -r buildpack_version runtime_version <<< "$version_info"

    service_binding_url=$($JQ -r '.entity.service_bindings_url' <<< "$app")

    services=$($CF curl "$service_binding_url" | $JQ -r -c '.resources[].entity.service_instance_url' | while read -r service_instance_url; do
        data="$($CF curl "$service_instance_url")"
        if [[ "$service_instance_url" == *"user_provided_service"* ]]; then
            service_name=$(echo "$data" | $JQ -r '.entity.name')
            service_plan=$(echo "$data" | $JQ -r '.entity.type')
        else
            service_url=$(echo "$data" | $JQ -r '.entity.service_url')
            service_plan_url=$(echo "$data" | $JQ -r '.entity.service_plan_url')
            service_name=$($CF curl "$service_url" | $JQ -r '.entity.service_broker_name')
            service_plan=$($CF curl "$service_plan_url" | $JQ -r '.entity.name')
        fi
        echo "${service_name} (${service_plan})"
    done | paste -sd ":" -)

    routes=$($CF curl "$routes_url" | $JQ -c '.resources[]' | while read -r routedata; do
        route_name=$(echo "$routedata" | $JQ -r '.entity.host')
        domain_url=$(echo "$routedata" | $JQ -r '.entity.domain_url')
        domain_name=$($CF curl "$domain_url" | $JQ -r '.entity.name')
        echo "${route_name}.${domain_name}"
    done | paste -sd ":" -)

    dev_usernames=$($CF curl "${space_url}/developers" | \
        $JQ -r '.resources[].entity | select(.username != null) | .username' | \
        paste -sd ":" -)

    echo "$org_name,$space_name,$created_at,$updated_at,$name,$app_guid,$instances,$memory,$disk_quota,$buildpack,$detected_buildpack,$detected_buildpack_guid,$buildpack_filename,$buildpack_version,$runtime_version,$droplet_size_bytes,$packages_size_bytes,$health_check,$app_state,$stack_name,$services,$routes,$dev_usernames"
}

# Header
echo "Org_Name,Space_Name,Created_At,Updated_At,Name,GUID,Instances,Memory,Disk_Quota,Requested_Buildpack,Detected_Buildpack,Detected_Buildpack_GUID,Buildpack_Filename,Buildpack_Version,Runtime_Version,DropletSizeBytes,PackagesSizeBytes,HealthCheckType,App_State,Stack_Name,Services,Routes,Developers"

# Main loop - no parallel for Git Bash compatibility
total_pages=$($CF curl "/v2/apps?results-per-page=100" | $JQ '.total_pages')

for i in $(seq 1 $total_pages); do
    app_list=$($CF curl "/v2/apps?page=$i&results-per-page=100" | $JQ -c '.resources[]')
    while IFS= read -r app; do
        process_app "$app"
    done <<< "$app_list"
done

