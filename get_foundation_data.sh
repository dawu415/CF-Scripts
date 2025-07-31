#!/bin/bash

get_version_info() {
    local app_guid=$1
    local detected_buildpack=$2
    local buildpack_filename=$3

    env_data=$(cf curl "/v2/apps/${app_guid}/env" 2>/dev/null || echo "{}")

    buildpack_version=$(echo "$buildpack_filename" | grep -oP 'v\K[\d.]+' | sed 's/[-.]$//' || echo "")
    runtime_version=""

    if [[ "$detected_buildpack" == *"java"* || "$detected_buildpack" == *"Java"* ]]; then
        runtime_version=$(echo "$env_data" | jq -r '.environment_json.JAVA_VERSION // empty' 2>/dev/null)
        if [[ -z "$runtime_version" || "$runtime_version" == "null" ]]; then
            jbp_config=$(echo "$env_data" | jq -r '.environment_json.JBP_CONFIG_OPEN_JDK_JRE // empty' 2>/dev/null)
            if [[ -n "$jbp_config" && "$jbp_config" != "null" ]]; then
                runtime_version=$(echo "$jbp_config" | grep -oP 'version:\s*["\047]?\K[^"\047,}]+' | head -1)
            fi
        fi
        buildpack_version=$(echo "$detected_buildpack" | grep -oP 'v\K[\d.]+' || echo "")

    elif [[ "$detected_buildpack" == *"node"* || "$detected_buildpack" == *"Node"* ]]; then
        runtime_version=$(echo "$env_data" | jq -r '.environment_json.NODE_VERSION // empty' 2>/dev/null)

    elif [[ "$detected_buildpack" == *"python"* || "$detected_buildpack" == *"Python"* ]]; then
        runtime_version=$(echo "$env_data" | jq -r '.environment_json.PYTHON_VERSION // empty' 2>/dev/null)
    fi

    [[ "$buildpack_version" == "null" || -z "$buildpack_version" ]] && buildpack_version=""
    [[ "$runtime_version" == "null" || -z "$runtime_version" ]] && runtime_version=""

    echo "${buildpack_version}|${runtime_version}"
}

echo "Org_Name,Space_Name,Created_At,Updated_At,Name,GUID,Instances,Memory,Disk_Quota,Requested_Buildpack,Detected_Buildpack,Detected_Buildpack_GUID,Buildpack_Filename,Buildpack_Version,Runtime_Version,DropletSizeBytes,PackagesSizeBytes,HealthCheckType,App_State,Stack_Name,Services,Routes,Developers"

process_app() {
    local app=$1

    name=$(echo "$app" | jq -r '.entity.name // empty')
    app_guid=$(echo "$app" | jq -r '.metadata.guid')
    instances=$(echo "$app" | jq -r '.entity.instances // 0')
    memory=$(echo "$app" | jq -r '.entity.memory // 0')
    disk_quota=$(echo "$app" | jq -r '.entity.disk_quota // 0')
    buildpack=$(echo "$app" | jq -r '.entity.buildpack // empty')
    detected_buildpack=$(echo "$app" | jq -r '.entity.detected_buildpack // empty')
    detected_buildpack_guid=$(echo "$app" | jq -r '.entity.detected_buildpack_guid // empty')

    droplet_download_link=$(cf curl "/v3/apps/${app_guid}/droplets" | jq -r '.resources // [] | .[0].links.download.href // empty' | sed 's|http[s]*://[^/]*||')
    packages_download_link=$(cf curl "/v3/apps/${app_guid}/packages" | jq -r '.resources // [] | .[0].links.download.href // empty' | sed 's|http[s]*://[^/]*||')

    droplet_size_bytes=""
    [[ -n "$droplet_download_link" ]] && droplet_size_bytes=$(cf curl -X HEAD -v "$droplet_download_link" 2>&1 | grep -i 'Content-Length:' | awk '{print $2}' | tr -d '\r')

    packages_size_bytes=""
    [[ -n "$packages_download_link" ]] && packages_size_bytes=$(cf curl -X HEAD -v "$packages_download_link" 2>&1 | grep -i 'Content-Length:' | awk '{print $2}' | tr -d '\r')

    health_check=$(echo "$app" | jq -r '.entity.health_check_type // empty')
    app_state=$(echo "$app" | jq -r '.entity.state // empty')
    stack_name=$(cf curl "$(echo "$app" | jq -r '.entity.stack_url // empty')" | jq -r '.entity.name // empty')
    space_url=$(echo "$app" | jq -r '.entity.space_url // empty')
    space_json=$(cf curl "$space_url")
    space_name=$(echo "$space_json" | jq -r '.entity.name // empty')
    org_url=$(echo "$space_json" | jq -r '.entity.organization_url // empty')
    org_name=$(cf curl "$org_url" | jq -r '.entity.name // empty')
    created_at=$(echo "$app" | jq -r '.metadata.created_at // empty')
    updated_at=$(echo "$app" | jq -r '.metadata.updated_at // empty')
    routes_url=$(echo "$app" | jq -r '.entity.routes_url // empty')

    input="${detected_buildpack:-$buildpack}"
    buildpack_filename=""
    if [[ "$detected_buildpack_guid" == "null" || -z "$detected_buildpack_guid" ]]; then
        if [[ -z "$stack_name" || "$stack_name" == "null" ]]; then
            buildpack_filename=$(cf curl "/v2/buildpacks?results-per-page=100" | jq -r --arg buildpack "$input" '.resources // [] | .[] | select(.entity.name == $buildpack) | .entity.filename' | head -n 1)
        else
            buildpack_filename=$(cf curl "/v2/buildpacks?results-per-page=100" | jq -r --arg buildpack "$input" '.resources // [] | .[] | select(.entity.name == $buildpack) | .entity.filename' | grep -i "$stack_name" | head -n 1)
        fi
    else
        buildpack_filename=$(cf curl "/v2/buildpacks/${detected_buildpack_guid}" | jq -r '.entity.filename // empty')
    fi

    version_info=$(get_version_info "$app_guid" "$detected_buildpack" "$buildpack_filename")
    IFS='|' read -r buildpack_version runtime_version <<< "$version_info"

    services=$(cf curl "$(echo "$app" | jq -r '.entity.service_bindings_url')" | jq -r '.resources // [] | .[] | .entity.service_instance_url' | while read -r service_instance_url; do
        data=$(cf curl "$service_instance_url")
        if [[ "$service_instance_url" == *"user_provided_service"* ]]; then
            service_name=$(echo "$data" | jq -r '.entity.name')
            service_plan=$(echo "$data" | jq -r '.entity.type')
        else
            service_url=$(echo "$data" | jq -r '.entity.service_url')
            service_plan_url=$(echo "$data" | jq -r '.entity.service_plan_url')
            service_name=$(cf curl "$service_url" | jq -r '.entity.service_broker_name')
            service_plan=$(cf curl "$service_plan_url" | jq -r '.entity.name')
        fi
        echo "${service_name} (${service_plan})"
    done | paste -sd ":" -)

    routes=$(cf curl "$routes_url" | jq -c '.resources // [] | .[]' | while read -r routedata; do
        route_name=$(echo "$routedata" | jq -r '.entity.host')
        domain_url=$(echo "$routedata" | jq -r '.entity.domain_url')
        domain_name=$(cf curl "$domain_url" | jq -r '.entity.name')
        echo "${route_name}.${domain_name}"
    done | paste -sd ":" -)

    dev_usernames=$(cf curl "${space_url}/developers" | jq -r '.resources // [] | .[] | .entity | select(.username != null) | .username' | paste -sd ":" -)

    echo "$org_name,$space_name,$created_at,$updated_at,$name,$app_guid,$instances,$memory,$disk_quota,$buildpack,$detected_buildpack,$detected_buildpack_guid,$buildpack_filename,$buildpack_version,$runtime_version,$droplet_size_bytes,$packages_size_bytes,$health_check,$app_state,$stack_name,$services,$routes,$dev_usernames"
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
        done | xargs -0 -P 10 -I {} bash -c 'process_app "$@"' _ {}
    fi
done
