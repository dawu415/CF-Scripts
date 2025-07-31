#!/bin/bash

# Windows-compatible version with better error handling

get_version_info() {
    local app_guid=$1
    local detected_buildpack=$2
    local buildpack_filename=$3

    # Add error handling for cf curl
    env_data=$(cf curl "/v2/apps/${app_guid}/env" 2>/dev/null)
    if [[ $? -ne 0 ]] || [[ -z "$env_data" ]]; then
        env_data="{}"
    fi

    # Validate JSON before processing
    if ! echo "$env_data" | jq empty 2>/dev/null; then
        env_data="{}"
    fi

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

safe_cf_curl() {
    local url=$1
    local result=$(cf curl "$url" 2>/dev/null)
    
    # Check if result is valid JSON
    if echo "$result" | jq empty 2>/dev/null; then
        echo "$result"
    else
        echo "{}"
    fi
}

echo "Org_Name,Space_Name,Created_At,Updated_At,Name,GUID,Instances,Memory,Disk_Quota,Requested_Buildpack,Detected_Buildpack,Detected_Buildpack_GUID,Buildpack_Filename,Buildpack_Version,Runtime_Version,DropletSizeBytes,PackagesSizeBytes,HealthCheckType,App_State,Stack_Name,Services,Routes,Developers"

process_app() {
    local app=$1

    # Validate input JSON
    if ! echo "$app" | jq empty 2>/dev/null; then
        echo "ERROR: Invalid app JSON" >&2
        return 1
    fi

    name=$(echo "$app" | jq -r '.entity.name // empty')
    app_guid=$(echo "$app" | jq -r '.metadata.guid')
    instances=$(echo "$app" | jq -r '.entity.instances // 0')
    memory=$(echo "$app" | jq -r '.entity.memory // 0')
    disk_quota=$(echo "$app" | jq -r '.entity.disk_quota // 0')
    buildpack=$(echo "$app" | jq -r '.entity.buildpack // empty')
    detected_buildpack=$(echo "$app" | jq -r '.entity.detected_buildpack // empty')
    detected_buildpack_guid=$(echo "$app" | jq -r '.entity.detected_buildpack_guid // empty')

    # Safe droplet and package size retrieval
    droplet_data=$(safe_cf_curl "/v3/apps/${app_guid}/droplets")
    packages_data=$(safe_cf_curl "/v3/apps/${app_guid}/packages")
    
    droplet_download_link=$(echo "$droplet_data" | jq -r '.resources // [] | .[0].links.download.href // empty' | sed 's|http[s]*://[^/]*||')
    packages_download_link=$(echo "$packages_data" | jq -r '.resources // [] | .[0].links.download.href // empty' | sed 's|http[s]*://[^/]*||')

    droplet_size_bytes=""
    if [[ -n "$droplet_download_link" ]]; then
        droplet_size_bytes=$(cf curl -X HEAD -v "$droplet_download_link" 2>&1 | grep -i 'Content-Length:' | awk '{print $2}' | tr -d '\r' 2>/dev/null || echo "")
    fi

    packages_size_bytes=""
    if [[ -n "$packages_download_link" ]]; then
        packages_size_bytes=$(cf curl -X HEAD -v "$packages_download_link" 2>&1 | grep -i 'Content-Length:' | awk '{print $2}' | tr -d '\r' 2>/dev/null || echo "")
    fi

    health_check=$(echo "$app" | jq -r '.entity.health_check_type // empty')
    app_state=$(echo "$app" | jq -r '.entity.state // empty')
    
    # Safe stack, space, and org retrieval
    stack_url=$(echo "$app" | jq -r '.entity.stack_url // empty')
    stack_name=""
    if [[ -n "$stack_url" && "$stack_url" != "null" ]]; then
        stack_data=$(safe_cf_curl "$stack_url")
        stack_name=$(echo "$stack_data" | jq -r '.entity.name // empty')
    fi
    
    space_url=$(echo "$app" | jq -r '.entity.space_url // empty')
    space_name=""
    org_name=""
    if [[ -n "$space_url" && "$space_url" != "null" ]]; then
        space_json=$(safe_cf_curl "$space_url")
        space_name=$(echo "$space_json" | jq -r '.entity.name // empty')
        
        org_url=$(echo "$space_json" | jq -r '.entity.organization_url // empty')
        if [[ -n "$org_url" && "$org_url" != "null" ]]; then
            org_data=$(safe_cf_curl "$org_url")
            org_name=$(echo "$org_data" | jq -r '.entity.name // empty')
        fi
    fi
    
    created_at=$(echo "$app" | jq -r '.metadata.created_at // empty')
    updated_at=$(echo "$app" | jq -r '.metadata.updated_at // empty')

    # Buildpack filename retrieval
    input="${detected_buildpack:-$buildpack}"
    buildpack_filename=""
    if [[ "$detected_buildpack_guid" == "null" || -z "$detected_buildpack_guid" ]]; then
        buildpacks_data=$(safe_cf_curl "/v2/buildpacks?results-per-page=100")
        if [[ -z "$stack_name" || "$stack_name" == "null" ]]; then
            buildpack_filename=$(echo "$buildpacks_data" | jq -r --arg buildpack "$input" '.resources // [] | .[] | select(.entity.name == $buildpack) | .entity.filename' | head -n 1)
        else
            buildpack_filename=$(echo "$buildpacks_data" | jq -r --arg buildpack "$input" '.resources // [] | .[] | select(.entity.name == $buildpack) | .entity.filename' | grep -i "$stack_name" | head -n 1 || echo "")
        fi
    else
        buildpack_data=$(safe_cf_curl "/v2/buildpacks/${detected_buildpack_guid}")
        buildpack_filename=$(echo "$buildpack_data" | jq -r '.entity.filename // empty')
    fi

    version_info=$(get_version_info "$app_guid" "$detected_buildpack" "$buildpack_filename")
    IFS='|' read -r buildpack_version runtime_version <<< "$version_info"

    # Services retrieval
    service_bindings_url=$(echo "$app" | jq -r '.entity.service_bindings_url')
    services=""
    if [[ -n "$service_bindings_url" && "$service_bindings_url" != "null" ]]; then
        service_bindings_data=$(safe_cf_curl "$service_bindings_url")
        services=$(echo "$service_bindings_data" | jq -r '.resources // [] | .[] | .entity.service_instance_url' | while read -r service_instance_url; do
            if [[ -n "$service_instance_url" ]]; then
                data=$(safe_cf_curl "$service_instance_url")
                if [[ "$service_instance_url" == *"user_provided_service"* ]]; then
                    service_name=$(echo "$data" | jq -r '.entity.name')
                    service_plan=$(echo "$data" | jq -r '.entity.type')
                else
                    service_url=$(echo "$data" | jq -r '.entity.service_url')
                    service_plan_url=$(echo "$data" | jq -r '.entity.service_plan_url')
                    service_name=""
                    service_plan=""
                    if [[ -n "$service_url" && "$service_url" != "null" ]]; then
                        service_data=$(safe_cf_curl "$service_url")
                        service_name=$(echo "$service_data" | jq -r '.entity.service_broker_name')
                    fi
                    if [[ -n "$service_plan_url" && "$service_plan_url" != "null" ]]; then
                        service_plan_data=$(safe_cf_curl "$service_plan_url")
                        service_plan=$(echo "$service_plan_data" | jq -r '.entity.name')
                    fi
                fi
                echo "${service_name} (${service_plan})"
            fi
        done | paste -sd ":" -)
    fi

    # Routes retrieval
    routes_url=$(echo "$app" | jq -r '.entity.routes_url // empty')
    routes=""
    if [[ -n "$routes_url" && "$routes_url" != "null" ]]; then
        routes_data=$(safe_cf_curl "$routes_url")
        routes=$(echo "$routes_data" | jq -c '.resources // [] | .[]' | while read -r routedata; do
            if [[ -n "$routedata" ]]; then
                route_name=$(echo "$routedata" | jq -r '.entity.host')
                domain_url=$(echo "$routedata" | jq -r '.entity.domain_url')
                domain_name=""
                if [[ -n "$domain_url" && "$domain_url" != "null" ]]; then
                    domain_data=$(safe_cf_curl "$domain_url")
                    domain_name=$(echo "$domain_data" | jq -r '.entity.name')
                fi
                echo "${route_name}.${domain_name}"
            fi
        done | paste -sd ":" -)
    fi

    # Developers retrieval
    dev_usernames=""
    if [[ -n "$space_url" && "$space_url" != "null" ]]; then
        developers_data=$(safe_cf_curl "${space_url}/developers")
        dev_usernames=$(echo "$developers_data" | jq -r '.resources // [] | .[] | .entity | select(.username != null) | .username' | paste -sd ":" -)
    fi

    echo "$org_name,$space_name,$created_at,$updated_at,$name,$app_guid,$instances,$memory,$disk_quota,$buildpack,$detected_buildpack,$detected_buildpack_guid,$buildpack_filename,$buildpack_version,$runtime_version,$droplet_size_bytes,$packages_size_bytes,$health_check,$app_state,$stack_name,$services,$routes,$dev_usernames"
}

# Main execution - SEQUENTIAL processing for Windows compatibility
echo "Starting app data collection..." >&2

apps_data=$(safe_cf_curl "/v2/apps?results-per-page=100")
total_pages=$(echo "$apps_data" | jq '.total_pages // 1')

echo "Total pages to process: $total_pages" >&2

for i in $(seq 1 $total_pages); do
    echo "Processing page $i of $total_pages..." >&2
    
    page_data=$(safe_cf_curl "/v2/apps?page=$i&results-per-page=100")
    
    # Process apps sequentially to avoid parallel issues
    echo "$page_data" | jq -c '.resources // [] | .[]' | while IFS= read -r app; do
        if [[ -n "$app" ]]; then
            process_app "$app"
        fi
    done
done

echo "Data collection complete!" >&2
