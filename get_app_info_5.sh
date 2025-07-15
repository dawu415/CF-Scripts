get_version_info() {
    local app_guid=$1
    local detected_buildpack=$2
    
    # Quick method - just get environment variables
    env_data=$(cf curl "/v2/apps/${app_guid}/env" 2>/dev/null || echo "{}")
    
    # Extract version based on buildpack type
    buildpack_version=""
    runtime_version=""
    
    if [[ "$detected_buildpack" == *"java"* ]] || [[ "$detected_buildpack" == *"Java"* ]]; then
        # For Java buildpack
        runtime_version=$(echo "$env_data" | jq -r '.environment_json.JAVA_VERSION // empty' 2>/dev/null)
        
        # If not found, try to parse from JBP_CONFIG
        if [[ -z "$runtime_version" || "$runtime_version" == "null" ]]; then
            jbp_config=$(echo "$env_data" | jq -r '.environment_json.JBP_CONFIG_OPEN_JDK_JRE // empty' 2>/dev/null)
            if [[ -n "$jbp_config" && "$jbp_config" != "null" ]]; then
                runtime_version=$(echo "$jbp_config" | grep -oP 'version:\s*["\047]?\K[^"\047,}]+' | head -1)
            fi
        fi
        
        # Try to get buildpack version from detected_buildpack string
        buildpack_version=$(echo "$detected_buildpack" | grep -oP 'v\K[\d.]+' || echo "")
        
    elif [[ "$detected_buildpack" == *"node"* ]] || [[ "$detected_buildpack" == *"Node"* ]]; then
        runtime_version=$(echo "$env_data" | jq -r '.environment_json.NODE_VERSION // empty' 2>/dev/null)
        
    elif [[ "$detected_buildpack" == *"python"* ]] || [[ "$detected_buildpack" == *"Python"* ]]; then
        runtime_version=$(echo "$env_data" | jq -r '.environment_json.PYTHON_VERSION // empty' 2>/dev/null)
    fi
    
    # Clean up null/empty values
    [[ "$buildpack_version" == "null" || -z "$buildpack_version" ]] && buildpack_version=""
    [[ "$runtime_version" == "null" || -z "$runtime_version" ]] && runtime_version=""
    
    echo "${buildpack_version}|${runtime_version}"
}


echo "Org_Name,Space_Name,Created_At,Updated_At,Name,GUID,Instances,Memory,Disk_Quota,Requested_Buildpack,Detected_Buildpack,Buikdpack_Version, Runtime_Version, HealthCheckType,App_State,Stack_Name,Services,Routes,Developers"

# Function to process a single app
process_app() {
    local app=$1
    
    # Your original logic here, unchanged
    name=$(echo "$app" | jq -r '.entity.name')
    app_guid=$(echo "$app" | jq -r '.metadata.guid')
    instances=$(echo "$app" | jq -r '.entity.instances')
    memory=$(echo "$app" | jq -r '.entity.memory')
    disk_quota=$(echo "$app" | jq -r '.entity.disk_quota')
    buildpack=$(echo "$app" | jq -r '.entity.buildpack')
    detected_buildpack=$(echo "$app" | jq -r '.entity.detected_buildpack')
    
    version_info=$(get_version_info "$app_guid" "$detected_buildpack")
    IFS='|' read -r buildpack_version runtime_version <<< "$version_info"

    health_check="$(echo "$app" | jq -r '.entity.health_check_type')"
    app_state=$(echo "$app" | jq -r '.entity.state')
    stack_name=$(cf curl "$(echo "$app" | jq -r '.entity.stack_url')" | jq -r '.entity.name')
    space_url=$(echo "$app" | jq -r '.entity.space_url')
    space_json=$(cf curl "$space_url")
    space_name=$(echo "$space_json" | jq -r '.entity.name')
    org_url=$(echo "$space_json" | jq -r '.entity.organization_url')
    org_name=$(cf curl "$org_url" | jq -r '.entity.name')
    created_at=$(echo "$app" | jq -r '.metadata.created_at')
    updated_at=$(echo "$app" | jq -r '.metadata.updated_at')
    routes_url=$(echo "$app" | jq -r '.entity.routes_url')
    service_binding_url=$(echo "$app" | jq -r '.entity.service_bindings_url')

    # Get all services bound to the app
    services=""
    while read -r service_instance_url ; do
        data="$(cf curl "$service_instance_url")"
        if [[ "$service_instance_url" == *"user_provided_service"* ]]; then
            service_name=$(echo "$data" | jq -r '.entity.name')
            service_plan=$(echo "$data" | jq -r '.entity.type')
        else
            service_url=$(echo "$data" | jq -r '.entity.service_url')
            service_plan_url=$(echo "$data" | jq -r '.entity.service_plan_url')
            service_name=$(cf curl "$service_url" | jq -r '.entity.service_broker_name')
            service_plan=$(cf curl "$service_plan_url" | jq -r '.entity.name')
        fi  
        services+="${service_name} (${service_plan}):"
    done < <(cf curl "$service_binding_url" | jq -r -c '.resources[].entity.service_instance_url')  

    # Get all routes of the app
    routes=""
    while read -r routedata ; do
        route_name=$(echo "$routedata" | jq -r '.entity.host')
        domain_name=$(cf curl "$(echo "$routedata" | jq -r '.entity.domain_url')" | jq -r '.entity.name')
        routes+="${route_name}.${domain_name}:"
    done < <(cf curl "$routes_url" | jq -c '.resources[]')

    # Get all developers in the space
    dev_usernames=""
    while read -r dev_username ; do
        dev_usernames+="${dev_username}:"
    done < <(cf curl "${space_url}/developers" | jq '.resources[].entity | select(.username != null) | .username')

    # Output Data
    echo "$org_name, $space_name, $created_at, $updated_at, $name, $app_guid, $instances, $memory, $disk_quota, $buildpack, $detected_buildpack,$buildpack_version, $runtime_version, $health_check, $app_state, $stack_name, $services, $routes, $dev_usernames"  
}

export -f process_app

# Main processing with GNU parallel (if available) or xargs
total_pages=$(cf curl "/v2/apps?results-per-page=100" | jq '.total_pages')

for i in $(seq 1 $total_pages); do
    echo "Processing page: $i of $total_pages" >&2
    
    # Option A: Use GNU parallel if available (FASTEST)
    if command -v parallel &> /dev/null; then
        cf curl "/v2/apps?page=$i&results-per-page=100" | \
            jq -r -c '.resources[]' | \
            parallel -j 10 process_app
    
    # Option B: Use xargs for parallelism
    else
        cf curl "/v2/apps?page=$i&results-per-page=100" | \
            jq -r -c '.resources[] | tostring | @sh' | \
            xargs -P 10 -I {} bash -c 'process_app "$@"' _ {}
    fi
done


