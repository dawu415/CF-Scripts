get_version_info() {
    local app_guid=$1
    local detected_buildpack=$2
    local buildpack_filename=$3
    
    # Quick method - just get environment variables
    env_data=$(cf curl "/v2/apps/${app_guid}/env" 2>/dev/null || echo "{}")
    
    # Extract version based on buildpack type. For buildpack version we can extract from buildpack_filename. 
    # The grep -oP 'v\K[\d.]+' will extract the version number from the filename but may have a dangling period or dash at the end.
    # We need to remove these dangling characters if they exist.
    buildpack_version=$(echo "$buildpack_filename" | grep -oP 'v\K[\d.]+' | sed 's/[-.]$//' || echo "")
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


echo "Org_Name,Space_Name,Created_At,Updated_At,Name,GUID,Instances,Memory,Disk_Quota,Requested_Buildpack,Detected_Buildpack,Detected_Buildpack_GUID, Buildpack_Filename, Buildpack_Version, Runtime_Version, DropletSizeBytes, PackagesSizeBytes, HealthCheckType,App_State,Stack_Name,Services,Routes,Developers"

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
    detected_buildpack_guid=$(echo "$app" | jq -r '.entity.detected_buildpack_guid')

    # Now we get the app's droplet and package sizes. We will need to use v3 as that uses the app GUID directly.
    # We will need to strip the http://<domain> and retain anything after the /v3 to use with cf curl.
    droplet_download_link=$(cf curl "/v3/apps/${app_guid}/droplets" | jq -r '.resources[0].links.download.href' | sed 's|http[s]*://[^/]*||')
    packages_download_link=$(cf curl "/v3/apps/${app_guid}/packages" | jq -r '.resources[0].links.download.href'| sed 's|http[s]*://[^/]*||')

    # Get the droplet size and package size. This is done via cf curl but with -X HEAD and -v to avoid downloading the content.
    # the size is in the Content-Length header of the response section.
    droplet_size_bytes=$(cf curl -X HEAD -v "$droplet_download_link" 2>&1 | grep -i 'Content-Length:' | awk '{print $2}' | tr -d '\r')
    packages_size_bytes=$(cf curl -X HEAD -v "$packages_download_link" 2>&1 | grep -i 'Content-Length:' | awk '{print $2}' | tr -d '\r')

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

    buildpack_filename=""
    if [[ "$detected_buildpack_guid" == "null" ]]; then
        # if detected_buildpack_guid is null, we grab the list of buildpacks via /v2/buildpacks.  We filter the name of the buildpack based on either
        # the detected_buildpack or the buildpack name. We obtain the filename of the buildpack from the list. If there are more multiple filenames ,e.g. due to cflinux stack, concatenate them.      
        if [[ -n "$detected_buildpack" ]]; then
            input="$detected_buildpack"
        else
            input="$buildpack"
        fi
        
        # use the stack name to help select the correct buildpack filename if there are multiple buildpacks with the same name.
        # If the stack name is not available (blank or null), we will just get the first matching buildpack filename
        if [[ -z "$stack_name" || "$stack_name" == "null" ]]; then
            buildpack_filename=$(cf curl "/v2/buildpacks?results-per-page=100" | jq -r --arg buildpack "$input" '.resources[] | select(.entity.name == $buildpack) | .entity.filename' | head -n 1)
        else
            buildpack_filename=$(cf curl "/v2/buildpacks?results-per-page=100" | jq -r --arg buildpack "$input" '.resources[] | select(.entity.name == $buildpack) | .entity.filename' | grep -i "$stack_name" | head -n 1)
        
    else
        # if detected_buildpack_guid is not null, we can use it to get the buildpack filename directly.
        buildpack_filename=$(cf curl "/v2/buildpacks/${detected_buildpack_guid}" | jq -r '.entity.filename')
    fi
    
    # Now we can extract the buildpack version and runtime version.
    # We will use the get_version_info function defined above.
    # This function will return the buildpack version and runtime version based on the detected_buildpack
    # and the app_guid. We will also use the buikdpack_filename to help identify the buildpack version.
    # If the buildpack version is not available, we will just return an empty string.
    # If the runtime version is not available, we will just return an empty string.

    version_info=$(get_version_info "$app_guid" "$detected_buildpack" "$buildpack_filename")
    IFS='|' read -r buildpack_version runtime_version <<< "$version_info"


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
    echo "$org_name, $space_name, $created_at, $updated_at, $name, $app_guid, $instances, $memory, $disk_quota, $buildpack, $detected_buildpack, $detected_buildpack_guid, $buildpack_filename, $buildpack_version, $runtime_version, $droplet_size_bytes, $packages_size_bytes, $health_check, $app_state, $stack_name, $services, $routes, $dev_usernames"  
}

export -f process_app
export -f get_version_info

# Main processing with GNU parallel (if available) or xargs
total_pages=$(cf curl "/v2/apps?results-per-page=100" | jq '.total_pages')

for i in $(seq 1 $total_pages); do
    #echo "Processing page: $i of $total_pages" >&2
    
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


