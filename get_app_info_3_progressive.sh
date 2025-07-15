#!/bin/bash

# Number of parallel jobs (adjust based on your CF API rate limits)
MAX_PARALLEL_JOBS=5

# Enable associative arrays for caching
declare -A space_cache
declare -A org_cache
declare -A stack_cache
declare -A domain_cache
declare -A service_cache
declare -A service_plan_cache
declare -A developers_cache

# Lock file for synchronized output
OUTPUT_LOCK="/tmp/cf_export_$$.lock"
trap "rm -f $OUTPUT_LOCK" EXIT

# Function to output with lock (prevents garbled output from parallel processes)
synchronized_output() {
    (
        flock -x 200
        echo "$1"
    ) 200>"$OUTPUT_LOCK"
}

# Function to show progress
show_progress() {
    local current=$1
    local total=$2
    local app_name=$3
    >&2 echo -ne "\rProcessing app $current/$total: $app_name                    "
}

# Function to fetch and cache data
fetch_with_cache() {
    local cache_array=$1
    local key=$2
    local url=$3
    local jq_filter=${4:-'.'}
    
    # Use indirect reference to access the associative array
    local cache_var="${cache_array}[$key]"
    if [[ -z "${!cache_var}" ]]; then
        local result=$(cf curl "$url" | jq -r "$jq_filter")
        eval "${cache_array}[$key]=\"$result\""
        echo "$result"
    else
        echo "${!cache_var}"
    fi
}

# Function to process a single app
process_app() {
    local app=$1
    local app_number=$2
    local total_apps=$3
    
    # Extract basic app info
    local name=$(echo "$app" | jq -r '.entity.name')
    show_progress "$app_number" "$total_apps" "$name"
    
    local app_guid=$(echo "$app" | jq -r '.metadata.guid')
    local instances=$(echo "$app" | jq -r '.entity.instances')
    local memory=$(echo "$app" | jq -r '.entity.memory')
    local disk_quota=$(echo "$app" | jq -r '.entity.disk_quota')
    local buildpack=$(echo "$app" | jq -r '.entity.buildpack')
    local detected_buildpack=$(echo "$app" | jq -r '.entity.detected_buildpack')
    local health_check=$(echo "$app" | jq -r '.entity.health_check_type')
    local app_state=$(echo "$app" | jq -r '.entity.state')
    local created_at=$(echo "$app" | jq -r '.metadata.created_at')
    local updated_at=$(echo "$app" | jq -r '.metadata.updated_at')
    
    # Extract URLs
    local stack_url=$(echo "$app" | jq -r '.entity.stack_url')
    local space_url=$(echo "$app" | jq -r '.entity.space_url')
    local routes_url=$(echo "$app" | jq -r '.entity.routes_url')
    local service_binding_url=$(echo "$app" | jq -r '.entity.service_bindings_url')
    
    # Fetch stack name (cached)
    local stack_name=$(fetch_with_cache "stack_cache" "$stack_url" "$stack_url" '.entity.name')
    
    # Fetch space data (cached)
    local space_json=$(fetch_with_cache "space_cache" "$space_url" "$space_url" '.')
    local space_name=$(echo "$space_json" | jq -r '.entity.name')
    local org_url=$(echo "$space_json" | jq -r '.entity.organization_url')
    
    # Fetch org name (cached)
    local org_name=$(fetch_with_cache "org_cache" "$org_url" "$org_url" '.entity.name')
    
    # Fetch services
    local services=""
    local service_bindings=$(cf curl "$service_binding_url" 2>/dev/null)
    while read -r service_instance_url ; do
        if [[ -n "$service_instance_url" ]]; then
            local data=$(cf curl "$service_instance_url" 2>/dev/null)
            if [[ "$service_instance_url" == *"user_provided_service"* ]]; then
                local service_name=$(echo "$data" | jq -r '.entity.name')
                local service_plan=$(echo "$data" | jq -r '.entity.type')
            else
                local service_url=$(echo "$data" | jq -r '.entity.service_url')
                local service_plan_url=$(echo "$data" | jq -r '.entity.service_plan_url')
                local service_name=$(fetch_with_cache "service_cache" "$service_url" "$service_url" '.entity.service_broker_name')
                local service_plan=$(fetch_with_cache "service_plan_cache" "$service_plan_url" "$service_plan_url" '.entity.name')
            fi
            services+="${service_name} (${service_plan}):"
        fi
    done < <(echo "$service_bindings" | jq -r '.resources[].entity.service_instance_url' 2>/dev/null)
    
    # Fetch routes
    local routes=""
    local routes_data=$(cf curl "$routes_url" 2>/dev/null)
    while read -r routedata ; do
        if [[ -n "$routedata" ]]; then
            local route_name=$(echo "$routedata" | jq -r '.entity.host')
            local domain_url=$(echo "$routedata" | jq -r '.entity.domain_url')
            local domain_name=$(fetch_with_cache "domain_cache" "$domain_url" "$domain_url" '.entity.name')
            routes+="${route_name}.${domain_name}:"
        fi
    done < <(echo "$routes_data" | jq -c '.resources[]' 2>/dev/null)
    
    # Fetch developers (cached per space)
    local dev_key="${space_url}_developers"
    if [[ -z "${developers_cache[$dev_key]}" ]]; then
        local dev_usernames=""
        while read -r dev_username ; do
            dev_usernames+="${dev_username}:"
        done < <(cf curl "${space_url}/developers" 2>/dev/null | jq -r '.resources[].entity | select(.username != null) | .username')
        developers_cache[$dev_key]="$dev_usernames"
    fi
    local dev_usernames="${developers_cache[$dev_key]}"
    
    # Output immediately with synchronization
    synchronized_output "$org_name, $space_name, $created_at, $updated_at, $name, $app_guid, $instances, $memory, $disk_quota, $buildpack, $detected_buildpack, $health_check, $app_state, $stack_name, $services, $routes, $dev_usernames"
}

# Export everything needed for subshells
export -f process_app fetch_with_cache synchronized_output show_progress
export OUTPUT_LOCK

# Print header
echo "Org_Name,Space_Name,Created_At,Updated_At,Name,GUID,Instances,Memory,Disk_Quota,Requested_Buildpack,Detected_Buildpack,HealthCheckType,App_State,Stack_Name,Services,Routes,Developers"

# Get total pages and count total apps for progress
>&2 echo "Fetching app list..."
total_pages=$(cf curl "/v2/apps?results-per-page=100" | jq '.total_pages')
total_apps=$(cf curl "/v2/apps?results-per-page=100" | jq '.total_results')
>&2 echo "Found $total_apps apps across $total_pages pages"

# Process all pages
app_counter=0
for i in $(seq 1 $total_pages); do
    >&2 echo -e "\nFetching page $i of $total_pages..."
    apps_json=$(cf curl "/v2/apps?page=$i&results-per-page=100")
    
    # Process apps with controlled parallelism
    while read -r app; do
        ((app_counter++))
        
        # Run in background with all necessary variables
        (
            # Re-declare arrays in subshell
            declare -A space_cache
            declare -A org_cache
            declare -A stack_cache
            declare -A domain_cache
            declare -A service_cache
            declare -A service_plan_cache
            declare -A developers_cache
            
            process_app "$app" "$app_counter" "$total_apps"
        ) &
        
        # Control parallelism
        if [[ $(jobs -r | wc -l) -ge $MAX_PARALLEL_JOBS ]]; then
            wait -n  # Wait for any job to finish
        fi
    done < <(echo "$apps_json" | jq -c '.resources[]')
done

# Wait for remaining jobs
wait

# Clear progress line
>&2 echo -e "\nCompleted processing $total_apps apps!"
