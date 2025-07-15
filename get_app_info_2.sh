#!/bin/bash

# Number of parallel jobs (adjust based on your CF API rate limits)
MAX_PARALLEL_JOBS=10

# Temp directory for intermediate results
TEMP_DIR=$(mktemp -d)
trap "rm -rf $TEMP_DIR" EXIT

# Cache directory for API responses
CACHE_DIR="$TEMP_DIR/cache"
mkdir -p "$CACHE_DIR"

# Function to fetch with cache
fetch_with_cache() {
    local url=$1
    local cache_key=$(echo "$url" | sed 's/[^a-zA-Z0-9]/_/g')
    local cache_file="$CACHE_DIR/$cache_key"
    
    if [[ -f "$cache_file" ]]; then
        cat "$cache_file"
    else
        local result=$(cf curl "$url")
        echo "$result" > "$cache_file"
        echo "$result"
    fi
}

# Function to process a single app
process_app() {
    local app=$1
    local output_file=$2
    
    # Extract basic app info
    local name=$(echo "$app" | jq -r '.entity.name')
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
    
    # Fetch stack name
    local stack_name=$(fetch_with_cache "$stack_url" | jq -r '.entity.name')
    
    # Fetch space and org info
    local space_json=$(fetch_with_cache "$space_url")
    local space_name=$(echo "$space_json" | jq -r '.entity.name')
    local org_url=$(echo "$space_json" | jq -r '.entity.organization_url')
    local org_name=$(fetch_with_cache "$org_url" | jq -r '.entity.name')
    
    # Fetch services
    local services=""
    local service_bindings=$(fetch_with_cache "$service_binding_url")
    while read -r service_instance_url ; do
        if [[ -n "$service_instance_url" ]]; then
            local data=$(fetch_with_cache "$service_instance_url")
            if [[ "$service_instance_url" == *"user_provided_service"* ]]; then
                local service_name=$(echo "$data" | jq -r '.entity.name')
                local service_plan=$(echo "$data" | jq -r '.entity.type')
            else
                local service_url=$(echo "$data" | jq -r '.entity.service_url')
                local service_plan_url=$(echo "$data" | jq -r '.entity.service_plan_url')
                local service_name=$(fetch_with_cache "$service_url" | jq -r '.entity.service_broker_name')
                local service_plan=$(fetch_with_cache "$service_plan_url" | jq -r '.entity.name')
            fi
            services+="${service_name} (${service_plan}):"
        fi
    done < <(echo "$service_bindings" | jq -r '.resources[].entity.service_instance_url')
    
    # Fetch routes
    local routes=""
    local routes_data=$(fetch_with_cache "$routes_url")
    while read -r routedata ; do
        if [[ -n "$routedata" ]]; then
            local route_name=$(echo "$routedata" | jq -r '.entity.host')
            local domain_url=$(echo "$routedata" | jq -r '.entity.domain_url')
            local domain_name=$(fetch_with_cache "$domain_url" | jq -r '.entity.name')
            routes+="${route_name}.${domain_name}:"
        fi
    done < <(echo "$routes_data" | jq -c '.resources[]')
    
    # Fetch developers
    local dev_usernames=""
    local developers_data=$(fetch_with_cache "${space_url}/developers")
    while read -r dev_username ; do
        dev_usernames+="${dev_username}:"
    done < <(echo "$developers_data" | jq -r '.resources[].entity | select(.username != null) | .username')
    
    # Output to file
    echo "$org_name, $space_name, $created_at, $updated_at, $name, $app_guid, $instances, $memory, $disk_quota, $buildpack, $detected_buildpack, $health_check, $app_state, $stack_name, $services, $routes, $dev_usernames" >> "$output_file"
}

# Export functions for parallel execution
export -f process_app fetch_with_cache
export CACHE_DIR

# Print header
echo "Org_Name,Space_Name,Created_At,Updated_At,Name,GUID,Instances,Memory,Disk_Quota,Requested_Buildpack,Detected_Buildpack,HealthCheckType,App_State,Stack_Name,Services,Routes,Developers"

# Get total pages
total_pages=$(cf curl "/v2/apps?results-per-page=100" | jq '.total_pages')

# Process all pages in parallel
job_count=0
for i in $(seq 1 $total_pages); do
    # Fetch all apps on this page
    apps_json=$(cf curl "/v2/apps?page=$i&results-per-page=100")
    
    # Process each app in parallel
    echo "$apps_json" | jq -c '.resources[]' | while read -r app; do
        output_file="$TEMP_DIR/app_${i}_${job_count}.txt"
        process_app "$app" "$output_file" &
        
        # Limit parallel jobs
        job_count=$((job_count + 1))
        if [[ $((job_count % MAX_PARALLEL_JOBS)) -eq 0 ]]; then
            wait
        fi
    done
done

# Wait for all background jobs to complete
wait

# Combine all results
cat $TEMP_DIR/app_*.txt 2>/dev/null | sort
