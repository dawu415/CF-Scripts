#!/bin/bash
echo "Org_Name,Space_Name,Created_At,Updated_At,Name,GUID,Instances,Memory,Disk_Quota,Requested_Buildpack,Detected_Buildpack,HealthCheckType,App_State,Stack_Name,Services,Routes,Developers"
for i in $(seq 1 $(cf curl "/v2/apps?results-per-page=100" | jq '.total_pages')); do \
    #echo "Checking page: $i"; \
    cf curl "/v2/apps?page=$i&results-per-page=100" | jq -c '.resources[]' | while read -r app; do
        name=$(echo "$app" | jq -r '.entity.name')
        app_guid=$(echo "$app" | jq -r '.metadata.guid')
        instances=$(echo "$app" | jq -r '.entity.instances')
        memory=$(echo "$app" | jq -r '.entity.memory')
        disk_quota=$(echo "$app" | jq -r '.entity.disk_quota')
        buildpack=$(echo "$app" | jq -r '.entity.buildpack')
        detected_buildpack=$(echo "$app" | jq -r '.entity.detected_buildpack')
        health_check="$(echo "$app" | jq -r '.entity.health_check_type')"
        app_state=$(echo "$app" | jq -r '.entity.state')
        #stackGUID=$(echo "$app" | jq -r '.entity.stack_guid')
        #stack_name=$(cf curl "/v2/stacks/${stackGUID}" | jq -r '.entity.name')
        stack_name=$(cf curl "$(echo "$app" | jq -r '.entity.stack_url')"  | jq -r '.entity.name' ) 
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
          
          # Check if the url contains user_provided_service string, if
          # so, it is a user provided service. Curl it to get the name and pre-pend the name with ups
          # Otherwise, it is a managed service. Curl the managed service url to get the name
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
          
          # Append the service name and plan to the services string
          services+="${service_name} (${service_plan}):"

        done < <(cf curl "$service_binding_url" | jq -c '.resources[].entity.service_instance_url')  

        # Get all routes of the app
        routes=""
        while read -r routedata ; do
          route_name=$(echo "$routedata" | jq -r '.entity.host')
          domain_name=$(cf curl "$(echo "$routedata" | jq -r '.entity.domain_url')" | jq -r '.entity.name')
          routes+="${route_name}.${domain_name}:"
        done < <(cf curl "$routes_url" | jq -c '.resources[]')
 
        # # Strip the last colon 
        # if [[ -n "$routes" ]]; then
        #     routes="${routes::-1}"
        # fi

        # Get all developers in the space
        dev_usernames=""
        while read -r dev_username ; do
          dev_usernames+="${dev_username}:"
        done < <(cf curl "${space_url}/developers" | jq '.resources[].entity | select(.username != null) | .username')

        # # Strip the last Colon
        # if [[ -n "$dev_usernames" ]]; then
        #     dev_usernames="${dev_usernames::-1}"
        # fi

        # Output Data to STDOUT
        echo "$org_name, $space_name, $created_at, $updated_at, $name, $app_guid, $instances, $memory, $disk_quota, $buildpack, $detected_buildpack, $health_check, $app_state, $stack_name, $services, $routes, $dev_usernames"  
    done
done