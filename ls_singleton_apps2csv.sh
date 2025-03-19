#!/bin/bash

# Output CSV header
echo "Org,Space,App Name"

# Initialize variables for pagination
page=1
per_page=50
total_pages=1

# Loop through all pages
while [ "$page" -le "$total_pages" ]; do
  # Fetch a page of applications
  response=$(cf curl "/v3/apps?page=$page&per_page=$per_page")

  # Extract pagination information
  total_pages=$(echo "$response" | jq -r '.pagination.total_pages')

  # Iterate over each application
  echo "$response" | jq -c '.resources[]' | while read -r app; do
    # Extract application details
    app_name=$(echo "$app" | jq -r '.name')
    app_guid=$(echo "$app" | jq -r '.guid')
    space_guid=$(echo "$app" | jq -r '.relationships.space.data.guid')
    desired_state=$(echo "$app" | jq -r '.state')

    # Check if the application is in the STARTED state
    if [ "$desired_state" == "STARTED" ]; then
      # Fetch space details
      space=$(cf curl "/v3/spaces/$space_guid")
      space_name=$(echo "$space" | jq -r '.name')
      org_guid=$(echo "$space" | jq -r '.relationships.organization.data.guid')

      # Fetch organization details
      org=$(cf curl "/v3/organizations/$org_guid")
      org_name=$(echo "$org" | jq -r '.name')

      # Fetch process details to get instance count
      process=$(cf curl "/v3/apps/$app_guid/processes")
      instance_count=$(echo "$process" | jq -r '.resources[0].instances')

      # Check if the application has a single instance
      if [ "$instance_count" -eq 1 ]; then
        # Output in CSV format
        echo "$org_name,$space_name,$app_name"
      fi
    fi
  done

  # Increment page number
  page=$((page + 1))
done
