#!/bin/bash

# Output CSV header
echo "Source Org,Source Space,Source App,Destination Org,Destination Space,Destination App,Port,Protocol"

# Fetch all networking policies
policies=$(cf curl /networking/v1/external/policies)

# Iterate over each policy
echo "$policies" | jq -c '.policies[]' | while read -r policy; do
  # Extract source and destination app GUIDs
  source_app_guid=$(echo "$policy" | jq -r '.source.id')
  dest_app_guid=$(echo "$policy" | jq -r '.destination.id')
  protocol=$(echo "$policy" | jq -r '.protocol')
  port=$(echo "$policy" | jq -r '.destination.port')

  # Fetch source app details
  source_app=$(cf curl "/v3/apps/$source_app_guid")
  source_app_name=$(echo "$source_app" | jq -r '.name')
  source_space_guid=$(echo "$source_app" | jq -r '.relationships.space.data.guid')
  source_space=$(cf curl "/v3/spaces/$source_space_guid")
  source_space_name=$(echo "$source_space" | jq -r '.name')
  source_org_guid=$(echo "$source_space" | jq -r '.relationships.organization.data.guid')
  source_org=$(cf curl "/v3/organizations/$source_org_guid")
  source_org_name=$(echo "$source_org" | jq -r '.name')

  # Fetch destination app details
  dest_app=$(cf curl "/v3/apps/$dest_app_guid")
  dest_app_name=$(echo "$dest_app" | jq -r '.name')
  dest_space_guid=$(echo "$dest_app" | jq -r '.relationships.space.data.guid')
  dest_space=$(cf curl "/v3/spaces/$dest_space_guid")
  dest_space_name=$(echo "$dest_space" | jq -r '.name')
  dest_org_guid=$(echo "$dest_space" | jq -r '.relationships.organization.data.guid')
  dest_org=$(cf curl "/v3/organizations/$dest_org_guid")
  dest_org_name=$(echo "$dest_org" | jq -r '.name')

  # Output policy details in CSV format
  echo "$source_org_name,$source_space_name,$source_app_name,$dest_org_name,$dest_space_name,$dest_app_name,$port,$protocol"
done

