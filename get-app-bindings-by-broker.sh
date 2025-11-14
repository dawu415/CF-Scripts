#!/usr/bin/env bash
#
# cf-broker-bindings-with-creds-orch.sh
#
# This script wraps the original service binding inventory script so it can
# participate nicely when launched by the Cloud Foundry environment
# orchestrator.  It honours the environment variables provided by the
# orchestrator for CSV output and writes rows under a file lock when
# CF_ORCH_DATA_OUT is set.  When no orchestrator is involved it
# behaves like the original script, emitting a header to stdout and
# then all rows.  The content of each row is unchanged from the
# original implementation.

set -euox pipefail

# -----------------------------------------------------------------------------
# Usage:
#   ./cf-broker-bindings-with-creds-orch.sh "<broker_name>"
#   ./cf-broker-bindings-with-creds-orch.sh "<broker_name>" "<offering_name_exact>"
#
# When CF_ORCH_DATA_OUT is defined the script writes its output to that
# path instead of stdout.  It will only write the CSV header once even
# if multiple concurrent instances run in parallel and append rows as
# they are generated.
#
# The remainder of this script is based closely on the original
# cf-broker-bindings-with-creds.sh provided by the user.  Only the
# header emission and row writing logic has been adjusted to play
# nicely with the orchestrator.
# -----------------------------------------------------------------------------

BROKER_NAME="${1:-}"
OFFERING_NAME_FILTER="${2:-}"

[[ -z "$BROKER_NAME" ]] && {
  echo "Usage: $0 <broker_name> [offering_name_exact]" >&2; exit 1;
}

cf api >/dev/null 2>&1 || {
  echo "cf CLI not logged in" >&2; exit 1;
}

enc() {
  jq -rn --arg s "$1" '$s|@uri'
}

# NOTE: added service_instance_guid after service_instance_name
HEADER="broker_name,binding_type,service_offering_name,service_plan_name,service_instance_name,service_instance_guid,service_binding_guid,binding_name,app_name,app_guid,space_name,space_guid,org_name,org_guid,credential_uri,credentials_json"

csv_open() {
  if [[ -n "${CF_ORCH_DATA_OUT:-}" ]]; then
    exec 200>>"$CF_ORCH_DATA_OUT"
    flock -x 200
    if [[ ! -s "$CF_ORCH_DATA_OUT" || "${CF_ORCH_FORCE_HEADER:-0}" == 1 ]]; then
      printf '%s\n' "$HEADER" >&200
    fi
    flock -u 200
  else
    printf '%s\n' "$HEADER"
  fi
}

csv_emit_raw() {
  local row="$1"
  if [[ -n "${CF_ORCH_DATA_OUT:-}" ]]; then
    { : >&200; } 2>/dev/null || csv_open
    flock -x 200
    printf '%s\n' "$row" >&200
    flock -u 200
  else
    printf '%s\n' "$row"
  fi
}

csv_open

get_all() {
  local path="$1"
  {
    while [[ -n "$path" ]]; do
      if [[ "$path" == *"?"* ]]; then
        path="${path}&per_page=5000"
      else
        path="${path}?per_page=5000"
      fi
      local resp
      resp="$(cf curl "$path" 2>/dev/null || echo '{}')"
      jq -rc '.resources[]' <<<"$resp"
      local next
      next="$(jq -r '.pagination.next.href // ""' <<<"$resp")"
      if [[ -n "$next" ]]; then
        if [[ "$next" == /* ]]; then
          path="$next"
        else
          path="${next#*//*/}"
        fi
      else
        path=""
      fi
    done
  } | jq -s '[.[]]'
}

fetch_with_param_chunks() {
  local base="$1" param="$2" size="$3" extra_q="$4"
  shift 4
  extra_q="${extra_q#\?}"
  extra_q="${extra_q#&}"
  local -a items=( "$@" )
  {
    local -a chunk=()
    local g
    for g in "${items[@]}"; do
      [[ -n "$g" ]] && chunk+=( "$g" )
      if (( ${#chunk[@]} >= size )); then
        local q
        printf -v q '%s,' "${chunk[@]}"
        q="${q%,}"
        local url="$base"
        if [[ -n "$extra_q" ]]; then
          url+="?${extra_q}&${param}=${q}"
        else
          url+="?${param}=${q}"
        fi
        get_all "$url" | jq -rc '.[]'
        chunk=()
      fi
    done
    if (( ${#chunk[@]} )); then
      local q
      printf -v q '%s,' "${chunk[@]}"
      q="${q%,}"
      local url="$base"
      if [[ -n "$extra_q" ]]; then
        url+="?${extra_q}&${param}=${q}"
      else
        url+="?${param}=${q}"
      fi
      get_all "$url" | jq -rc '.[]'
    fi
  } | jq -s '[.[]]'
}

parallel_get_objs() {
  local base="$1"
  xargs -I{} -P 16 bash -c 'cf curl "'"$base"'/{}" 2>/dev/null' |
  jq -s '[.[] | select(type=="object" and .guid != null)]'
}

parallel_get_binding_details_map() {
  # Build a map of binding GUIDs to their detailed credential information.
  xargs -I% -P 16 bash -c '
    guid="$1"
    cf curl "/v3/service_credential_bindings/$guid/details" 2>/dev/null |
    GUID="$guid" jq -c "{ (env.GUID): . }"
  ' _ % |
  jq -s 'add'
}

broker_guid="$(cf curl "/v3/service_brokers?names=$(enc "$BROKER_NAME")" | jq -r '.resources[0].guid // empty')"
if [[ -z "$broker_guid" ]]; then
  echo "No broker named: $BROKER_NAME" >&2
  exit 1
fi

offerings="$(get_all "/v3/service_offerings?service_broker_guids=${broker_guid}")"
if [[ -n "$OFFERING_NAME_FILTER" ]]; then
  offerings="$(jq -c --arg n "$OFFERING_NAME_FILTER" '[.[] | select(.name==$n)]' <<<"$offerings")"
fi
if [[ "$(jq 'length' <<<"$offerings")" -eq 0 ]]; then
  exit 0
fi

offer_guids_csv="$(jq -r '.[].guid' <<<"$offerings" | paste -sd, -)"
plans="$(get_all "/v3/service_plans?service_offering_guids=${offer_guids_csv}")"
mapfile -t PLAN_GUIDS < <(jq -r '.[].guid' <<<"$plans")
if (( ${#PLAN_GUIDS[@]} == 0 )); then
  exit 0
fi

instances="$(fetch_with_param_chunks "/v3/service_instances" "service_plan_guids" 50 "" "${PLAN_GUIDS[@]}")"
if [[ "$(jq 'length' <<<"$instances")" -eq 0 ]]; then
  exit 0
fi
mapfile -t INSTANCE_GUIDS < <(jq -r '.[].guid' <<<"$instances")

app_bindings="$(fetch_with_param_chunks "/v3/service_credential_bindings" "service_instance_guids" 50 "type=app" "${INSTANCE_GUIDS[@]}")"
key_bindings="$(fetch_with_param_chunks "/v3/service_credential_bindings" "service_instance_guids" 50 "type=key" "${INSTANCE_GUIDS[@]}")"

mapfile -t APP_GUIDS < <(jq -r '.[].relationships.app.data.guid' <<<"$app_bindings" | sort -u)
apps='[]'
if (( ${#APP_GUIDS[@]} )); then
  apps="$(printf '%s\n' "${APP_GUIDS[@]}" | parallel_get_objs "/v3/apps")"
fi

# spaces from both apps and instances (so unbound instances still resolve space/org)
mapfile -t SPACE_GUIDS < <(
  {
    jq -r '.[].relationships.space.data.guid' <<<"$apps"
    jq -r '.[].relationships.space.data.guid' <<<"$instances"
  } | sort -u
)

spaces='[]'
if (( ${#SPACE_GUIDS[@]} )); then
  spaces="$(printf '%s\n' "${SPACE_GUIDS[@]}" | parallel_get_objs "/v3/spaces")"
fi
mapfile -t ORG_GUIDS < <(jq -r '.[].relationships.organization.data.guid' <<<"$spaces" | sort -u)
orgs='[]'
if (( ${#ORG_GUIDS[@]} )); then
  orgs="$(printf '%s\n' "${ORG_GUIDS[@]}" | parallel_get_objs "/v3/organizations")"
fi

mapfile -t APP_BINDING_GUIDS < <(jq -r '.[].guid' <<<"$app_bindings")
mapfile -t KEY_BINDING_GUIDS < <(jq -r '.[].guid' <<<"$key_bindings")
app_detail_map='{}'
if (( ${#APP_BINDING_GUIDS[@]} )); then
  app_detail_map="$(printf '%s\n' "${APP_BINDING_GUIDS[@]}" | parallel_get_binding_details_map)"
fi
key_detail_map='{}'
if (( ${#KEY_BINDING_GUIDS[@]} )); then
  key_detail_map="$(printf '%s\n' "${KEY_BINDING_GUIDS[@]}" | parallel_get_binding_details_map)"
fi

offer_map="$(jq -c 'map({key:.guid, value:{name:.name}}) | from_entries' <<<"$offerings")"
plan_map="$(jq -c 'map({key:.guid, value:{name:.name, offering_guid:.relationships.service_offering.data.guid}}) | from_entries' <<<"$plans")"

# instance map includes space_guid for unbound instances
inst_map="$(jq -c 'map({key:.guid, value:{name:.name, plan_guid:.relationships.service_plan.data.guid, space_guid:.relationships.space.data.guid}}) | from_entries' <<<"$instances")"
app_map="$(jq -c 'map({key:.guid, value:{name:.name, space_guid:.relationships.space.data.guid}}) | from_entries' <<<"$apps")"
space_map="$(jq -c 'map({key:.guid, value:{name:.name, org_guid:.relationships.organization.data.guid}}) | from_entries' <<<"$spaces")"
org_map="$(jq -c 'map({key:.guid, value:{name:.name}}) | from_entries' <<<"$orgs")"

# Persist large lookup maps to temporary files.
tmpdir="$(mktemp -d)"
offer_file="$tmpdir/offer.json"
plan_file="$tmpdir/plan.json"
inst_file="$tmpdir/inst.json"
app_file="$tmpdir/app.json"
space_file="$tmpdir/space.json"
org_file="$tmpdir/org.json"
det_app_file="$tmpdir/app_details.json"
det_key_file="$tmpdir/key_details.json"

printf '%s' "$offer_map"      >"$offer_file"
printf '%s' "$plan_map"       >"$plan_file"
printf '%s' "$inst_map"       >"$inst_file"
printf '%s' "$app_map"        >"$app_file"
printf '%s' "$space_map"      >"$space_file"
printf '%s' "$org_map"        >"$org_file"
printf '%s' "$app_detail_map" >"$det_app_file"
printf '%s' "$key_detail_map" >"$det_key_file"

# Also persist bindings so we can compute unbound instances in jq
app_bind_file="$tmpdir/app_bindings.json"
key_bind_file="$tmpdir/key_bindings.json"
printf '%s' "$app_bindings" >"$app_bind_file"
printf '%s' "$key_bindings" >"$key_bind_file"

# Ensure temporary directory is cleaned up when the script exits
cleanup_tmp() {
  if [[ -n "$tmpdir" && -d "$tmpdir" ]]; then
    rm -rf "$tmpdir"
  fi
}
trap cleanup_tmp EXIT

app_jq_script=$(cat <<'JQAPP'
def safe_name(m; k): (m[0][k]? | objects | .name?) // "N/A";
def safe_val(m; k; f): (m[0][k]? | objects | .[f]?) // null;
.[]
| . as $b
| ($b.guid // "N/A") as $bid
| "app" as $btype
| ($b.name // "") as $bname
| ($b.relationships.service_instance.data.guid? // null) as $si
| ($b.relationships.app.data.guid?              // null) as $appg
| (safe_val($INST; $si; "plan_guid"))            as $plan_guid
| (safe_val($PLAN; $plan_guid; "offering_guid")) as $off_guid
| ($DET[0][$bid].credentials? // {}) as $creds
| ($creds.url // $creds.uri // $creds.connection // $creds.jdbcUrl // $creds.jdbc_url // "") as $uri
| [
    $BROKER,
    $btype,
    safe_name($OFFER; $off_guid),
    safe_name($PLAN;  $plan_guid),
    safe_name($INST;  $si),
    ($si // ""),
    $bid,
    $bname,
    safe_name($APP;   $appg),
    ($appg // ""),
    safe_name($SPACE; (safe_val($APP; $appg; "space_guid"))),
    (safe_val($APP; $appg; "space_guid") // ""),
    safe_name($ORG;   (safe_val($SPACE; (safe_val($APP; $appg; "space_guid")); "org_guid"))),
    (safe_val($SPACE; (safe_val($APP; $appg; "space_guid")); "org_guid") // ""),
    ($uri // ""),
    (if $creds=={} then "" else ($creds|tojson) end)
  ] | @csv
JQAPP
)

key_jq_script=$(cat <<'JQKEY'
def safe_name(m; k): (m[0][k]? | objects | .name?) // "N/A";
def safe_val(m; k; f): (m[0][k]? | objects | .[f]?) // null;
.[]
| . as $b
| ($b.guid // "N/A") as $bid
| "key" as $btype
| ($b.name // "") as $bname
| ($b.relationships.service_instance.data.guid? // null) as $si
| (safe_val($INST; $si; "plan_guid"))            as $plan_guid
| (safe_val($PLAN; $plan_guid; "offering_guid")) as $off_guid
| ($DET[0][$bid].credentials? // {}) as $creds
| ($creds.url // $creds.uri // $creds.connection // $creds.jdbcUrl // $creds.jdbc_url // "") as $uri
| [
    $BROKER,
    $btype,
    safe_name($OFFER; $off_guid),
    safe_name($PLAN;  $plan_guid),
    safe_name($INST;  $si),
    ($si // ""),
    $bid,
    $bname,
    "", "", "", "", "", "",
    ($uri // ""),
    (if $creds=={} then "" else ($creds|tojson) end)
  ] | @csv
JQKEY
)

# Unbound instances: binding_type = "none", include service_instance_guid
unbound_jq_script=$(cat <<'JQUNB'
def safe_name(m; k): (m[0][k]? | objects | .name?) // "N/A";
def safe_val(m; k; f): (m[0][k]? | objects | .[f]?) // null;
def bound_si_guids:
  (( $APPB[0] // [] ) + ( $KEYB[0] // [] ))
  | map(.relationships.service_instance.data.guid // empty)
  | unique;

(bound_si_guids) as $bound
| .[]
| . as $i
| ($i.guid // "N/A") as $si
| select( $bound | index($si) | not )
| "none" as $btype
| "" as $bname
| (safe_val($INST; $si; "plan_guid"))            as $plan_guid
| (safe_val($PLAN; $plan_guid; "offering_guid")) as $off_guid
| (safe_val($INST; $si; "space_guid"))           as $space_guid
| [
    $BROKER,
    $btype,
    safe_name($OFFER; $off_guid),
    safe_name($PLAN;  $plan_guid),
    safe_name($INST;  $si),
    ($si // ""),
    "",
    $bname,
    "",
    "",
    safe_name($SPACE; $space_guid),
    ($space_guid // ""),
    safe_name($ORG;   (safe_val($SPACE; $space_guid; "org_guid"))),
    (safe_val($SPACE; $space_guid; "org_guid") // ""),
    "",
    ""
  ] | @csv
JQUNB
)

# App bindings
jq -r \
  --arg BROKER "$BROKER_NAME" \
  --slurpfile OFFER "$offer_file" \
  --slurpfile PLAN  "$plan_file" \
  --slurpfile INST  "$inst_file" \
  --slurpfile APP   "$app_file" \
  --slurpfile SPACE "$space_file" \
  --slurpfile ORG   "$org_file" \
  --slurpfile DET   "$det_app_file" \
  "$app_jq_script" <<<"$app_bindings" |
while IFS= read -r line; do
  csv_emit_raw "$line"
done

# Key bindings
jq -r \
  --arg BROKER "$BROKER_NAME" \
  --slurpfile OFFER "$offer_file" \
  --slurpfile PLAN  "$plan_file" \
  --slurpfile INST  "$inst_file" \
  --slurpfile DET   "$det_key_file" \
  "$key_jq_script" <<<"$key_bindings" |
while IFS= read -r line; do
  csv_emit_raw "$line"
done

# Unbound instances (no app/key bindings)
jq -r \
  --arg BROKER "$BROKER_NAME" \
  --slurpfile OFFER "$offer_file" \
  --slurpfile PLAN  "$plan_file" \
  --slurpfile INST  "$inst_file" \
  --slurpfile SPACE "$space_file" \
  --slurpfile ORG   "$org_file" \
  --slurpfile APPB  "$app_bind_file" \
  --slurpfile KEYB  "$key_bind_file" \
  "$unbound_jq_script" <<<"$instances" |
while IFS= read -r line; do
  csv_emit_raw "$line"
done
