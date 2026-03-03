#!/usr/bin/env bash
set -euo pipefail

# =============================================================================
# Bulk Export Kafka Connect Connectors Configuration to CSV
# Uses /connectors/{name}/config endpoint (better for config retrieval)
# Works with Confluent Platform (handles masked sensitive fields)
# =============================================================================

DEFAULT_HOST="https://localhost:8083"

echo ""
echo "Kafka Connect REST URL [${DEFAULT_HOST}]:"
read -r input_host
HOST="${input_host:-$DEFAULT_HOST}"

echo ""
echo "Credentials (password will not be shown):"
echo ""

read -r -p "Username: " CONNECT_USER
read -s -r -p "Password: " CONNECT_PASS
echo ""

if [ -z "$CONNECT_USER" ] || [ -z "$CONNECT_PASS" ]; then
    echo "Error: Username and password are required." >&2
    exit 1
fi

# Uncomment if self-signed/internal CA causes SSL errors
# INSECURE="--insecure"
INSECURE=""

OUTPUT_FILE="kafka-connectors-export-$(date +%Y%m%d-%H%M%S).csv"
DEBUG_DIR="debug_connect_export_$(date +%Y%m%d-%H%M%S)"
mkdir -p "$DEBUG_DIR" || { echo "Cannot create debug directory"; exit 1; }

echo "Debug files → ${DEBUG_DIR}"
echo ""

# CSV header (added 'notes' column for masking info)
cat << 'EOF' > "$OUTPUT_FILE"
connector_name,connector_type,connector_class,state,tasks_total,connection_url,username_or_user,principal_or_jaas,other_sensitive_keys,full_config_json,notes
EOF

echo "Fetching connector list from: ${HOST}/connectors"
echo ""

# Get list + status code
raw_list=$(curl -s -f -w "\n%{http_code}" ${INSECURE} -u "${CONNECT_USER}:${CONNECT_PASS}" "${HOST}/connectors" 2> "${DEBUG_DIR}/list_curl.err")

http_code=$(echo "$raw_list" | tail -n1)
body=$(echo "$raw_list" | sed '$d')

if [ "$http_code" -ne 200 ]; then
    echo "ERROR: Failed to list connectors (HTTP ${http_code})"
    echo "Curl error:"
    cat "${DEBUG_DIR}/list_curl.err"
    echo ""
    echo "Manual test:"
    echo "  curl -v ${INSECURE} -u \"${CONNECT_USER}:<pass>\" \"${HOST}/connectors\""
    exit 1
fi

mapfile -t connectors < <(echo "$body" | jq -r '.[]' 2>/dev/null)

if [ ${#connectors[@]} -eq 0 ]; then
    echo "No connectors found (empty array returned)."
    echo "Check Confluent Control Center or connect-configs topic."
    exit 0
fi

echo "Found ${#connectors[@]} connector(s)"
echo "Names:"
printf '  • %s\n' "${connectors[@]}"
echo ""

row_count=0

for name in "${connectors[@]}"; do
    echo ""
    echo "──────────────────────────────────────────────"
    echo "Processing: $name"

    safe_name=$(printf '%s' "$name" | tr -cd '[:alnum:]_.-' | head -c 100)
    [ -z "$safe_name" ] && safe_name="connector_${row_count}"

    # URL-encode connector name
    encoded=$(printf '%s' "$name" | jq -sRr @uri 2>/dev/null || echo "$name")
    config_url="${HOST}/connectors/${encoded}/config"

    raw_file="${DEBUG_DIR}/${safe_name}_config.json"
    err_file="${DEBUG_DIR}/${safe_name}_err.txt"
    status_file="${DEBUG_DIR}/${safe_name}_http_status.txt"

    # Fetch pure config endpoint
    curl ${INSECURE} -s -w "\n%{http_code}" -u "${CONNECT_USER}:${CONNECT_PASS}" "$config_url" \
        > "$raw_file" 2> "$err_file"

    http_code=$(tail -n1 "$raw_file")
    config_body=$(sed '$d' "$raw_file")
    echo "$http_code" > "$status_file"

    echo "Config endpoint HTTP: $http_code   → $config_url"

    if [ "$http_code" != "200" ]; then
        echo "  → Failed (HTTP $http_code)"
        echo "  Error output:"
        cat "$err_file"
        continue
    fi

    if [ -z "$config_body" ]; then
        echo "  → Empty config response"
        continue
    fi

    if ! echo "$config_body" | jq . >/dev/null 2>&1; then
        echo "  → Not valid JSON"
        continue
    fi

    echo "  → Valid config JSON"

    config=$(echo "$config_body" | jq .)

    # Try to get type/state/tasks from /connectors/{name} if needed
    info_url="${HOST}/connectors/${encoded}?expand=status"
    info_raw=$(curl ${INSECURE} -s -u "${CONNECT_USER}:${CONNECT_PASS}" "$info_url" 2>/dev/null)

    type=$(echo "$info_raw" | jq -r '.info.type // "unknown"')
    state=$(echo "$info_raw" | jq -r '.status.connector.state // "UNKNOWN"')
    tasks_total=$(echo "$info_raw" | jq -r '.status.tasks | length // 0')

    conn_url=$(echo "$config" | jq -r '.["connection.url"] // ""' | sed 's/"/""/g')

    user_fields=$(echo "$config" | jq -r 'to_entries[]
        | select(.key | test("user|username|principal|owner|sasl.mechanism|sasl.jaas"; "i"))
        | "\(.key)=\(.value)"' | tr '\n' ';' | sed 's/;$//; s/"/""/g' || echo "none")

    jaas=$(echo "$config" | jq -r 'to_entries[]
        | select(.key | test("jaas|principal|sasl.jaas|sasl.kerberos"; "i"))
        | .value' | head -n1 | sed 's/"/""/g' || echo "none")

    sensitive_keys=$(echo "$config" | jq -r 'keys[]
        | select(test("pass|secret|key|token|cred|private|sasl|truststore|keystore|password"; "i"))
        | .' | sort | uniq | paste -sd, - | sed 's/"/""/g' || echo "none")

    full_json=$(echo "$config" | jq -c . | sed 's/"/""/g' || echo "{}")

    # Detect masking / empty config
    notes=""
    if [ "$full_json" = "{}" ] || [ -z "$full_json" ] || echo "$full_json" | grep -q '\*\*\*\*'; then
        notes="CONFIG_CONTAINS_MASKED_FIELDS_(Confluent_security_behavior)"
        echo "  → Config is masked or empty (normal in Confluent Platform)"
    fi

    # Write row
    printf '"%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s"\n' \
        "$name" \
        "$type" \
        "$class" \
        "$state" \
        "$tasks_total" \
        "$conn_url" \
        "${user_fields}" \
        "${jaas}" \
        "${sensitive_keys}" \
        "$full_json" \
        "$notes" \
        >> "$OUTPUT_FILE"

    ((row_count++))
    echo "  → Row written (total: $row_count)"
done

echo ""
echo "──────────────────────────────────────────────"
echo "Summary:"
echo "  Connectors processed : ${#connectors[@]}"
echo "  Rows written to CSV  : $row_count"
echo "  Output file          : $OUTPUT_FILE"
echo "  Debug folder         : $DEBUG_DIR"
echo ""

if [ $row_count -eq 0 ]; then
    echo "No rows written — most likely because configs are masked by Confluent Platform."
    echo "The full plaintext configuration (including secrets) is not available via REST API."
    echo ""
    echo "To get real values:"
    echo "  • Original creation JSON (Terraform/Ansible/deployment script)"
    echo "  • Confluent CLI: confluent connect describe <name>"
    echo "  • Consume connect-configs topic"
    echo "  • Secret Registry (if enabled)"
fi
