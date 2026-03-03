#!/usr/bin/env bash
set -euo pipefail

# =============================================================================
# Bulk Export Kafka Connect Connectors Configuration to CSV
# Uses /connectors/{name}/config for configuration
# Fetches type/state/tasks from separate /connectors/{name} call
# =============================================================================

DEFAULT_HOST="https://localhost:8083"

echo ""
echo "Kafka Connect REST URL [${DEFAULT_HOST}]:"
read -r input_host
HOST="${input_host:-$DEFAULT_HOST}"

echo ""
echo "Credentials (password hidden):"
read -r -p "Username: " CONNECT_USER
read -s -r -p "Password: " CONNECT_PASS
echo ""

[ -z "$CONNECT_USER" ] || [ -z "$CONNECT_PASS" ] && { echo "Username and password required"; exit 1; }

# INSECURE="--insecure"   # uncomment for self-signed cert
INSECURE=""

OUTPUT_FILE="connectors-export-$(date +%Y%m%d-%H%M%S).csv"
DEBUG_DIR="debug_connect_$(date +%Y%m%d-%H%M%S)"
mkdir -p "$DEBUG_DIR"

cat << 'EOF' > "$OUTPUT_FILE"
connector_name,connector_type,connector_class,state,tasks_total,connection_url,username_or_user,principal_or_jaas,other_sensitive_keys,full_config_json,notes
EOF

# List connectors
raw_list=$(curl -s -f -w "\n%{http_code}" ${INSECURE} -u "${CONNECT_USER}:${CONNECT_PASS}" "${HOST}/connectors" 2> "$DEBUG_DIR/list.err")
http_list=$(echo "$raw_list" | tail -n1)
body_list=$(echo "$raw_list" | sed '$d')

[ "$http_list" != "200" ] && { echo "List failed - HTTP $http_list"; cat "$DEBUG_DIR/list.err"; exit 1; }

mapfile -t connectors < <(echo "$body_list" | jq -r '.[]' 2>/dev/null)

[ ${#connectors[@]} -eq 0 ] && { echo "No connectors found"; exit 0; }

echo "Found ${#connectors[@]} connector(s)"
printf '  • %s\n' "${connectors[@]}"
echo ""

row_count=0

for name in "${connectors[@]}"; do
    ((row_count++))

    echo ""
    echo "──────────────────────────────────"
    echo "[$row_count] $name"

    safe_name=$(printf '%s' "$name" | tr -cd '[:alnum:]_.-' | head -c 100)
    [ -z "$safe_name" ] && safe_name="conn_${row_count}"

    encoded=$(printf '%s' "$name" | jq -sRr @uri 2>/dev/null || echo "$name")

    config_url="${HOST}/connectors/${encoded}/config"
    info_url="${HOST}/connectors/${encoded}?expand=status"

    config_file="$DEBUG_DIR/${safe_name}_config.json"
    info_file="$DEBUG_DIR/${safe_name}_info.json"
    err_file="$DEBUG_DIR/${safe_name}_err.txt"

    # 1. Get config
    curl ${INSECURE} -s -w "\n%{http_code}" -u "${CONNECT_USER}:${CONNECT_PASS}" "$config_url" \
        > "$config_file" 2> "$err_file"

    config_http=$(tail -n1 "$config_file")
    config_body=$(sed '$d' "$config_file")

    # 2. Get type/state/tasks
    curl ${INSECURE} -s -u "${CONNECT_USER}:${CONNECT_PASS}" "$info_url" > "$info_file" 2>> "$err_file"

    echo "Config HTTP: $config_http"

    if [ "$config_http" != "200" ]; then
        echo "  → Config fetch failed"
        cat "$err_file"
        continue
    fi

    # Extract fields
    connector_class=$(echo "$config_body" | jq -r '."connector.class" // "missing"')
    conn_url=$(echo "$config_body" | jq -r '."connection.url" // ""' | sed 's/"/""/g')

    user_fields=$(echo "$config_body" | jq -r 'to_entries[]
        | select(.key | test("user|username|principal|owner|sasl.mechanism|sasl.jaas"; "i"))
        | "\(.key)=\(.value)"' | tr '\n' ';' | sed 's/;$//; s/"/""/g' || echo "none")

    jaas=$(echo "$config_body" | jq -r 'to_entries[]
        | select(.key | test("jaas|principal|sasl.jaas|sasl.kerberos"; "i"))
        | .value' | head -n1 | sed 's/"/""/g' || echo "none")

    sensitive_keys=$(echo "$config_body" | jq -r 'keys[]
        | select(test("pass|secret|key|token|cred|private|sasl|truststore|keystore|password"; "i"))
        | .' | sort | uniq | paste -sd, - | sed 's/"/""/g' || echo "none")

    full_json=$(echo "$config_body" | jq -c . | sed 's/"/""/g' || echo "{}")

    # Status fields from info endpoint
    type=$(echo "$(cat "$info_file")" | jq -r '.info.type // "unknown"')
    state=$(echo "$(cat "$info_file")" | jq -r '.status.connector.state // "UNKNOWN"')
    tasks_total=$(echo "$(cat "$info_file")" | jq -r '.status.tasks | length // 0')

    notes=""
    if echo "$full_json" | grep -q '\*\*\*\*'; then
        notes="CONTAINS_MASKED_FIELDS"
    elif [ "$full_json" = "{}" ]; then
        notes="CONFIG_EMPTY"
    fi

    printf '"%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s"\n' \
        "$name" \
        "$type" \
        "$connector_class" \
        "$state" \
        "$tasks_total" \
        "$conn_url" \
        "${user_fields}" \
        "${jaas}" \
        "${sensitive_keys}" \
        "$full_json" \
        "$notes" \
        >> "$OUTPUT_FILE"

    echo "  → Row written"
done

echo ""
echo "Summary:"
echo "  Connectors: ${#connectors[@]}"
echo "  Rows in CSV: $row_count"
echo "  File: $OUTPUT_FILE"
echo "  Debug: $DEBUG_DIR"
echo ""
echo "If most configs are empty/masked → normal Confluent Platform behavior"
echo "Secrets are intentionally hidden in REST API responses since Kafka Connect 2.6+"
