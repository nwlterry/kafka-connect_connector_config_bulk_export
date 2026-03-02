#!/usr/bin/env bash
set -euo pipefail

# ──────────────────────────────────────────────────────────────────────────────
# Bulk export Kafka Connect connectors config → CSV (array-based loop fix)
# ──────────────────────────────────────────────────────────────────────────────

DEFAULT_HOST="https://connect.example.com:8083"

read -p "Kafka Connect REST URL [${DEFAULT_HOST}]: " input_host
HOST="${input_host:-$DEFAULT_HOST}"

echo ""
echo "Please enter your credentials (password will not be shown):"
echo ""

read -p "Username: " CONNECT_USER
read -s -p "Password: " CONNECT_PASS
echo ""

if [ -z "$CONNECT_USER" ] || [ -z "$CONNECT_PASS" ]; then
    echo "Error: Both username and password are required." >&2
    exit 1
fi

# Optional for self-signed certs
# INSECURE="--insecure"
INSECURE=""

OUTPUT_FILE="kafka-connectors-export-$(date +%Y%m%d-%H%M%S).csv"
DEBUG_DIR="debug_connect_$(date +%Y%m%d-%H%M%S)"
mkdir -p "$DEBUG_DIR"

echo "Debug files → $DEBUG_DIR"
echo ""

# CSV header
cat << 'EOF' > "$OUTPUT_FILE"
connector_name,connector_type,connector_class,state,tasks_total,connection_url,username_or_user,principal_or_jaas,other_sensitive_keys,full_config_json
EOF

# Get connectors as array
mapfile -t connector_array < <(curl -s -f ${INSECURE} -u "${CONNECT_USER}:${CONNECT_PASS}" "${HOST}/connectors" | jq -r '.[]' 2>/dev/null)

if [ ${#connector_array[@]} -eq 0 ]; then
    echo "No connectors found or API error."
    exit 0
fi

echo "Found ${#connector_array[@]} connector(s)"
echo "First few names (for debug):"
printf '  - %s\n' "${connector_array[@]:0:3}"
echo ""

connector_count=0

for name in "${connector_array[@]}"; do
    ((connector_count++))

    echo "Processing [$connector_count]: '$name'"

    # Sanitize filename (replace bad chars)
    safe_name="${name//[^a-zA-Z0-9._-]/_}"
    raw_file="$DEBUG_DIR/${safe_name}_raw.json"
    err_file="$DEBUG_DIR/${safe_name}.err"

    # Fetch details
    curl -s -f ${INSECURE} -u "${CONNECT_USER}:${CONNECT_PASS}" "${HOST}/connectors/${name}?expand=status,info" \
        > "$raw_file" 2> "$err_file"

    if [ $? -ne 0 ]; then
        echo "  → FAILED fetch (HTTP error or timeout) → see $err_file"
        cat "$err_file"
        continue
    fi

    if [ ! -s "$raw_file" ]; then
        echo "  → Empty response file → see $raw_file"
        continue
    fi

    # Validate JSON lightly
    if ! jq . "$raw_file" > /dev/null 2>&1; then
        echo "  → Invalid JSON → see $raw_file"
        continue
    fi

    echo "  → OK (response saved)"

    info=$(cat "$raw_file")

    type=$(jq -r '.info.type // "unknown"' <<< "$info")
    class=$(jq -r '.info.config["connector.class"] // ""' <<< "$info")
    state=$(jq -r '.status.connector.state // "UNKNOWN"' <<< "$info")
    tasks_total=$(jq -r '.status.tasks | length // 0' <<< "$info")

    config=$(jq '.info.config // {}' <<< "$info")

    conn_url=$(jq -r '.["connection.url"] // ""' <<< "$config" | sed 's/"/""/g')

    user_fields=$(jq -r 'to_entries[]
        | select(.key | test("user|username|principal|owner|sasl.mechanism|sasl.jaas"; "i"))
        | "\(.key)=\(.value)"' <<< "$config" | tr '\n' ';' | sed 's/;$//; s/"/""/g' || echo "none")

    jaas=$(jq -r 'to_entries[]
        | select(.key | test("jaas|principal|sasl.jaas|sasl.kerberos"; "i"))
        | .value' <<< "$config" | head -n1 | sed 's/"/""/g' || echo "none")

    sensitive_keys=$(jq -r 'keys[]
        | select(test("pass|secret|key|token|cred|private|sasl|truststore|keystore|password"; "i"))
        | .' <<< "$config" | sort | uniq | paste -sd, - | sed 's/"/""/g' || echo "none")

    full_json=$(jq -c . <<< "$config" | sed 's/"/""/g' || echo "{}")

    printf '"%s","%s","%s","%s","%s","%s","%s","%s","%s","%s"\n' \
        "$name" "$type" "$class" "$state" "$tasks_total" \
        "$conn_url" "${user_fields}" "${jaas}" "${sensitive_keys}" "$full_json" \
        >> "$OUTPUT_FILE"

done

echo ""
echo "Done. Processed $connector_count connector(s)."
echo "CSV: $OUTPUT_FILE"
echo "Debug: $DEBUG_DIR"
echo "If still empty rows → check files in debug folder"
