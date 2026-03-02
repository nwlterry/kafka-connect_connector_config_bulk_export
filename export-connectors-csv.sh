#!/usr/bin/env bash
set -euo pipefail

# ──────────────────────────────────────────────────────────────────────────────
# Bulk export Kafka Connect connectors config → CSV (with debug)
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

echo "Debug files will be saved to: $DEBUG_DIR"
echo ""

# Header
cat << 'EOF' > "$OUTPUT_FILE"
connector_name,connector_type,connector_class,state,tasks_total,connection_url,username_or_user,principal_or_jaas,other_sensitive_keys,full_config_json
EOF

connectors=$(curl -s -f ${INSECURE} -u "${CONNECT_USER}:${CONNECT_PASS}" "${HOST}/connectors" | jq -r '.[]' 2>/dev/null) || {
    echo "ERROR: Cannot list connectors" >&2
    exit 1
}

if [ -z "$connectors" ]; then
    echo "No connectors found."
    exit 0
fi

echo "Found $(echo "$connectors" | wc -l) connector(s)"
echo ""

connector_count=0

while IFS= read -r name; do
    ((connector_count++))

    echo "Processing: $name"

    # Save raw response for debug
    raw_file="$DEBUG_DIR/${name//[^a-zA-Z0-9]/_}_raw.json"
    curl -s -f ${INSECURE} -u "${CONNECT_USER}:${CONNECT_PASS}" "${HOST}/connectors/${name}?expand=status,info" > "$raw_file" 2> "$raw_file.err"

    if [ $? -ne 0 ]; then
        echo "  → FAILED to fetch (see $raw_file.err)"
        cat "$raw_file.err"
        continue
    fi

    # Check if valid JSON
    if ! jq . "$raw_file" > /dev/null 2>&1; then
        echo "  → Invalid JSON in response (see $raw_file)"
        continue
    fi

    info=$(cat "$raw_file")

    type=$(echo "$info" | jq -r '.info.type // "unknown"')
    class=$(echo "$info" | jq -r '.info.config["connector.class"] // ""')
    state=$(echo "$info" | jq -r '.status.connector.state // "UNKNOWN"')
    tasks_total=$(echo "$info" | jq -r '.status.tasks | length // 0')

    config=$(echo "$info" | jq '.info.config // {}')

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

    # Write row
    printf '"%s","%s","%s","%s","%s","%s","%s","%s","%s","%s"\n' \
        "$name" "$type" "$class" "$state" "$tasks_total" \
        "$conn_url" "${user_fields}" "${jaas}" "${sensitive_keys}" "$full_json" \
        >> "$OUTPUT_FILE"

    echo "  → OK (row added)"

done <<< "$connectors"

echo ""
echo "Done. Processed ${connector_count} connector(s)."
echo "CSV: ${OUTPUT_FILE}"
echo "Debug folder: ${DEBUG_DIR} (check .json and .err files if rows missing)"
