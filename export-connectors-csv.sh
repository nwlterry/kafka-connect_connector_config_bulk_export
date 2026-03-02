#!/usr/bin/env bash
set -euo pipefail

# ──────────────────────────────────────────────────────────────────────────────
# Bulk export Kafka Connect connectors config → CSV
# Prompts interactively for username & password (no env vars needed)
# Supports HTTPS + self-signed cert workaround
# ──────────────────────────────────────────────────────────────────────────────

# Default host – change if you have a common one
DEFAULT_HOST="https://connect.example.com:8083"

read -p "Kafka Connect REST URL [${DEFAULT_HOST}]: " input_host
HOST="${input_host:-$DEFAULT_HOST}"

# ────────────────────────────────────────────────────────────────
# Interactive credential prompt (Fix 2 applied)
# ────────────────────────────────────────────────────────────────
echo ""
echo "Please enter your credentials (password will not be shown):"
echo ""

read -p "Username: " CONNECT_USER
read -s -p "Password: " CONNECT_PASS
echo ""   # new line after hidden password

if [ -z "$CONNECT_USER" ] || [ -z "$CONNECT_PASS" ]; then
    echo "Error: Both username and password are required." >&2
    exit 1
fi

echo "Using user: ${CONNECT_USER}"
echo ""

# Optional: uncomment this line if you have self-signed / internal CA cert issues
# INSECURE="--insecure"
INSECURE=""

OUTPUT_FILE="kafka-connectors-export-$(date +%Y%m%d-%H%M%S).csv"

echo "Exporting connectors from ${HOST} ..."
echo "Output file: ${OUTPUT_FILE}"
echo ""

# Write CSV header
cat << 'EOF' > "$OUTPUT_FILE"
connector_name,connector_type,connector_class,state,tasks_total,connection_url,username_or_user,principal_or_jaas,other_sensitive_keys,full_config_json
EOF

# List all connectors
connectors=$(curl -s -f ${INSECURE} -u "${CONNECT_USER}:${CONNECT_PASS}" "${HOST}/connectors" | jq -r '.[]' 2>/dev/null) || {
    echo ""
    echo "ERROR: Failed to list connectors."
    echo "Possible causes:"
    echo "  • Wrong username or password"
    echo "  • Self-signed certificate → try uncommenting INSECURE=\"--insecure\""
    echo "  • Wrong URL / port"
    echo "  • Insufficient permissions (RBAC)"
    echo ""
    echo "Debug command:"
    echo "  curl -v ${INSECURE} -u \"${CONNECT_USER}:<password>\" \"${HOST}/connectors\""
    exit 1
}

if [ -z "$connectors" ]; then
    echo "API call succeeded, but no connectors found (empty list)."
    echo "This is normal if no connectors are currently running/deployed."
    exit 0
fi

echo "Found $(echo "$connectors" | wc -l | xargs) connector(s)"
echo ""

connector_count=0

while IFS= read -r name; do
    ((connector_count++))

    echo "Processing: $name"

    info=$(curl -s -f ${INSECURE} -u "${CONNECT_USER}:${CONNECT_PASS}" "${HOST}/connectors/${name}?expand=status,info") || {
        echo "  → Warning: failed to fetch details for ${name}"
        continue
    }

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

    # Write row to CSV
    printf '"%s","%s","%s","%s","%s","%s","%s","%s","%s","%s"\n' \
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
        >> "$OUTPUT_FILE"

done <<< "$connectors"

echo ""
echo "Done. Processed ${connector_count} connector(s)."
echo "Results saved to: ${OUTPUT_FILE}"
echo "Tip: Open in Excel / Google Sheets / LibreOffice Calc (comma delimiter)"
