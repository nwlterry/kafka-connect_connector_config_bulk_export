#!/usr/bin/env bash
set -euo pipefail

DEFAULT_HOST="https://connect.example.com:8083"

read -p "Kafka Connect REST URL [${DEFAULT_HOST}]: " input_host
HOST="${input_host:-$DEFAULT_HOST}"

echo ""
echo "Credentials (password hidden):"
read -p "Username: " CONNECT_USER
read -s -p "Password: " CONNECT_PASS
echo ""

if [ -z "$CONNECT_USER" ] || [ -z "$CONNECT_PASS" ]; then
    echo "Error: Username and password required." >&2
    exit 1
fi

# Uncomment if self-signed cert
# INSECURE="--insecure"
INSECURE=""

OUTPUT_FILE="connectors-export-$(date +%Y%m%d-%H%M%S).csv"
DEBUG_DIR="debug_$(date +%Y%m%d-%H%M%S)"
mkdir -p "$DEBUG_DIR"

echo "Debug goes to: $DEBUG_DIR"
echo ""

cat << 'EOF' > "$OUTPUT_FILE"
connector_name,connector_type,connector_class,state,tasks_total,connection_url,username_or_user,principal_or_jaas,other_sensitive_keys,full_config_json
EOF

# Get list as array (safest)
mapfile -t connectors < <(curl -s -f ${INSECURE} -u "${CONNECT_USER}:${CONNECT_PASS}" "${HOST}/connectors" | jq -r '.[]' 2>/dev/null)

echo "Number of connectors: ${#connectors[@]}"
if [ ${#connectors[@]} -eq 0 ]; then
    echo "No connectors → exiting"
    exit 0
fi

echo "Connector names:"
printf '  • %s\n' "${connectors[@]}"
echo ""

row_count=0

for name in "${connectors[@]}"; do
    if [ -z "$name" ]; then
        echo "SKIP: empty connector name detected"
        continue
    fi

    echo ""
    echo "Processing: [$connector_count] →${name}← (length: ${#name})"

    # Very conservative filename
    safe_name=$(echo -n "$name" | tr -cd '[:alnum:]_.-' | head -c 100)
    if [ -z "$safe_name" ]; then
        safe_name="connector_${connector_count}"
    fi

    raw_file="$DEBUG_DIR/${safe_name}_response.json"
    err_file="$DEBUG_DIR/${safe_name}_curl.err"

    echo "  → Writing to: $raw_file"

    # Quote URL carefully + encode name
    encoded_name=$(printf '%s' "$name" | jq -sRr @uri 2>/dev/null || echo "$name")
    url="${HOST}/connectors/${encoded_name}?expand=status,info"

    # Run curl
    curl ${INSECURE} -s -f -u "${CONNECT_USER}:${CONNECT_PASS}" "$url" \
        > "$raw_file" 2> "$err_file"

    curl_rc=$?
    echo "  → curl return code = $curl_rc"

    ls -l "$raw_file" "$err_file" 2>/dev/null || echo "  → No files created!"

    if [ $curl_rc -ne 0 ]; then
        echo "  → curl failed - see $err_file"
        cat "$err_file" 2>/dev/null || echo "  (err file empty or missing)"
        continue
    fi

    echo "  → Valid JSON"

    # Extract fields
    type=$(jq -r '.info.type // "unknown"' "$raw_file")
    class=$(jq -r '.info.config["connector.class"] // "missing"' "$raw_file")
    state=$(jq -r '.status.connector.state // "UNKNOWN"' "$raw_file")
    tasks_total=$(jq -r '.status.tasks | length // 0' "$raw_file")

    config_json=$(jq -c '.info.config // {}' "$raw_file")

    echo "  Extracted type: $type"
    echo "  Extracted class: $class"
    echo "  Extracted state: $state"
    echo "  Extracted tasks: $tasks_total"
    echo "  Config length: ${#config_json} chars"

    # If we got this far → try to write row
    conn_url=$(jq -r '.["connection.url"] // ""' <<< "$config_json" | sed 's/"/""/g')
    # ... (rest of your field extraction - keep as is)

    printf '"%s","%s","%s","%s","%s","%s","%s","%s","%s","%s"\n' \
        "$name" "$type" "$class" "$state" "$tasks_total" \
        "$conn_url" "..." "..." "..." "$config_json" \
        >> "$OUTPUT_FILE"

    ((row_count++))
    echo "  → Row written (total now: $row_count)"
done

echo ""
echo "Final summary:"
echo "  Connectors found: ${#connectors[@]}"
echo "  Rows written to CSV: $row_count"
echo "  Output file: $OUTPUT_FILE"
echo "  Debug folder: $DEBUG_DIR"
echo ""
echo "If rows=0 but debug files exist → open one *_response.json and tell me what it contains."
