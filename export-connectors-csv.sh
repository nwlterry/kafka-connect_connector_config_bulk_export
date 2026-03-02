#!/usr/bin/env bash
set -euo pipefail

# =============================================================================
# Bulk Export Kafka Connect Connectors Configuration to CSV
#
# Features:
#   - Interactive prompt for URL, username, password
#   - Lists connectors from /connectors endpoint
#   - Fetches detailed config + status for each connector
#   - Exports to CSV: name, type, class, state, tasks, connection_url, usernames/principals, sensitive keys, full config
#   - Saves debug files (response + curl error) for troubleshooting
#   - URL-encodes connector names to handle special characters
# =============================================================================

DEFAULT_HOST="https://localhost:8083"

echo ""
echo "Kafka Connect REST URL [${DEFAULT_HOST}]:"
read -r input_host
HOST="${input_host:-$DEFAULT_HOST}"

echo ""
echo "Credentials (password will not be displayed):"
echo ""

read -r -p "Username: " CONNECT_USER
read -s -r -p "Password: " CONNECT_PASS
echo ""

if [ -z "$CONNECT_USER" ] || [ -z "$CONNECT_PASS" ]; then
    echo "Error: Username and password are both required." >&2
    exit 1
fi

# Uncomment if you are getting SSL certificate errors (self-signed / internal CA)
# INSECURE="--insecure"
INSECURE=""

OUTPUT_FILE="kafka-connectors-export-$(date +%Y%m%d-%H%M%S).csv"
DEBUG_DIR="debug_connect_export_$(date +%Y%m%d-%H%M%S)"
mkdir -p "$DEBUG_DIR" || { echo "Cannot create debug directory"; exit 1; }

echo "Debug files will be saved in: ${DEBUG_DIR}"
echo ""

# Write CSV header
cat << 'EOF' > "$OUTPUT_FILE"
connector_name,connector_type,connector_class,state,tasks_total,connection_url,username_or_user,principal_or_jaas,other_sensitive_keys,full_config_json
EOF

echo "Fetching list of connectors..."
echo "URL: ${HOST}/connectors"

# Capture full response + HTTP status
raw_response=$(curl -s -f -w "\n%{http_code}" ${INSECURE} -u "${CONNECT_USER}:${CONNECT_PASS}" "${HOST}/connectors" 2> "${DEBUG_DIR}/list_connectors_curl.err")

http_code=$(echo "$raw_response" | tail -n1)
body=$(echo "$raw_response" | sed '$d')

echo "HTTP status: ${http_code}"
echo "Response length: ${#body} bytes"

if [ "$http_code" -ne 200 ]; then
    echo ""
    echo "ERROR: Non-200 response from /connectors"
    echo "HTTP code: ${http_code}"
    echo ""
    echo "Curl error output:"
    cat "${DEBUG_DIR}/list_connectors_curl.err" || echo "(error file empty)"
    echo ""
    echo "First 500 characters of body:"
    echo "$body" | head -c 500 | cat -vet || echo "(empty)"
    echo ""
    echo "Manual debug command:"
    echo "  curl -v ${INSECURE} -u \"${CONNECT_USER}:<password>\" \"${HOST}/connectors\""
    exit 1
fi

# Try to parse connector names
mapfile -t connectors < <(echo "$body" | jq -r '.[]' 2> "${DEBUG_DIR}/list_jq.err")

if [ ${#connectors[@]} -eq 0 ]; then
    echo ""
    echo "No connectors found (empty array returned)."
    echo "This usually means:"
    echo "  • No connectors are deployed/visible in this Connect cluster"
    echo "  • User lacks permission to list connectors (RBAC)"
    echo "  • Response is valid JSON but not an array"
    echo ""
    echo "Raw response saved in: ${DEBUG_DIR}/list_connectors_raw.txt"
    echo "$body" > "${DEBUG_DIR}/list_connectors_raw.txt"
    echo ""
    echo "Check:"
    echo "  • Confluent Control Center → Connectors"
    echo "  • Kafka topic 'connect-configs' (if distributed mode)"
    exit 0
fi

echo ""
echo "Found ${#connectors[@]} connector(s)"
echo "Names:"
for name in "${connectors[@]}"; do
    echo "  • $name"
done
echo ""

connector_count=0
row_count=0

for name in "${connectors[@]}"; do
    ((connector_count++))

    echo ""
    echo "──────────────────────────────────────────────"
    echo "[$connector_count / ${#connectors[@]}]  $name"

    # Safe filename
    safe_name=$(printf '%s' "$name" | tr -cd '[:alnum:]_.-' | head -c 100)
    [ -z "$safe_name" ] && safe_name="connector_${connector_count}"

    raw_file="${DEBUG_DIR}/${safe_name}_response.json"
    err_file="${DEBUG_DIR}/${safe_name}_curl.err"

    # URL-encode name
    encoded_name=$(printf '%s' "$name" | jq -sRr @uri 2>/dev/null || echo "$name" | sed 's/ /%20/g;s/:/%3A/g;s/\//%2F/g')

    url="${HOST}/connectors/${encoded_name}?expand=status,info"

    echo "  → ${url}"

    curl ${INSECURE} -s -f -u "${CONNECT_USER}:${CONNECT_PASS}" "${url}" \
        > "${raw_file}" 2> "${err_file}"

    curl_rc=$?

    echo "  curl exit code: ${curl_rc}"

    if [ ${curl_rc} -ne 0 ]; then
        echo "  → FAILED"
        echo "  Error:"
        cat "${err_file}" || echo "(error file empty)"
        continue
    fi

    if [ ! -s "${raw_file}" ]; then
        echo "  → Empty response"
        continue
    fi

    echo "  Response saved: ${raw_file}"
    echo "  First 300 chars:"
    head -c 300 "${raw_file}" | cat -vet || echo "(empty/binary)"
    echo ""

    if ! jq . "${raw_file}" >/dev/null 2>&1; then
        echo "  → Invalid JSON"
        continue
    fi

    echo "  → Valid JSON"

    # Extract fields
    type=$(jq -r '.info.type // "unknown"' "${raw_file}")
    class=$(jq -r '.info.config["connector.class"] // "missing"' "${raw_file}")
    state=$(jq -r '.status.connector.state // "UNKNOWN"' "${raw_file}")
    tasks_total=$(jq -r '.status.tasks | length // 0' "${raw_file}")

    config=$(jq '.info.config // {}' "${raw_file}")

    conn_url=$(jq -r '.["connection.url"] // ""' <<< "${config}" | sed 's/"/""/g')

    user_fields=$(jq -r 'to_entries[]
        | select(.key | test("user|username|principal|owner|sasl.mechanism|sasl.jaas"; "i"))
        | "\(.key)=\(.value)"' <<< "${config}" \
        | tr '\n' ';' | sed 's/;$//; s/"/""/g' || echo "none")

    jaas=$(jq -r 'to_entries[]
        | select(.key | test("jaas|principal|sasl.jaas|sasl.kerberos"; "i"))
        | .value' <<< "${config}" | head -n1 | sed 's/"/""/g' || echo "none")

    sensitive_keys=$(jq -r 'keys[]
        | select(test("pass|secret|key|token|cred|private|sasl|truststore|keystore|password"; "i"))
        | .' <<< "${config}" | sort | uniq | paste -sd, - | sed 's/"/""/g' || echo "none")

    full_json=$(jq -c . <<< "${config}" | sed 's/"/""/g' || echo "{}")

    # Write row
    printf '"%s","%s","%s","%s","%s","%s","%s","%s","%s","%s"\n' \
        "$name" "$type" "$class" "$state" "$tasks_total" \
        "$conn_url" "${user_fields}" "${jaas}" "${sensitive_keys}" "$full_json" \
        >> "$OUTPUT_FILE"

    ((row_count++))
    echo "  → Row written (total: ${row_count})"
done

echo ""
echo "──────────────────────────────────────────────"
echo "Summary:"
echo "  Connectors found : ${connector_count}"
echo "  Rows exported    : ${row_count}"
echo "  CSV file         : ${OUTPUT_FILE}"
echo "  Debug folder     : ${DEBUG_DIR}"
echo ""

if [ ${row_count} -eq 0 ] && [ ${connector_count} -gt 0 ]; then
    echo "No rows written — check debug folder:"
    echo "  • Open *_response.json files"
    echo "  • Look for \"error_code\", \"message\", or missing \"config\" object"
fi
