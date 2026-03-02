#!/usr/bin/env bash
set -euo pipefail

# =============================================================================
# Bulk export Kafka Connect connectors configuration to CSV
# Exports: name, type, class, state, tasks count, connection_url, usernames/principals, sensitive keys, full config JSON
# =============================================================================

DEFAULT_HOST="https://localhost:8083"

echo ""
echo "Kafka Connect REST URL [${DEFAULT_HOST}]:"
read -r input_host
HOST="${input_host:-$DEFAULT_HOST}"

echo ""
echo "Please enter your credentials (password input will be hidden):"
echo ""

read -r -p "Username: " CONNECT_USER
read -s -r -p "Password: " CONNECT_PASS
echo ""

if [ -z "$CONNECT_USER" ] || [ -z "$CONNECT_PASS" ]; then
    echo "Error: Username and password are both required." >&2
    exit 1
fi

# Uncomment next line if using self-signed/internal cert without trusted CA
# INSECURE="--insecure"
INSECURE=""

OUTPUT_FILE="kafka-connectors-export-$(date +%Y%m%d-%H%M%S).csv"
DEBUG_DIR="debug_connect_export_$(date +%Y%m%d-%H%M%S)"
mkdir -p "$DEBUG_DIR"

echo "Debug files will be saved to: ${DEBUG_DIR}"
echo ""

# Write CSV header
cat << 'EOF' > "$OUTPUT_FILE"
connector_name,connector_type,connector_class,state,tasks_total,connection_url,username_or_user,principal_or_jaas,other_sensitive_keys,full_config_json
EOF

# Get list of connector names as array
mapfile -t connectors < <(curl -s -f ${INSECURE} -u "${CONNECT_USER}:${CONNECT_PASS}" "${HOST}/connectors" | jq -r '.[]' 2>/dev/null)

if [ ${#connectors[@]} -eq 0 ]; then
    echo "No connectors found or failed to reach /connectors endpoint."
    echo "Try manual check:"
    echo "  curl -v ${INSECURE} -u \"${CONNECT_USER}:<password>\" \"${HOST}/connectors\""
    exit 0
fi

echo "Found ${#connectors[@]} connector(s)"
echo "Names:"
printf '  • %s\n' "${connectors[@]}"
echo ""

connector_count=0
row_count=0

for name in "${connectors[@]}"; do
    ((connector_count++))

    echo ""
    echo "──────────────────────────────────────────────"
    echo "Processing connector [$connector_count / ${#connectors[@]}]: '$name'"

    # Conservative safe filename (max 100 chars)
    safe_name=$(printf '%s' "$name" | tr -cd '[:alnum:]_.-' | head -c 100)
    [ -z "$safe_name" ] && safe_name="connector_${connector_count}"

    raw_file="${DEBUG_DIR}/${safe_name}_response.json"
    err_file="${DEBUG_DIR}/${safe_name}_curl.err"

    # URL-encode connector name
    encoded_name=$(printf '%s' "$name" | jq -sRr @uri 2>/dev/null || printf '%s' "$name" | sed 's/ /%20/g')

    url="${HOST}/connectors/${encoded_name}?expand=status,info"

    echo "  URL: ${url}"

    # Fetch connector details
    curl ${INSECURE} -s -f -u "${CONNECT_USER}:${CONNECT_PASS}" "${url}" \
        > "${raw_file}" 2> "${err_file}"

    curl_rc=$?

    echo "  curl exit code: ${curl_rc}"

    if [ ${curl_rc} -ne 0 ]; then
        echo "  → FAILED to fetch details"
        echo "  Error output:"
        cat "${err_file}" || echo "  (error file empty or missing)"
        echo "  Manual test command:"
        echo "    curl -v ${INSECURE} -u \"${CONNECT_USER}:<pass>\" \"${url}\""
        continue
    fi

    if [ ! -s "${raw_file}" ]; then
        echo "  → Response file is empty"
        continue
    fi

    echo "  Response saved → ${raw_file}"
    echo "  First 300 chars:"
    head -c 300 "${raw_file}" | cat -vet || echo "(binary or empty)"
    echo ""

    # Validate JSON
    if ! jq . "${raw_file}" >/dev/null 2>&1; then
        echo "  → Response is not valid JSON"
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

    # Write CSV row
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

    ((row_count++))
    echo "  → Row written (total rows now: ${row_count})"
done

echo ""
echo "──────────────────────────────────────────────"
echo "Summary:"
echo "  Connectors discovered : ${connector_count}"
echo "  Rows written to CSV   : ${row_count}"
echo "  Output file           : ${OUTPUT_FILE}"
echo "  Debug folder          : ${DEBUG_DIR}"
echo ""
echo "If rows written = 0 but debug files exist:"
echo "  → Open one *_response.json file and check if it contains \"config\""
echo "  → Look for \"error_code\" or \"message\" fields"
echo ""
