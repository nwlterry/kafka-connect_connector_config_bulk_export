#!/usr/bin/env bash
# set -euo pipefail   # COMMENT OUT -e to avoid early exit on curl failure
set -uo pipefail

# =============================================================================
# Export Kafka Connect connector information (non-sensitive fields only)
# Continues on failure for individual connectors
# =============================================================================

DEFAULT_HOST="https://localhost:8083"

echo ""
echo "Kafka Connect REST URL [${DEFAULT_HOST}]:"
read -r input_host
HOST="${input_host:-$DEFAULT_HOST}"

echo ""
echo "Credentials:"
read -r -p "Username: " CONNECT_USER
read -s -r -p "Password: " CONNECT_PASS
echo ""

[ -z "$CONNECT_USER" ] || [ -z "$CONNECT_PASS" ] && { echo "Missing credentials"; exit 1; }

INSECURE=""   # "--insecure" if needed

OUTPUT_FILE="connectors-user-info-$(date +%Y%m%d-%H%M%S).csv"

cat << 'EOF' > "$OUTPUT_FILE"
connector_name,connector_class,connection_url,username_or_user,principal_or_jaas,http_status,notes
EOF

echo "Fetching list..."
raw=$(curl -s -f ${INSECURE} -u "${CONNECT_USER}:${CONNECT_PASS}" "${HOST}/connectors" 2>/tmp/list.err)

if [ $? -ne 0 ]; then
    echo "List fetch failed"
    cat /tmp/list.err
    exit 1
fi

mapfile -t connectors < <(echo "$raw" | jq -r '.[]' 2>/dev/null)

echo ""
echo "Found ${#connectors[@]} connector(s)"
printf '  • %s\n' "${connectors[@]}"
echo ""

row_count=0

for name in "${connectors[@]}"; do
    echo ""
    echo "Processing: '$name'"

    encoded=$(printf '%s' "$name" | jq -sRr @uri 2>/dev/null || echo "$name")
    config_url="${HOST}/connectors/${encoded}/config"

    echo "  URL: $config_url"

    config_raw=$(curl -s ${INSECURE} -u "${CONNECT_USER}:${CONNECT_PASS}" "$config_url" 2>/tmp/fetch_err_${name//[^a-zA-Z0-9]/_}.txt)
    curl_exit=$?

    if [ $curl_exit -ne 0 ]; then
        echo "  → curl failed (exit code $curl_exit)"
        cat /tmp/fetch_err_${name//[^a-zA-Z0-9]/_}.txt
        printf '"%s","failed","none","none","none","failed","curl_error_%s"\n' "$name" "$curl_exit" >> "$OUTPUT_FILE"
        ((row_count++))
        continue
    fi

    # Get HTTP status from response (if -w was used, but here we use curl -I for simplicity)
    http_status=$(curl -s -I ${INSECURE} -u "${CONNECT_USER}:${CONNECT_PASS}" "$config_url" | head -n1 | awk '{print $2}')

    echo "  HTTP: $http_status   length: ${#config_raw}"

    if [ -z "$config_raw" ]; then
        echo "  → empty body"
        printf '"%s","missing","none","none","none","%s","empty_body"\n' "$name" "$http_status" >> "$OUTPUT_FILE"
        ((row_count++))
        continue
    fi

    connector_class=$(echo "$config_raw" | jq -r '."connector.class" // "missing"' 2>/dev/null || echo "parse_error")
    conn_url=$(echo "$config_raw" | jq -r '."connection.url" // ""' | sed 's/"/""/g' 2>/dev/null || echo "parse_error")

    user_info=$(echo "$config_raw" | jq -r 'to_entries[]
        | select(.key | test("user|username|principal|owner|sasl.mechanism"; "i"))
        | select(.key | test("password|pass|secret|key|cred|token|private"; "i") | not)
        | "\(.key)=\(.value)"' | tr '\n' ';' | sed 's/;$//; s/"/""/g' || echo "none")

    notes="ok"
    if echo "$config_raw" | grep -q '\*\*\*\*'; then
        notes="masked"
    elif [ "$config_raw" = "{}" ]; then
        notes="empty_config"
    fi

    printf '"%s","%s","%s","%s","%s","%s","%s"\n' \
        "$name" "$connector_class" "$conn_url" "$user_info" "—" "$http_status" "$notes" \
        >> "$OUTPUT_FILE"

    echo "  → row added"
    ((row_count++))
done

echo ""
echo "Summary:"
echo "  Connectors listed : ${#connectors[@]}"
echo "  Rows exported     : $row_count"
echo "  CSV file          : $OUTPUT_FILE"
echo ""
echo "Check /tmp/fetch_err_* files for connectors that failed"
echo "Most likely cause: connector name has special characters, or permission denied on some connectors"
