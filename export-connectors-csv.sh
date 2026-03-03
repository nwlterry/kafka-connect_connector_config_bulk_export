#!/usr/bin/env bash
set -euo pipefail

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
connector_name,connector_class,connection_url,username_or_user,principal_or_jaas,http_status,raw_config_length,notes
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

    config_raw=$(curl -s -f ${INSECURE} -u "${CONNECT_USER}:${CONNECT_PASS}" "$config_url" 2>/tmp/fetch_err.txt)

    http_status=$(echo "$config_raw" | tail -n1 || echo "unknown")
    body=$(echo "$config_raw" | sed '$d' || echo "")

    if [ $? -ne 0 ] || [ "$http_status" != "200" ]; then
        echo "  → FAILED (status $http_status)"
        cat /tmp/fetch_err.txt
        printf '"%s","failed","none","none","none","%s","0","fetch_failed"\n' "$name" "$http_status" >> "$OUTPUT_FILE"
        ((row_count++))
        continue
    fi

    echo "  HTTP: $http_status   length: ${#body}"

    if [ -z "$body" ]; then
        echo "  → empty body"
        printf '"%s","missing","none","none","none","200","0","empty_response"\n' "$name" >> "$OUTPUT_FILE"
        ((row_count++))
        continue
    fi

    # Show first part for debug
    echo "  First 200 chars:"
    echo "$body" | head -c 200 | cat -vet
    echo ""

    connector_class=$(echo "$body" | jq -r '."connector.class" // "missing"')
    conn_url=$(echo "$body" | jq -r '."connection.url" // ""' | sed 's/"/""/g')

    # Very permissive user/principal collection (exclude obvious password keys)
    user_info=$(echo "$body" | jq -r 'to_entries[]
        | select(.key | test("user|username|principal|owner|sasl.mechanism"; "i"))
        | select(.key | test("password|pass|secret|key|cred|token|private"; "i") | not)
        | "\(.key)=\(.value)"' | tr '\n' ';' | sed 's/;$//; s/"/""/g' || echo "none")

    notes="ok"
    if echo "$body" | grep -q '\*\*\*\*'; then
        notes="masked"
    elif [ "$body" = "{}" ]; then
        notes="empty_config"
    fi

    printf '"%s","%s","%s","%s","%s","%s","%s","%s"\n' \
        "$name" \
        "$connector_class" \
        "$conn_url" \
        "$user_info" \
        "—" \
        "$http_status" \
        "${#body}" \
        "$notes" \
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
echo "If most rows show 'empty_config' or 'none' → normal Confluent masking"
echo "If only one row → only one connector has visible non-sensitive user fields"
