#!/usr/bin/env bash
set -euo pipefail

# =============================================================================
# Export Kafka Connect connector information (non-sensitive fields only)
# Explicitly excludes all password / secret / key fields
# Only keeps username / principal / connection.url related info
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

OUTPUT_FILE="connectors-user-info-only-$(date +%Y%m%d-%H%M%S).csv"

cat << 'EOF' > "$OUTPUT_FILE"
connector_name,connector_class,connection_url,username_or_user,principal_or_jaas,notes
EOF

echo "Fetching connector list..."
raw=$(curl -s -f ${INSECURE} -u "${CONNECT_USER}:${CONNECT_PASS}" "${HOST}/connectors" 2>/tmp/list.err)

if [ $? -ne 0 ]; then
    echo "Cannot get connector list"
    cat /tmp/list.err
    exit 1
fi

mapfile -t connectors < <(echo "$raw" | jq -r '.[]' 2>/dev/null)

echo ""
echo "Found ${#connectors[@]} connector(s)"
[ ${#connectors[@]} -eq 0 ] && { echo "No connectors"; exit 0; }

printf '  • %s\n' "${connectors[@]}"
echo ""

row_count=0

for name in "${connectors[@]}"; do
    echo "Processing: $name"

    encoded=$(printf '%s' "$name" | jq -sRr @uri 2>/dev/null || echo "$name")
    config_url="${HOST}/connectors/${encoded}/config"

    config_raw=$(curl -s -f ${INSECURE} -u "${CONNECT_USER}:${CONNECT_PASS}" "$config_url" 2>/tmp/fetch_err.txt)

    if [ $? -ne 0 ]; then
        echo "  → config fetch failed"
        continue
    fi

    if [ -z "$config_raw" ]; then
        echo "  → empty config"
        printf '"%s","missing","none","none","none","empty_config"\n' "$name" >> "$OUTPUT_FILE"
        ((row_count++))
        continue
    fi

    # Extract only non-sensitive user-related fields
    connector_class=$(echo "$config_raw" | jq -r '."connector.class" // "missing"')
    conn_url=$(echo "$config_raw" | jq -r '."connection.url" // ""' | sed 's/"/""/g')

    # Collect only username/principal/owner/sasl.mechanism/sasl.jaas fields
    # Explicitly exclude anything that looks like password/secret/key/cred/token
    user_info=$(echo "$config_raw" | jq -r 'to_entries[]
        | select(.key | test("user|username|principal|owner|sasl.mechanism|sasl.jaas"; "i"))
        | select(.key | test("password|pass|secret|key|cred|token|private"; "i") | not)
        | "\(.key)=\(.value)"' | tr '\n' ';' | sed 's/;$//; s/"/""/g' || echo "none")

    # Notes
    notes="OK"
    if echo "$config_raw" | grep -q '\*\*\*\*'; then
        notes="masked_fields"
    elif [ "$config_raw" = "{}" ]; then
        notes="empty_config"
    fi

    printf '"%s","%s","%s","%s","%s","%s"\n' \
        "$name" \
        "$connector_class" \
        "$conn_url" \
        "$user_info" \
        "—" \
        "$notes" \
        >> "$OUTPUT_FILE"

    echo "  → row added"
    ((row_count++))
done

echo ""
echo "Done."
echo "Connectors processed : ${#connectors[@]}"
echo "Rows exported        : $row_count"
echo "Output file          : $OUTPUT_FILE"
echo ""
echo "Only non-sensitive fields are exported (usernames, principals, connection urls, etc.)"
echo "All password/secret/key/token/cred fields are excluded."
