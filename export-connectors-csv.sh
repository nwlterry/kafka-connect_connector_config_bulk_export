#!/usr/bin/env bash
set -euo pipefail

# =============================================================================
# Export Kafka Connect connector information (non-sensitive fields only)
# Only keeps username / principal / connection.url related info
# Removes all password / secret / key fields
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

[ -z "$CONNECT_USER" ] || [ -z "$CONNECT_PASS" ] && { echo "Missing credentials"; exit 1; }

INSECURE=""   # set to "--insecure" if needed

OUTPUT_FILE="connectors-user-info-$(date +%Y%m%d-%H%M%S).csv"

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

echo "Names:"
printf '  • %s\n' "${connectors[@]}"
echo ""

row_count=0

for name in "${connectors[@]}"; do
    echo "Processing: $name"

    encoded=$(printf '%s' "$name" | jq -sRr @uri 2>/dev/null || echo "$name")
    config_url="${HOST}/connectors/${encoded}/config"

    config_raw=$(curl -s -f ${INSECURE} -u "${CONNECT_USER}:${CONNECT_PASS}" "$config_url" 2>/tmp/${name//[^a-zA-Z0-9]/_}_err.txt)

    if [ $? -ne 0 ]; then
        echo "  → config fetch failed"
        continue
    fi

    if [ -z "$config_raw" ]; then
        echo "  → empty config"
        continue
    fi

    # Extract only non-sensitive user-related fields
    connector_class=$(echo "$config_raw" | jq -r '."connector.class" // "missing"')
    conn_url=$(echo "$config_raw" | jq -r '."connection.url" // ""' | sed 's/"/""/g')

    # Collect username / principal / jaas / owner fields
    user_info=$(echo "$config_raw" | jq -r 'to_entries[]
        | select(.key | test("user|username|principal|owner|sasl.mechanism|sasl.jaas"; "i"))
        | "\(.key)=\(.value)"' | tr '\n' ';' | sed 's/;$//; s/"/""/g' || echo "none")

    # Notes if config looks masked or empty
    notes=""
    if echo "$config_raw" | grep -q '\*\*\*\*'; then
        notes="some_fields_masked"
    elif [ "$config_raw" = "{}" ] || [ -z "$config_raw" ]; then
        notes="config_empty"
    fi

    # Only write row if at least one useful field is present
    if [ "$connector_class" != "missing" ] || [ -n "$conn_url" ] || [ "$user_info" != "none" ]; then
        printf '"%s","%s","%s","%s","%s"\n' \
            "$name" \
            "$connector_class" \
            "$conn_url" \
            "$user_info" \
            "$notes" \
            >> "$OUTPUT_FILE"

        echo "  → row added"
        ((row_count++))
    else
        echo "  → no useful user info found"
    fi
done

echo ""
echo "Done."
echo "Connectors processed : ${#connectors[@]}"
echo "Rows exported        : $row_count"
echo "Output file          : $OUTPUT_FILE"
echo ""
echo "Only non-password fields are included (usernames, principals, connection urls, etc.)"
echo "Passwords and secrets are automatically excluded."
