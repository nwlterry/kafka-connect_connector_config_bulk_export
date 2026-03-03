#!/usr/bin/env bash
set -euo pipefail

# =============================================================================
# Bulk Export Kafka Connect Connectors to CSV
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

INSECURE=""   # set to "--insecure" if needed

OUTPUT_FILE="connectors-export-$(date +%Y%m%d-%H%M%S).csv"

cat << 'EOF' > "$OUTPUT_FILE"
connector_name,connector_class,connection_url,username_or_user,principal_or_jaas,notes
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
echo "=== DEBUG AFTER LIST ==="
echo "Connectors found: ${#connectors[@]}"
if [ ${#connectors[@]} -eq 0 ]; then
    echo "Empty list → exiting"
    exit 0
fi

echo "Names (should see this):"
for i in "${!connectors[@]}"; do
    echo "  [$i] →${connectors[i]}← (length ${#connectors[i]})"
done
echo ""

echo "Starting loop over ${#connectors[@]} connectors..."
echo ""

row_count=0

for ((i=0; i<${#connectors[@]}; i++)); do
    name="${connectors[i]}"

    echo "Processing [$((i+1))/${#connectors[@]}]: '$name'"

    safe_name=$(printf '%s' "$name" | tr -cd '[:alnum:]_.-' | head -c 80)
    [ -z "$safe_name" ] && safe_name="conn_$i"

    encoded=$(printf '%s' "$name" | jq -sRr @uri 2>/dev/null || echo "$name")

    config_url="${HOST}/connectors/${encoded}/config"

    echo "  → Fetching: $config_url"

    config_raw=$(curl -s -f ${INSECURE} -u "${CONNECT_USER}:${CONNECT_PASS}" "$config_url" 2>/tmp/${safe_name}_err.txt)

    if [ $? -ne 0 ]; then
        echo "  → Fetch failed"
        cat /tmp/${safe_name}_err.txt
        continue
    fi

    if [ -z "$config_raw" ]; then
        echo "  → Empty config response"
        continue
    fi

    connector_class=$(echo "$config_raw" | jq -r '."connector.class" // "missing"')
    conn_url=$(echo "$config_raw" | jq -r '."connection.url" // ""' | sed 's/"/""/g')

    user_fields=$(echo "$config_raw" | jq -r 'to_entries[] | select(.key | test("user|username|principal|sasl.jaas"; "i")) | "\(.key)=\(.value)"' | tr '\n' ';' | sed 's/;$//; s/"/""/g' || echo "none")

    notes="OK"
    if echo "$config_raw" | grep -q '\*\*\*\*'; then
        notes="MASKED"
    elif [ "$config_raw" = "{}" ]; then
        notes="EMPTY_CONFIG"
    fi

    printf '"%s","%s","%s","%s","%s"\n' \
        "$name" "$connector_class" "$conn_url" "${user_fields}" "$notes" \
        >> "$OUTPUT_FILE"

    echo "  → Row added"
    ((row_count++))
done

echo ""
echo "──────────────────────────────────"
echo "FINAL SUMMARY"
echo "Connectors listed : ${#connectors[@]}"
echo "Rows in CSV       : $row_count"
echo "CSV file          : $OUTPUT_FILE"
echo ""
echo "If rows = 0 but names were printed:"
echo "  • Check /tmp/*_err.txt files"
echo "  • Run manual: curl -v -u user:pass \"${HOST}/connectors/one-name/config\""
echo "  • Most likely: 403 (no read permission) or name encoding issue"
