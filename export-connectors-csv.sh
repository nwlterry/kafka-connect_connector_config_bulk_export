#!/usr/bin/env bash
set -uo pipefail

# =============================================================================
# Export Kafka Connect connector basic info to CSV
# Only selected non-sensitive fields
# Cleans basic.auth.user.info → keeps only username (removes :password)
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

OUTPUT_FILE="connectors-basic-info-$(date +%Y%m%d-%H%M%S).csv"

cat << 'EOF' > "$OUTPUT_FILE"
connector_name,connector_class,connection_url,basic_auth_user_info,connection_user,principal_service_name
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
        printf '"%s","failed","none","none","none","none"\n' "$name" >> "$OUTPUT_FILE"
        ((row_count++))
        continue
    fi

    if [ -z "$config_raw" ]; then
        echo "  → empty config"
        printf '"%s","missing","none","none","none","none"\n' "$name" >> "$OUTPUT_FILE"
        ((row_count++))
        continue
    fi

    connector_class=$(echo "$config_raw" | jq -r '."connector.class" // "missing"' 2>/dev/null || echo "parse_error")

    conn_url=$(echo "$config_raw" | jq -r '."connection.url" // ""' | sed 's/"/""/g' 2>/dev/null || echo "none")

    # Extract basic.auth.user.info and remove password part
    basic_auth_raw=$(echo "$config_raw" | jq -r '."basic.auth.user.info" // "none"' 2>/dev/null || echo "none")

    # Keep only username (everything before first :)
    basic_auth_user_info=$(echo "$basic_auth_raw" | sed 's/:.*$//' | sed 's/"/""/g' || echo "none")
    # If no : was present, keep original value
    [ "$basic_auth_user_info" = "$basic_auth_raw" ] && basic_auth_user_info=$(echo "$basic_auth_raw" | sed 's/"/""/g')

    connection_user=$(echo "$config_raw" | jq -r '
        ."connection.user" // 
        ."user" // 
        ."username" // 
        ."db.user" // 
        ."db.username" // 
        "none"' | sed 's/"/""/g' 2>/dev/null || echo "none")

    principal_service_name=$(echo "$config_raw" | jq -r '
        ."principal.service.name" // 
        ."principal" // 
        ."service.principal" // 
        ."principal.name" // 
        "none"' | sed 's/"/""/g' 2>/dev/null || echo "none")

    printf '"%s","%s","%s","%s","%s","%s"\n' \
        "$name" \
        "$connector_class" \
        "$conn_url" \
        "$basic_auth_user_info" \
        "$connection_user" \
        "$principal_service_name" \
        >> "$OUTPUT_FILE"

    echo "  → row added"
    ((row_count++))
done

echo ""
echo "Summary:"
echo "  Connectors processed : ${#connectors[@]}"
echo "  Rows exported        : $row_count"
echo "  Output file          : $OUTPUT_FILE"
echo ""
echo "Fields exported:"
echo "  • connector_name"
echo "  • connector_class"
echo "  • connection_url"
echo "  • basic.auth.user.info (password part removed)"
echo "  • connection.user / user / username / db.user / db.username"
echo "  • principal.service.name / principal / service.principal"
echo ""
echo "No password, secret, token, key, or cred fields are included."
