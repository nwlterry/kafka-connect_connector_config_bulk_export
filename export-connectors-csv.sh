#!/usr/bin/env bash
set -euo pipefail

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

[ -z "$CONNECT_USER" ] || [ -z "$CONNECT_PASS" ] && { echo "Username and password required"; exit 1; }

# INSECURE="--insecure"   # uncomment if needed
INSECURE=""

OUTPUT_FILE="connectors-export-$(date +%Y%m%d-%H%M%S).csv"
DEBUG_DIR="debug_connect_$(date +%Y%m%d-%H%M%S)"
mkdir -p "$DEBUG_DIR"

cat << 'EOF' > "$OUTPUT_FILE"
connector_name,connector_type,connector_class,state,tasks_total,connection_url,username_or_user,principal_or_jaas,other_sensitive_keys,full_config_json,notes
EOF

# List connectors
raw_list=$(curl -s -f -w "\n%{http_code}" ${INSECURE} -u "${CONNECT_USER}:${CONNECT_PASS}" "${HOST}/connectors" 2> "$DEBUG_DIR/list.err")
http_list=$(echo "$raw_list" | tail -n1)
body_list=$(echo "$raw_list" | sed '$d')

[ "$http_list" != "200" ] && { echo "List failed - HTTP $http_list"; cat "$DEBUG_DIR/list.err"; exit 1; }

mapfile -t connectors < <(echo "$body_list" | jq -r '.[]' 2>/dev/null)

[ ${#connectors[@]} -eq 0 ] && { echo "No connectors found"; exit 0; }

echo "Found ${#connectors[@]} connector(s)"
printf '  • %s\n' "${connectors[@]}"
echo ""

row_count=0

for name in "${connectors[@]}"; do
    ((row_count++))

    echo ""
    echo "──────────────────────────────────"
    echo "[$row_count] $name"

    safe_name=$(printf '%s' "$name" | tr -cd '[:alnum:]_.-' | head -c 100)
    [ -z "$safe_name" ] && safe_name="conn_${row_count}"

    encoded=$(printf '%s' "$name" | jq -sRr @uri 2>/dev/null || echo "$name")

    config_url="${HOST}/connectors/${encoded}/config"

    config_file="$DEBUG_DIR/${safe_name}_config.json"
    err_file="$DEBUG_DIR/${safe_name}_err.txt"
    status_file="$DEBUG_DIR/${safe_name}_http.txt"

    # Fetch config with fail-on-error + status code
    curl ${INSECURE} -f -s -w "\n%{http_code}" -u "${CONNECT_USER}:${CONNECT_PASS}" "$config_url" \
        > "$config_file" 2> "$err_file"

    config_http=$(tail -n1 "$config_file")
    config_body=$(sed '$d' "$config_file")
    echo "$config_http" > "$status_file"

    echo "HTTP: $config_http   → $config_url"

    if [ "$config_http" != "200" ]; then
        echo "  → FAILED (HTTP $config_http)"
        echo "  Error output:"
        cat "$err_file" || echo "(err file empty)"
        continue
    fi

    if [ -z "$config_body" ]; then
        echo "  → Empty response body"
        continue
    fi

    echo "  Response first 300 chars:"
    echo "$config_body" | head -c 300 | cat -vet || echo "(empty)"
    echo ""

    connector_class=$(echo "$config_body" | jq -r '."connector.class" // "missing"')
    conn_url=$(echo "$config_body" | jq -r '."connection.url" // ""' | sed 's/"/""/g')

    user_fields=$(echo "$config_body" | jq -r 'to_entries[]
        | select(.key | test("user|username|principal|owner|sasl.mechanism|sasl.jaas"; "i"))
        | "\(.key)=\(.value)"' | tr '\n' ';' | sed 's/;$//; s/"/""/g' || echo "none")

    jaas=$(echo "$config_body" | jq -r 'to_entries[]
        | select(.key | test("jaas|principal|sasl.jaas|sasl.kerberos"; "i"))
        | .value' | head -n1 | sed 's/"/""/g' || echo "none")

    sensitive_keys=$(echo "$config_body" | jq -r 'keys[]
        | select(test("pass|secret|key|token|cred|private|sasl|truststore|keystore|password"; "i"))
        | .' | sort | uniq | paste -sd, - | sed 's/"/""/g' || echo "none")

    full_json=$(echo "$config_body" | jq -c . | sed 's/"/""/g' || echo "{}")

    notes=""
    if echo "$full_json" | grep -q '\*\*\*\*'; then
        notes="MASKED_FIELDS_PRESENT"
    elif [ "$full_json" = "{}" ]; then
        notes="CONFIG_EMPTY"
    fi

    # Status from separate call
    info_url="${HOST}/connectors/${encoded}?expand=status"
    info_raw=$(curl ${INSECURE} -s -u "${CONNECT_USER}:${CONNECT_PASS}" "$info_url" 2>/dev/null)

    type=$(echo "$info_raw" | jq -r '.info.type // "unknown"')
    state=$(echo "$info_raw" | jq -r '.status.connector.state // "UNKNOWN"')
    tasks_total=$(echo "$info_raw" | jq -r '.status.tasks | length // 0')

    printf '"%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s"\n' \
        "$name" "$type" "$connector_class" "$state" "$tasks_total" \
        "$conn_url" "${user_fields}" "${jaas}" "${sensitive_keys}" "$full_json" "$notes" \
        >> "$OUTPUT_FILE"

    echo "  → Row written"
done

echo ""
echo "Summary:"
echo "  Connectors: ${#connectors[@]}"
echo "  Rows: $row_count"
echo "  CSV: $OUTPUT_FILE"
echo "  Debug: $DEBUG_DIR"
echo ""
echo "If rows=0 → check HTTP codes in *_http.txt files and *_err.txt files"
