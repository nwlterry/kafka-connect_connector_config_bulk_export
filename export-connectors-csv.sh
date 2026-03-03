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

# INSECURE="--insecure"   # uncomment for self-signed cert problems
INSECURE=""

OUTPUT_FILE="connectors-export-$(date +%Y%m%d-%H%M%S).csv"
DEBUG_DIR="debug_connect_$(date +%Y%m%d-%H%M%S)"
mkdir -p "$DEBUG_DIR"

cat << 'EOF' > "$OUTPUT_FILE"
connector_name,connector_type,connector_class,state,tasks_total,connection_url,username_or_user,principal_or_jaas,other_sensitive_keys,full_config_json
EOF

echo "Fetching connector list..."
raw_list=$(curl -s -f -w "\n%{http_code}" ${INSECURE} -u "${CONNECT_USER}:${CONNECT_PASS}" "${HOST}/connectors" 2> "$DEBUG_DIR/list.err")

http_list=$(echo "$raw_list" | tail -n1)
body_list=$(echo "$raw_list" | sed '$d')

if [ "$http_list" != "200" ]; then
    echo "List endpoint failed - HTTP $http_list"
    cat "$DEBUG_DIR/list.err"
    exit 1
fi

mapfile -t connectors < <(echo "$body_list" | jq -r '.[]' 2>/dev/null)

[ ${#connectors[@]} -eq 0 ] && { echo "No connectors found"; exit 0; }

echo "Found ${#connectors[@]} connector(s)"
printf '  • %s\n' "${connectors[@]}"
echo ""

row_count=0

for name in "${connectors[@]}"; do
    echo ""
    echo "──────────────────────────────────"
    echo "Connector: $name"

    # URL-encode name properly
    encoded=$(printf '%s' "$name" | jq -sRr @uri 2>/dev/null || echo "$name")
    url="${HOST}/connectors/${encoded}?expand=status,info"

    raw_file="$DEBUG_DIR/$(echo "$name" | tr -C '[:alnum:]-' '_')_raw.json"
    err_file="$DEBUG_DIR/$(echo "$name" | tr -C '[:alnum:]-' '_')_err.txt"
    status_file="$DEBUG_DIR/$(echo "$name" | tr -C '[:alnum:]-' '_')_status.txt"

    # Fetch with status code
    curl ${INSECURE} -s -w "\n%{http_code}" -u "${CONNECT_USER}:${CONNECT_PASS}" "$url" \
        > "$raw_file" 2> "$err_file"

    http_code=$(tail -n1 "$raw_file")
    body=$(sed '$d' "$raw_file")
    echo "$http_code" > "$status_file"

    echo "HTTP: $http_code"

    if [ "$http_code" != "200" ]; then
        echo "→ Failed ($http_code) - see $err_file"
        cat "$err_file"
        continue
    fi

    if [ -z "$body" ]; then
        echo "→ Empty body"
        continue
    fi

    if ! echo "$body" | jq . >/dev/null 2>&1; then
        echo "→ Not valid JSON"
        continue
    fi

    echo "→ Valid JSON"

    config=$(echo "$body" | jq '.info.config // empty')
    if [ -z "$config" ] || [ "$config" = "{}" ]; then
        echo "→ Warning: .info.config is missing or empty"
    fi

    # Extract fields
    type=$(echo "$body" | jq -r '.info.type // "unknown"')
    class=$(echo "$body" | jq -r '.info.config["connector.class"] // "missing"')
    state=$(echo "$body" | jq -r '.status.connector.state // "UNKNOWN"')
    tasks_total=$(echo "$body" | jq -r '.status.tasks | length // 0')

    conn_url=$(echo "$config" | jq -r '.["connection.url"] // ""' | sed 's/"/""/g')

    user_fields=$(echo "$config" | jq -r 'to_entries[] | select(.key | test("user|username|principal|owner|sasl.mechanism|sasl.jaas"; "i")) | "\(.key)=\(.value)"' | tr '\n' ';' | sed 's/;$//; s/"/""/g' || echo "none")

    jaas=$(echo "$config" | jq -r 'to_entries[] | select(.key | test("jaas|principal|sasl.jaas|sasl.kerberos"; "i")) | .value' | head -n1 | sed 's/"/""/g' || echo "none")

    sensitive_keys=$(echo "$config" | jq -r 'keys[] | select(test("pass|secret|key|token|cred|private|sasl|truststore|keystore|password"; "i"))' | sort | uniq | paste -sd, - | sed 's/"/""/g' || echo "none")

    full_json=$(echo "$config" | jq -c . | sed 's/"/""/g' || echo "{}")

    printf '"%s","%s","%s","%s","%s","%s","%s","%s","%s","%s"\n' \
        "$name" "$type" "$class" "$state" "$tasks_total" \
        "$conn_url" "${user_fields}" "${jaas}" "${sensitive_keys}" "$full_json" \
        >> "$OUTPUT_FILE"

    ((row_count++))
    echo "→ Row added (total: $row_count)"
done

echo ""
echo "Summary:"
echo "  Connectors processed : ${#connectors[@]}"
echo "  Rows written         : $row_count"
echo "  CSV file             : $OUTPUT_FILE"
echo "  Debug folder         : $DEBUG_DIR"
echo ""
echo "If rows = 0 → look at HTTP codes in *_status.txt files and *_err.txt"
echo "Most likely: 401/403 → missing permission to read connector configs"
