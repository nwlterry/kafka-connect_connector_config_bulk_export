#!/usr/bin/env bash
set -euo pipefail

# ──────────────────────────────────────────────────────────────────────────────
# Bulk export Kafka Connect connectors config → CSV
# Uses env vars CONNECT_USER / CONNECT_PASS (set them before running!)
# ──────────────────────────────────────────────────────────────────────────────

DEFAULT_HOST="https://connect.example.com:8083"

read -p "Kafka Connect REST URL [${DEFAULT_HOST}]: " input_host
HOST="${input_host:-$DEFAULT_HOST}"

# Require env vars (set before running the script)
if [ -z "$$   {CONNECT_USER:-}" ] || [ -z "   $${CONNECT_PASS:-}" ]; then
  echo "Error: Set CONNECT_USER and CONNECT_PASS environment variables first."
  echo "Examples:"
  echo "  export CONNECT_USER=\"admin\""
  echo "  export CONNECT_PASS=\"secret123\""
  echo "Or:"
  echo "  read -p \"Username: \" CONNECT_USER"
  echo "  read -s -p \"Password: \" CONNECT_PASS"
  echo "  export CONNECT_USER CONNECT_PASS"
  exit 1
fi

echo "Using credentials: user=${CONNECT_USER} (from env var)"
echo "Host: ${HOST}"
echo ""

# Optional: uncomment for self-signed certs
# INSECURE="--insecure"
INSECURE=""

OUTPUT_FILE="kafka-connectors-export-$(date +%Y%m%d-%H%M%S).csv"

# Header
cat << 'EOF' > "$OUTPUT_FILE"
connector_name,connector_type,connector_class,state,tasks_total,connection_url,username_or_user,principal_or_jaas,other_sensitive_keys,full_config_json
EOF

# Fetch list of connectors — exactly like your working manual curl
connectors=$(curl -s -f $$   {INSECURE} -u "   $${CONNECT_USER}:$$   {CONNECT_PASS}" "   $${HOST}/connectors" | jq -r '.[]' 2>/dev/null) || {
  echo "ERROR: Failed to list connectors. Check credentials, URL, or TLS."
  echo "Manual debug command:"
  echo "  curl -v $$   {INSECURE} -u \"   $${CONNECT_USER}:$$   {CONNECT_PASS}\" \"   $${HOST}/connectors\""
  exit 1
}

if [ -z "$connectors" ]; then
  echo "Success (API reachable), but no connectors found (empty [])."
  echo "If unexpected → verify in Confluent Control Center / Connect logs."
  exit 0
fi

echo "Found $(echo "$connectors" | wc -l) connector(s)"
echo ""

connector_count=0

while IFS= read -r name; do
  ((connector_count++))

  echo "Processing: $name"

  info=$$ (curl -s -f \[  {INSECURE} -u "  \]{CONNECT_USER}:\[  {CONNECT_PASS}" "  \]{HOST}/connectors/ $${name}?expand=status,info") || {
    echo "  → Warning: failed to get details for ${name}"
    continue
  }

  # The rest of your extraction logic remains unchanged...
  type=$(echo "$info" | jq -r '.info.type // "unknown"')
  class=$(echo "$info" | jq -r '.info.config["connector.class"] // ""')
  state=$(echo "$info" | jq -r '.status.connector.state // "UNKNOWN"')
  tasks_total=$(echo "$info" | jq -r '.status.tasks | length // 0')

  config=$(echo "$info" | jq '.info.config // {}')

  conn_url=$(echo "$config" | jq -r '.["connection.url"] // ""' | sed 's/"/""/g')

  user_fields=$(echo "$config" | jq -r 'to_entries[]
    | select(.key | test("user|username|principal|owner|sasl.mechanism|sasl.jaas"; "i"))
    | "$$   .key)=\(.value)"' | tr '\n' ';' | sed 's/;$//; s/"/""/g' || echo "none")

  jaas=$(echo "$config" | jq -r 'to_entries[]
    | select(.key | test("jaas|principal|sasl.jaas|sasl.kerberos"; "i"))
    | .value' | head -n1 | sed 's/"/""/g' || echo "none")

  sensitive_keys=$(echo "$config" | jq -r 'keys[]
    | select(test("pass|secret|key|token|cred|private|sasl|truststore|keystore|password"; "i"))
    | .' | sort | uniq | paste -sd, - | sed 's/"/""/g' || echo "none")

  full_json=$(echo "$config" | jq -c . | sed 's/"/""/g' || echo "{}")

  printf '"%s","%s","%s","%s","%s","%s","%s","%s","%s","%s"\n' \
    "$name" "$type" "$class" "$state" "$tasks_total" \
    "\( conn_url" "   $${user_fields}" "$$   {jaas}" "   $${sensitive_keys}" "$full_json" \
    >> "$OUTPUT_FILE"

done <<< "$connectors"

echo ""
echo "Done. Processed ${connector_count} connector(s)."
echo "Exported to: ${OUTPUT_FILE}"
