#!/usr/bin/env bash
set -euo pipefail

# ──────────────────────────────────────────────────────────────────────────────
# Bulk export Kafka Connect connectors config → CSV (HTTPS + prompted auth)
# ──────────────────────────────────────────────────────────────────────────────

# Default - change this to match your environment
DEFAULT_HOST="https://connect.example.com:8083"

# Prompt for connection details
read -p "Kafka Connect REST URL [${DEFAULT_HOST}]: " input_host
HOST="${input_host:-$DEFAULT_HOST}"

read -p "Username: " USERNAME
read -s -p "Password: " PASSWORD
echo ""   # new line after hidden password

if [ -z "$USERNAME" ] || [ -z "$PASSWORD" ]; then
  echo "Error: Username and password are required." >&2
  exit 1
fi

OUTPUT_FILE="kafka-connectors-export-$(date +%Y%m%d-%H%M%S).csv"

echo "Exporting connectors from ${HOST} ..."
echo "Output → ${OUTPUT_FILE}"
echo ""

# Header
cat << 'EOF' > "$OUTPUT_FILE"
connector_name,connector_type,connector_class,state,tasks_total,connection_url,username_or_user,principal_or_jaas,other_sensitive_keys,full_config_json
EOF

# Get all connector names
connectors=$(curl -s -f -u "${USERNAME}:${PASSWORD}" "${HOST}/connectors" | jq -r '.[]' 2>/dev/null) || {
  echo "Error: Failed to reach Kafka Connect REST API."
  echo "Check URL, credentials, certificate trust, or network."
  exit 1
}

if [ -z "$connectors" ]; then
  echo "No connectors found (or empty response)."
  exit 0
fi

connector_count=0

while IFS= read -r name; do
  ((connector_count++))

  echo "Processing: $name"

  # Get expanded info (config + type + status)
  info=$(curl -s -f -u "${USERNAME}:${PASSWORD}" "${HOST}/connectors/${name}?expand=status,info") || {
    echo "  → Warning: failed to get details for ${name}"
    continue
  }

  # Extract fields
  type=$(echo "$info" | jq -r '.info.type // "unknown"')
  class=$(echo "$info" | jq -r '.info.config["connector.class"] // ""')
  state=$(echo "$info" | jq -r '.status.connector.state // "UNKNOWN"')
  tasks_total=$(echo "$info" | jq -r '.status.tasks | length // 0')

  config=$(echo "$info" | jq '.info.config // {}')

  # connection.url
  conn_url=$(echo "$config" | jq -r '.["connection.url"] // ""' | sed 's/"/""/g')

  # Common username / user / principal related fields
  user_fields=$(echo "$config" | jq -r 'to_entries[]
    | select(.key | test("user|username|principal|owner|sasl.mechanism|sasl.jaas"; "i"))
    | "\(.key)=\(.value)"' | tr '\n' ';' | sed 's/;$//; s/"/""/g' || echo "none")

  # JAAS/principal (often masked)
  jaas=$(echo "$config" | jq -r 'to_entries[]
    | select(.key | test("jaas|principal|sasl.jaas|sasl.kerberos"; "i"))
    | .value' | head -n1 | sed 's/"/""/g' || echo "none")

  # Other potentially sensitive keys (just names)
  sensitive_keys=$(echo "$config" | jq -r 'keys[]
    | select(test("pass|secret|key|token|cred|private|sasl|truststore|keystore|password"; "i"))
    | .' | sort | uniq | paste -sd, - | sed 's/"/""/g' || echo "none")

  # Full config as escaped JSON
  full_json=$(echo "$config" | jq -c . | sed 's/"/""/g' || echo "{}")

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

done <<< "$connectors"

echo ""
echo "Done. Processed ${connector_count} connector(s)."
echo "Exported to: ${OUTPUT_FILE}"
echo "Tip: Open in spreadsheet software (comma separated, quoted fields)"
