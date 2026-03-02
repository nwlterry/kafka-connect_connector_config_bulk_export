#!/usr/bin/env bash
set -euo pipefail

# ──────────────────────────────────────────────────────────────────────────────
# Bulk export Kafka Connect connectors config → CSV
#   - Prompts only for username
#   - Lets curl interactively prompt for password (most secure for interactive use)
# ──────────────────────────────────────────────────────────────────────────────

# Default - change this to match your environment
DEFAULT_HOST="https://connect.example.com:8083"

# Prompt for connection details
read -p "Kafka Connect REST URL [${DEFAULT_HOST}]: " input_host
HOST="${input_host:-$DEFAULT_HOST}"

read -p "Username: " USERNAME

if [ -z "$USERNAME" ]; then
  echo "Error: Username is required." >&2
  exit 1
fi

echo ""
echo "Note: You will now be prompted for the password by curl (it will not be echoed)."
echo "      Press Ctrl+C if you want to cancel."
echo ""

OUTPUT_FILE="kafka-connectors-export-$(date +%Y%m%d-%H%M%S).csv"

echo "Exporting connectors from ${HOST} (user: ${USERNAME}) ..."
echo "Output → ${OUTPUT_FILE}"
echo ""

# Header
cat << 'EOF' > "$OUTPUT_FILE"
connector_name,connector_type,connector_class,state,tasks_total,connection_url,username_or_user,principal_or_jaas,other_sensitive_keys,full_config_json
EOF

# ────────────────────────────────────────────────────────────────
# Let curl prompt for password interactively (-u username:  ← note the trailing colon)
# This is the key change: curl will ask "Password:" itself
# ────────────────────────────────────────────────────────────────

# Test connectivity / get list of connectors
connectors=$(curl -s -f -u "${USERNAME}:" "${HOST}/connectors" | jq -r '.[]' 2>/dev/null) || {
  echo ""
  echo "Error: Failed to reach or authenticate with Kafka Connect REST API."
  echo "Common causes:"
  echo "  • Wrong username/password (you were prompted above)"
  echo "  • Self-signed certificate → try adding --insecure temporarily"
  echo "  • Wrong URL / port / firewall"
  echo "  • User lacks permission to list connectors"
  echo ""
  echo "Try running this manually to debug:"
  echo "  curl -v -u \"${USERNAME}:\" \"${HOST}/connectors\""
  exit 1
}

if [ -z "$connectors" ]; then
  echo "No connectors found (empty list from API)."
  echo "This is normal if no connectors are deployed."
  exit 0
fi

echo "Found $(echo "$connectors" | wc -l | xargs) connector(s)"
echo ""

connector_count=0

while IFS= read -r name; do
  ((connector_count++))

  echo "Processing: $name"

  # Get detailed info — curl will NOT ask for password again in the same session
  # (it re-uses credentials from the first successful call in most cases)
  info=$(curl -s -f -u "${USERNAME}:" "${HOST}/connectors/${name}?expand=status,info") || {
    echo "  → Warning: failed to get details for ${name}"
    continue
  }

  # Extract fields (same as before)
  type=$(echo "$info" | jq -r '.info.type // "unknown"')
  class=$(echo "$info" | jq -r '.info.config["connector.class"] // ""')
  state=$(echo "$info" | jq -r '.status.connector.state // "UNKNOWN"')
  tasks_total=$(echo "$info" | jq -r '.status.tasks | length // 0')

  config=$(echo "$info" | jq '.info.config // {}')

  conn_url=$(echo "$config" | jq -r '.["connection.url"] // ""' | sed 's/"/""/g')

  user_fields=$(echo "$config" | jq -r 'to_entries[]
    | select(.key | test("user|username|principal|owner|sasl.mechanism|sasl.jaas"; "i"))
    | "\(.key)=\(.value)"' | tr '\n' ';' | sed 's/;$//; s/"/""/g' || echo "none")

  jaas=$(echo "$config" | jq -r 'to_entries[]
    | select(.key | test("jaas|principal|sasl.jaas|sasl.kerberos"; "i"))
    | .value' | head -n1 | sed 's/"/""/g' || echo "none")

  sensitive_keys=$(echo "$config" | jq -r 'keys[]
    | select(test("pass|secret|key|token|cred|private|sasl|truststore|keystore|password"; "i"))
    | .' | sort | uniq | paste -sd, - | sed 's/"/""/g' || echo "none")

  full_json=$(echo "$config" | jq -c . | sed 's/"/""/g' || echo "{}")

  # Write row
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
echo "Tip: Open in Excel / LibreOffice Calc (comma delimiter, quoted fields)"
