#!/usr/bin/env bash
set -euo pipefail

# ──────────────────────────────────────────────────────────────────────────────
# Bulk export Kafka Connect connectors config → CSV
# ──────────────────────────────────────────────────────────────────────────────

HOST="${CONNECT_HOST:-http://localhost:8083}"
# If auth needed → uncomment & adjust:
# AUTH=(-u "admin:secret")    # basic auth
# AUTH=(--cacert ca.crt --cert client.crt --key client.key)  # mTLS

OUTPUT_FILE="kafka-connectors-export-$(date +%Y%m%d-%H%M%S).csv"

echo "Exporting connectors from $HOST → $OUTPUT_FILE"
echo ""

# Header
cat << 'EOF' > "$OUTPUT_FILE"
connector_name,connector_type,connector_class,state,tasks_total,connection_url,username_or_user,principal_or_jaas,other_sensitive_keys,full_config_json
EOF

# Get all connector names
connectors=$(curl -s "${AUTH[@]}" "$HOST/connectors" | jq -r '.[]' || { echo "Error: cannot reach Connect REST API" >&2; exit 1; })

if [ -z "$connectors" ]; then
  echo "No connectors found."
  exit 0
fi

while IFS= read -r name; do
  echo "Processing: $name"

  # Get expanded info (config + type + status)
  info=$(curl -s "${AUTH[@]}" "$HOST/connectors/$name?expand=status,info")

  # Extract fields with jq
  type=$(echo "$info" | jq -r '.info.type // "unknown"')
  class=$(echo "$info" | jq -r '.info.config["connector.class"] // ""')
  state=$(echo "$info" | jq -r '.status.connector.state // "UNKNOWN"')
  tasks_total=$(echo "$info" | jq -r '.status.tasks | length // 0')

  config=$(echo "$info" | jq '.info.config')

  # connection.url
  conn_url=$(echo "$config" | jq -r '.["connection.url"] // ""' | sed 's/"/""/g')

  # Try common username / user fields (case insensitive search)
  user_fields=$(echo "$config" | jq -r 'to_entries[]
    | select(.key | test("user|username|principal|owner|sasl.mechanism|sasl.jaas"; "i"))
    | "\(.key)=\(.value)"' | tr '\n' ';' | sed 's/;$//; s/"/""/g')

  # JAAS/principal (often masked)
  jaas=$(echo "$config" | jq -r 'to_entries[]
    | select(.key | test("jaas|principal|sasl.jaas|sasl.kerberos"; "i"))
    | .value' | head -n1 | sed 's/"/""/g')

  # Other potentially sensitive keys (for awareness)
  sensitive_keys=$(echo "$config" | jq -r 'keys[]
    | select(test("pass|secret|key|token|cred|private|sasl|truststore|keystore|password"; "i"))
    | .' | sort | uniq | paste -sd, - | sed 's/"/""/g')

  # Full config as escaped JSON string
  full_json=$(echo "$config" | jq -c . | sed 's/"/""/g')

  # Build CSV row
  printf '"%s","%s","%s","%s","%s","%s","%s","%s","%s","%s"\n' \
    "$name" \
    "$type" \
    "$class" \
    "$state" \
    "$tasks_total" \
    "$conn_url" \
    "${user_fields:-none}" \
    "${jaas:-none}" \
    "${sensitive_keys:-none}" \
    "$full_json" \
    >> "$OUTPUT_FILE"

done <<< "$connectors"

echo ""
echo "Done. Exported $(echo "$connectors" | wc -l) connectors → $OUTPUT_FILE"
echo "Tip: Open in Excel/LibreOffice Calc (use comma delimiter, quote escaping)"
