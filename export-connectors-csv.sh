#!/usr/bin/env bash
set -euo pipefail

# ──────────────────────────────────────────────────────────────────────────────
# Bulk export Kafka Connect connectors → CSV (HTTPS + curl password prompt)
# ──────────────────────────────────────────────────────────────────────────────

DEFAULT_HOST="https://connect.example.com:8083"

read -p "Kafka Connect REST URL [${DEFAULT_HOST}]: " input_host
HOST="${input_host:-$DEFAULT_HOST}"

read -p "Username: " USERNAME

if [ -z "$USERNAME" ]; then
  echo "Error: Username required." >&2
  exit 1
fi

echo ""
echo "curl will now prompt for password (not echoed)."
echo "If it fails → see diagnostic tips below."
echo ""

# ───── Optional: uncomment for self-signed/internal cert testing ─────
# INSECURE="--insecure"
INSECURE=""

OUTPUT_FILE="kafka-connectors-export-$(date +%Y%m%d-%H%M%S).csv"

echo "Trying: ${HOST}/connectors (user: ${USERNAME})"
echo ""

# Header
cat << 'EOF' > "$OUTPUT_FILE"
connector_name,connector_type,connector_class,state,tasks_total,connection_url,username_or_user,principal_or_jaas,other_sensitive_keys,full_config_json
EOF

# Fetch connectors list with verbose on error
connectors_output=$(curl -s -f -v ${INSECURE} -u "${USERNAME}" "${HOST}/connectors" 2>&1) || {
  echo ""
  echo "┌────────────────────────────────────────────────────────────┐"
  echo "│               AUTHENTICATION / CONNECTION FAILED           │"
  echo "└────────────────────────────────────────────────────────────┘"
  echo ""
  echo "Last curl output:"
  echo "$connectors_output" | tail -n 30
  echo ""
  echo "Common fixes:"
  echo "1. Wrong password/user → re-run and type carefully"
  echo "2. Certificate issue → uncomment INSECURE=\"--insecure\" line above and retry"
  echo "   (Then add real CA cert later with --cacert /path/to/ca.crt)"
  echo "3. No permission → ask admin to grant CONNECTOR_READ or similar"
  echo "4. Wrong URL/port → confirm Connect listens on ${HOST} (check logs/server)"
  echo "5. Not Basic Auth → might need Bearer token, mTLS, etc. (check docs)"
  echo ""
  echo "Manual test command:"
  echo "  curl -v ${INSECURE} -u \"${USERNAME}:\" \"${HOST}/connectors\""
  exit 1
}

connectors=$(echo "$connectors_output" | tail -n +$(($(echo "$connectors_output" | grep -n '^}' | cut -d: -f1 | head -1) + 1)) | jq -r '.[]' 2>/dev/null || echo "")

if [ -z "$connectors" ]; then
  echo "Success reaching API, but no connectors found ([] returned)."
  echo "If you expect connectors → check Connect logs / deployment."
  exit 0
fi

echo "Found $(echo "$connectors" | wc -l | xargs) connector(s)"
echo ""

# ... the rest of the while loop remains exactly the same as before ...

connector_count=0

while IFS= read -r name; do
  ((connector_count++))

  echo "Processing: $name"

  info=$(curl -s -f ${INSECURE} -u "${USERNAME}:" "${HOST}/connectors/${name}?expand=status,info") || {
    echo "  → Warning: failed to get details for ${name}"
    continue
  }

  # (rest of extraction and printf → same as your previous version)

done <<< "$connectors"

echo ""
echo "Done. Processed ${connector_count} connector(s)."
echo "Saved to: ${OUTPUT_FILE}"
