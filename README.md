# kafka-connect_connector_config_bulk_export

Export selected non-secret Kafka Connect connector config fields to CSV via the Connect REST API.

## Layout

```
scripts/export-connectors-csv.sh
GROUP.md
README.md
```

```bash
bash scripts/export-connectors-csv.sh
```

Prompts for Connect REST URL (default `https://localhost:8083`) and credentials. Writes `connectors-basic-info-YYYYMMDD-HHMMSS.csv`. Passwords after the first `:` in `basic.auth.user.info` are stripped. Requires `curl` and `jq`.

---

See [GROUP.md](GROUP.md) for sibling repositories. Catalog: https://github.com/nwlterry/nwlterry
