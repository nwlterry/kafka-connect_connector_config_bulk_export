# kafka-connect_connector_config_bulk_export

Exports selected non-secret Kafka Connect connector config fields to CSV via the Connect REST API.

## Script

`export-connectors-csv.sh`

```bash
bash export-connectors-csv.sh
```

Prompts for:

- Connect REST URL (default `https://localhost:8083`)
- Username and password

Writes `connectors-basic-info-YYYYMMDD-HHMMSS.csv` with:

- `connector_name`
- `connector_class`
- `connection_url`
- `basic.auth.user.info` (password after the first `:` is stripped)
- `connection.user` / `user` / `username` / `db.user` / `db.username`
- `principal.service.name` / `principal` / `service.principal`

Passwords, tokens, and keys are not exported. Requires `curl` and `jq`.
