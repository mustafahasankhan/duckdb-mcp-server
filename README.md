# DuckDB MCP Server

[![PyPI - Version](https://img.shields.io/pypi/v/duckdb-mcp-server)](https://pypi.org/project/duckdb-mcp-server/)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/duckdb-mcp-server)](https://pypi.org/project/duckdb-mcp-server/)
[![PyPI - License](https://img.shields.io/pypi/l/duckdb-mcp-server)](LICENSE)

A [Model Context Protocol (MCP)](https://modelcontextprotocol.io/) server that gives AI assistants full access to [DuckDB](https://duckdb.org/) — query local files, S3 buckets, and in-memory data using plain SQL.

---

## What it does

This server exposes DuckDB to any MCP-compatible client (Claude Desktop, Cursor, VS Code, etc.). The assistant can:

- Run arbitrary SQL against local CSV, Parquet, and JSON files
- Query S3 / GCS buckets directly or cache them as local tables
- Inspect schemas, compute statistics, and suggest visualizations
- Use DuckDB's analytical SQL extensions (window functions, `GROUP BY ALL`, `SELECT * EXCLUDE`, etc.)

## Tools

| Tool | Description |
|---|---|
| `query` | Execute any SQL query. Results capped at 10 000 rows. |
| `analyze_schema` | Describe the columns and types of a file or table. |
| `analyze_data` | Row count, numeric stats (min/max/avg/median), date ranges, top categorical values. |
| `suggest_visualizations` | Suggest chart types and ready-to-run SQL queries based on column types. |
| `create_session` | Create or reset a session for cross-call context tracking. |

## Resources

Three built-in XML documentation resources are always available to the assistant:

| Resource URI | Content |
|---|---|
| `duckdb-ref://friendly-sql` | DuckDB SQL extensions (GROUP BY ALL, SELECT * EXCLUDE/REPLACE, FROM-first syntax, …) |
| `duckdb-ref://data-import` | Loading CSV, Parquet, JSON, and S3/GCS data; glob patterns; multi-file reads |
| `duckdb-ref://visualization` | Chart patterns and query templates for time series, bar, scatter, and heatmap |

---

## Requirements

- Python 3.10+
- An MCP-compatible client

## Installation

```bash
pip install duckdb-mcp-server
```

**From source:**

```bash
git clone https://github.com/mustafahasankhan/duckdb-mcp-server.git
cd duckdb-mcp-server
pip install -e .
```

---

## Configuration

```
duckdb-mcp-server --db-path <path> [options]
```

| Flag | Required | Description |
|---|---|---|
| `--db-path` | Yes | Path to the DuckDB file. Created automatically if it does not exist. |
| `--readonly` | No | Open the database read-only. Errors if the file does not exist. |
| `--s3-region` | No | AWS region. Defaults to `AWS_DEFAULT_REGION` env var, then `us-east-1`. |
| `--s3-profile` | No | AWS profile name. Defaults to `AWS_PROFILE` env var, then `default`. |
| `--creds-from-env` | No | Read `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` from environment. |

---

## Client setup

### Claude Desktop

Edit `~/Library/Application Support/Claude/claude_desktop_config.json` (macOS) or `%APPDATA%\Claude\claude_desktop_config.json` (Windows):

```json
{
  "mcpServers": {
    "duckdb": {
      "command": "duckdb-mcp-server",
      "args": ["--db-path", "~/claude-duckdb/data.db"]
    }
  }
}
```

### With S3 access

```json
{
  "mcpServers": {
    "duckdb": {
      "command": "duckdb-mcp-server",
      "args": [
        "--db-path", "~/claude-duckdb/data.db",
        "--s3-region", "us-east-1",
        "--creds-from-env"
      ],
      "env": {
        "AWS_ACCESS_KEY_ID": "YOUR_KEY",
        "AWS_SECRET_ACCESS_KEY": "YOUR_SECRET"
      }
    }
  }
}
```

### Read-only mode

Useful when the database is shared or managed separately:

```json
{
  "mcpServers": {
    "duckdb": {
      "command": "duckdb-mcp-server",
      "args": ["--db-path", "/shared/analytics.db", "--readonly"]
    }
  }
}
```

---

## Example conversations

### Querying a local file

> "Load sales.csv and show me the top 5 products by revenue."

```sql
SELECT
    product_name,
    SUM(quantity * price) AS revenue
FROM read_csv('sales.csv')
GROUP BY ALL
ORDER BY revenue DESC
LIMIT 5;
```

### Working with S3 data

> "Cache this month's signups from S3 and show me a daily breakdown."

```sql
-- Step 1: cache the remote data locally
CREATE TABLE signups AS
  SELECT * FROM read_parquet('s3://my-bucket/signups/2026-03/*.parquet',
                             union_by_name = true);

-- Step 2: query the cached table
SELECT
    date_trunc('day', signup_at) AS day,
    COUNT(*)                     AS signups
FROM signups
GROUP BY ALL
ORDER BY day DESC;
```

### Statistical analysis

> "Give me a statistical summary of the orders table."

The assistant calls `analyze_data("orders")` which returns row count, numeric stats per column, date ranges, and top categorical values — no SQL required from you.

---

## AWS credential resolution order

1. Environment variables (`--creds-from-env`): `AWS_ACCESS_KEY_ID` + `AWS_SECRET_ACCESS_KEY`
2. Named profile (`--s3-profile`): reads `~/.aws/credentials`
3. Credential chain: environment → shared credentials file → instance profile (EC2/ECS)

---

## Development

```bash
git clone https://github.com/mustafahasankhan/duckdb-mcp-server.git
cd duckdb-mcp-server

python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"

pytest
```

---

## License

[MIT](LICENSE)
