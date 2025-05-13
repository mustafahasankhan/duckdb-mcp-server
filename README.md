# DuckDB MCP Server

[![PyPI - Version](https://img.shields.io/pypi/v/duckdb-mcp-server)](https://pypi.org/project/duckdb-mcp-server/)
[![PyPI - License](https://img.shields.io/pypi/l/duckdb-mcp-server)](LICENSE)

A Model Context Protocol (MCP) server implementation that enables AI assistants like Claude to interact with DuckDB for powerful data analysis capabilities.

## 🌟 What is DuckDB MCP Server?

DuckDB MCP Server connects AI assistants to [DuckDB](https://duckdb.org/) - a high-performance analytical database - through the [Model Context Protocol (MCP)](https://modelcontextprotocol.io/). This allows AI models to:

- Query data directly from various sources like CSV, Parquet, JSON, etc.
- Access data from cloud storage (S3, etc.) without complex setup
- Perform sophisticated data analysis using SQL
- Generate data insights with proper context and understanding

## 🚀 Key Features

- **SQL Query Tool**: Execute any SQL query with DuckDB's powerful syntax
- **Multiple Data Sources**: Query directly from:
  - Local files (CSV, Parquet, JSON, etc.)
  - S3 buckets and cloud storage
  - SQLite databases
  - All other data sources supported by DuckDB
- **Auto-Connection Management**: Automatic database file creation and connection handling
- **Smart Credential Handling**: Seamless AWS/S3 credential management
- **Documentation Resources**: Built-in DuckDB SQL and data import reference for AI assistants

## 📋 Requirements

- Python 3.10+
- An MCP-compatible client (Claude Desktop, Cursor, VS Code with Copilot, etc.)

## 💻 Installation

### Using pip

```bash
pip install duckdb-mcp-server
```

### From source

```bash
git clone https://github.com/mustafahasankhan/duckdb-mcp-server.git
cd duckdb-mcp-server
pip install -e .
```

## 🔧 Configuration

### Command Line Options

```bash
duckdb-mcp-server --db-path path/to/database.db [options]
```

#### Required Parameters:
- `--db-path` - Path to DuckDB database file (will be created if doesn't exist)

#### Optional Parameters:
- `--readonly` - Run in read-only mode (will error if database doesn't exist)
- `--s3-region` - AWS S3 region (default: uses AWS_DEFAULT_REGION env var)
- `--s3-profile` - AWS profile for S3 credentials (default: uses AWS_PROFILE or 'default')
- `--creds-from-env` - Use AWS credentials from environment variables

## 🔌 Setting Up with Claude Desktop

1. Install Claude Desktop from [claude.ai/download](https://claude.ai/download)
2. Edit Claude Desktop's configuration file:

   **macOS**: `~/Library/Application Support/Claude/claude_desktop_config.json`  
   **Windows**: `%APPDATA%/Claude/claude_desktop_config.json`

3. Add DuckDB MCP Server configuration:

```json
{
  "mcpServers": {
    "duckdb": {
      "command": "duckdb-mcp-server",
      "args": [
        "--db-path",
        "~/claude-duckdb/data.db"
      ]
    }
  }
}
```

## 📊 Example Usage

Once configured, you can ask your AI assistant to analyze data using DuckDB:

```
"Load the sales.csv file and show me the top 5 products by revenue"
```

The AI will generate and execute the appropriate SQL:

```sql
-- Load and query the CSV data
SELECT 
    product_name,
    SUM(quantity * price) AS revenue
FROM read_csv('sales.csv')
GROUP BY product_name
ORDER BY revenue DESC
LIMIT 5;
```

### Working with S3 Data

Query data directly from S3 buckets:

```
"Analyze the daily user signups from our analytics data in S3"
```

The AI will generate appropriate SQL to query S3:

```sql
SELECT 
    date_trunc('day', signup_timestamp) AS day,
    COUNT(*) AS num_signups
FROM read_parquet('s3://my-analytics-bucket/signups/*.parquet')
GROUP BY day
ORDER BY day DESC;
```

## 🌩️ Cloud Storage Authentication

DuckDB MCP Server handles AWS authentication in this order:

1. Explicit credentials (if `--creds-from-env` is enabled)
2. Named profile credentials (via `--s3-profile`)
3. Default credential chain (environment, shared credentials file, etc.)

## 🛠️ Development

```bash
# Clone the repository
git clone https://github.com/yourusername/duckdb-mcp-server.git
cd duckdb-mcp-server

# Set up a virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install in development mode
pip install -e ".[dev]"

# Run tests
pytest
```

## 📜 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙏 Contributing

Contributions are welcome! Please feel free to submit a Pull Request.
