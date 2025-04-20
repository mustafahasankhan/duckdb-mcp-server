# DuckDB MCP Server - Project Summary

This document provides an overview of the DuckDB MCP Server project, explaining the purpose and functionality of each component.

## Overview

The DuckDB MCP Server is a Model Context Protocol (MCP) server implementation that allows Large Language Models (LLMs) such as Claude to interact with DuckDB databases. This enables Claude to analyze data from various sources, including:

- Local files (CSV, Parquet, JSON, etc.)
- S3 storage and other cloud storage services
- SQLite databases and other data sources

The server provides Claude with DuckDB documentation, allowing it to generate accurate SQL queries and analyze data effectively.

## Key Components

### Core Server Components

- `config.py` - Handles command-line arguments and configuration
- `database.py` - Manages DuckDB connections and query execution
- `credentials.py` - Handles cloud storage credentials (e.g., AWS S3)
- `server.py` - Main MCP server implementation

### MCP Integration

- `resources/docs.py` - Documentation loaders for DuckDB features
- `resources/xml/` - XML-formatted documentation for DuckDB features

### Examples

- `examples/query_local_csv.py` - Demonstrates querying local CSV files
- `examples/query_parquet_from_s3.py` - Demonstrates querying Parquet files from S3
- `examples/setup_claude_desktop.py` - Helper script for setting up Claude Desktop

### Tests

- `tests/test_config.py` - Tests for the Config class
- `tests/test_database.py` - Tests for the DuckDBClient class

## How It Works

1. **Initialization**: The server parses command-line arguments, sets up the DuckDB database, and initializes cloud credentials.

2. **MCP Integration**: The server registers MCP handlers for:
   - Resources - DuckDB documentation resources
   - Prompts - Initial prompts for Claude to understand how to use DuckDB
   - Tools - SQL query execution tool

3. **Query Execution**: When Claude sends a query via the MCP tool, the server:
   - Establishes a DuckDB connection
   - Executes the SQL query
   - Formats and returns the results

4. **Credential Management**: The server automatically:
   - Configures AWS credentials for S3 access
   - Installs and loads necessary DuckDB extensions

## Using the MCP Server with Claude

Claude can perform various data analysis tasks with the MCP server:

1. **Data Exploration**:
   - Query file metadata
   - Examine data schemas
   - Preview data samples

2. **Data Analysis**:
   - Perform complex SQL queries
   - Generate statistical summaries
   - Identify patterns and trends

3. **Visualization Preparation**:
   - Query and transform data for visualization
   - Select relevant data points
   - Aggregate and structure results for charts

## Project Structure

```
duckdb-mcp-server/
├── pyproject.toml          # Project metadata and dependencies
├── README.md               # Project documentation
├── LICENSE                 # MIT License
├── .gitignore              # Git ignore file
├── src/
│   └── duckdb_mcp_server/
│       ├── __init__.py     # Package initialization
│       ├── server.py       # Main MCP server implementation
│       ├── config.py       # Configuration handling
│       ├── database.py     # DuckDB connection handling
│       ├── credentials.py  # S3 and cloud credentials management
│       ├── resources/      # Documentation and resources
│       │   ├── __init__.py
│       │   ├── docs.py     # Documentation loader
│       │   └── xml/        # XML documentation files
│       │       ├── duckdb_friendly_sql.xml
│       │       └── duckdb_data_import.xml
├── tests/                  # Unit tests
│   ├── __init__.py
│   ├── test_config.py
│   └── test_database.py
└── examples/               # Example usage scripts
    ├── query_local_csv.py
    ├── query_parquet_from_s3.py
    └── setup_claude_desktop.py
```

## Setup and Usage

See the main README.md file for detailed instructions on setting up and using the DuckDB MCP Server.