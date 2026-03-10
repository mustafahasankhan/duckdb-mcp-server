"""
Main MCP server implementation for DuckDB.
"""

import json
import logging
import uuid
from typing import Any

import mcp.server.stdio
import mcp.types as types
from mcp.server import Server
from pydantic import AnyUrl

from .config import Config
from .database import DuckDBClient
from .resources import docs

logger = logging.getLogger("duckdb-mcp-server.server")


class SessionManager:
    """Manages user sessions and their state."""

    def __init__(self):
        self.sessions: dict[str, dict[str, Any]] = {}

    def create_session(self, session_id: str | None = None) -> str:
        if not session_id:
            session_id = str(uuid.uuid4())
        self.sessions[session_id] = {
            "current_table": None,
            "analyzed_files": set(),
            "has_accessed_sql_docs": False,
            "query_history": [],
            "visualization_history": [],
        }
        return session_id

    def get_session(self, session_id: str) -> dict[str, Any] | None:
        return self.sessions.get(session_id)

    def update_session(self, session_id: str, data: dict[str, Any]) -> None:
        if session_id not in self.sessions:
            raise ValueError(f"Session not found: {session_id}")
        self.sessions[session_id].update(data)

    def add_to_query_history(self, session_id: str, query: str) -> None:
        if session_id not in self.sessions:
            raise ValueError(f"Session not found: {session_id}")
        history = self.sessions[session_id]["query_history"]
        history.append(query)
        if len(history) > 20:
            history.pop(0)

    def set_current_table(self, session_id: str, table_name: str) -> None:
        if session_id not in self.sessions:
            raise ValueError(f"Session not found: {session_id}")
        self.sessions[session_id]["current_table"] = table_name

    def set_current_file(self, session_id: str, file_path: str) -> None:
        if session_id not in self.sessions:
            raise ValueError(f"Session not found: {session_id}")
        self.sessions[session_id]["analyzed_files"].add(file_path)

    def has_accessed_sql_docs(self, session_id: str) -> bool:
        if session_id not in self.sessions:
            raise ValueError(f"Session not found: {session_id}")
        return self.sessions[session_id].get("has_accessed_sql_docs", False)


async def start_server(config: Config) -> None:
    """Start the MCP server with the given configuration."""
    logger.info(f"Starting DuckDB MCP Server with DB path: {config.db_path}")

    db_client = DuckDBClient(config)
    session_manager = SessionManager()
    server = Server("duckdb-mcp-server")

    _register_handlers(server, db_client, session_manager)

    options = server.create_initialization_options()
    async with mcp.server.stdio.stdio_server() as (read_stream, write_stream):
        logger.info("DuckDB MCP Server running with stdio transport")
        await server.run(read_stream, write_stream, options)


def _is_file_path(value: str) -> bool:
    """Determine whether a string looks like a file/remote path rather than a table name."""
    return value.startswith("s3://") or any(
        ext in value for ext in [".parquet", ".csv", ".json", ".parq", ".jsonl"]
    )


def _register_handlers(
    server: Server, db_client: DuckDBClient, session_manager: SessionManager
) -> None:
    """Register all MCP server handlers."""

    # ------------------------------------------------------------------ #
    # Tool handler helpers                                                  #
    # ------------------------------------------------------------------ #

    def _handle_create_session(
        arguments: dict[str, Any] | None, session_id: str
    ) -> list[types.TextContent]:
        payload = {"session_id": session_id}
        return [types.TextContent(type="text", text=json.dumps(payload))]

    def _handle_query(
        arguments: dict[str, Any] | None, session_id: str
    ) -> list[types.TextContent]:
        if not arguments or "query" not in arguments:
            raise ValueError("Missing required argument: query")

        query = arguments["query"]

        if query.strip().lower() == "--duckdb-ref://friendly-sql":
            session_manager.sessions[session_id]["has_accessed_sql_docs"] = True

        session_manager.add_to_query_history(session_id, query)

        table_name = db_client.extract_table_name_from_query(query)
        if table_name:
            session_manager.set_current_table(session_id, table_name)
            for fp in db_client.extract_file_paths_from_query(query):
                session_manager.set_current_file(session_id, fp)

        result = db_client.handle_query_tool(query, session_id)
        return [types.TextContent(type="text", text=result)]

    def _handle_analyze_schema(
        arguments: dict[str, Any] | None, session_id: str
    ) -> list[types.TextContent]:
        file_path = (arguments or {}).get("file_path")
        if not file_path:
            return [types.TextContent(type="text", text="Error: No file path provided")]

        session_manager.set_current_file(session_id, file_path)

        if _is_file_path(file_path):
            suggestion = db_client.generate_table_cache_suggestion(file_path, session_id)
            return [types.TextContent(type="text", text=suggestion)]

        result = db_client.handle_analyze_schema_tool(file_path)
        return [types.TextContent(type="text", text=result)]

    def _handle_analyze_data(
        arguments: dict[str, Any] | None, session_id: str
    ) -> list[types.TextContent]:
        args = arguments or {}
        target = args.get("table_name") or args.get("file_path")
        if not target:
            return [types.TextContent(type="text", text="Error: No table_name or file_path provided")]

        if _is_file_path(target):
            session_manager.set_current_file(session_id, target)
            suggestion = db_client.generate_table_cache_suggestion(target, session_id)
            return [types.TextContent(type="text", text=suggestion)]

        session_manager.set_current_table(session_id, target)
        result = db_client.handle_analyze_data_tool(target)
        return [types.TextContent(type="text", text=result)]

    def _handle_suggest_visualizations(
        arguments: dict[str, Any] | None, session_id: str
    ) -> list[types.TextContent]:
        args = arguments or {}
        target = args.get("table_name") or args.get("file_path")
        if not target:
            return [types.TextContent(type="text", text="Error: No table_name or file_path provided")]

        if _is_file_path(target):
            session_manager.set_current_file(session_id, target)
            suggestion = db_client.generate_table_cache_suggestion(target, session_id)
            return [types.TextContent(type="text", text=suggestion)]

        session_manager.set_current_table(session_id, target)
        result = db_client.handle_suggest_visualizations_tool(target)
        return [types.TextContent(type="text", text=result)]

    # ------------------------------------------------------------------ #
    # Resources                                                             #
    # ------------------------------------------------------------------ #

    @server.list_resources()
    async def handle_list_resources() -> list[types.Resource]:
        logger.debug("Listing resources")
        return [
            types.Resource(
                uri="duckdb-ref://friendly-sql",  # type: ignore[arg-type]
                name="duckdb_friendly_sql",
                title="DuckDB Friendly SQL",
                description="Documentation on DuckDB's friendly SQL features and syntactic sugar",
                mimeType="application/xml",
            ),
            types.Resource(
                uri="duckdb-ref://data-import",  # type: ignore[arg-type]
                name="duckdb_data_import",
                title="DuckDB Data Import",
                description=(
                    "Documentation on importing data from various sources "
                    "(local, S3, GCS) and formats (CSV, Parquet, JSON) in DuckDB"
                ),
                mimeType="application/xml",
            ),
            types.Resource(
                uri="duckdb-ref://visualization",  # type: ignore[arg-type]
                name="duckdb_visualization",
                title="DuckDB Data Visualization",
                description="Guidelines and query patterns for visualizing DuckDB query results",
                mimeType="application/xml",
            ),
        ]

    @server.read_resource()
    async def handle_read_resource(uri: AnyUrl) -> str:
        logger.debug(f"Reading resource: {uri}")

        if uri.scheme != "duckdb-ref":
            raise ValueError(f"Unsupported URI scheme: {uri.scheme}")

        path = uri.host

        if path == "friendly-sql":
            return docs.get_friendly_sql_docs()
        elif path == "data-import":
            return docs.get_data_import_docs()
        elif path == "visualization":
            return docs.get_visualization_docs()
        else:
            raise ValueError(f"Unknown resource: {path}")

    # ------------------------------------------------------------------ #
    # Prompts                                                               #
    # ------------------------------------------------------------------ #

    @server.list_prompts()
    async def handle_list_prompts() -> list[types.Prompt]:
        logger.debug("Listing prompts")
        return [
            types.Prompt(
                name="duckdb-initial-prompt",
                title="DuckDB Assistant Setup",
                description=(
                    "System prompt that configures an assistant for DuckDB data analysis, "
                    "covering caching patterns, multi-file operations, and resource access"
                ),
            ),
        ]

    @server.get_prompt()
    async def handle_get_prompt(
        name: str, arguments: dict[str, str] | None
    ) -> types.GetPromptResult:
        logger.debug(f"Getting prompt: {name}")

        if name != "duckdb-initial-prompt":
            raise ValueError(f"Unknown prompt: {name}")

        prompt_text = (
            "You are connected to a DuckDB database through the Model Context Protocol (MCP).\n\n"
            "WORKFLOW — always follow these steps:\n\n"
            "1. Before writing any query, consult the relevant documentation resource:\n"
            "   - SQL syntax & features  → duckdb-ref://friendly-sql\n"
            "   - Loading remote/local files → duckdb-ref://data-import\n"
            "   - Visualization patterns → duckdb-ref://visualization\n\n"
            "2. For remote files (S3, GCS, HTTP), always cache first:\n"
            "   ```sql\n"
            "   -- single file\n"
            "   CREATE TABLE cached AS SELECT * FROM read_parquet('s3://bucket/file.parquet');\n"
            "   -- multiple files (glob)\n"
            "   CREATE TABLE cached AS\n"
            "     SELECT * FROM read_parquet('s3://bucket/*.parquet', union_by_name = true);\n"
            "   ```\n"
            "   Then query the cached table — never repeat remote reads.\n\n"
            "3. Inspect schema before aggregating: use analyze_schema on any new table/file.\n\n"
            "4. Use DuckDB-specific SQL where applicable (GROUP BY ALL, SELECT * EXCLUDE, etc.) "
            "— see duckdb-ref://friendly-sql.\n\n"
            "Available resources:\n"
            "  • duckdb-ref://friendly-sql   — SQL extensions & syntactic sugar\n"
            "  • duckdb-ref://data-import    — CSV / Parquet / JSON / S3 loading\n"
            "  • duckdb-ref://visualization  — chart query patterns\n"
        )

        return types.GetPromptResult(
            description="Initial setup prompt for DuckDB data analysis workflows",
            messages=[
                types.PromptMessage(
                    role="user",
                    content=types.TextContent(type="text", text=prompt_text),
                )
            ],
        )

    # ------------------------------------------------------------------ #
    # Tools                                                                 #
    # ------------------------------------------------------------------ #

    @server.list_tools()
    async def handle_list_tools() -> list[types.Tool]:
        logger.debug("Listing tools")
        return [
            types.Tool(
                name="query",
                title="Execute SQL Query",
                description=(
                    "Execute any SQL query against DuckDB. "
                    "Check duckdb-ref://friendly-sql before writing queries to use "
                    "DuckDB-specific syntax. Results are truncated at "
                    f"{db_client.MAX_RESULT_ROWS} rows."
                ),
                inputSchema={
                    "type": "object",
                    "properties": {
                        "query": {
                            "type": "string",
                            "description": "SQL query to execute",
                        },
                        "session_id": {
                            "type": "string",
                            "description": "Optional session ID for tracking context across calls",
                        },
                    },
                    "required": ["query"],
                },
                outputSchema={
                    "type": "object",
                    "properties": {
                        "result": {"type": "string", "description": "Formatted query result table"},
                        "truncated": {
                            "type": "boolean",
                            "description": "True if the result was truncated due to row limit",
                        },
                        "row_count": {"type": "integer", "description": "Number of rows returned"},
                    },
                    "required": ["result"],
                },
                annotations=types.ToolAnnotations(
                    readOnlyHint=False,
                    destructiveHint=False,
                    idempotentHint=False,
                    openWorldHint=True,
                ),
            ),
            types.Tool(
                name="analyze_schema",
                title="Analyze Schema",
                description=(
                    "Inspect the schema (column names and types) of a local file, "
                    "S3 path, or existing DuckDB table. "
                    "For remote/file paths this suggests a caching query instead of "
                    "reading the file repeatedly."
                ),
                inputSchema={
                    "type": "object",
                    "properties": {
                        "file_path": {
                            "type": "string",
                            "description": (
                                "File path (local or s3://), glob pattern, or table name"
                            ),
                        },
                        "session_id": {
                            "type": "string",
                            "description": "Optional session ID",
                        },
                    },
                    "required": ["file_path"],
                },
                outputSchema={
                    "type": "object",
                    "properties": {
                        "schema": {"type": "string", "description": "Column names and data types"},
                    },
                    "required": ["schema"],
                },
                annotations=types.ToolAnnotations(
                    readOnlyHint=True,
                    destructiveHint=False,
                    idempotentHint=True,
                    openWorldHint=True,
                ),
            ),
            types.Tool(
                name="analyze_data",
                title="Analyze Data Statistics",
                description=(
                    "Perform statistical analysis on a DuckDB table: row count, "
                    "numeric stats (min/max/avg/median), date ranges, and top categorical values. "
                    "Pass a table name — cache remote files first using the query tool."
                ),
                inputSchema={
                    "type": "object",
                    "properties": {
                        "table_name": {
                            "type": "string",
                            "description": "Name of the DuckDB table to analyze",
                        },
                        "session_id": {
                            "type": "string",
                            "description": "Optional session ID",
                        },
                    },
                    "required": ["table_name"],
                },
                outputSchema={
                    "type": "object",
                    "properties": {
                        "analysis": {
                            "type": "string",
                            "description": "Statistical summary of the table",
                        },
                    },
                    "required": ["analysis"],
                },
                annotations=types.ToolAnnotations(
                    readOnlyHint=True,
                    destructiveHint=False,
                    idempotentHint=True,
                    openWorldHint=False,
                ),
            ),
            types.Tool(
                name="suggest_visualizations",
                title="Suggest Visualizations",
                description=(
                    "Suggest chart types and ready-to-run SQL queries for visualizing "
                    "a DuckDB table based on its column types (time series, bar, scatter). "
                    "Pass a table name — cache remote files first using the query tool."
                ),
                inputSchema={
                    "type": "object",
                    "properties": {
                        "table_name": {
                            "type": "string",
                            "description": "Name of the DuckDB table to visualize",
                        },
                        "session_id": {
                            "type": "string",
                            "description": "Optional session ID",
                        },
                    },
                    "required": ["table_name"],
                },
                outputSchema={
                    "type": "object",
                    "properties": {
                        "suggestions": {
                            "type": "string",
                            "description": "Visualization suggestions with SQL queries",
                        },
                    },
                    "required": ["suggestions"],
                },
                annotations=types.ToolAnnotations(
                    readOnlyHint=True,
                    destructiveHint=False,
                    idempotentHint=True,
                    openWorldHint=False,
                ),
            ),
            types.Tool(
                name="create_session",
                title="Create or Reset Session",
                description=(
                    "Create a new analysis session or reset an existing one. "
                    "Returns the session_id to pass to subsequent tool calls for context tracking."
                ),
                inputSchema={
                    "type": "object",
                    "properties": {
                        "session_id": {
                            "type": "string",
                            "description": "Optional existing session ID to reset",
                        },
                    },
                },
                outputSchema={
                    "type": "object",
                    "properties": {
                        "session_id": {"type": "string", "description": "The active session ID"},
                    },
                    "required": ["session_id"],
                },
                annotations=types.ToolAnnotations(
                    readOnlyHint=True,
                    destructiveHint=False,
                    idempotentHint=False,
                    openWorldHint=False,
                ),
            ),
        ]

    @server.call_tool()
    async def handle_call_tool(
        name: str, arguments: dict[str, Any] | None
    ) -> list[types.TextContent | types.ImageContent | types.EmbeddedResource]:
        logger.debug(f"Calling tool: {name} with args: {arguments}")

        # Resolve or create session
        session_id = (arguments or {}).get("session_id")
        if not session_id or session_id not in session_manager.sessions:
            session_id = session_manager.create_session(session_id)

        if name == "create_session":
            return _handle_create_session(arguments, session_id)
        elif name == "query":
            return _handle_query(arguments, session_id)
        elif name == "analyze_schema":
            return _handle_analyze_schema(arguments, session_id)
        elif name == "analyze_data":
            return _handle_analyze_data(arguments, session_id)
        elif name == "suggest_visualizations":
            return _handle_suggest_visualizations(arguments, session_id)
        else:
            raise ValueError(f"Unknown tool: {name}")
