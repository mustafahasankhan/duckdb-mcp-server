"""
Main MCP server implementation for DuckDB.
"""

import asyncio
import json
import logging
import uuid
from typing import Any, Dict, List, Optional

import mcp.server.stdio
import mcp.types as types
from mcp.server import Server
from pydantic import AnyUrl

from .config import Config
from .database import DuckDBClient
from .resources import docs

logger = logging.getLogger("duckdb-mcp-server.server")


class SessionManager:
    """
    Manages user sessions and their state.
    
    This class tracks information about user sessions to maintain
    context between requests.
    """
    
    def __init__(self):
        """Initialize the session manager."""
        self.sessions: Dict[str, Dict[str, Any]] = {}
        
    def create_session(self, session_id: Optional[str] = None) -> str:
        """
        Create a new session or reset an existing one.
        
        Args:
            session_id: Optional session ID to use or reset
            
        Returns:
            Session ID
        """
        # Generate a new ID if none provided
        if not session_id:
            session_id = str(uuid.uuid4())
            
        # Initialize or reset session data
        self.sessions[session_id] = {
            "current_table": None,
            "analyzed_files": set(),
            "has_accessed_sql_docs": False,
            "query_history": [],
            "visualization_history": []
        }
        
        return session_id
        
    def get_session(self, session_id: str) -> Optional[Dict[str, Any]]:
        """
        Get session data for the given ID.
        
        Args:
            session_id: Session ID
            
        Returns:
            Session data or None if not found
        """
        return self.sessions.get(session_id)
        
    def update_session(self, session_id: str, data: Dict[str, Any]) -> None:
        """
        Update session data.
        
        Args:
            session_id: Session ID
            data: Data to update
            
        Raises:
            ValueError: If session doesn't exist
        """
        if session_id not in self.sessions:
            raise ValueError(f"Session not found: {session_id}")
            
        # Update session data
        self.sessions[session_id].update(data)
        
    def add_to_query_history(self, session_id: str, query: str) -> None:
        """
        Add a query to the session history.
        
        Args:
            session_id: Session ID
            query: SQL query
            
        Raises:
            ValueError: If session doesn't exist
        """
        if session_id not in self.sessions:
            raise ValueError(f"Session not found: {session_id}")
            
        # Add to history, keep max 20 most recent queries
        self.sessions[session_id]["query_history"].append(query)
        if len(self.sessions[session_id]["query_history"]) > 20:
            self.sessions[session_id]["query_history"].pop(0)
            
    def set_current_table(self, session_id: str, table_name: str) -> None:
        """
        Set the current table for the session.
        
        Args:
            session_id: Session ID
            table_name: Name of the table
            
        Raises:
            ValueError: If session doesn't exist
        """
        if session_id not in self.sessions:
            raise ValueError(f"Session not found: {session_id}")
            
        self.sessions[session_id]["current_table"] = table_name
        
    def set_current_file(self, session_id: str, file_path: str) -> None:
        """
        Track a file as being analyzed in this session.
        
        Args:
            session_id: Session ID
            file_path: Path to the file
            
        Raises:
            ValueError: If session doesn't exist
        """
        if session_id not in self.sessions:
            raise ValueError(f"Session not found: {session_id}")
            
        self.sessions[session_id]["analyzed_files"].add(file_path)

    def has_accessed_sql_docs(self, session_id: str) -> bool:
        """
        Check if the SQL documentation resource has been accessed in this session.
        
        Args:
            session_id: Session ID
            
        Returns:
            True if the SQL docs have been accessed, False otherwise
            
        Raises:
            ValueError: If session doesn't exist
        """
        if session_id not in self.sessions:
            raise ValueError(f"Session not found: {session_id}")
            
        return self.sessions[session_id].get("has_accessed_sql_docs", False)


async def start_server(config: Config) -> None:
    """
    Start the MCP server with the given configuration.
    
    Args:
        config: Server configuration
    """
    logger.info(f"Starting DuckDB MCP Server with DB path: {config.db_path}")
    
    # Initialize the DuckDB client
    db_client = DuckDBClient(config)
    
    # Initialize session manager
    session_manager = SessionManager()
    
    # Initialize server
    server = Server("duckdb-mcp-server")
    
    # Register handlers
    _register_handlers(server, db_client, session_manager)
    
    # Run the server using stdin/stdout streams
    options = server.create_initialization_options()
    async with mcp.server.stdio.stdio_server() as (read_stream, write_stream):
        logger.info("DuckDB MCP Server running with stdio transport")
        await server.run(
            read_stream,
            write_stream,
            options,
        )


def _register_handlers(server: Server, db_client: DuckDBClient, session_manager: SessionManager) -> None:
    """
    Register all MCP server handlers.
    
    Args:
        server: MCP server instance
        db_client: DuckDB client instance
        session_manager: Session manager instance
    """
    
    # Helper functions for tool implementations
    def _handle_create_session(arguments: Dict[str, Any] | None, session_id: str) -> List[types.TextContent]:
        """Handle create_session tool."""
        # Return the session ID (which was already created/reset in the calling function)
        return [types.TextContent(type="text", text=json.dumps({"session_id": session_id}))]
    
    def _handle_query(arguments: Dict[str, Any] | None, session_id: str) -> List[types.TextContent]:
        """Handle query tool."""
        if not arguments or "query" not in arguments:
            raise ValueError("Missing required argument: query")
            
        query = arguments["query"]

        if query.strip().lower() == "--duckdb-ref://friendly-sql":
            # Mark that they've checked the docs
            session_manager.sessions[session_id]["has_accessed_sql_docs"] = True
            
        # Add the query to history
        session_manager.add_to_query_history(session_id, query)
        
        # Extract table name from CREATE TABLE statements
        table_name = db_client.extract_table_name_from_query(query)
        if table_name:
            # Save the created table name in the session
            session_manager.set_current_table(session_id, table_name)
            
            # Extract file paths from the query and track them
            file_paths = db_client.extract_file_paths_from_query(query)
            for file_path in file_paths:
                session_manager.set_current_file(session_id, file_path)
        
        # Execute the query and get results with suggestions
        result = db_client.handle_query_tool(query, session_id)
        
        
        return [types.TextContent(type="text", text=result)]
    
    def _handle_analyze_schema(arguments: Dict[str, Any] | None, session_id: str) -> List[types.TextContent]:
        """Handle analyze_schema tool."""
        file_path = arguments.get("file_path")
        if not file_path:
            return [types.TextContent(type="text", text="Error: No file path provided")]
        
        # Track that we're analyzing this file
        session_manager.set_current_file(session_id, file_path)
        
        # Check if it's a file path that should be cached first
        if file_path.startswith("s3://") or any(ext in file_path for ext in [".parquet", ".csv", ".json"]):
            # Return suggestion to cache the file first
            suggestion = db_client.generate_table_cache_suggestion(file_path, session_id)
            return [types.TextContent(type="text", text=suggestion)]
        
        # Use the database client to analyze the schema
        result = db_client.handle_analyze_schema_tool(file_path)
        return [types.TextContent(type="text", text=result)]
    
    def _handle_analyze_data(arguments: Dict[str, Any] | None, session_id: str) -> List[types.TextContent]:
        """Handle analyze_data tool."""
        table_name = arguments.get("table_name") or arguments.get("file_path")
        if not table_name:
            return [types.TextContent(type="text", text="Error: No table name provided")]
        
        # Check if it's a file path that should be cached first
        if table_name.startswith("s3://") or any(ext in table_name for ext in [".parquet", ".csv", ".json"]):
            # Track this file as being analyzed
            session_manager.set_current_file(session_id, table_name)
            
            # Return suggestion to cache the file first
            suggestion = db_client.generate_table_cache_suggestion(table_name, session_id)
            return [types.TextContent(type="text", text=suggestion)]
        
        # It's a table, track it in the session
        session_manager.set_current_table(session_id, table_name)
        
        # Use the database client to analyze the data
        result = db_client.handle_analyze_data_tool(table_name)
        return [types.TextContent(type="text", text=result)]
    
    def _handle_suggest_visualizations(arguments: Dict[str, Any] | None, session_id: str) -> List[types.TextContent]:
        """Handle suggest_visualizations tool."""
        table_name = arguments.get("table_name") or arguments.get("file_path")
        if not table_name:
            return [types.TextContent(type="text", text="Error: No table name provided")]
        
        # Check if it's a file path that should be cached first
        if table_name.startswith("s3://") or any(ext in table_name for ext in [".parquet", ".csv", ".json"]):
            # Track this file
            session_manager.set_current_file(session_id, table_name)
            
            # Return suggestion to cache the file first
            suggestion = db_client.generate_table_cache_suggestion(table_name, session_id)
            return [types.TextContent(type="text", text=suggestion)]
        
        # Mark this table as the current one in the session
        session_manager.set_current_table(session_id, table_name)
        
        # Use the database client to suggest visualizations
        result = db_client.handle_suggest_visualizations_tool(table_name)
        return [types.TextContent(type="text", text=result)]

    # Define MCP server handlers
    @server.list_resources()
    async def handle_list_resources() -> List[types.Resource]:
        """
        List available DuckDB documentation resources.
        """
        logger.debug("Listing resources")
        return [
            types.Resource(
                uri="duckdb-ref://friendly-sql",
                name="DuckDB Friendly SQL",
                description="Documentation on DuckDB's friendly SQL features",
            ),
            types.Resource(
                uri="duckdb-ref://data-import",
                name="DuckDB Data Import",
                description="Documentation on importing data from various sources (local, S3, etc.) and formats (CSV, Parquet, JSON, etc.) in DuckDB",
            ),
            types.Resource(
                uri="duckdb-ref://visualization",
                name="DuckDB Data Visualization",
                description="Guidelines for visualizing data from DuckDB queries",
            ),
        ]

    @server.read_resource()
    async def handle_read_resource(uri: AnyUrl) -> str:
        """
        Read a specific DuckDB documentation resource.
        
        Args:
            uri: Resource URI
            
        Returns:
            Resource content
            
        Raises:
            ValueError: If the resource doesn't exist
        """
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

    @server.list_prompts()
    async def handle_list_prompts() -> List[types.Prompt]:
        """
        List available DuckDB prompts.
        """
        logger.debug("Listing prompts")
        return [
            types.Prompt(
                name="duckdb-initial-prompt",
                description="Initial prompt for interacting with DuckDB",
            ),
        ]

    @server.get_prompt()
    async def handle_get_prompt(name: str, arguments: Dict[str, str] | None) -> types.GetPromptResult:
        """
        Generate a prompt for interacting with DuckDB.
        
        Args:
            name: Prompt name
            arguments: Prompt arguments
            
        Returns:
            GetPromptResult with the prompt
            
        Raises:
            ValueError: If the prompt doesn't exist
        """
        logger.debug(f"Getting prompt: {name}")
        
        if name == "duckdb-initial-prompt":
            prompt = """
You are now connected to a DuckDB database through the Model Context Protocol (MCP).

IMPORTANT - ALWAYS follow these steps when working with the database:

1. ALWAYS check the DuckDB documentation resources BEFORE writing any queries:
   - For working with remote data and importing it: request 'duckdb-ref://data-import' 
   - For understanding SQL features: request 'duckdb-ref://friendly-sql'

2. When analyzing remote files like S3, always:
   a. First check the data resource documentation using 'duckdb-ref://data-import'
   b. When working with remote files, ALWAYS create a local table first to cache the data:
      ```sql
      -- For single files
      CREATE TABLE cached_data AS SELECT * FROM read_parquet('s3://bucket-name/path/to/file.parquet');
      
      -- For multiple files using arrays
      CREATE TABLE cached_data AS SELECT * FROM read_parquet(['s3://bucket/file1.parquet', 's3://bucket/file2.parquet']);
      
      -- For multiple files using glob patterns
      CREATE TABLE cached_data AS SELECT * FROM read_parquet('s3://bucket/*.parquet');
      ```
   c. Analyze the schema of the cached table
   d. Check 'duckdb-ref://friendly-sql' before writing your queries
   e. Run queries against the cached table, not directly against the remote data

3. When analyzing new files, examine their schema and structure before querying
4. Formulate appropriate SQL queries based on the schema and user's requirements
5. Explain your reasoning and the SQL functionality you're using
6. ALWAYS check the 'DuckDB Friendly SQL' resource (duckdb-ref://friendly-sql) before designing queries.

DuckDB is an in-process analytical database similar to SQLite but optimized for OLAP workloads. It's particularly good at:
- Loading and analyzing CSV, Parquet, and JSON files directly with SQL
- Efficient analytical queries with advanced aggregations
- Handling complex joins and window functions
- Direct querying of data stored in S3
- Processing multiple files at once using arrays or glob patterns

MULTI-FILE OPERATIONS:

DuckDB excels at working with multiple files:

1. Using arrays of files:
   ```sql
   SELECT * FROM read_parquet(['file1.parquet', 'file2.parquet', 'file3.parquet']);
   ```

2. Using glob patterns:
   ```sql
   -- All parquet files in a directory
   SELECT * FROM read_parquet('directory/*.parquet');
   
   -- All CSV files in S3 bucket
   SELECT * FROM read_csv('s3://bucket/*.csv');
   
   -- Files with specific pattern
   SELECT * FROM read_json('data_*_2023.json');
   ```

3. Always cache the data locally for better performance and don't forget to use union_by_name property to avoid schema conflicts:
   ```sql
   -- Create a cached table using a unique name based on the session
   CREATE TABLE cached_data_session123 AS 
   SELECT * FROM read_parquet('s3://bucket/*.parquet', union_by_name = true);
   
   -- Then query the cached table
   SELECT * FROM cached_data_session123 WHERE column_name > 100;
   ```

Available resources:
- duckdb-ref://friendly-sql - Documentation on DuckDB's friendly SQL features [ALWAYS check this resource before writing ANY query]
- duckdb-ref://data-import - Documentation on importing data from various sources (local, S3, etc.) and formats (CSV, Parquet, JSON, etc.) in DuckDB
- duckdb-ref://visualization - Guidelines for visualizing data

Always check these resources before writing queries to ensure you're using DuckDB's features correctly.
"""
            return types.GetPromptResult(prompt=prompt)
        else:
            raise ValueError(f"Unknown prompt: {name}")

    @server.list_tools()
    async def handle_list_tools() -> List[types.Tool]:
        """
        List available DuckDB tools.
        """
        logger.debug("Listing tools")
        return [
            types.Tool(
                name="query",
                description="Execute a SQL query against DuckDB. IMPORTANT: You should ALWAYS first check the 'DuckDB Friendly SQL' resource (duckdb-ref://friendly-sql) before designing queries to ensure you're using DuckDB-specific syntax correctly.",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "query": {
                            "type": "string",
                            "description": "SQL query to execute",
                        },
                        "session_id": {
                            "type": "string",
                            "description": "Session ID for tracking context",
                        },
                    },
                    "required": ["query"],
                },
            ),
            types.Tool(
                name="analyze_schema",
                description="Analyze the schema of a file (local or S3)",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "file_path": {
                            "type": "string",
                            "description": "Path to the file (can be local or S3 URL)",
                        },
                        "session_id": {
                            "type": "string",
                            "description": "Session ID for tracking context",
                        },
                    },
                    "required": ["file_path"],
                },
            ),
            types.Tool(
                name="analyze_data",
                description="Perform statistical analysis on a file",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "file_path": {
                            "type": "string",
                            "description": "Path to the file (can be local or S3 URL)",
                        },
                        "session_id": {
                            "type": "string",
                            "description": "Session ID for tracking context",
                        },
                    },
                    "required": ["file_path"],
                },
            ),
            types.Tool(
                name="suggest_visualizations",
                description="Suggest possible visualizations for a data file",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "file_path": {
                            "type": "string",
                            "description": "Path to the file (can be local or S3 URL)",
                        },
                        "session_id": {
                            "type": "string",
                            "description": "Session ID for tracking context",
                        },
                    },
                    "required": ["file_path"],
                },
            ),
            types.Tool(
                name="create_session",
                description="Create or reset a session",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "session_id": {
                            "type": "string",
                            "description": "Optional session ID to reset",
                        },
                    },
                },
            ),
        ]

    @server.call_tool()
    async def handle_call_tool(
        name: str, arguments: Dict[str, Any] | None
    ) -> List[types.TextContent | types.ImageContent | types.EmbeddedResource]:
        """
        Handle tool calls from the LLM.
        
        Args:
            name: Tool name
            arguments: Tool arguments
            
        Returns:
            Tool result content
            
        Raises:
            ValueError: If the tool doesn't exist or arguments are invalid
        """
        logger.debug(f"Calling tool: {name} with args: {arguments}")
        
        # Extract session ID from arguments or create a new one
        session_id = arguments.get("session_id") if arguments else None
        if not session_id or session_id not in session_manager.sessions:
            session_id = session_manager.create_session(session_id)
        
        # Dispatch to appropriate tool handler
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