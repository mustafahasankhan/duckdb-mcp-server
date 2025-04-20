"""
MCP Server for DuckDB - Enables LLMs to query and analyze data through DuckDB.
"""

import asyncio
import logging
from importlib.metadata import version

from .config import Config

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] [%(name)s] [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

logger = logging.getLogger("duckdb-mcp-server")

# Package version - useful for debugging
__version__ = "0.1.0"

try:
    __version__ = version("duckdb-mcp-server")
except Exception:
    # If package is not installed, use the hardcoded version
    pass


def main():
    """Main entry point for the MCP DuckDB server."""
    from .server import start_server
    
    logger.info(f"Starting MCP DuckDB Server v{__version__}")
    
    # Parse command line arguments to create configuration
    config = Config.from_arguments()
    
    # Run the server
    asyncio.run(start_server(config))


# Expose important components at package level
from .server import start_server
from .database import DuckDBClient

__all__ = ["main", "start_server", "DuckDBClient", "Config"]