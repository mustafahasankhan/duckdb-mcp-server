"""
Tests for the DuckDBClient class.
"""

import os
import tempfile
from pathlib import Path

import duckdb
import pytest

from duckdb_mcp_server.config import Config
from duckdb_mcp_server.database import DuckDBClient


@pytest.fixture
def temp_dir():
    """Create a temporary directory for tests."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


def test_create_database_in_nonexistent_directory(temp_dir):
    """
    Test that DuckDBClient creates the database directory if it doesn't exist.
    """
    db_path = temp_dir / "subdir" / "test.db"
    config = Config(db_path=db_path)
    
    db = DuckDBClient(config)
    
    # Check that directory and file were created
    assert db_path.parent.exists()
    assert db_path.exists()


def test_readonly_mode_missing_directory(temp_dir):
    """
    Test that DuckDBClient raises an error when readonly=True and the directory doesn't exist.
    """
    db_path = temp_dir / "subdir" / "test.db"
    config = Config(db_path=db_path, readonly=True)
    
    # The directory doesn't exist, so it should raise an error in readonly mode
    with pytest.raises(ValueError, match="directory does not exist"):
        DuckDBClient(config)


def test_normal_mode_create_and_query(temp_dir):
    """
    Test that DuckDBClient can create a database and execute queries.
    """
    db_path = temp_dir / "test.db"
    config = Config(db_path=db_path)
    db = DuckDBClient(config)
    
    # Execute a simple query
    result = db.query("SELECT 1 AS test")
    
    # Check the result
    assert "test" in result
    assert "1" in result


def test_format_result():
    """
    Test that DuckDBClient.format_result formats results correctly.
    """
    config = Config(db_path=Path(":memory:"))
    db = DuckDBClient(config)
    
    # Test with simple results
    results = [(1, "Alice"), (2, "Bob")]
    column_names = ["id", "name"]
    
    formatted = db.format_result(results, column_names)
    
    # Check that the header and all rows are in the result
    assert "id | name" in formatted
    assert "1 | Alice" in formatted
    assert "2 | Bob" in formatted
    
    # Test with empty results
    empty_result = db.format_result([], ["col1", "col2"])
    assert "No results" in empty_result