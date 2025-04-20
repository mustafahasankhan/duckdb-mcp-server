"""
Tests for the Config class in the DuckDB MCP server.
"""

import os
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

from duckdb_mcp_server.config import Config


def test_config_required_db_path():
    """Test that db_path is required when parsing arguments."""
    with patch.object(sys, "argv", ["prog"]):
        with pytest.raises(SystemExit):
            Config.from_arguments()


def test_config_from_arguments_minimal():
    """Test Config creation with minimal arguments."""
    test_db_file = "test.db"
    test_args = [
        "--db-path",
        test_db_file,
    ]
    with patch.object(sys, "argv", ["prog"] + test_args):
        config = Config.from_arguments()
        assert config.db_path == Path(test_db_file)
        assert config.readonly is False
        assert config.creds_from_env is False


def test_config_from_arguments_full():
    """Test Config creation with all arguments."""
    test_db_file = "test.db"
    test_args = [
        "--db-path", test_db_file,
        "--readonly",
        "--s3-region", "us-west-2",
        "--s3-profile", "test-profile",
        "--creds-from-env",
    ]
    with patch.object(sys, "argv", ["prog"] + test_args):
        config = Config.from_arguments()
        assert config.db_path == Path(test_db_file)
        assert config.readonly is True
        assert config.s3_region == "us-west-2"
        assert config.s3_profile == "test-profile"
        assert config.creds_from_env is True


def test_config_path_expansion():
    """Test that paths with ~ are expanded correctly."""
    with patch.object(sys, "argv", ["prog", "--db-path", "~/test.db"]):
        with patch.object(os.path, "expanduser", return_value="/home/user/test.db"):
            config = Config.from_arguments()
            assert config.db_path == Path("/home/user/test.db")