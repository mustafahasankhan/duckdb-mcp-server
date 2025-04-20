"""
Configuration handling for the MCP DuckDB server.
"""

import argparse
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional


@dataclass
class Config:
    """
    Configuration for the MCP DuckDB server.
    """

    db_path: Path
    """Path to DuckDB database file."""

    readonly: bool = False
    """Run server in read-only mode."""
    
    s3_region: Optional[str] = None
    """AWS S3 region. If None, uses AWS_DEFAULT_REGION env var or boto3 defaults."""
    
    s3_profile: Optional[str] = None
    """AWS profile to use for credentials. If None, uses AWS_PROFILE env var or 'default'."""
    
    creds_from_env: bool = False
    """Use AWS credentials from environment variables instead of profiles."""
    
    @staticmethod
    def from_arguments() -> "Config":
        """Parse command line arguments to create a configuration."""
        parser = argparse.ArgumentParser(description="DuckDB MCP Server")

        parser.add_argument(
            "--db-path",
            type=str,
            required=True,
            help="Path to DuckDB database file",
        )

        parser.add_argument(
            "--readonly",
            action="store_true",
            help="Run server in read-only mode. "
                 "If the file does not exist, it is not created when connecting in read-only mode.",
        )
        
        parser.add_argument(
            "--s3-region",
            type=str,
            default=os.getenv("AWS_DEFAULT_REGION"),
            help="AWS S3 region (default: uses AWS_DEFAULT_REGION env var or boto3 defaults)",
        )
        
        parser.add_argument(
            "--s3-profile",
            type=str,
            default=os.getenv("AWS_PROFILE", "default"),
            help="AWS profile to use for credentials (default: uses AWS_PROFILE env var or 'default')",
        )
        
        parser.add_argument(
            "--creds-from-env",
            action="store_true",
            help="Use AWS credentials from environment variables instead of profiles",
        )

        args = parser.parse_args()
        
        # Convert string path to Path object and expand user directory (e.g., ~/)
        db_path = Path(os.path.expanduser(args.db_path))
        
        return Config(
            db_path=db_path,
            readonly=args.readonly,
            s3_region=args.s3_region,
            s3_profile=args.s3_profile,
            creds_from_env=args.creds_from_env,
        )