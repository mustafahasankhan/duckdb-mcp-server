"""
Credential management for cloud services like AWS S3.
"""

import logging
import os
from typing import Optional

import duckdb

from .config import Config

logger = logging.getLogger("duckdb-mcp-server.credentials")


def setup_s3_credentials(connection: duckdb.DuckDBPyConnection, config: Config) -> None:
    """
    Configure S3 credentials for DuckDB connection using secrets-based authentication.
    
    This function sets up S3 access for DuckDB httpfs extension using
    AWS credentials from the environment or AWS profiles.
    
    Args:
        connection: DuckDB connection to configure
        config: Server configuration
    """
    # Skip if we're not setting up S3 credentials
    if not _should_setup_s3_credentials():
        return
    
    try:
        # Create and configure a secret with appropriate provider
        if config.creds_from_env:
            _setup_from_environment_as_secret(connection)
        else:
            # Use profile-based credentials with credential_chain provider
            _setup_from_profile_as_secret(connection, config.s3_profile, config.s3_region)
            
        logger.info("S3 credentials configured successfully with secrets-based auth")
        
    except Exception as e:
        logger.warning(f"Failed to configure S3 credentials: {str(e)}")
        logger.warning("S3 access might be limited or unavailable")


def _should_setup_s3_credentials() -> bool:
    """Check if we should attempt to set up S3 credentials."""
    # Check if AWS_ACCESS_KEY_ID or AWS_PROFILE is set
    # or if there's a credentials file
    return (
        os.getenv("AWS_ACCESS_KEY_ID") is not None
        or os.getenv("AWS_PROFILE") is not None
        or os.path.exists(os.path.expanduser("~/.aws/credentials"))
    )


def _setup_from_environment_as_secret(connection: duckdb.DuckDBPyConnection) -> None:
    """
    Set up S3 credentials from environment variables using secrets.
    
    Args:
        connection: DuckDB connection to configure
    """
    # Check for required environment variables
    access_key = os.getenv("AWS_ACCESS_KEY_ID")
    secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    
    if not access_key or not secret_key:
        logger.warning("AWS_ACCESS_KEY_ID or AWS_SECRET_ACCESS_KEY not set")
        # Fall back to credential chain
        _setup_credential_chain_secret(connection)
        return
    
    # Get region and session token
    session_token = os.getenv("AWS_SESSION_TOKEN")
    region = os.getenv("AWS_DEFAULT_REGION") or "us-east-1"
    
    try:
        # Create a secret with config provider - use direct string formatting instead of parameterized queries
        # since CREATE SECRET doesn't work well with parameterized queries
        if session_token:
            create_secret_stmt = f"""
            CREATE OR REPLACE SECRET mcp_s3_secret (
                TYPE s3,
                PROVIDER config,
                KEY_ID '{access_key}',
                SECRET '{secret_key}',
                SESSION_TOKEN '{session_token}',
                REGION '{region}'
            );
            """
            connection.execute(create_secret_stmt)
        else:
            create_secret_stmt = f"""
            CREATE OR REPLACE SECRET mcp_s3_secret (
                TYPE s3,
                PROVIDER config,
                KEY_ID '{access_key}',
                SECRET '{secret_key}',
                REGION '{region}'
            );
            """
            connection.execute(create_secret_stmt)
            
        logger.info("Created S3 secret with environment credentials")
    except Exception as e:
        logger.warning(f"Failed to create S3 secret: {str(e)}")
        # Try fallback to credential chain
        _setup_credential_chain_secret(connection)


def _setup_from_profile_as_secret(
    connection: duckdb.DuckDBPyConnection, 
    profile_name: Optional[str] = None, 
    region: Optional[str] = None
) -> None:
    """
    Set up S3 credentials from AWS profile using secrets.
    
    Args:
        connection: DuckDB connection to configure
        profile_name: AWS profile name to use
        region: AWS region to use
    """
    # Use credential_chain provider with profile
    region = region or "ap-south-1"
    profile_name = profile_name or "default"
    
    try:
        # Use direct string formatting instead of parameterized queries
        create_secret_stmt = f"""
        CREATE OR REPLACE SECRET mcp_s3_secret (
            TYPE s3,
            PROVIDER credential_chain,
            PROFILE '{profile_name}',
            REGION '{region}'
        );
        """
        
        connection.execute(create_secret_stmt)
        logger.info(f"Created S3 secret with profile: {profile_name}")
    except Exception as e:
        logger.warning(f"Failed to create S3 secret with profile: {str(e)}")
        # Try fallback to basic credential chain
        _setup_credential_chain_secret(connection)


def _setup_credential_chain_secret(connection: duckdb.DuckDBPyConnection) -> None:
    """
    Set up S3 credentials using the AWS credential chain.
    This allows automatic credential discovery from environment,
    instance profiles, config files, etc.
    
    Args:
        connection: DuckDB connection to configure
    """
    create_secret_stmt = """
    CREATE OR REPLACE SECRET mcp_s3_secret (
        TYPE s3,
        PROVIDER credential_chain
    );
    """
    
    connection.execute(create_secret_stmt)