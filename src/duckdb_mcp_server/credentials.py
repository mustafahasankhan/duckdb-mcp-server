"""
Credential management for cloud services like AWS S3.
"""

import logging
import os
from typing import Optional

import duckdb

from .config import Config

logger = logging.getLogger("duckdb-mcp-server.credentials")


def _sanitize(value: str) -> str:
    """
    Escape single quotes in a credential value to prevent SQL injection in
    CREATE SECRET statements, which do not support parameterized queries.
    """
    return value.replace("'", "''")


def setup_s3_credentials(connection: duckdb.DuckDBPyConnection, config: Config) -> None:
    """
    Configure S3 credentials for a DuckDB connection via the secrets API.

    Strategy (in order):
    1. If --creds-from-env: read AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY.
    2. Otherwise: use credential_chain with optional profile / region.

    Falls back to a bare credential_chain secret on any failure.
    """
    if not _should_setup_s3_credentials():
        return

    try:
        if config.creds_from_env:
            _setup_from_environment_as_secret(connection)
        else:
            _setup_from_profile_as_secret(connection, config.s3_profile, config.s3_region)
        logger.info("S3 credentials configured successfully")
    except Exception as e:
        logger.warning("Failed to configure S3 credentials: %s", e)
        logger.warning("S3 access might be limited or unavailable")


def _should_setup_s3_credentials() -> bool:
    return (
        os.getenv("AWS_ACCESS_KEY_ID") is not None
        or os.getenv("AWS_PROFILE") is not None
        or os.path.exists(os.path.expanduser("~/.aws/credentials"))
    )


def _setup_from_environment_as_secret(connection: duckdb.DuckDBPyConnection) -> None:
    """Set up S3 credentials from environment variables."""
    access_key = os.getenv("AWS_ACCESS_KEY_ID")
    secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")

    if not access_key or not secret_key:
        logger.warning(
            "AWS_ACCESS_KEY_ID or AWS_SECRET_ACCESS_KEY not set; "
            "falling back to credential_chain"
        )
        _setup_credential_chain_secret(connection)
        return

    session_token = os.getenv("AWS_SESSION_TOKEN")
    region = _sanitize(os.getenv("AWS_DEFAULT_REGION") or "us-east-1")
    key = _sanitize(access_key)
    secret = _sanitize(secret_key)

    try:
        if session_token:
            token = _sanitize(session_token)
            connection.execute(
                f"""
                CREATE OR REPLACE SECRET mcp_s3_secret (
                    TYPE s3,
                    PROVIDER config,
                    KEY_ID '{key}',
                    SECRET '{secret}',
                    SESSION_TOKEN '{token}',
                    REGION '{region}'
                );
                """
            )
        else:
            connection.execute(
                f"""
                CREATE OR REPLACE SECRET mcp_s3_secret (
                    TYPE s3,
                    PROVIDER config,
                    KEY_ID '{key}',
                    SECRET '{secret}',
                    REGION '{region}'
                );
                """
            )
        logger.info("Created S3 secret from environment credentials")
    except Exception as e:
        logger.warning("Failed to create S3 secret from environment: %s; trying credential_chain", e)
        _setup_credential_chain_secret(connection)


def _setup_from_profile_as_secret(
    connection: duckdb.DuckDBPyConnection,
    profile_name: Optional[str] = None,
    region: Optional[str] = None,
) -> None:
    """Set up S3 credentials from an AWS profile via the credential_chain provider."""
    safe_region = _sanitize(region or "us-east-1")
    safe_profile = _sanitize(profile_name or "default")

    try:
        connection.execute(
            f"""
            CREATE OR REPLACE SECRET mcp_s3_secret (
                TYPE s3,
                PROVIDER credential_chain,
                PROFILE '{safe_profile}',
                REGION '{safe_region}'
            );
            """
        )
        logger.info("Created S3 secret with profile: %s", safe_profile)
    except Exception as e:
        logger.warning(
            "Failed to create S3 secret with profile %s: %s; trying bare credential_chain",
            safe_profile,
            e,
        )
        _setup_credential_chain_secret(connection)


def _setup_credential_chain_secret(connection: duckdb.DuckDBPyConnection) -> None:
    """Fall back to automatic AWS credential discovery."""
    connection.execute(
        """
        CREATE OR REPLACE SECRET mcp_s3_secret (
            TYPE s3,
            PROVIDER credential_chain
        );
        """
    )
    logger.info("Created S3 secret using credential_chain")
