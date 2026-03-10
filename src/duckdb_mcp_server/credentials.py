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
    """Escape single quotes so credential values are safe to interpolate into CREATE SECRET."""
    return value.replace("'", "''")


def setup_s3_credentials(connection: duckdb.DuckDBPyConnection, config: Config) -> None:
    """
    Configure S3 credentials for the DuckDB connection via the Secrets Manager.

    Three modes, tried in order:

    1. ``--creds-from-env``  — explicit KEY_ID/SECRET from environment variables.
       Falls back to credential_chain if the required env vars are absent.

    2. ``--s3-profile NAME``  — reads a named profile from ~/.aws/credentials using
       ``PROVIDER credential_chain, CHAIN 'config'``.

    3. Default  — bare ``credential_chain`` with no CHAIN override.
       DuckDB tries every provider automatically: environment variables,
       ~/.aws/credentials (default profile), ~/.aws/config, EC2 instance
       metadata (IMDSv1/v2), ECS task roles, etc.
       This is always attempted so that IAM-role-based environments (EC2,
       Lambda, ECS) work without any explicit configuration.
    """
    try:
        if config.creds_from_env:
            _setup_from_env(connection, config.s3_region)
        elif config.s3_profile:
            _setup_from_profile(connection, config.s3_profile, config.s3_region)
        else:
            _setup_credential_chain(connection)
        logger.info("S3 credentials configured successfully")
    except Exception as e:
        logger.warning("Failed to configure S3 credentials: %s", e)
        logger.warning("S3 access might be limited or unavailable")


# ------------------------------------------------------------------ #
# Private helpers                                                       #
# ------------------------------------------------------------------ #

def _setup_from_env(connection: duckdb.DuckDBPyConnection, region: Optional[str]) -> None:
    """
    Create an S3 secret from explicit environment variables.

    Required: AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY
    Optional: AWS_SESSION_TOKEN, AWS_DEFAULT_REGION
    """
    key_id = os.getenv("AWS_ACCESS_KEY_ID")
    secret = os.getenv("AWS_SECRET_ACCESS_KEY")

    if not key_id or not secret:
        logger.warning(
            "AWS_ACCESS_KEY_ID or AWS_SECRET_ACCESS_KEY not set; "
            "falling back to credential_chain"
        )
        _setup_credential_chain(connection)
        return

    session_token = os.getenv("AWS_SESSION_TOKEN")
    effective_region = _sanitize(
        region or os.getenv("AWS_DEFAULT_REGION") or "us-east-1"
    )

    fields = [
        f"KEY_ID '{_sanitize(key_id)}'",
        f"SECRET '{_sanitize(secret)}'",
        f"REGION '{effective_region}'",
    ]
    if session_token:
        fields.append(f"SESSION_TOKEN '{_sanitize(session_token)}'")

    _execute_create_secret(connection, "config", fields)
    logger.info("Created S3 secret from environment credentials")


def _setup_from_profile(
    connection: duckdb.DuckDBPyConnection,
    profile: str,
    region: Optional[str],
) -> None:
    """
    Create an S3 secret using a named AWS profile from ~/.aws/credentials.

    Uses ``credential_chain`` with ``CHAIN 'config'`` — the PROFILE option
    only takes effect when CHAIN is explicitly set to 'config'.
    """
    effective_region = _sanitize(region or "us-east-1")
    fields = [
        "CHAIN 'config'",
        f"PROFILE '{_sanitize(profile)}'",
        f"REGION '{effective_region}'",
    ]
    _execute_create_secret(connection, "credential_chain", fields)
    logger.info("Created S3 secret using profile '%s'", profile)


def _setup_credential_chain(connection: duckdb.DuckDBPyConnection) -> None:
    """
    Create a bare credential_chain secret.

    DuckDB will auto-discover credentials from all available sources:
    environment variables, ~/.aws/credentials & ~/.aws/config (default
    profile), EC2 instance metadata (IMDSv1/v2), ECS task roles, etc.
    """
    _execute_create_secret(connection, "credential_chain", [])
    logger.info("Created S3 secret using credential_chain (auto-discovery)")


def _execute_create_secret(
    connection: duckdb.DuckDBPyConnection,
    provider: str,
    extra_fields: list[str],
) -> None:
    fields_sql = ""
    if extra_fields:
        fields_sql = ",\n        " + ",\n        ".join(extra_fields)
    sql = f"""
    CREATE OR REPLACE SECRET mcp_s3_secret (
        TYPE s3,
        PROVIDER {provider}{fields_sql}
    );
    """
    connection.execute(sql)
