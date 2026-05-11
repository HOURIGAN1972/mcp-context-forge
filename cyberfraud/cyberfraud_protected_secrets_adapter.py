# Standard
import logging
import os
import sys
from typing import Any

# Local
from . import protected_secrets

# Configure logging to ensure output appears in console
logger = logging.getLogger(__name__)
if not logger.handlers:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(
        logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            datefmt="%Y-%m-%dT%H:%M:%S"
        )
    )
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)

protected_secrets_dict: dict[Any, Any] = protected_secrets.get_config()


protected_secrets_to_env_map: dict[str, str] ={
    "POSTGRES_PASSWORD": "PG__PASSWORD",
    "POSTGRES_USER": "PG__USERNAME",
    "POSTGRES_HOST": "PG__HOST",
    "POSTGRES_PORT": "PG__PORT",
    "REDIS_HOST": "REDIS__HOST",
    "REDIS_PASSWORD": "REDIS__PASSWORD",
    "BASIC_AUTH_USER": "systemadminuser__admin_username",
    "BASIC_AUTH_PASSWORD": "systemadminuser__admin_password",
    "PLATFORM_ADMIN_EMAIL": "systemadminuser__admin_email",
    "PLATFORM_ADMIN_PASSWORD": "systemadminuser__admin_password"
    }


def read_protected_secrets() -> None:
    """
    Reads secrets from the protected_secrets_dict using paths defined in
    protected_secrets_to_env_map and sets them as environment variables.
    """
    logger.info("Populating env from protected secrets")
    for env_var, secret_path in protected_secrets_to_env_map.items():
        logger.info("  %s -> %s", env_var, secret_path)
    for key in protected_secrets_to_env_map:
        logger.info("Getting Protected Secret %s", key)
        ps_path = protected_secrets_to_env_map[key]
        ps_path = ps_path.lower()
        nodes: list[str] = ps_path.split(sep="__")
        value = ""
        loc: Any = protected_secrets_dict
        for node in nodes:
            if node in loc:
                loc = loc[node]
                value = loc
            else:
                value = ""
                logger.info("Entry not found in Protected Secrets %s", node)
                break
        os.environ[key] = str(value)
        
    db_url_template= "postgresql+psycopg://${POSTGRES_USER}:${POSTGRES_PASSWORD}@${POSTGRES_HOST}:${POSTGRES_PORT}/${POSTGRES_DB}"
    os.environ["DATABASE_URL"] = os.path.expandvars(db_url_template)
    logger.info("DATABASE_URL updated")
    # Build Redis URL with TLS support
    # Use rediss:// scheme for TLS - the Redis client will use SSL_CERT_FILE from environment
    redis_url_template = "rediss://:${REDIS_PASSWORD}@${REDIS_HOST}:${REDIS_PORT}/0"
    redis_url = os.path.expandvars(redis_url_template)
    os.environ["REDIS_URL"] = redis_url
    
    # Log the TLS configuration
    ssl_cert_file = os.environ.get("SSL_CERT_FILE", "")
    if ssl_cert_file:
        logger.info("REDIS_URL updated (TLS enabled, SSL_CERT_FILE=%s)", ssl_cert_file)
    else:
        logger.info("REDIS_URL updated (TLS enabled, using system CA bundle)")
    
    # Debug: log the actual URL structure (without password)
    sanitized_url = redis_url.replace(os.environ.get("REDIS_PASSWORD", ""), "***") if os.environ.get("REDIS_PASSWORD") else redis_url
    logger.info("Constructed REDIS_URL: %s", sanitized_url)


if os.environ.get("USE_PROTECTED_SECRETS", "").lower() == "true":
    logger.info("Initializing Protected Secrets")
    read_protected_secrets()
else:
    logger.info("Protected Secrets Disabled")
