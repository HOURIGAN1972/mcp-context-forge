# -*- coding: utf-8 -*-
"""Location: ./mcp_reverse_proxy/cli.py
Copyright 2025
SPDX-License-Identifier: Apache-2.0
Authors: Mihai Criveti

CLI entry point and transport factory for reverse proxy.
"""

# Future
from __future__ import annotations

# Standard
import argparse
import asyncio
from contextlib import suppress
import logging
import os
import signal
import sys
from typing import List, Optional

# Third-Party
import orjson

try:
    # Third-Party
    import yaml
except ImportError:
    yaml = None  # type: ignore[assignment]

# First-Party
from mcp_reverse_proxy.base import GatewayTransport, McpServerTransport
from mcp_reverse_proxy.client import (
    DEFAULT_MCP_HEALTH_CHECK_RETRY_INTERVAL,
    DEFAULT_MCP_HEALTH_CHECK_TIMEOUT,
    ReverseProxyClient,
    StdioSubprocessTerminated,
)
from mcp_reverse_proxy.transports.sse_adapter import SseAdapter
from mcp_reverse_proxy.transports.stdio_adapter import StdioAdapter
from mcp_reverse_proxy.transports.streamablehttp_adapter import (
    StreamableHttpAdapter,
)
from mcp_reverse_proxy.transports.websocket_adapter import WebSocketAdapter
from mcp_reverse_proxy.logging_config import LoggingService

# Initialize logging
logging_service = LoggingService()
LOGGER = logging_service.get_logger("mcp_reverse_proxy.cli")

# Environment variable names
ENV_GATEWAY = "REVERSE_PROXY_GATEWAY"
ENV_TOKEN = "REVERSE_PROXY_TOKEN"  # nosec B105

# Defaults
DEFAULT_RECONNECT_DELAY = 1.0
DEFAULT_MAX_RETRIES = 0
# CRITICAL: Must be less than gateway's MCPGATEWAY_REVERSE_PROXY_HEARTBEAT_TIMEOUT
# For gateway with 5s timeout, use 2s. For default 90s timeout, can use 30s.
DEFAULT_KEEPALIVE_INTERVAL = 2


def create_mcp_transport(
    local_stdio: Optional[str] = None,
    local_streamable_http: Optional[str] = None,
    local_sse: Optional[str] = None,
    cert: Optional[str] = None,
) -> McpServerTransport:
    """Create MCP server transport based on configuration.

    Args:
        local_stdio: Stdio command for MCP server.
        local_streamable_http: Streamable HTTP URL for MCP server (http(s)://.../mcp).
        local_sse: SSE URL for MCP server (http(s)://.../sse).
        cert: Optional CA certificate.

    Returns:
        Configured MCP server transport.

    Raises:
        ValueError: If no transport is specified or multiple are specified.
    """
    transports = [local_stdio, local_streamable_http, local_sse]
    specified = [t for t in transports if t is not None]

    if len(specified) == 0:
        raise ValueError("Must specify one MCP server transport " "(--local-stdio, --local-streamable-http, or --local-sse)")
    if len(specified) > 1:
        raise ValueError("Can only specify one MCP server transport")

    if local_stdio:
        LOGGER.info(f"Using stdio transport: {local_stdio}")
        return StdioAdapter(local_stdio)
    elif local_streamable_http:
        LOGGER.info(f"Using Streamable HTTP transport: {local_streamable_http}")
        return StreamableHttpAdapter(local_streamable_http, cert=cert)
    elif local_sse:
        LOGGER.info(f"Using SSE transport: {local_sse}")
        return SseAdapter(local_sse, cert=cert)

    raise ValueError("No valid transport specified")


def create_gateway_transport(
    gateway_url: str,
    session_id: str,
    token: Optional[str] = None,
    cert: Optional[str] = None,
) -> GatewayTransport:
    """Create gateway transport (currently WebSocket only).

    Args:
        gateway_url: Gateway URL.
        session_id: Session identifier.
        token: Optional bearer token.
        cert: Optional CA certificate.

    Returns:
        Configured gateway transport.
    """
    LOGGER.info(f"Using WebSocket gateway transport: {gateway_url}")
    return WebSocketAdapter(gateway_url, session_id, token=token, cert=cert)


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        prog="mcp-reverse-proxy",
        description="Bridge MCP servers to remote gateways",
    )

    # MCP server transport options
    mcp_group = parser.add_argument_group("MCP Server Transport")
    mcp_group.add_argument(
        "--local-stdio",
        help="MCP server command to run via stdio",
    )
    mcp_group.add_argument(
        "--local-streamable-http",
        help="MCP server Streamable HTTP URL (e.g., https://server.com/mcp)",
    )
    mcp_group.add_argument(
        "--local-sse",
        help="MCP server SSE URL (e.g., https://server.com/sse)",
    )

    # Gateway options
    gateway_group = parser.add_argument_group("Gateway Connection")
    gateway_group.add_argument(
        "--gateway",
        help=f"Gateway URL (or use {ENV_GATEWAY} env var)",
    )
    gateway_group.add_argument(
        "--token",
        help=f"Bearer token for authentication (or use {ENV_TOKEN} env var)",
    )
    gateway_group.add_argument(
        "--server-id",
        help="Session identifier (auto-generated if not provided)",
    )
    gateway_group.add_argument(
        "--server-name",
        help="Server name for registration",
    )
    gateway_group.add_argument(
        "--server-description",
        help="Server description for registration",
    )
    gateway_group.add_argument(
        "--cert",
        help="CA certificate for SSL verification",
    )

    # Connection options
    conn_group = parser.add_argument_group("Connection Options")
    conn_group.add_argument(
        "--reconnect-delay",
        type=float,
        default=DEFAULT_RECONNECT_DELAY,
        help=f"Initial reconnection delay in seconds (default: {DEFAULT_RECONNECT_DELAY})",
    )
    conn_group.add_argument(
        "--max-retries",
        type=int,
        default=DEFAULT_MAX_RETRIES,
        help=f"Maximum reconnection attempts, 0=infinite (default: {DEFAULT_MAX_RETRIES})",
    )
    conn_group.add_argument(
        "--keepalive",
        type=int,
        default=DEFAULT_KEEPALIVE_INTERVAL,
        help=f"Keepalive interval in seconds (default: {DEFAULT_KEEPALIVE_INTERVAL})",
    )
    conn_group.add_argument(
        "--mcp-health-check-timeout",
        type=float,
        default=DEFAULT_MCP_HEALTH_CHECK_TIMEOUT,
        help=f"Timeout for MCP health check calls in seconds (default: {DEFAULT_MCP_HEALTH_CHECK_TIMEOUT})",
    )
    conn_group.add_argument(
        "--mcp-health-check-retry-interval",
        type=float,
        default=DEFAULT_MCP_HEALTH_CHECK_RETRY_INTERVAL,
        help=f"Interval between MCP health check retries when server is down (default: {DEFAULT_MCP_HEALTH_CHECK_RETRY_INTERVAL})",
    )

    # Configuration file
    parser.add_argument(
        "--config",
        help="Configuration file (YAML or JSON)",
    )

    # Logging
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        default="INFO",
        help="Log level (default: INFO)",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose logging (same as --log-level DEBUG)",
    )

    args = parser.parse_args(argv)

    # Load configuration file if provided
    if args.config:
        try:
            config_path = os.path.expanduser(args.config)
            with open(config_path, "r", encoding="utf-8") as f:
                # Determine format by file extension
                if config_path.endswith((".yaml", ".yml")):
                    if not yaml:
                        parser.error("PyYAML package required for YAML configuration file support")
                    config = yaml.safe_load(f)
                else:
                    config = orjson.loads(f.read())

            # Validate config is a dict
            if not isinstance(config, dict):
                parser.error("Configuration file must contain a JSON/YAML object at the top level")

            # Merge configuration (command line takes precedence)
            for key, value in config.items():
                key_normalized = key.replace("-", "_")
                if not hasattr(args, key_normalized) or getattr(args, key_normalized) is None:
                    setattr(args, key_normalized, value)
        except FileNotFoundError:
            parser.error(f"Configuration file not found: {config_path}")
        except Exception as e:
            parser.error(f"Error loading configuration file: {e}")

    # Handle verbose flag
    if args.verbose:
        args.log_level = "DEBUG"

    # Get gateway from environment if not provided
    if not args.gateway:
        args.gateway = os.getenv(ENV_GATEWAY)
        if not args.gateway:
            parser.error(f"--gateway or {ENV_GATEWAY} environment variable required")

    # Get token from environment if not provided
    if not args.token:
        args.token = os.getenv(ENV_TOKEN)

    # Generate session ID if not provided
    if not args.server_id:
        # Standard
        import uuid

        args.server_id = str(uuid.uuid4())

    return args


async def main(argv: Optional[List[str]] = None) -> None:
    """Main entry point for reverse proxy."""
    args = parse_args(argv)

    # Configure logging using LoggingService to respect JSON format settings
    # This ensures the reverse proxy CLI uses the same logging format as the main gateway
    root_logger = logging.getLogger()
    root_logger.handlers.clear()

    # Set log level from args
    log_level = getattr(logging, args.log_level)
    root_logger.setLevel(log_level)

    # Use JSON formatter if LOG_FORMAT=json, otherwise use text formatter
    log_format = os.getenv("LOG_FORMAT", "text").lower()
    if log_format == "json":
        # First-Party
        from mcp_reverse_proxy.logging_config import json_formatter

        console_handler = logging.StreamHandler(sys.stderr)
        console_handler.setFormatter(json_formatter)
    else:
        # First-Party
        from mcp_reverse_proxy.logging_config import text_formatter

        console_handler = logging.StreamHandler(sys.stderr)
        console_handler.setFormatter(text_formatter)

    console_handler.setLevel(log_level)
    root_logger.addHandler(console_handler)

    # Create transports
    mcp_transport = create_mcp_transport(
        local_stdio=args.local_stdio,
        local_streamable_http=args.local_streamable_http,
        local_sse=args.local_sse,
        cert=args.cert,
    )

    gateway_transport = create_gateway_transport(
        gateway_url=args.gateway,
        session_id=args.server_id,
        token=args.token,
        cert=args.cert,
    )

    # Create client
    client = ReverseProxyClient(
        mcp_transport=mcp_transport,
        gateway_transport=gateway_transport,
        session_id=args.server_id,
        server_name=args.server_name,
        server_description=args.server_description,
        reconnect_delay=args.reconnect_delay,
        max_retries=args.max_retries,
        keepalive_interval=args.keepalive,
        mcp_health_check_timeout=args.mcp_health_check_timeout,
        mcp_health_check_retry_interval=args.mcp_health_check_retry_interval,
    )

    # Handle shutdown signals
    shutdown_event = asyncio.Event()

    def signal_handler(*_args: object) -> None:
        """Handle shutdown signals gracefully."""
        LOGGER.info("Shutdown signal received")
        shutdown_event.set()

    # Register signal handlers
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        with suppress(NotImplementedError):
            loop.add_signal_handler(sig, signal_handler)

    # Run client with reconnection
    client_task = asyncio.create_task(client.run_with_reconnect())

    try:
        # Wait for either shutdown signal or client task to complete
        done, pending = await asyncio.wait([asyncio.create_task(shutdown_event.wait()), client_task], return_when=asyncio.FIRST_COMPLETED)

        # Check if client_task completed with an exception
        if client_task in done:
            try:
                client_task.result()  # This will re-raise any exception
            except StdioSubprocessTerminated as e:
                LOGGER.error(f"Client task failed with StdioSubprocessTerminated: {e}")
                raise  # Re-raise to trigger clean exit
            except Exception as e:
                LOGGER.error(f"Client task failed with unexpected exception: {e}", exc_info=True)
                raise
    finally:
        await client.disconnect()
        client_task.cancel()
        with suppress(asyncio.CancelledError):
            await client_task


def run() -> None:
    """Console script entry point."""
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        LOGGER.info("Shutdown complete")
        sys.exit(0)
    except Exception as e:
        # Check if this is a StdioSubprocessTerminated exception
        # First-Party
        from mcp_reverse_proxy.client import StdioSubprocessTerminated

        if isinstance(e, StdioSubprocessTerminated):
            LOGGER.error(f"Stdio subprocess terminated: {e}")
            LOGGER.info("Exiting so process supervisor can restart with fresh subprocess")
            sys.exit(1)
        else:
            LOGGER.error(f"Error: {e}", exc_info=True)
            sys.exit(1)


if __name__ == "__main__":
    run()
