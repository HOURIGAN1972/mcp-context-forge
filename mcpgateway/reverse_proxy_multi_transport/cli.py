# -*- coding: utf-8 -*-
"""Location: ./mcpgateway/reverse_proxy_multi_transport/cli.py
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

# First-Party
from mcpgateway.reverse_proxy_multi_transport.base import GatewayTransport, McpServerTransport
from mcpgateway.reverse_proxy_multi_transport.client import ReverseProxyClient
from mcpgateway.reverse_proxy_multi_transport.transports.stdio_adapter import StdioAdapter
from mcpgateway.reverse_proxy_multi_transport.transports.streamablehttp_adapter import (
    StreamableHttpAdapter,
)
from mcpgateway.reverse_proxy_multi_transport.transports.websocket_adapter import WebSocketAdapter
from mcpgateway.services.logging_service import LoggingService

# Initialize logging
logging_service = LoggingService()
LOGGER = logging_service.get_logger("mcpgateway.reverse_proxy_multi_transport.cli")

# Environment variable names
ENV_GATEWAY = "REVERSE_PROXY_GATEWAY"
ENV_TOKEN = "REVERSE_PROXY_TOKEN"  # nosec B105

# Defaults
DEFAULT_RECONNECT_DELAY = 1.0
DEFAULT_MAX_RETRIES = 0
DEFAULT_KEEPALIVE_INTERVAL = 30


def create_mcp_transport(
    local_stdio: Optional[str] = None,
    streamable_http: Optional[str] = None,
    cert: Optional[str] = None,
) -> McpServerTransport:
    """Create MCP server transport based on configuration.

    Args:
        local_stdio: Stdio command for MCP server.
        streamable_http: Streamable HTTP URL for MCP server (http(s)://.../mcp).
        cert: Optional CA certificate.

    Returns:
        Configured MCP server transport.

    Raises:
        ValueError: If no transport is specified or multiple are specified.
    """
    transports = [local_stdio, streamable_http]
    specified = [t for t in transports if t is not None]

    if len(specified) == 0:
        raise ValueError("Must specify one MCP server transport " "(--local-stdio or --streamable-http)")
    if len(specified) > 1:
        raise ValueError("Can only specify one MCP server transport")

    if local_stdio:
        LOGGER.info(f"Using stdio transport: {local_stdio}")
        return StdioAdapter(local_stdio)
    elif streamable_http:
        LOGGER.info(f"Using Streamable HTTP transport: {streamable_http}")
        return StreamableHttpAdapter(streamable_http, cert=cert)

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
        prog="mcpgateway.reverse_proxy_multi_transport",
        description="Bridge MCP servers to remote gateways",
    )

    # MCP server transport options
    mcp_group = parser.add_argument_group("MCP Server Transport")
    mcp_group.add_argument(
        "--local-stdio",
        help="MCP server command to run via stdio",
    )
    mcp_group.add_argument(
        "--streamable-http",
        help="MCP server Streamable HTTP URL (e.g., https://server.com/mcp)",
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

    # Configuration file
    parser.add_argument(
        "--config",
        help="Configuration file (JSON format)",
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
            with open(args.config, "r", encoding="utf-8") as f:
                config = orjson.loads(f.read())

            # Merge configuration (command line takes precedence)
            for key, value in config.items():
                key_normalized = key.replace("-", "_")
                if not hasattr(args, key_normalized) or getattr(args, key_normalized) is None:
                    setattr(args, key_normalized, value)
        except FileNotFoundError:
            parser.error(f"Configuration file not found: {args.config}")
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

    # Configure logging
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
        stream=sys.stderr,
    )

    # Create transports
    mcp_transport = create_mcp_transport(
        local_stdio=args.local_stdio,
        streamable_http=args.streamable_http,
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
        await shutdown_event.wait()
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
        print("\nShutdown complete", file=sys.stderr)
        sys.exit(0)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    run()

# Made with Bob
