# -*- coding: utf-8 -*-
"""Location: ./mcpgateway/reverse_proxy_multi_transport/transports/sse_adapter.py
Copyright 2025
SPDX-License-Identifier: Apache-2.0
Authors: Mihai Criveti

SSE transport adapter for MCP servers (stub for future implementation).
"""

# Future
from __future__ import annotations

# Standard
from typing import Awaitable, Callable, Optional

# First-Party
from mcpgateway.reverse_proxy_multi_transport.base import McpServerTransport
from mcpgateway.services.logging_service import LoggingService

# Initialize logging
logging_service = LoggingService()
LOGGER = logging_service.get_logger("mcpgateway.reverse_proxy_multi_transport.sse_adapter")


class SseAdapter(McpServerTransport):
    """Transport adapter for SSE-based MCP servers (not yet implemented).

    This is a placeholder for future SSE transport support.
    """

    def __init__(self, server_url: str, cert: Optional[str] = None):
        """Initialize SSE adapter.

        Args:
            server_url: MCP server SSE URL.
            cert: Optional CA certificate for SSL verification.
        """
        self.server_url = server_url
        self.cert = cert

    async def start(self) -> None:
        """Start SSE connection (not implemented)."""
        raise NotImplementedError("SSE transport is not yet implemented. " "Use stdio (--local-stdio) or Streamable HTTP (--local-http) instead.")

    async def stop(self) -> None:
        """Stop SSE connection (not implemented)."""

    async def send(self, message: str) -> None:
        """Send message via SSE (not implemented)."""
        raise NotImplementedError("SSE transport is not yet implemented")

    def add_message_handler(self, handler: Callable[[str], Awaitable[None]]) -> None:
        """Add message handler (not implemented)."""


# Made with Bob
