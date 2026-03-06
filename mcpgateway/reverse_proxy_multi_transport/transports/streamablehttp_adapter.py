# -*- coding: utf-8 -*-
"""Location: ./mcpgateway/reverse_proxy_multi_transport/transports/streamablehttp_adapter.py
Copyright 2025
SPDX-License-Identifier: Apache-2.0
Authors: Mihai Criveti

Streamable HTTP transport adapter for MCP servers.
Implements HTTP-based communication with MCP servers using httpx.
This is an alternative to stdio for servers that expose HTTP endpoints.
The MCP server can be local or remote - the proxy connects via HTTP.
"""

# Future
from __future__ import annotations

# Standard
import asyncio
import ssl
from typing import Awaitable, Callable, List, Optional

# Third-Party
import httpx

# First-Party
from mcpgateway.reverse_proxy_multi_transport.base import McpServerTransport
from mcpgateway.services.logging_service import LoggingService

# Initialize logging
logging_service = LoggingService()
LOGGER = logging_service.get_logger("mcpgateway.reverse_proxy_multi_transport.streamablehttp_adapter")


class StreamableHttpAdapter(McpServerTransport):
    """Transport adapter for Streamable HTTP MCP servers.

    Communicates with MCP servers via HTTP/2 streaming instead of stdio.
    The server can be local or remote - this adapter connects via HTTP.
    """

    def __init__(
        self,
        server_url: str,
        cert: Optional[str] = None,
        timeout: float = 90.0,
    ):
        """Initialize Streamable HTTP adapter.

        Args:
            server_url: MCP server HTTP URL (can be local or remote).
            cert: Optional CA certificate for SSL verification.
            timeout: Request timeout in seconds.
        """
        self.server_url = server_url.rstrip("/")
        self.cert = cert
        self.timeout = timeout

        self._client: Optional[httpx.AsyncClient] = None
        self._connected = False
        self._message_handlers: List[Callable[[str], Awaitable[None]]] = []
        self._receive_task: Optional[asyncio.Task[None]] = None
        # Streamable HTTP uses the main endpoint for communication
        self._endpoint_url = self.server_url
        # Session management (MCP protocol requirement)
        self._session_id: Optional[str] = None
        self._protocol_version: Optional[str] = None

    async def start(self) -> None:
        """Start HTTP client connection to MCP server."""
        if self._connected:
            return

        LOGGER.info(f"Connecting to MCP server via HTTP: {self.server_url}")

        # Configure SSL context only for HTTPS
        is_https = self.server_url.startswith("https://")
        ssl_context = None

        if is_https:
            if self.cert is not None:
                ssl_context = ssl.create_default_context(cadata=self.cert)
                ssl_context.check_hostname = True
                ssl_context.verify_mode = ssl.CERT_REQUIRED
            else:
                ssl_context = ssl.create_default_context()
                ssl_context.check_hostname = False
                ssl_context.verify_mode = ssl.CERT_NONE  # noqa: DUO122

        # Create HTTP client with HTTP/2 support
        self._client = httpx.AsyncClient(
            timeout=httpx.Timeout(self.timeout),
            http2=True,
            verify=ssl_context if is_https else False,
        )

        self._connected = True

        # Start receiving messages via SSE streaming
        self._receive_task = asyncio.create_task(self._receive_stream())

        LOGGER.info("HTTP connection to MCP server established")

    async def stop(self) -> None:
        """Stop HTTP client connection."""
        if not self._connected:
            return

        LOGGER.info("Disconnecting from MCP server")
        self._connected = False

        if self._receive_task:
            self._receive_task.cancel()
            try:
                await self._receive_task
            except asyncio.CancelledError:
                pass

        if self._client:
            await self._client.aclose()
            self._client = None

    async def send(self, message: str) -> None:
        """Send a message to the MCP server via HTTP POST and handle inline response."""
        if not self._connected or not self._client:
            raise RuntimeError("Not connected to MCP server")

        LOGGER.debug(f"→ HTTP: {message[:200]}...")
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json, text/event-stream",
        }

        # Add session headers if available (required after initialization)
        if self._session_id:
            headers["mcp-session-id"] = self._session_id
        if self._protocol_version:
            headers["mcp-protocol-version"] = self._protocol_version

        try:
            response = await self._client.post(
                self._endpoint_url,
                content=message,
                headers=headers,
            )
            response.raise_for_status()

            # Extract session ID from response headers (first request)
            session_id = response.headers.get("mcp-session-id")
            if session_id and not self._session_id:
                self._session_id = session_id
                LOGGER.info(f"Received session ID: {self._session_id}")
            LOGGER.info(f"HTTP POST successful: status={response.status_code}, content_length={len(response.content) if response.content else 0}")

            # Streamable HTTP returns responses inline - forward to handlers
            if response.content:
                response_text = response.text
                LOGGER.info(f"← HTTP response received: {response_text[:200]}... (total length: {len(response_text)})")
                LOGGER.info(f"Number of message handlers: {len(self._message_handlers)}")

                # Parse SSE format if present (streamable HTTP may return SSE-formatted responses)
                # SSE format: "event: message\ndata: {json}\n\n"
                json_message = response_text
                if response_text.startswith("event:") or response_text.startswith("data:"):
                    # Extract JSON from SSE format
                    lines = response_text.strip().split("\n")
                    for line in lines:
                        if line.startswith("data:"):
                            json_message = line[5:].strip()  # Remove "data:" prefix
                            LOGGER.info(f"Extracted JSON from SSE format: {json_message[:200]}...")
                            break

                # Extract protocol version from initialize response
                if not self._protocol_version:
                    try:
                        # Standard
                        import json

                        msg_data = json.loads(json_message)
                        if msg_data.get("result", {}).get("protocolVersion"):
                            self._protocol_version = msg_data["result"]["protocolVersion"]
                            LOGGER.info(f"Negotiated protocol version: {self._protocol_version}")
                    except Exception:
                        pass  # Not an initialize response or parsing failed

                # Notify all message handlers of the response
                for idx, handler in enumerate(self._message_handlers):
                    try:
                        LOGGER.info(f"Calling handler {idx+1}/{len(self._message_handlers)}")
                        await handler(json_message)
                        LOGGER.info(f"Handler {idx+1} completed successfully")
                    except Exception as handler_error:
                        LOGGER.error(f"Handler {idx+1} failed: {handler_error}", exc_info=True)
            else:
                LOGGER.warning("HTTP response has no content - this may indicate a problem with the MCP server")

        except httpx.HTTPError as e:
            LOGGER.error(f"HTTP send error: {e}")
            raise RuntimeError(f"Failed to send message: {e}") from e

    def add_message_handler(self, handler: Callable[[str], Awaitable[None]]) -> None:
        """Add a handler for messages from the MCP server."""
        self._message_handlers.append(handler)

    async def _receive_stream(self) -> None:
        """Monitor connection for streamable HTTP.

        Streamable HTTP protocol handles bidirectional communication through
        the main endpoint with proper Accept headers. Responses come back
        inline with POST requests, not via a separate SSE stream.
        """
        LOGGER.debug("Streamable HTTP uses inline responses, monitoring connection")

        # Keep the task alive to maintain connection state
        try:
            while self._connected:
                await asyncio.sleep(1)
        except asyncio.CancelledError:
            LOGGER.debug("Connection monitoring cancelled")
            raise


# Made with Bob
