# -*- coding: utf-8 -*-
"""Location: ./mcpgateway/reverse_proxy_multi_transport/client.py
Copyright 2025
SPDX-License-Identifier: Apache-2.0
Authors: Mihai Criveti

Refactored reverse proxy client using transport abstractions.
"""

# Future
from __future__ import annotations

# Standard
import asyncio
from typing import Any, Dict, Optional

# Third-Party
import orjson

# First-Party
from mcpgateway.reverse_proxy_multi_transport.base import (
    ConnectionState,
    GatewayTransport,
    McpServerTransport,
    MessageType,
)
from mcpgateway.services.logging_service import LoggingService

# Initialize logging
logging_service = LoggingService()
LOGGER = logging_service.get_logger("mcpgateway.reverse_proxy_multi_transport.client")

# Default configuration
DEFAULT_RECONNECT_DELAY = 1.0
DEFAULT_MAX_RETRIES = 0
DEFAULT_KEEPALIVE_INTERVAL = 30


class ReverseProxyClient:
    """Reverse proxy client using transport abstractions.

    Bridges MCP servers to remote gateways using pluggable transports.
    """

    def __init__(
        self,
        mcp_transport: McpServerTransport,
        gateway_transport: GatewayTransport,
        session_id: str,
        server_name: Optional[str] = None,
        server_description: Optional[str] = None,
        reconnect_delay: float = DEFAULT_RECONNECT_DELAY,
        max_retries: int = DEFAULT_MAX_RETRIES,
        keepalive_interval: float = DEFAULT_KEEPALIVE_INTERVAL,
    ):
        """Initialize reverse proxy client.

        Args:
            mcp_transport: Transport for MCP server communication.
            gateway_transport: Transport for gateway communication.
            session_id: Session identifier.
            server_name: Optional server name.
            server_description: Optional server description.
            reconnect_delay: Initial reconnection delay in seconds.
            max_retries: Maximum reconnection attempts (0 = infinite).
            keepalive_interval: Heartbeat interval in seconds.
        """
        self.mcp_transport = mcp_transport
        self.gateway_transport = gateway_transport
        self.session_id = session_id
        self.reconnect_delay = reconnect_delay
        self.max_retries = max_retries
        self.keepalive_interval = keepalive_interval

        self.server_name = server_name or f"reverse-proxy-{session_id[:8]}"
        self.description = server_description or "Reverse proxied MCP server"

        self.state = ConnectionState.DISCONNECTED
        self.retry_count = 0

        self._keepalive_task: Optional[asyncio.Task[None]] = None
        self._pending_requests: Dict[Any, asyncio.Future[Any]] = {}

        # Register message handlers
        self.mcp_transport.add_message_handler(self._handle_mcp_message)
        self.gateway_transport.add_message_handler(self._handle_gateway_message)

    async def connect(self) -> None:
        """Establish connection to gateway and MCP server."""
        if self.state != ConnectionState.DISCONNECTED:
            return

        self.state = ConnectionState.CONNECTING
        LOGGER.info("Establishing reverse proxy connection...")

        try:
            # Start MCP server transport
            LOGGER.info("Starting MCP server transport...")
            await self.mcp_transport.start()

            # Connect to gateway
            LOGGER.info("Connecting to gateway...")
            await self.gateway_transport.connect()

            self.state = ConnectionState.CONNECTED
            self.retry_count = 0

            # Register with gateway
            LOGGER.info("Registering with gateway...")
            await self._register()

            # Start keepalive
            self._keepalive_task = asyncio.create_task(self._keepalive_loop())

            LOGGER.info("Reverse proxy connected successfully")

        except Exception as e:
            LOGGER.error(f"Connection failed: {e}")
            self.state = ConnectionState.DISCONNECTED
            raise

    async def disconnect(self) -> None:
        """Disconnect from gateway and stop MCP server."""
        if self.state == ConnectionState.SHUTTING_DOWN:
            return

        self.state = ConnectionState.SHUTTING_DOWN
        LOGGER.info("Disconnecting reverse proxy...")

        if self._keepalive_task:
            self._keepalive_task.cancel()
            try:
                await self._keepalive_task
            except asyncio.CancelledError:
                pass

        # Send unregister message
        if await self.gateway_transport.is_connected():
            try:
                unregister = {
                    "type": MessageType.UNREGISTER.value,
                    "sessionId": self.session_id,
                }
                await self.gateway_transport.send(orjson.dumps(unregister).decode())
            except Exception:
                pass  # nosec B110

        await self.gateway_transport.disconnect()
        await self.mcp_transport.stop()

        self.state = ConnectionState.DISCONNECTED
        LOGGER.info("Reverse proxy disconnected")

    async def run_with_reconnect(self) -> None:
        """Run the reverse proxy with automatic reconnection."""
        while True:
            try:
                if self.state == ConnectionState.SHUTTING_DOWN:
                    break

                await self.connect()

                # Wait for disconnection by monitoring gateway connection
                while self.state == ConnectionState.CONNECTED:
                    # Check if gateway is still connected
                    if not await self.gateway_transport.is_connected():
                        LOGGER.warning("Gateway connection lost, triggering reconnection")
                        self.state = ConnectionState.DISCONNECTED
                        break
                    await asyncio.sleep(1)

                if self.state == ConnectionState.SHUTTING_DOWN:
                    break

            except Exception as e:
                LOGGER.error(f"Connection error: {e}")

            # Check retry limit
            self.retry_count += 1
            if self.max_retries > 0 and self.retry_count >= self.max_retries:
                LOGGER.error(f"Max retries ({self.max_retries}) exceeded")
                break

            # Calculate backoff delay
            delay = min(self.reconnect_delay * (2**self.retry_count), 60)
            LOGGER.info(f"Reconnecting in {delay}s (attempt {self.retry_count})")

            self.state = ConnectionState.RECONNECTING
            await asyncio.sleep(delay)
            self.state = ConnectionState.DISCONNECTED

    async def _register(self) -> None:
        """Register MCP server with gateway."""
        register_msg = {
            "type": MessageType.REGISTER.value,
            "sessionId": self.session_id,
            "server": {
                "name": self.server_name,
                "description": self.description,
                "protocol": "mcp",
            },
        }
        LOGGER.info(f"Sending registration: session={self.session_id}, name={self.server_name}")
        await self.gateway_transport.send(orjson.dumps(register_msg).decode())
        LOGGER.debug(f"Registration message sent: {register_msg}")

    async def _handle_mcp_message(self, message: str) -> None:
        """Handle message from MCP server."""
        try:
            LOGGER.debug(f"Handling MCP message: {message[:200]}...")
            data = orjson.loads(message)

            result = data.get("result")
            LOGGER.info(f"MCP response result: {result}, type: {type(result)}")
            request_id = data.get("id")

            if request_id and request_id in self._pending_requests:
                LOGGER.info(f"Request ID {request_id} found in pending requests")
                future = self._pending_requests.pop(request_id)
                LOGGER.info(f"Future retrieved: {future}")
                if not future.done():
                    LOGGER.info("Setting result on future")
                    # Create envelope with full payload for gateway
                    envelope = {
                        "type": MessageType.RESPONSE.value,
                        "sessionId": self.session_id,
                        "payload": data,
                    }
                    future.set_result(envelope)
            else:
                # Forward to gateway
                envelope = {
                    "type": (MessageType.RESPONSE.value if "id" in data else MessageType.NOTIFICATION.value),
                    "sessionId": self.session_id,
                    "payload": data,
                }
                LOGGER.debug(f"Forwarding MCP message to gateway: {envelope}")
                await self.gateway_transport.send(orjson.dumps(envelope).decode())

        except Exception as e:
            LOGGER.error(f"Error handling MCP message: {e}")

    async def _handle_gateway_message(self, message: str) -> None:
        """Handle message from gateway."""
        try:
            LOGGER.debug(f"Handling gateway message: {message[:200]}...")
            data = orjson.loads(message)
            msg_type = data.get("type")

            if msg_type == MessageType.REQUEST.value:
                LOGGER.info("=" * 80)
                LOGGER.info("[REVERSE_PROXY_CLIENT] Received REQUEST from gateway")
                payload = data.get("payload", {})
                authentication = data.get("authentication")
                auth_type = data.get("authType")

                LOGGER.info(f"[REVERSE_PROXY_CLIENT] Message keys: {list(data.keys())}")
                LOGGER.info(f"[REVERSE_PROXY_CLIENT] Authentication present: {authentication is not None}")

                if authentication:
                    LOGGER.info(f"[REVERSE_PROXY_CLIENT] ✓ Gateway provided authentication (type: {auth_type})")
                    LOGGER.info(f"[REVERSE_PROXY_CLIENT] ✓ Auth headers: {list(authentication.keys())}")
                    # Store authentication for this request, passing auth_type for proper formatting
                    self.mcp_transport.set_authentication(authentication, auth_type)
                else:
                    LOGGER.warning("[REVERSE_PROXY_CLIENT] ✗ NO authentication in gateway message")

                LOGGER.info(f"[REVERSE_PROXY_CLIENT] Gateway request payload: {payload}")
                LOGGER.info("=" * 80)
                await self.mcp_transport.send(orjson.dumps(payload).decode())

            elif msg_type == MessageType.HEARTBEAT.value:
                LOGGER.debug("Received HEARTBEAT from gateway, sending pong")
                pong = {
                    "type": MessageType.HEARTBEAT.value,
                    "sessionId": self.session_id,
                }
                await self.gateway_transport.send(orjson.dumps(pong).decode())

            elif msg_type == MessageType.ERROR.value:
                LOGGER.error(f"Gateway error: {data.get('message', 'Unknown')}")

            else:
                LOGGER.warning(f"Unknown message type from gateway: {msg_type}")

        except Exception as e:
            LOGGER.error(f"Error handling gateway message: {e}")

    async def _keepalive_loop(self) -> None:
        """Send periodic keepalive messages."""
        while self.state == ConnectionState.CONNECTED:
            await asyncio.sleep(self.keepalive_interval)

            heartbeat = {
                "type": MessageType.HEARTBEAT.value,
                "sessionId": self.session_id,
            }

            try:
                LOGGER.debug(f"Sending keepalive heartbeat for session {self.session_id}")
                await self.gateway_transport.send(orjson.dumps(heartbeat).decode())
            except Exception as e:
                LOGGER.warning(f"Keepalive failed: {e}")
                break


# Made with Bob
