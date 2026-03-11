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
DEFAULT_MCP_HEALTH_CHECK_TIMEOUT = 5.0
DEFAULT_MCP_HEALTH_CHECK_RETRY_INTERVAL = 10.0


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
        mcp_health_check_timeout: float = DEFAULT_MCP_HEALTH_CHECK_TIMEOUT,
        mcp_health_check_retry_interval: float = DEFAULT_MCP_HEALTH_CHECK_RETRY_INTERVAL,
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
            mcp_health_check_timeout: Timeout for MCP health check calls in seconds.
            mcp_health_check_retry_interval: Interval between MCP health check retries when server is down.
        """
        self.mcp_transport = mcp_transport
        self.gateway_transport = gateway_transport
        self.session_id = session_id
        self.reconnect_delay = reconnect_delay
        self.max_retries = max_retries
        self.keepalive_interval = keepalive_interval
        self.mcp_health_check_timeout = mcp_health_check_timeout
        self.mcp_health_check_retry_interval = mcp_health_check_retry_interval

        self.server_name = server_name or f"reverse-proxy-{session_id[:8]}"
        self.description = server_description or "Reverse proxied MCP server"

        self.state = ConnectionState.DISCONNECTED
        self.retry_count = 0
        self._mcp_server_healthy = True
        self._consecutive_mcp_failures = 0

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

            # Check MCP server health before connecting to gateway
            LOGGER.info("[CONNECT] Checking MCP server health before connecting to gateway...")
            mcp_healthy = await self._check_mcp_server_health()

            if not mcp_healthy:
                LOGGER.warning("[CONNECT] MCP server is not reachable, aborting gateway connection | " "Will retry connection later")
                self.state = ConnectionState.DISCONNECTED
                raise RuntimeError("MCP server is not reachable")

            LOGGER.info("[CONNECT] MCP server is healthy, proceeding with gateway connection")

            # Connect to gateway
            LOGGER.info("Connecting to gateway...")
            await self.gateway_transport.connect()

            self.state = ConnectionState.CONNECTED
            self.retry_count = 0

            # Register with gateway
            LOGGER.info("Registering with gateway...")
            await self._register()

            # Start keepalive (only if not already running)
            if self._keepalive_task is None or self._keepalive_task.done():
                LOGGER.info("Starting new keepalive task")
                self._keepalive_task = asyncio.create_task(self._keepalive_loop())
            else:
                LOGGER.warning("Keepalive task already running, not starting a new one")

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

            # Before reconnecting to gateway, verify MCP server is reachable
            LOGGER.info("[RECONNECT] Checking MCP server health before reconnecting to gateway...")
            mcp_healthy = await self._check_mcp_server_health()

            if not mcp_healthy:
                LOGGER.warning(f"[RECONNECT] MCP server still unreachable, delaying gateway reconnection | " f"Will retry in {self.mcp_health_check_retry_interval}s")
                # Wait before checking again
                await asyncio.sleep(self.mcp_health_check_retry_interval)
                # Don't increment retry count for MCP health check failures
                self.retry_count -= 1
                continue

            LOGGER.info("[RECONNECT] MCP server is healthy, proceeding with gateway reconnection")
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

            # Check if this is a health check response (don't forward to gateway)
            is_health_check = request_id and str(request_id).startswith("health_check_")

            if request_id and request_id in self._pending_requests:
                LOGGER.info(f"Request ID {request_id} found in pending requests")
                future = self._pending_requests.pop(request_id)
                LOGGER.info(f"Future retrieved: {future}")
                if not future.done():
                    LOGGER.info("Setting result on future")
                    # For health checks, just resolve the future (don't forward to gateway)
                    # For normal requests, create envelope and resolve future
                    if is_health_check:
                        future.set_result(data)
                    else:
                        envelope = {
                            "type": MessageType.RESPONSE.value,
                            "sessionId": self.session_id,
                            "payload": data,
                        }
                        future.set_result(envelope)
            elif not is_health_check:
                # Forward to gateway (but not health check responses)
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
                # Gateway heartbeat is just an acknowledgment, no pong needed
                LOGGER.debug("Received HEARTBEAT acknowledgment from gateway")

            elif msg_type == MessageType.ERROR.value:
                LOGGER.error(f"Gateway error: {data.get('message', 'Unknown')}")

            else:
                LOGGER.warning(f"Unknown message type from gateway: {msg_type}")

        except Exception as e:
            LOGGER.error(f"Error handling gateway message: {e}")

    async def _check_mcp_server_health(self) -> bool:
        """Check MCP server health by calling tools/list.

        If the MCP transport is not connected, attempts to restart it first.
        This allows detection of MCP server recovery after downtime.

        Returns:
            True if MCP server is healthy, False otherwise.
        """
        request_id = None
        try:
            # Check if MCP transport is connected
            is_connected = getattr(self.mcp_transport, "_connected", True)
            LOGGER.info(f"[MCP_HEALTH] Starting health check | is_connected={is_connected}")

            # If not connected, try to restart the transport
            if not is_connected:
                LOGGER.info("[MCP_HEALTH] MCP transport not connected, attempting restart")
                try:
                    await self.mcp_transport.stop()
                    await self.mcp_transport.start()
                    LOGGER.info("[MCP_HEALTH] MCP transport restarted successfully")
                    # Re-check connection status after restart
                    is_connected = getattr(self.mcp_transport, "_connected", False)
                    LOGGER.info(f"[MCP_HEALTH] After restart: is_connected={is_connected}")
                except Exception as e:
                    LOGGER.warning(f"[MCP_HEALTH] Failed to restart MCP transport: {e}")
                    return False

            # Check if MCP transport is ready (has message endpoint for SSE/HTTP transports)
            has_endpoint_attr = hasattr(self.mcp_transport, "_message_endpoint")
            endpoint_value = getattr(self.mcp_transport, "_message_endpoint", None) if has_endpoint_attr else None
            LOGGER.info(f"[MCP_HEALTH] Endpoint check | has_attr={has_endpoint_attr}, value={endpoint_value}")

            if has_endpoint_attr and not endpoint_value:
                LOGGER.info("[MCP_HEALTH] MCP transport not ready yet (waiting for endpoint), skipping health check")
                return False

            # Create tools/list request
            request_id = f"health_check_{asyncio.get_event_loop().time()}"
            health_check_request = {
                "jsonrpc": "2.0",
                "id": request_id,
                "method": "tools/list",
            }

            # Create future to wait for response
            future: asyncio.Future[Dict[str, Any]] = asyncio.Future()
            self._pending_requests[request_id] = future

            # Send health check request to MCP server
            LOGGER.info(f"[MCP_HEALTH] Sending tools/list health check to MCP server | request_id={request_id}")
            await self.mcp_transport.send(orjson.dumps(health_check_request).decode())

            # Wait for response with timeout
            try:
                _ = await asyncio.wait_for(future, timeout=self.mcp_health_check_timeout)
                LOGGER.info("[MCP_HEALTH] MCP server responded successfully ✓")
                return True
            except asyncio.TimeoutError:
                LOGGER.warning(f"[MCP_HEALTH] MCP server health check timed out after {self.mcp_health_check_timeout}s")
                return False

        except Exception as e:
            LOGGER.error(f"[MCP_HEALTH] MCP server health check failed: {e}", exc_info=True)
            return False
        finally:
            # Clean up pending request if it wasn't resolved
            if request_id is not None:
                self._pending_requests.pop(request_id, None)

    async def _keepalive_loop(self) -> None:
        """Send periodic heartbeat messages, conditional on MCP server health.

        This implements the MCP-based heartbeat strategy:
        1. Check MCP server health by calling tools/list
        2. Only send heartbeat to gateway if MCP server is healthy
        3. If MCP server is unhealthy, skip heartbeat (gateway will detect timeout)
        4. Continue checking MCP server health and reconnect when it recovers
        """
        LOGGER.info(
            f"[HEARTBEAT_LOOP] Session {self.session_id[:8]}... | "
            f"Starting keepalive loop | Interval: {self.keepalive_interval}s | "
            f"MCP health check timeout: {self.mcp_health_check_timeout}s | "
            f"MCP retry interval: {self.mcp_health_check_retry_interval}s"
        )

        heartbeat_count = 0

        while self.state == ConnectionState.CONNECTED:
            await asyncio.sleep(self.keepalive_interval)

            # Check MCP server health before sending heartbeat
            LOGGER.debug(f"[HEARTBEAT_LOOP] Session {self.session_id[:8]}... | " f"Checking MCP server health (heartbeat #{heartbeat_count + 1})")
            mcp_healthy = await self._check_mcp_server_health()

            if mcp_healthy:
                # MCP server is healthy - send heartbeat to gateway
                if not self._mcp_server_healthy:
                    # MCP server recovered
                    LOGGER.info(
                        f"[HEARTBEAT_RECOVERY] Session {self.session_id[:8]}... | " f"MCP server recovered after {self._consecutive_mcp_failures} failures | " f"Resuming heartbeats to gateway"
                    )
                    self._mcp_server_healthy = True
                    self._consecutive_mcp_failures = 0

                    # Reconnect to gateway if we were disconnected
                    if not await self.gateway_transport.is_connected():
                        LOGGER.info(f"[HEARTBEAT_RECOVERY] Session {self.session_id[:8]}... | " f"Reconnecting to gateway after MCP server recovery")
                        try:
                            await self.gateway_transport.connect()
                            await self._register()
                        except Exception as e:
                            LOGGER.error(f"[HEARTBEAT_RECOVERY] Session {self.session_id[:8]}... | " f"Failed to reconnect to gateway: {e}")
                            continue

                # Send heartbeat
                heartbeat = {
                    "type": MessageType.HEARTBEAT.value,
                    "sessionId": self.session_id,
                }

                try:
                    heartbeat_count += 1
                    LOGGER.info(f"[HEARTBEAT_SENT] Session {self.session_id[:8]}... | " f"Sending heartbeat #{heartbeat_count} to gateway | " f"MCP server: healthy | Consecutive failures: 0")
                    await self.gateway_transport.send(orjson.dumps(heartbeat).decode())
                except Exception as e:
                    LOGGER.warning(f"[HEARTBEAT_ERROR] Session {self.session_id[:8]}... | " f"Failed to send heartbeat to gateway: {e}")
                    break

            else:
                # MCP server is unhealthy - skip heartbeat
                self._consecutive_mcp_failures += 1

                if self._mcp_server_healthy:
                    # First failure detected
                    LOGGER.warning(
                        f"[HEARTBEAT_SKIPPED] Session {self.session_id[:8]}... | "
                        f"MCP server unhealthy - skipping heartbeat #{heartbeat_count + 1} | "
                        f"Gateway will detect timeout and mark unreachable | "
                        f"Consecutive failures: {self._consecutive_mcp_failures}"
                    )
                    self._mcp_server_healthy = False
                else:
                    # Ongoing failure
                    LOGGER.info(
                        f"[HEARTBEAT_SKIPPED] Session {self.session_id[:8]}... | "
                        f"MCP server still unhealthy - skipping heartbeat #{heartbeat_count + 1} | "
                        f"Consecutive failures: {self._consecutive_mcp_failures}"
                    )

                # Continue checking MCP server with shorter interval during outage
                # This allows faster recovery detection
                # Note: We already slept for keepalive_interval at the start of the loop,
                # so we don't need to sleep again here. The next iteration will sleep
                # for keepalive_interval before checking health again.
                LOGGER.debug(f"[HEARTBEAT_RETRY] Session {self.session_id[:8]}... | " f"Will retry MCP health check in {self.keepalive_interval}s (next loop iteration)")

        LOGGER.info(f"[HEARTBEAT_LOOP] Session {self.session_id[:8]}... | " f"Keepalive loop ended | Total heartbeats sent: {heartbeat_count} | " f"Final state: {self.state.value}")


# Made with Bob
