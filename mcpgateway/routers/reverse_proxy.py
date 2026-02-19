# -*- coding: utf-8 -*-
"""Location: ./mcpgateway/routers/reverse_proxy.py
Copyright 2025
SPDX-License-Identifier: Apache-2.0
Authors: Mihai Criveti

FastAPI router for handling reverse proxy connections.

This module provides WebSocket and SSE endpoints for reverse proxy clients
to connect and tunnel their local MCP servers through the gateway.
"""

# Standard
import asyncio
from functools import partial
from datetime import datetime, timezone
from typing import Any, Dict, Optional
import uuid
from urllib.parse import urlparse

# Third-Party
from fastapi import APIRouter, Depends, HTTPException, Request, status, WebSocket, WebSocketDisconnect
from fastapi.responses import StreamingResponse
import orjson
from sqlalchemy.orm import Session

# First-Party
from mcpgateway.config import settings
from mcpgateway.db import get_db
from mcpgateway.schemas import GatewayCreate, TransportType, ServerCreate
from mcpgateway.services.server_service import ServerService, ServerNameConflictError
from mcpgateway.services.logging_service import LoggingService
from mcpgateway.utils.verify_credentials import require_auth, verify_jwt_token
from mcpgateway.utils.verify_credentials import require_auth
from mcpgateway.config import Settings

# Initialize logging
logging_service = LoggingService()
LOGGER = logging_service.get_logger("mcpgateway.routers.reverse_proxy")

router = APIRouter(prefix="/reverse-proxy", tags=["reverse-proxy"])


class ReverseProxySession:
    """Manages a reverse proxy session."""

    def __init__(self, session_id: str, websocket: WebSocket, user: Optional[str | dict] = None):
        """Initialize reverse proxy session.

        Args:
            session_id: Unique session identifier.
            websocket: WebSocket connection.
            user: Authenticated user info (if any).
        """
        self.session_id = session_id
        self.websocket = websocket
        self.user = user
        self.server_info: Dict[str, Any] = {}
        self.connected_at = datetime.now(tz=timezone.utc)
        self.last_activity = datetime.now(tz=timezone.utc)
        self.message_count = 0
        self.bytes_transferred = 0

    async def send_message(self, message: Dict[str, Any]) -> None:
        """Send message to the client.

        Args:
            message: Message dictionary to send.
        """
        data = orjson.dumps(message).decode()
        await self.websocket.send_text(data)
        self.bytes_transferred += len(data)
        self.last_activity = datetime.now(tz=timezone.utc)

    async def receive_message(self) -> Dict[str, Any]:
        """Receive message from the client.

        Returns:
            Parsed message dictionary.
        """
        data = await self.websocket.receive_text()
        self.bytes_transferred += len(data)
        self.message_count += 1
        self.last_activity = datetime.now(tz=timezone.utc)
        return orjson.loads(data)


class ReverseProxyManager:
    """Manages all reverse proxy sessions."""

    def __init__(self):
        """Initialize the manager."""
        self.sessions: Dict[str, ReverseProxySession] = {}
        self._lock = asyncio.Lock()

    async def add_session(self, session: ReverseProxySession) -> None:
        """Add a new session.

        Args:
            session: Session to add.
        """
        LOGGER.info(f"add_session called {session.session_id}")

        async with self._lock:
            self.sessions[session.session_id] = session
            LOGGER.info(f"Added reverse proxy session: {session.session_id}")

        LOGGER.info(f"add_session Now have len(self.sessions): {len(self.sessions)}")


    async def remove_session(self, session_id: str) -> None:
        """Remove a session.

        Args:
            session_id: Session ID to remove.
        """

        LOGGER.info(f"Removed reverse proxy session: {session_id}")

        async with self._lock:
            if session_id in self.sessions:
                del self.sessions[session_id]
                LOGGER.info(f"Removed reverse proxy session: {session_id}")

        LOGGER.info(f"remove_session Now have len(self.sessions): {len(self.sessions)}")


    async def get_session(self, session_id: str) -> Optional[ReverseProxySession]:
        """Get a session by ID.

        Args:
            session_id: Session ID to get.

        Returns:
            Session if found, None otherwise.
        """

        """Get a session safely."""
        async with self._lock:
            return self.sessions.get(session_id)

    async def list_sessions(self) -> list[Dict[str, Any]]:
        """List all active sessions.

        Returns:
            List of session information dictionaries.

        Examples:
            >>> from fastapi import WebSocket
            >>> manager = ReverseProxyManager()
            >>> sessions = manager.list_sessions()
            >>> sessions
            []
            >>> isinstance(sessions, list)
            True
        """
        async with self._lock:
            LOGGER.info(
                f"list_sessions manager {hex(id(self))} sessions {hex(id(self.sessions))} sessions.values {self.sessions.values()}")

            # Return a shallow copy to prevent external mutation
            return [
                    {
                        "session_id": session.session_id,
                        "server_info": session.server_info,
                        "connected_at": session.connected_at.isoformat(),
                        "last_activity": session.last_activity.isoformat(),
                        "message_count": session.message_count,
                        "bytes_transferred": session.bytes_transferred,
                        "user": session.user if isinstance(session.user, str) else session.user.get("sub") if isinstance(session.user, dict) else None,
                    }
                    for session in self.sessions.values()
                ]


# Global manager instance
manager = ReverseProxyManager()
pending_responses = {}



def extract_session_id_from_url( url: str) -> str:
    LOGGER.info(f"extract_session_id_from_url {url}")
    path_parts = urlparse(url).path.strip("/").split("/")
    try:
        # Find the index of "sessions" and return the next element
        session_index = path_parts.index("sessions")
        return path_parts[session_index + 1]
    except (ValueError, IndexError):
        raise ValueError("Invalid URL format — could not extract session ID.")



async def forward_request_to_session(
    session_id: str,
    mcp_request: Dict[str, Any],
    authentication: Optional[Dict[str, str]] = None,
    auth_type: Optional[str] = None,
):
    """Forward an MCP request to a reverse proxy session.

    Args:
        session_id: Session ID to forward the request to.
        mcp_request: MCP request dictionary to forward.
        authentication: Optional dictionary containing authentication headers.
        auth_type: Type of authentication being used (for logging/debugging).

    Returns:
        Response from the proxied server, or None for notifications.

    Raises:
        ValueError: If session is not found.
        asyncio.TimeoutError: If request times out.
    """
    LOGGER.info(f"**** forward_request_to_session session_id {session_id}  mcp_request {mcp_request}")
    if authentication:
        LOGGER.debug(f"Authentication provided: type={auth_type}")
    session = await manager.get_session(session_id)
    if not session:
        LOGGER.info("Session with ID '{session_id}' was not found.")
        raise ValueError(f"Session with ID '{session_id}' was not found.")

    # Check if this is a notification (no id field) or a request (has id field)
    request_id = mcp_request.get("id")
    is_notification = request_id is None

    # Wrap the request in reverse proxy envelope
    message = {"type": "request", "sessionId": session_id, "payload": mcp_request}

    try:
        await session.send_message(message)

        # Notifications don't expect a response
        if is_notification:
            LOGGER.info(f"Sent notification (no response expected)")
            return None

        # For requests, create a future and wait for response
        loop = asyncio.get_event_loop()
        future = loop.create_future()
        pending_responses[request_id] = future

        # Wait for the response with a timeout
        response = await asyncio.wait_for(future, timeout=30)
        LOGGER.info(f"response {response}" )
        return response

    except asyncio.TimeoutError:
        if request_id:
            pending_responses.pop(request_id, None)
        raise

    except Exception as e:
        if request_id:
            pending_responses.pop(request_id, None)
        raise


@router.websocket("/ws")
async def websocket_endpoint(
    websocket: WebSocket,
    db: Session = Depends(get_db),
):
    """WebSocket endpoint for reverse proxy connections.

    Authentication is REQUIRED when:
    - settings.auth_required is True, OR
    - settings.mcp_client_auth_enabled is True

    Supports:
    - Bearer token in Authorization header
    - Token in query parameter (?token=...)
    - Proxy authentication (when trust_proxy_auth is True and mcp_client_auth_enabled is False)

    Args:
        websocket: WebSocket connection.
        db: Database session.

    Raises:
        ValueError: If token is missing required subject claim.
    """
    # Check authentication BEFORE accepting connection
    user = None
    auth_header = websocket.headers.get("Authorization", "")

    # Determine if auth is required
    # auth_required = settings.auth_required or settings.mcp_client_auth_enabled
    auth_required = False

    if auth_required:
        # Try Bearer token authentication from header
        if auth_header.startswith("Bearer "):
            try:
                token = auth_header.split(" ", 1)[1]
                payload = await verify_jwt_token(token)
                user = payload.get("sub") or payload.get("email")
                if not user:
                    raise ValueError("Token missing subject claim")
                LOGGER.debug(f"WebSocket authenticated via JWT: {user}")
            except HTTPException as e:
                LOGGER.warning(f"WebSocket JWT authentication failed: {e.detail}")
                await websocket.close(code=status.WS_1008_POLICY_VIOLATION, reason="Authentication failed")
                return
            except Exception as e:
                LOGGER.warning(f"WebSocket JWT authentication failed: {e}")
                await websocket.close(code=status.WS_1008_POLICY_VIOLATION, reason="Authentication failed")
                return
        # Try token from query parameter
        elif "token" in websocket.query_params:
            try:
                token = websocket.query_params["token"]
                payload = await verify_jwt_token(token)
                user = payload.get("sub") or payload.get("email")
                if not user:
                    raise ValueError("Token missing subject claim")
                LOGGER.debug(f"WebSocket authenticated via query token: {user}")
            except HTTPException as e:
                LOGGER.warning(f"WebSocket query token authentication failed: {e.detail}")
                await websocket.close(code=status.WS_1008_POLICY_VIOLATION, reason="Authentication failed")
                return
            except Exception as e:
                LOGGER.warning(f"WebSocket query token authentication failed: {e}")
                await websocket.close(code=status.WS_1008_POLICY_VIOLATION, reason="Authentication failed")
                return
        # Try proxy authentication (when mcp_client_auth_enabled is False and trust_proxy_auth is True)
        elif settings.trust_proxy_auth and not settings.mcp_client_auth_enabled:
            proxy_user = websocket.headers.get(settings.proxy_user_header)
            if proxy_user:
                user = proxy_user
                LOGGER.debug(f"WebSocket authenticated via proxy header: {user}")
            else:
                LOGGER.warning("WebSocket proxy authentication failed: no proxy header")
                await websocket.close(code=status.WS_1008_POLICY_VIOLATION, reason="Authentication required")
                return
        else:
            LOGGER.warning("WebSocket authentication required but no credentials provided")
            await websocket.close(code=status.WS_1008_POLICY_VIOLATION, reason="Authentication required")
            return

    # Accept connection only after successful authentication (or when auth not required)
    await websocket.accept()

    # Generate session ID server-side to prevent session hijacking
    # Client-supplied X-Session-ID is ignored for security (prevents collision/hijack attacks)
    # Get session ID from headers or generate new one
    session_id = websocket.headers.get("X-Session-ID", uuid.uuid4().hex)
    LOGGER.info(f"websocket_endpoint session_id {session_id}")


    LOGGER.info(f" session_id {session_id}")

    # Create session with authenticated user
    session = ReverseProxySession(session_id, websocket, user)
    await manager.add_session(session)

    try:
        LOGGER.info(f"Reverse proxy connected: {session_id}")

        # Main message loop
        while True:
            try:
                message = await session.receive_message()
                msg_type = message.get("type")

                if msg_type == "register":
                    # Register the server
                    session.server_info = message.get("server", {})
                    LOGGER.info(f"session.server_info  {session.server_info}")

                    # Send immediate acknowledgment so client knows we received the registration
                    await session.send_message({"type": "register_ack", "sessionId": session_id, "status": "processing"})

                    # Process registration in background to avoid blocking the message loop
                    async def process_registration():
                        # Use separate database sessions for gateway and server registration
                        # to avoid transaction conflicts
                        from mcpgateway.db import SessionLocal

                        dbsession = SessionLocal()

                        try:
                            LOGGER.info(f" register session_id {session_id}")
                            app_domain = Settings().app_domain
                            url = f"{app_domain}reverse-proxy/sessions/{session_id}/mcp"

                            gateway = GatewayCreate(
                                name=session.server_info.get("name"),
                                url=url,
                                description=session.server_info.get("description"),
                                tags=[],
                                transport=TransportType.PROXIED,
                                auth_type=None,
                                auth_username="",
                                auth_password="",
                                auth_token="",
                                auth_header_key="",
                                auth_header_value="",
                                auth_headers=None,
                                oauth_config=None,
                                passthrough_headers=None,
                                visibility="public",
                            )

                            # Gateway registration - flush and commit before server registration
                            from mcpgateway.services import GatewayService

                            try:
                                gateway, tool_ids, resource_ids, prompt_ids = await GatewayService().register_proxy_gateway(
                                    db=dbsession,
                                    gateway=gateway,
                                    session_id=session_id,
                                    forward_request_func=forward_request_to_session
                                )

                                LOGGER.info(f"**** Gateway {gateway.name} registered successfully with {len(tool_ids)} tools")
                                server_in = ServerCreate(
                                    id=gateway.id,
                                    name="virtual-"+gateway.name,
                                    description=gateway.description,
                                    icon=None,
                                    associated_tools=tool_ids,
                                    associated_resources=resource_ids,
                                    associated_prompts=prompt_ids,
                                    associated_a2a_agents=[],
                                    team_id=gateway.team_id,
                                    tags=gateway.tags,
                                    visibility=gateway.visibility,
                                    owner_email=None
                                )

                                server = await ServerService().register_server(
                                    dbsession,
                                    server_in,
                                    created_by=None,
                                    created_from_ip=gateway.created_from_ip,
                                    created_via=gateway.created_via,
                                    created_user_agent=gateway.created_user_agent,
                                    team_id=gateway.team_id,
                                    visibility=gateway.visibility,
                                )
                                LOGGER.info(f"Virtual server {server.name} registered successfully with {len(tool_ids)} tools")
                            except Exception as e:
                                LOGGER.error(f"Failed to register gateway/server: {e}")
                                dbsession.rollback()
                                raise
                            finally:
                                dbsession.close()

                            # Send final success acknowledgment
                            await session.send_message({"type": "register_complete", "sessionId": session_id, "status": "success"})
                        except Exception as e:
                            LOGGER.error(f"Failed to register gateway: {e}", exc_info=True)
                            await session.send_message({"type": "register_complete", "sessionId": session_id, "status": "error", "message": str(e)})

                    # Start registration task in background
                    asyncio.create_task(process_registration())

                elif msg_type == "unregister":
                    # Unregister the server
                    LOGGER.info(f"Unregistering server for session {session_id}")
                    break

                elif msg_type == "heartbeat":
                    # Respond to heartbeat
                    await session.send_message({"type": "heartbeat", "sessionId": session_id, "timestamp": datetime.now(tz=timezone.utc).isoformat()})

                elif msg_type in ("response", "notification"):
                    # Handle MCP response/notification from the proxied server
                    LOGGER.info(f"Received {msg_type} from session {session_id} message type {type(message)} message {orjson.dumps(message).decode()}")

                    payload = message.get("payload")
                    LOGGER.info(f"response payload {payload}  type payload {type(payload)}")
                    request_id = payload["id"]
                    LOGGER.info(f"response request_id {request_id}")
                    if request_id and request_id in pending_responses:
                        LOGGER.info(f"request_id found in pending_responses")
                        future = pending_responses.pop(request_id)
                        LOGGER.info(f"future found {future}")
                        if not future.done():
                            LOGGER.info(f"set result on future ")
                            future.set_result(message)


                else:
                    LOGGER.warning(f"Unknown message type from session {session_id}: {msg_type}")

            except WebSocketDisconnect:
                LOGGER.info(f"WebSocket disconnected: {session_id}")
                break
            except orjson.JSONDecodeError as e:
                LOGGER.error(f"Invalid JSON from session {session_id}: {e}")
                await session.send_message({"type": "error", "message": "Invalid JSON format"})
            except Exception as e:
                LOGGER.error(f"Error handling message from session {session_id}: {e}")
                await session.send_message({"type": "error", "message": str(e)})

    finally:
        await manager.remove_session(session_id)
        LOGGER.info(f"Reverse proxy session ended: {session_id}")


@router.get("/sessions")
async def list_sessions(
    request: Request,
    credentials: str | dict = Depends(require_auth),
):
    """List active reverse proxy sessions.

    Returns only sessions owned by the authenticated user, unless
    the user is an admin (in which case all sessions are returned).

    Args:
        request: HTTP request.
        credentials: Authenticated user credentials.

    Returns:
        List of session information (filtered by ownership).
    """
    requesting_user, is_admin = _get_user_from_credentials(credentials)

    LOGGER.info(f"list_sessions manager {hex(id(manager))} sessions {hex(id(manager.sessions))} sessions.values {manager.sessions.values()}")
    # Admins see all sessions
    if is_admin:
        return {"sessions": await manager.list_sessions(), "total": len(manager.sessions)}

    # Regular users see only their own sessions
    all_sessions = await manager.list_sessions()
    owned_sessions = []
    for session_info in all_sessions:
        session_owner = session_info.get("user")
        # Include if: user owns the session, or session has no owner (anonymous)
        if not session_owner or session_owner == requesting_user:
            owned_sessions.append(session_info)

    return {"sessions": owned_sessions, "total": len(owned_sessions)}


@router.delete("/sessions/{session_id}")
async def disconnect_session(
    session_id: str,
    request: Request,
    credentials: str | dict = Depends(require_auth),
):
    """Disconnect a reverse proxy session.

    Requires authentication and validates session ownership.
    Only the session owner or an admin can disconnect a session.

    Args:
        session_id: Session ID to disconnect.
        request: HTTP request.
        credentials: Authenticated user credentials.

    Returns:
        Disconnection status.

    Raises:
        HTTPException: If session is not found or user is not authorized.
    """
    session = await manager.get_session(session_id)
    if not session:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Session {session_id} not found")

    # Validate session ownership
    _validate_session_ownership(session, credentials, "disconnect")

    # Close the WebSocket connection
    await session.websocket.close()
    await manager.remove_session(session_id)

    return {"status": "disconnected", "session_id": session_id}


@router.post("/sessions/{session_id}/request")
async def send_request_to_session(
    session_id: str,
    mcp_request: Dict[str, Any],
    request: Request,
    credentials: str | dict = Depends(require_auth),
):
    """Send an MCP request to a reverse proxy session.

    Requires authentication and validates session ownership.
    Only the session owner or an admin can send requests to a session.

    Args:
        session_id: Session ID to send request to.
        mcp_request: MCP request to send.
        request: HTTP request.
        credentials: Authenticated user credentials.

    Returns:
        Request acknowledgment.

    Raises:
        HTTPException: If session is not found, user is not authorized, or request fails.
    """
    session = await manager.get_session(session_id)
    if not session:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Session {session_id} not found")

    # Validate session ownership
    _validate_session_ownership(session, credentials, "send request to")

    # Wrap the request in reverse proxy envelope
    message = {"type": "request", "sessionId": session_id, "payload": mcp_request}

    try:

        response = await forward_request_to_session(session_id, mcp_request)
        return response
    except asyncio.TimeoutError as e:
        raise HTTPException(status_code=status.HTTP_504_GATEWAY_TIMEOUT, detail=f"Failed to send request: {e}")

    except asyncio.TimeoutError as e:
        raise HTTPException(status_code=status.HTTP_504_GATEWAY_TIMEOUT, detail=f"Failed to send request: {e}")

    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Failed to send request: {e}")


def _get_user_from_credentials(credentials: str | dict) -> tuple[str | None, bool]:
    """Extract user and admin status from credentials.

    Args:
        credentials: Auth credentials (dict from JWT or string)

    Returns:
        Tuple of (username, is_admin)
    """
    if isinstance(credentials, dict):
        user = credentials.get("sub") or credentials.get("email")
        # Check both top-level is_admin and nested user.is_admin (JWT tokens may nest it)
        is_admin = credentials.get("is_admin", False) or credentials.get("user", {}).get("is_admin", False)
        return user, is_admin
    elif credentials and credentials != "anonymous":
        return credentials, False
    return None, False


def _validate_session_ownership(session: ReverseProxySession, credentials: str | dict, action: str) -> None:
    """Validate that the requesting user owns the session or is admin.

    Args:
        session: The session to validate ownership for
        credentials: Auth credentials from require_auth
        action: Description of the action for logging

    Raises:
        HTTPException: 403 if user is not authorized for the session
    """
    if not session.user:
        # Session was created without auth - allow access
        return

    requesting_user, is_admin = _get_user_from_credentials(credentials)

    # Admins can access any session
    if is_admin:
        return

    # Session owner can access their own session
    session_owner = session.user if isinstance(session.user, str) else session.user.get("sub") if isinstance(session.user, dict) else None
    if requesting_user and session_owner and requesting_user == session_owner:
        return

    # Not authorized
    LOGGER.warning(f"Session access denied: user {requesting_user} attempted to {action} session owned by {session_owner}")
    raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Not authorized for this session")


@router.get("/sse/{session_id}")
async def sse_endpoint(
    session_id: str,
    request: Request,
    credentials: str | dict = Depends(require_auth),
):
    """SSE endpoint for receiving messages from a reverse proxy session.

    Requires authentication via require_auth dependency.
    Additionally validates that the authenticated user owns the session.

    Args:
        session_id: Session ID to subscribe to.
        request: HTTP request.
        credentials: Authenticated user credentials.

    Returns:
        SSE stream.

    Raises:
        HTTPException: If session is not found or user is not authorized.
    """
    session = await manager.get_session(session_id)
    if not session:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Session {session_id} not found")

    # Validate session ownership
    _validate_session_ownership(session, credentials, "subscribe to SSE for")

    async def event_generator():
        """Generate SSE events.

        Yields:
            dict: SSE event data.
        """
        try:
            # Send initial connection event
            yield {"event": "connected", "data": orjson.dumps({"sessionId": session_id, "serverInfo": session.server_info}).decode()}

            # TODO: Implement message queue for SSE delivery
            while not await request.is_disconnected():
                await asyncio.sleep(30)  # Keepalive
                yield {"event": "keepalive", "data": orjson.dumps({"timestamp": datetime.now(tz=timezone.utc).isoformat()}).decode()}

        except asyncio.CancelledError:
            pass

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


