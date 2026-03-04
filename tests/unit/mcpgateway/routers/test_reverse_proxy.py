# -*- coding: utf-8 -*-
"""Location: ./tests/unit/mcpgateway/routers/test_reverse_proxy.py
Copyright 2025
SPDX-License-Identifier: Apache-2.0
Authors: Mihai Criveti

Unit tests for reverse proxy router.
This module tests the reverse proxy functionality including WebSocket connections,
session management, and HTTP endpoints.
"""

# Standard
import asyncio
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, Mock, patch

# Third-Party
import orjson

# Third-Party
from fastapi import HTTPException, WebSocket, WebSocketDisconnect
from fastapi.testclient import TestClient
import pytest

# First-Party
from mcpgateway.routers.reverse_proxy import (
    manager,
    ReverseProxyManager,
    ReverseProxySession,
    router,
)
from mcpgateway.utils.verify_credentials import require_auth

# --------------------------------------------------------------------------- #
# Test Fixtures                                                              #
# --------------------------------------------------------------------------- #


@pytest.fixture
def mock_websocket():
    """Create a mock WebSocket."""
    ws = Mock(spec=WebSocket)
    ws.accept = AsyncMock()
    ws.send_text = AsyncMock()
    ws.receive_text = AsyncMock()
    ws.close = AsyncMock()
    ws.headers = {"X-Session-ID": "test-session-123"}
    ws.query_params = {}
    ws.client = Mock(host="127.0.0.1")
    return ws


@pytest.fixture
def reverse_proxy_manager():
    """Create a fresh ReverseProxyManager instance."""
    return ReverseProxyManager()


@pytest.fixture
def sample_session(mock_websocket):
    """Create a sample ReverseProxySession."""
    return ReverseProxySession("test-session", mock_websocket, "test-user")


# --------------------------------------------------------------------------- #
# ReverseProxySession Tests                                                  #
# --------------------------------------------------------------------------- #


class TestReverseProxySession:
    """Test ReverseProxySession class."""

    def test_init(self, mock_websocket):
        """Test session initialization."""
        session = ReverseProxySession("test-id", mock_websocket, "test-user")

        assert session.session_id == "test-id"
        assert session.websocket is mock_websocket
        assert session.user == "test-user"
        assert session.server_info == {}
        assert isinstance(session.connected_at, datetime)
        assert isinstance(session.last_activity, datetime)
        assert session.message_count == 0
        assert session.bytes_transferred == 0

    def test_init_with_dict_user(self, mock_websocket):
        """Test session initialization with dict user."""
        user_dict = {"sub": "user123", "name": "Test User"}
        session = ReverseProxySession("test-id", mock_websocket, user_dict)

        assert session.user == user_dict

    def test_init_with_none_user(self, mock_websocket):
        """Test session initialization with None user."""
        session = ReverseProxySession("test-id", mock_websocket, None)

        assert session.user is None

    @pytest.mark.asyncio
    async def test_send_message(self, sample_session):
        """Test sending a message."""
        message = {"type": "test", "data": "hello"}

        await sample_session.send_message(message)

        expected_data = orjson.dumps(message).decode()
        sample_session.websocket.send_text.assert_called_once_with(expected_data)
        assert sample_session.bytes_transferred == len(expected_data)

    @pytest.mark.asyncio
    async def test_send_message_updates_activity(self, sample_session):
        """Test that sending a message updates last activity."""
        original_activity = sample_session.last_activity
        await asyncio.sleep(0.001)  # Small delay

        await sample_session.send_message({"test": "data"})

        assert sample_session.last_activity > original_activity

    @pytest.mark.asyncio
    async def test_receive_message(self, sample_session):
        """Test receiving a message."""
        test_data = {"type": "test", "content": "hello"}
        sample_session.websocket.receive_text.return_value = orjson.dumps(test_data).decode()

        result = await sample_session.receive_message()

        assert result == test_data
        assert sample_session.message_count == 1
        assert sample_session.bytes_transferred == len(orjson.dumps(test_data).decode())

    @pytest.mark.asyncio
    async def test_receive_message_updates_activity(self, sample_session):
        """Test that receiving a message updates last activity."""
        sample_session.websocket.receive_text.return_value = '{"test": "data"}'
        original_activity = sample_session.last_activity
        await asyncio.sleep(0.001)  # Small delay

        await sample_session.receive_message()

        assert sample_session.last_activity > original_activity

    @pytest.mark.asyncio
    async def test_receive_message_invalid_json(self, sample_session):
        """Test receiving invalid JSON."""
        sample_session.websocket.receive_text.return_value = "invalid json"

        with pytest.raises(orjson.JSONDecodeError):
            await sample_session.receive_message()


# --------------------------------------------------------------------------- #
# ReverseProxyManager Tests                                                  #
# --------------------------------------------------------------------------- #


class TestReverseProxyManager:
    """Test ReverseProxyManager class."""

    def test_init(self, reverse_proxy_manager):
        """Test manager initialization."""
        assert reverse_proxy_manager.sessions == {}
        assert reverse_proxy_manager._lock is not None

    @pytest.mark.asyncio
    async def test_add_session(self, reverse_proxy_manager, sample_session):
        """Test adding a session."""
        await reverse_proxy_manager.add_session(sample_session)

        assert sample_session.session_id in reverse_proxy_manager.sessions
        assert reverse_proxy_manager.sessions[sample_session.session_id] is sample_session

    @pytest.mark.asyncio
    async def test_remove_session(self, reverse_proxy_manager, sample_session):
        """Test removing a session."""
        await reverse_proxy_manager.add_session(sample_session)
        await reverse_proxy_manager.remove_session(sample_session.session_id)

        assert sample_session.session_id not in reverse_proxy_manager.sessions

    @pytest.mark.asyncio
    async def test_remove_nonexistent_session(self, reverse_proxy_manager):
        """Test removing a session that doesn't exist."""
        # Should not raise an exception
        await reverse_proxy_manager.remove_session("nonexistent")

        assert len(reverse_proxy_manager.sessions) == 0

    @pytest.mark.asyncio
    async def test_get_session(self, reverse_proxy_manager, sample_session):
        """Test getting a session."""
        reverse_proxy_manager.sessions[sample_session.session_id] = sample_session

        result = await reverse_proxy_manager.get_session(sample_session.session_id)
        assert result is sample_session

    @pytest.mark.asyncio
    async def test_get_nonexistent_session(self, reverse_proxy_manager):
        """Test getting a session that doesn't exist."""
        result = await reverse_proxy_manager.get_session("nonexistent")
        assert result is None

    @pytest.mark.asyncio
    async def test_list_sessions_empty(self, reverse_proxy_manager):
        """Test listing sessions when empty."""
        result = await reverse_proxy_manager.list_sessions()

        assert result == []
        assert isinstance(result, list)

    @pytest.mark.asyncio
    async def test_list_sessions_with_string_user(self, reverse_proxy_manager, mock_websocket):
        """Test listing sessions with string user."""
        session = ReverseProxySession("test-id", mock_websocket, "test-user")
        session.server_info = {"name": "test-server"}
        session.message_count = 5
        session.bytes_transferred = 1024
        reverse_proxy_manager.sessions["test-id"] = session

        result = await reverse_proxy_manager.list_sessions()

        assert len(result) == 1
        session_info = result[0]
        assert session_info["session_id"] == "test-id"
        assert session_info["server_info"] == {"name": "test-server"}
        assert session_info["message_count"] == 5
        assert session_info["bytes_transferred"] == 1024
        assert session_info["user"] == "test-user"
        assert "connected_at" in session_info
        assert "last_activity" in session_info

    @pytest.mark.asyncio
    async def test_list_sessions_with_dict_user(self, reverse_proxy_manager, mock_websocket):
        """Test listing sessions with dict user."""
        user_dict = {"sub": "user123", "name": "Test User"}
        session = ReverseProxySession("test-id", mock_websocket, user_dict)
        reverse_proxy_manager.sessions["test-id"] = session

        result = await reverse_proxy_manager.list_sessions()

        assert len(result) == 1
        assert result[0]["user"] == "user123"

    @pytest.mark.asyncio
    async def test_list_sessions_with_none_user(self, reverse_proxy_manager, mock_websocket):
        """Test listing sessions with None user."""
        session = ReverseProxySession("test-id", mock_websocket, None)
        reverse_proxy_manager.sessions["test-id"] = session

        result = await reverse_proxy_manager.list_sessions()

        assert len(result) == 1
        assert result[0]["user"] is None

    @pytest.mark.asyncio
    async def test_list_sessions_with_invalid_dict_user(self, reverse_proxy_manager, mock_websocket):
        """Test listing sessions with dict user without 'sub' key."""
        user_dict = {"name": "Test User"}  # No 'sub' key
        session = ReverseProxySession("test-id", mock_websocket, user_dict)
        reverse_proxy_manager.sessions["test-id"] = session

        result = await reverse_proxy_manager.list_sessions()

        assert len(result) == 1
        assert result[0]["user"] is None


# --------------------------------------------------------------------------- #
# WebSocket Endpoint Tests                                                   #
# --------------------------------------------------------------------------- #


class TestWebSocketEndpoint:
    """Test WebSocket endpoint functionality.

    Note: These tests disable authentication to test WebSocket message handling.
    See TestWebSocketAuthentication for authentication tests.
    """

    @pytest.fixture(autouse=True)
    def mock_auth_settings(self):
        """Disable authentication for WebSocket endpoint tests."""
        with patch("mcpgateway.routers.reverse_proxy.settings") as mock_settings:
            mock_settings.auth_required = False
            mock_settings.mcp_client_auth_enabled = False
            mock_settings.trust_proxy_auth = False
            yield mock_settings

    @pytest.mark.asyncio
    async def test_websocket_accept(self, mock_websocket):
        """Test WebSocket connection acceptance."""
        mock_websocket.headers = {"X-Session-ID": "test-session"}
        mock_websocket.receive_text.side_effect = asyncio.CancelledError()

        # First-Party
        from mcpgateway.routers.reverse_proxy import websocket_endpoint

        with patch("mcpgateway.routers.reverse_proxy.get_db") as mock_get_db:
            mock_get_db.return_value = Mock()

            try:
                await websocket_endpoint(mock_websocket, Mock())
            except asyncio.CancelledError:
                pass

        mock_websocket.accept.assert_called_once()

    @pytest.mark.asyncio
    async def test_websocket_generates_session_id(self, mock_websocket):
        """Test WebSocket generates session ID when not provided."""
        mock_websocket.headers = {}  # No X-Session-ID header
        mock_websocket.receive_text.side_effect = asyncio.CancelledError()

        # First-Party
        from mcpgateway.routers.reverse_proxy import websocket_endpoint

        with patch("mcpgateway.routers.reverse_proxy.get_db") as mock_get_db, patch("mcpgateway.routers.reverse_proxy.uuid.uuid4") as mock_uuid:
            mock_get_db.return_value = Mock()
            mock_uuid.return_value.hex = "generated-session-id"

            try:
                await websocket_endpoint(mock_websocket, Mock())
            except asyncio.CancelledError:
                pass

        mock_uuid.assert_called_once()

    @pytest.mark.asyncio
    async def test_websocket_register_message(self, mock_websocket):
        """Test handling register message."""
        mock_websocket.headers = {"X-Session-ID": "test-session"}
        register_msg = {"type": "register", "server": {"name": "test-server", "version": "1.0"}}
        heartbeat_msg = {"type": "heartbeat"}

        # Provide register message, then heartbeat to keep loop alive, then cancel
        # The heartbeat gives the background registration task time to complete
        mock_websocket.receive_text.side_effect = [
            orjson.dumps(register_msg).decode(),
            orjson.dumps(heartbeat_msg).decode(),
            asyncio.CancelledError()
        ]

        # First-Party
        from mcpgateway.routers.reverse_proxy import websocket_endpoint

        with patch("mcpgateway.routers.reverse_proxy.get_db") as mock_get_db, \
             patch("mcpgateway.services.GatewayService") as mock_gateway_service, \
             patch("mcpgateway.routers.reverse_proxy.ServerService") as mock_server_service:

            mock_get_db.return_value = Mock()

            # Mock the gateway service to return a mock gateway object
            mock_gateway = Mock()
            mock_gateway.id = "550e8400-e29b-41d4-a716-446655440000"
            mock_gateway.name = "test-server"
            mock_gateway.description = None
            mock_gateway.team_id = None
            mock_gateway.tags = []
            mock_gateway.visibility = "public"
            mock_gateway.created_from_ip = None
            mock_gateway.created_via = None
            mock_gateway.created_user_agent = None

            mock_gateway_service.return_value.register_proxy_gateway = AsyncMock(
                return_value=(mock_gateway, [], [], [])
            )
            mock_server_service.return_value.register_server = AsyncMock()

            try:
                await websocket_endpoint(mock_websocket, Mock())
            except asyncio.CancelledError:
                pass

            # Give background task a moment to complete
            await asyncio.sleep(0.1)

        # Should send register acknowledgment with "processing" status as the first message
        # (immediate ack before async registration), then "register_complete" when done
        mock_websocket.send_text.assert_called()
        first_call_data = orjson.loads(mock_websocket.send_text.call_args_list[0][0][0])
        assert first_call_data["type"] == "register_ack"
        assert first_call_data["status"] == "processing"
        # Final message should be register_complete with success
        last_call_data = orjson.loads(mock_websocket.send_text.call_args[0][0])
        assert last_call_data["type"] == "register_complete"
        assert last_call_data["status"] == "success"

    @pytest.mark.asyncio
    async def test_websocket_unregister_message(self, mock_websocket):
        """Test handling unregister message."""
        mock_websocket.headers = {"X-Session-ID": "test-session"}
        unregister_msg = {"type": "unregister"}
        mock_websocket.receive_text.return_value = orjson.dumps(unregister_msg).decode()

        # First-Party
        from mcpgateway.routers.reverse_proxy import websocket_endpoint

        with patch("mcpgateway.routers.reverse_proxy.get_db") as mock_get_db:
            mock_get_db.return_value = Mock()

            await websocket_endpoint(mock_websocket, Mock())

    @pytest.mark.asyncio
    async def test_websocket_heartbeat_message(self, mock_websocket):
        """Test handling heartbeat message."""
        mock_websocket.headers = {"X-Session-ID": "test-session"}
        heartbeat_msg = {"type": "heartbeat"}
        mock_websocket.receive_text.side_effect = [orjson.dumps(heartbeat_msg).decode(), asyncio.CancelledError()]

        # First-Party
        from mcpgateway.routers.reverse_proxy import websocket_endpoint

        with patch("mcpgateway.routers.reverse_proxy.get_db") as mock_get_db:
            mock_get_db.return_value = Mock()

            try:
                await websocket_endpoint(mock_websocket, Mock())
            except asyncio.CancelledError:
                pass

        # Should send heartbeat response
        mock_websocket.send_text.assert_called()
        sent_data = orjson.loads(mock_websocket.send_text.call_args[0][0])
        assert sent_data["type"] == "heartbeat"
        assert "timestamp" in sent_data

    @pytest.mark.asyncio
    async def test_websocket_response_message(self, mock_websocket):
        """Test handling response message."""
        mock_websocket.headers = {"X-Session-ID": "test-session"}
        response_msg = {"type": "response", "id": 1, "result": {"data": "test"}}
        mock_websocket.receive_text.side_effect = [orjson.dumps(response_msg).decode(), asyncio.CancelledError()]

        # First-Party
        from mcpgateway.routers.reverse_proxy import websocket_endpoint

        with patch("mcpgateway.routers.reverse_proxy.get_db") as mock_get_db:
            mock_get_db.return_value = Mock()

            try:
                await websocket_endpoint(mock_websocket, Mock())
            except asyncio.CancelledError:
                pass

    @pytest.mark.asyncio
    async def test_websocket_notification_message(self, mock_websocket):
        """Test handling notification message."""
        mock_websocket.headers = {"X-Session-ID": "test-session"}
        notification_msg = {"type": "notification", "method": "test/notification"}
        mock_websocket.receive_text.side_effect = [orjson.dumps(notification_msg).decode(), asyncio.CancelledError()]

        # First-Party
        from mcpgateway.routers.reverse_proxy import websocket_endpoint

        with patch("mcpgateway.routers.reverse_proxy.get_db") as mock_get_db:
            mock_get_db.return_value = Mock()

            try:
                await websocket_endpoint(mock_websocket, Mock())
            except asyncio.CancelledError:
                pass

    @pytest.mark.asyncio
    async def test_websocket_unknown_message_type(self, mock_websocket):
        """Test handling unknown message type."""
        mock_websocket.headers = {"X-Session-ID": "test-session"}
        unknown_msg = {"type": "unknown", "data": "test"}
        mock_websocket.receive_text.side_effect = [orjson.dumps(unknown_msg).decode(), asyncio.CancelledError()]

        # First-Party
        from mcpgateway.routers.reverse_proxy import websocket_endpoint

        with patch("mcpgateway.routers.reverse_proxy.get_db") as mock_get_db:
            mock_get_db.return_value = Mock()

            try:
                await websocket_endpoint(mock_websocket, Mock())
            except asyncio.CancelledError:
                pass

    @pytest.mark.asyncio
    async def test_websocket_invalid_json(self, mock_websocket):
        """Test handling invalid JSON."""
        mock_websocket.headers = {"X-Session-ID": "test-session"}
        mock_websocket.receive_text.side_effect = ["invalid json", asyncio.CancelledError()]

        # First-Party
        from mcpgateway.routers.reverse_proxy import websocket_endpoint

        with patch("mcpgateway.routers.reverse_proxy.get_db") as mock_get_db:
            mock_get_db.return_value = Mock()

            try:
                await websocket_endpoint(mock_websocket, Mock())
            except asyncio.CancelledError:
                pass

        # Should send error message
        mock_websocket.send_text.assert_called()
        sent_data = orjson.loads(mock_websocket.send_text.call_args[0][0])
        assert sent_data["type"] == "error"
        assert "Invalid JSON format" in sent_data["message"]

    @pytest.mark.asyncio
    async def test_websocket_general_exception(self, mock_websocket):
        """Test handling general exception during message processing."""
        mock_websocket.headers = {"X-Session-ID": "test-session"}
        # First call succeeds, second call raises exception, third call cancels
        mock_websocket.receive_text.side_effect = [orjson.dumps({"type": "register", "server": {"name": "test"}}).decode(), Exception("Test exception"), asyncio.CancelledError()]

        # First-Party
        from mcpgateway.routers.reverse_proxy import websocket_endpoint

        with patch("mcpgateway.routers.reverse_proxy.get_db") as mock_get_db:
            mock_get_db.return_value = Mock()

            try:
                await websocket_endpoint(mock_websocket, Mock())
            except asyncio.CancelledError:
                pass

        # Should send register ack and error message
        assert mock_websocket.send_text.call_count >= 2


class TestWebSocketAuthentication:
    """Test WebSocket authentication functionality."""

    @pytest.mark.asyncio
    async def test_websocket_rejects_unauthenticated_when_auth_required(self, mock_websocket):
        """Test WebSocket rejects connection when auth required but no token provided."""
        mock_websocket.headers = {"X-Session-ID": "test-session"}  # No Authorization header
        mock_websocket.query_params = {}

        # First-Party
        from mcpgateway.routers.reverse_proxy import websocket_endpoint

        with patch("mcpgateway.routers.reverse_proxy.settings") as mock_settings:
            mock_settings.auth_required = True
            mock_settings.mcp_client_auth_enabled = False
            mock_settings.trust_proxy_auth = False

            with patch("mcpgateway.routers.reverse_proxy.get_db") as mock_get_db:
                mock_get_db.return_value = Mock()

                await websocket_endpoint(mock_websocket, Mock())

        # Should NOT accept the connection
        mock_websocket.accept.assert_not_called()
        # Should close with policy violation
        mock_websocket.close.assert_called_once()
        assert mock_websocket.close.call_args[1]["code"] == 1008  # WS_1008_POLICY_VIOLATION

    @pytest.mark.asyncio
    async def test_websocket_accepts_with_valid_token(self, mock_websocket):
        """Test WebSocket accepts connection with valid JWT token."""
        mock_websocket.headers = {"X-Session-ID": "test-session", "Authorization": "Bearer valid-token"}
        mock_websocket.query_params = {}
        mock_websocket.receive_text.side_effect = asyncio.CancelledError()

        # First-Party
        from mcpgateway.routers.reverse_proxy import websocket_endpoint

        with patch("mcpgateway.routers.reverse_proxy.settings") as mock_settings:
            mock_settings.auth_required = True
            mock_settings.mcp_client_auth_enabled = False
            mock_settings.trust_proxy_auth = False

            with (
                patch("mcpgateway.routers.reverse_proxy.get_db") as mock_get_db,
                patch("mcpgateway.routers.reverse_proxy.get_current_user") as mock_get_user,
                patch("mcpgateway.routers.reverse_proxy.PermissionChecker.has_any_permission", new_callable=AsyncMock, return_value=True),
            ):
                mock_get_db.return_value = Mock()
                mock_get_user.return_value = Mock(email="test@example.com", full_name="Test User", is_admin=False)

                try:
                    await websocket_endpoint(mock_websocket, Mock())
                except asyncio.CancelledError:
                    pass

        # Should accept the connection
        mock_websocket.accept.assert_called_once()

    @pytest.mark.asyncio
    async def test_websocket_rejects_query_token_auth(self, mock_websocket):
        """Query-string bearer tokens must be rejected for reverse-proxy WebSocket auth."""
        mock_websocket.headers = {"X-Session-ID": "test-session"}  # No Authorization header
        mock_websocket.query_params = {"token": "valid-token"}
        mock_websocket.receive_text.side_effect = asyncio.CancelledError()

        # First-Party
        from mcpgateway.routers.reverse_proxy import websocket_endpoint

        with patch("mcpgateway.routers.reverse_proxy.settings") as mock_settings:
            mock_settings.auth_required = True
            mock_settings.mcp_client_auth_enabled = False
            mock_settings.trust_proxy_auth = False

            with (
                patch("mcpgateway.routers.reverse_proxy.get_db") as mock_get_db,
                patch("mcpgateway.routers.reverse_proxy.get_current_user") as mock_get_user,
                patch("mcpgateway.routers.reverse_proxy.PermissionChecker.has_any_permission", new_callable=AsyncMock, return_value=True),
            ):
                mock_get_db.return_value = Mock()
                mock_get_user.return_value = Mock(email="test@example.com", full_name="Test User", is_admin=False)

                try:
                    await websocket_endpoint(mock_websocket, Mock())
                except asyncio.CancelledError:
                    pass

        mock_websocket.accept.assert_not_called()
        mock_websocket.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_websocket_accepts_proxy_auth(self, mock_websocket):
        """Test WebSocket accepts proxy authentication."""
        mock_websocket.headers = {"X-Session-ID": "test-session", "X-Authenticated-User": "proxy-user"}
        mock_websocket.query_params = {}
        mock_websocket.receive_text.side_effect = asyncio.CancelledError()

        # First-Party
        from mcpgateway.routers.reverse_proxy import websocket_endpoint

        with patch("mcpgateway.routers.reverse_proxy.settings") as mock_settings:
            mock_settings.auth_required = True
            mock_settings.mcp_client_auth_enabled = False
            mock_settings.trust_proxy_auth = True
            mock_settings.trust_proxy_auth_dangerously = True
            mock_settings.proxy_user_header = "X-Authenticated-User"

            with (
                patch("mcpgateway.routers.reverse_proxy.get_db") as mock_get_db,
                patch("mcpgateway.routers.reverse_proxy.PermissionChecker.has_any_permission", new_callable=AsyncMock, return_value=True),
            ):
                mock_get_db.return_value = Mock()

                try:
                    await websocket_endpoint(mock_websocket, Mock())
                except asyncio.CancelledError:
                    pass

        # Should accept the connection
        mock_websocket.accept.assert_called_once()


# --------------------------------------------------------------------------- #
# HTTP Endpoint Tests                                                        #
# --------------------------------------------------------------------------- #


class TestHTTPEndpoints:
    """Test HTTP endpoints."""

    @pytest.fixture
    def client(self):
        """Create test client."""
        # Third-Party
        from fastapi import FastAPI

        app = FastAPI()

        # Override the auth dependency
        def mock_require_auth():
            return "test-user"

        app.dependency_overrides[require_auth] = mock_require_auth
        app.include_router(router)
        return TestClient(app)

    @pytest.fixture
    def mock_auth(self):
        """Mock authentication dependency (for reference)."""
        return "test-user"

    @pytest.mark.asyncio
    async def test_list_sessions_empty(self, client, mock_auth):
        """Test listing sessions when empty."""
        # Clear any existing sessions
        manager.sessions.clear()

        response = client.get("/reverse-proxy/sessions")

        assert response.status_code == 200
        data = response.json()
        assert data["sessions"] == []
        assert data["total"] == 0

    @pytest.mark.asyncio
    async def test_list_sessions_with_data(self, client, mock_auth, mock_websocket):
        """Test listing sessions with data."""
        # Add a test session
        session = ReverseProxySession("test-session", mock_websocket, "test-user")
        session.server_info = {"name": "test-server"}
        manager.sessions["test-session"] = session

        try:
            response = client.get("/reverse-proxy/sessions")

            assert response.status_code == 200
            data = response.json()
            assert len(data["sessions"]) == 1
            assert data["total"] == 1
            assert data["sessions"][0]["session_id"] == "test-session"
        finally:
            # Clean up
            manager.sessions.clear()

    def test_disconnect_session_success(self, client, mock_auth, mock_websocket):
        """Test disconnecting an existing session."""
        # Add a test session
        session = ReverseProxySession("test-session", mock_websocket, "test-user")
        manager.sessions["test-session"] = session

        try:
            response = client.delete("/reverse-proxy/sessions/test-session")

            assert response.status_code == 200
            data = response.json()
            assert data["status"] == "disconnected"
            assert data["session_id"] == "test-session"

            # Session should be removed
            assert "test-session" not in manager.sessions
        finally:
            # Clean up
            manager.sessions.clear()

    def test_disconnect_session_not_found(self, client, mock_auth):
        """Test disconnecting a non-existent session."""
        response = client.delete("/reverse-proxy/sessions/nonexistent")

        assert response.status_code == 404
        data = response.json()
        assert "not found" in data["detail"]

    @pytest.mark.asyncio
    async def test_send_request_to_session_success(self, client, mock_auth, mock_websocket):
        """Test sending request to existing session."""
        # Add a test session
        session = ReverseProxySession("test-session", mock_websocket, "test-user")
        manager.sessions["test-session"] = session

        try:
            mcp_request = {"method": "tools/list", "id": 1}

            # Mock the forward_request_to_session to return immediately
            with patch("mcpgateway.routers.reverse_proxy.forward_request_to_session") as mock_forward:
                mock_response = {"type": "response", "payload": {"id": 1, "result": {"tools": []}}}
                mock_forward.return_value = mock_response

                response = client.post("/reverse-proxy/sessions/test-session/request", json=mcp_request)

                assert response.status_code == 200
                data = response.json()
                assert data["type"] == "response"
                assert "payload" in data

                # Verify forward_request_to_session was called
                mock_forward.assert_called_once_with("test-session", mcp_request)
        finally:
            # Clean up
            manager.sessions.clear()

    def test_send_request_to_session_not_found(self, client, mock_auth):
        """Test sending request to non-existent session."""
        mcp_request = {"method": "tools/list", "id": 1}
        response = client.post("/reverse-proxy/sessions/nonexistent/request", json=mcp_request)

        assert response.status_code == 404
        data = response.json()
        assert "not found" in data["detail"]

    def test_send_request_to_session_websocket_error(self, client, mock_auth, mock_websocket):
        """Test sending request when WebSocket fails."""
        # Add a test session with failing WebSocket
        mock_websocket.send_text.side_effect = Exception("WebSocket error")
        session = ReverseProxySession("test-session", mock_websocket, "test-user")
        manager.sessions["test-session"] = session

        try:
            mcp_request = {"method": "tools/list", "id": 1}
            response = client.post("/reverse-proxy/sessions/test-session/request", json=mcp_request)

            assert response.status_code == 500
            data = response.json()
            assert "Failed to send request" in data["detail"]
        finally:
            # Clean up
            manager.sessions.clear()

    def test_sse_endpoint_success(self, mock_websocket):
        """Test SSE endpoint with existing session."""
        # Add a test session
        session = ReverseProxySession("test-session", mock_websocket, "test-user")
        session.server_info = {"name": "test-server"}
        manager.sessions["test-session"] = session

        try:
            # This test does not use TestClient streaming; it validates the underlying
            # async generator behavior directly to avoid hanging on keepalive sleeps.
            from mcpgateway.routers.reverse_proxy import sse_endpoint

            class DummyRequest:
                def __init__(self):
                    self._calls = 0

                async def is_disconnected(self):
                    self._calls += 1
                    # First check: connected (run one keepalive). Second: disconnected.
                    return self._calls >= 2

            dummy_request = DummyRequest()

            async def _run():
                response = await sse_endpoint("test-session", dummy_request, credentials="test-user")
                agen = response.body_iterator
                first = await agen.__anext__()
                second = await agen.__anext__()
                with pytest.raises(StopAsyncIteration):
                    await agen.__anext__()
                return first, second

            with patch("mcpgateway.routers.reverse_proxy.asyncio.sleep", new=AsyncMock()):
                connected, keepalive = asyncio.run(_run())

            assert connected["event"] == "connected"
            assert keepalive["event"] == "keepalive"
        finally:
            # Clean up
            manager.sessions.clear()

    def test_sse_endpoint_handles_cancelled_error(self, mock_websocket):
        """SSE generator should re-raise CancelledError after yielding connected event."""
        session = ReverseProxySession("test-session", mock_websocket, "test-user")
        session.server_info = {"name": "test-server"}
        manager.sessions["test-session"] = session

        try:
            from mcpgateway.routers.reverse_proxy import sse_endpoint

            class DummyRequest:
                async def is_disconnected(self):
                    return False

            async def _run():
                response = await sse_endpoint("test-session", DummyRequest(), credentials="test-user")
                agen = response.body_iterator
                first = await agen.__anext__()
                with pytest.raises(asyncio.CancelledError):
                    await agen.__anext__()
                return first

            with patch("mcpgateway.routers.reverse_proxy.asyncio.sleep", new=AsyncMock(side_effect=asyncio.CancelledError())):
                connected = asyncio.run(_run())

            assert connected["event"] == "connected"
        finally:
            manager.sessions.clear()

    def test_sse_endpoint_not_found(self, client):
        """Test SSE endpoint with non-existent session."""
        # Don't mock the endpoint for this test since we want the real 404 behavior
        response = client.get("/reverse-proxy/sse/nonexistent")

        assert response.status_code == 404
        data = response.json()
        assert "not found" in data["detail"]


# --------------------------------------------------------------------------- #
# Integration Tests                                                          #
# --------------------------------------------------------------------------- #


class TestIntegration:
    """Integration tests for reverse proxy functionality."""

    @pytest.mark.asyncio
    async def test_session_lifecycle(self, reverse_proxy_manager, mock_websocket):
        """Test complete session lifecycle."""
        # Create session
        session = ReverseProxySession("lifecycle-test", mock_websocket, "test-user")

        # Add to manager
        await reverse_proxy_manager.add_session(session)
        assert await reverse_proxy_manager.get_session("lifecycle-test") is session

        # Update session info
        session.server_info = {"name": "test-server", "version": "1.0"}

        # Send and receive messages
        await session.send_message({"type": "test", "data": "hello"})
        mock_websocket.receive_text.return_value = '{"type": "response", "id": 1}'
        received = await session.receive_message()

        assert received["type"] == "response"
        assert session.message_count == 1
        assert session.bytes_transferred > 0

        # List sessions
        sessions = await reverse_proxy_manager.list_sessions()
        assert len(sessions) == 1
        assert sessions[0]["session_id"] == "lifecycle-test"

        # Remove session
        await reverse_proxy_manager.remove_session("lifecycle-test")
        assert await reverse_proxy_manager.get_session("lifecycle-test") is None

    @pytest.mark.asyncio
    async def test_concurrent_sessions(self, reverse_proxy_manager):
        """Test handling multiple concurrent sessions."""
        sessions = []

        # Create multiple sessions
        for i in range(5):
            ws = Mock(spec=WebSocket)
            ws.send_text = AsyncMock()
            session = ReverseProxySession(f"session-{i}", ws, f"user-{i}")
            sessions.append(session)
            await reverse_proxy_manager.add_session(session)

        # Verify all sessions are tracked
        assert len(reverse_proxy_manager.sessions) == 5

        # List sessions
        session_list = await reverse_proxy_manager.list_sessions()
        assert len(session_list) == 5

        # Remove all sessions
        for session in sessions:
            await reverse_proxy_manager.remove_session(session.session_id)

        assert len(reverse_proxy_manager.sessions) == 0


# --------------------------------------------------------------------------- #
# Helper function tests                                                       #
# --------------------------------------------------------------------------- #


class TestGetUserFromCredentials:
    """Test _get_user_from_credentials function."""

    def test_get_websocket_bearer_token_accepts_lowercase_scheme(self):
        """Reverse-proxy WebSocket token parser should accept lowercase bearer scheme."""
        # First-Party
        from mcpgateway.routers import reverse_proxy as rp

        websocket = Mock(spec=WebSocket)
        websocket.query_params = {}
        websocket.headers = {"authorization": "bearer lower-case-token"}

        assert rp._get_websocket_bearer_token(websocket) == "lower-case-token"

    def test_get_websocket_bearer_token_ignores_query_token(self):
        """Reverse-proxy WebSocket token parser should ignore query-string tokens."""
        from mcpgateway.routers import reverse_proxy as rp

        websocket = Mock(spec=WebSocket)
        websocket.query_params = {"token": "legacy-token"}
        websocket.headers = {}

        assert rp._get_websocket_bearer_token(websocket) is None

    @pytest.mark.asyncio
    async def test_authenticate_reverse_proxy_websocket_denies_without_permissions(self):
        """Authenticated users without server-management permissions should be rejected."""
        # First-Party
        from mcpgateway.routers import reverse_proxy as rp

        websocket = Mock(spec=WebSocket)
        websocket.query_params = {}
        websocket.headers = {"authorization": "Bearer valid-token"}
        websocket.client = Mock(host="127.0.0.1")
        websocket.state = Mock(team_id=None, token_teams=None, token_use=None)

        mock_user = Mock(email="user@example.com", full_name="Test User", is_admin=False)

        with (
            patch("mcpgateway.routers.reverse_proxy.settings") as mock_settings,
            patch("mcpgateway.routers.reverse_proxy.get_current_user", new=AsyncMock(return_value=mock_user)),
            patch("mcpgateway.routers.reverse_proxy.PermissionChecker.has_any_permission", new_callable=AsyncMock, return_value=False),
        ):
            mock_settings.auth_required = True
            mock_settings.mcp_client_auth_enabled = True
            mock_settings.trust_proxy_auth = False

            with pytest.raises(HTTPException) as exc_info:
                await rp._authenticate_reverse_proxy_websocket(websocket)

        assert exc_info.value.status_code == 403
        assert exc_info.value.detail == "Insufficient permissions"

    def test_dict_with_sub(self):
        from mcpgateway.routers.reverse_proxy import _get_user_from_credentials
        user, is_admin = _get_user_from_credentials({"sub": "user@test.com", "is_admin": False})
        assert user == "user@test.com"
        assert is_admin is False

    def test_dict_with_email_fallback(self):
        from mcpgateway.routers.reverse_proxy import _get_user_from_credentials
        user, is_admin = _get_user_from_credentials({"email": "user@test.com"})
        assert user == "user@test.com"
        assert is_admin is False

    def test_dict_nested_admin(self):
        from mcpgateway.routers.reverse_proxy import _get_user_from_credentials
        user, is_admin = _get_user_from_credentials({"sub": "admin@test.com", "user": {"is_admin": True}})
        assert user == "admin@test.com"
        assert is_admin is True

    def test_dict_top_level_admin(self):
        from mcpgateway.routers.reverse_proxy import _get_user_from_credentials
        user, is_admin = _get_user_from_credentials({"sub": "admin@test.com", "is_admin": True})
        assert user == "admin@test.com"
        assert is_admin is True

    def test_string_credentials(self):
        from mcpgateway.routers.reverse_proxy import _get_user_from_credentials
        user, is_admin = _get_user_from_credentials("user@test.com")
        assert user == "user@test.com"
        assert is_admin is False

    def test_anonymous_credentials(self):
        from mcpgateway.routers.reverse_proxy import _get_user_from_credentials
        user, is_admin = _get_user_from_credentials("anonymous")
        assert user is None
        assert is_admin is False

    def test_none_credentials(self):
        from mcpgateway.routers.reverse_proxy import _get_user_from_credentials
        user, is_admin = _get_user_from_credentials(None)
        assert user is None
        assert is_admin is False

    def test_empty_string_credentials(self):
        from mcpgateway.routers.reverse_proxy import _get_user_from_credentials
        user, is_admin = _get_user_from_credentials("")
        assert user is None
        assert is_admin is False


class TestValidateSessionOwnership:
    """Test _validate_session_ownership function."""

    def test_no_session_user_allows_access(self, mock_websocket):
        from mcpgateway.routers.reverse_proxy import _validate_session_ownership
        session = ReverseProxySession("test-id", mock_websocket, None)
        # Should not raise
        _validate_session_ownership(session, "any-user", "test")

    def test_admin_bypasses_ownership(self, mock_websocket):
        from mcpgateway.routers.reverse_proxy import _validate_session_ownership
        session = ReverseProxySession("test-id", mock_websocket, "owner@test.com")
        # Admin should not raise
        _validate_session_ownership(session, {"sub": "admin@test.com", "is_admin": True}, "test")

    def test_owner_match_allows_access(self, mock_websocket):
        from mcpgateway.routers.reverse_proxy import _validate_session_ownership
        session = ReverseProxySession("test-id", mock_websocket, "owner@test.com")
        _validate_session_ownership(session, {"sub": "owner@test.com"}, "test")

    def test_owner_match_dict_user(self, mock_websocket):
        from mcpgateway.routers.reverse_proxy import _validate_session_ownership
        session = ReverseProxySession("test-id", mock_websocket, {"sub": "owner@test.com"})
        _validate_session_ownership(session, {"sub": "owner@test.com"}, "test")

    def test_non_owner_denied(self, mock_websocket):
        from mcpgateway.routers.reverse_proxy import _validate_session_ownership
        from fastapi import HTTPException
        session = ReverseProxySession("test-id", mock_websocket, "owner@test.com")
        with pytest.raises(HTTPException) as exc_info:
            _validate_session_ownership(session, {"sub": "other@test.com"}, "disconnect")
        assert exc_info.value.status_code == 403


class TestWebSocketAuthEdgeCases:
    """Test WebSocket authentication edge cases."""

    @pytest.mark.asyncio
    async def test_websocket_bearer_auth_http_exception(self, mock_websocket):
        """JWT verification raises HTTPException."""
        from fastapi import HTTPException
        from mcpgateway.routers.reverse_proxy import websocket_endpoint

        mock_websocket.headers = {"Authorization": "Bearer bad-token"}
        mock_websocket.query_params = {}

        with patch("mcpgateway.routers.reverse_proxy.settings") as mock_settings:
            mock_settings.auth_required = True
            mock_settings.mcp_client_auth_enabled = False
            mock_settings.trust_proxy_auth = False

            with patch("mcpgateway.routers.reverse_proxy.get_current_user") as mock_get_user:
                mock_get_user.side_effect = HTTPException(status_code=401, detail="Invalid token")
                await websocket_endpoint(mock_websocket, Mock())

        mock_websocket.accept.assert_not_called()
        mock_websocket.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_websocket_bearer_auth_general_exception(self, mock_websocket):
        """JWT verification raises generic exception."""
        from mcpgateway.routers.reverse_proxy import websocket_endpoint

        mock_websocket.headers = {"Authorization": "Bearer bad-token"}
        mock_websocket.query_params = {}

        with patch("mcpgateway.routers.reverse_proxy.settings") as mock_settings:
            mock_settings.auth_required = True
            mock_settings.mcp_client_auth_enabled = False
            mock_settings.trust_proxy_auth = False

            with patch("mcpgateway.routers.reverse_proxy.get_current_user") as mock_get_user:
                mock_get_user.side_effect = ValueError("Malformed token")
                await websocket_endpoint(mock_websocket, Mock())

        mock_websocket.accept.assert_not_called()
        mock_websocket.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_websocket_query_token_http_exception(self, mock_websocket):
        """Query token verification raises HTTPException."""
        from fastapi import HTTPException
        from mcpgateway.routers.reverse_proxy import websocket_endpoint

        mock_websocket.headers = {}
        mock_websocket.query_params = {"token": "bad-query-token"}

        with patch("mcpgateway.routers.reverse_proxy.settings") as mock_settings:
            mock_settings.auth_required = True
            mock_settings.mcp_client_auth_enabled = False
            mock_settings.trust_proxy_auth = False

            with patch("mcpgateway.routers.reverse_proxy.get_current_user") as mock_get_user:
                mock_get_user.side_effect = HTTPException(status_code=401, detail="Invalid token")
                await websocket_endpoint(mock_websocket, Mock())

        mock_websocket.accept.assert_not_called()
        mock_websocket.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_websocket_query_token_general_exception(self, mock_websocket):
        """Query token verification raises generic exception."""
        from mcpgateway.routers.reverse_proxy import websocket_endpoint

        mock_websocket.headers = {}
        mock_websocket.query_params = {"token": "bad-query-token"}

        with patch("mcpgateway.routers.reverse_proxy.settings") as mock_settings:
            mock_settings.auth_required = True
            mock_settings.mcp_client_auth_enabled = False
            mock_settings.trust_proxy_auth = False

            with patch("mcpgateway.routers.reverse_proxy.get_current_user") as mock_get_user:
                mock_get_user.side_effect = ValueError("Bad token")
                await websocket_endpoint(mock_websocket, Mock())

        mock_websocket.accept.assert_not_called()
        mock_websocket.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_websocket_proxy_auth_no_header(self, mock_websocket):
        """Proxy auth enabled but no proxy header → reject."""
        from mcpgateway.routers.reverse_proxy import websocket_endpoint

        mock_websocket.headers = {}
        mock_websocket.query_params = {}

        with patch("mcpgateway.routers.reverse_proxy.settings") as mock_settings:
            mock_settings.auth_required = True
            mock_settings.mcp_client_auth_enabled = False
            mock_settings.trust_proxy_auth = True
            mock_settings.trust_proxy_auth_dangerously = True
            mock_settings.proxy_user_header = "X-Authenticated-User"

            await websocket_endpoint(mock_websocket, Mock())

        mock_websocket.accept.assert_not_called()
        mock_websocket.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_websocket_disconnect_exception(self, mock_websocket):
        """WebSocketDisconnect during message loop."""
        from mcpgateway.routers.reverse_proxy import websocket_endpoint

        mock_websocket.headers = {}
        mock_websocket.query_params = {}
        mock_websocket.receive_text.side_effect = WebSocketDisconnect()

        with patch("mcpgateway.routers.reverse_proxy.settings") as mock_settings:
            mock_settings.auth_required = False
            mock_settings.mcp_client_auth_enabled = False

            await websocket_endpoint(mock_websocket, Mock())

        # Should have accepted and then cleanly disconnected
        mock_websocket.accept.assert_called_once()


class TestListSessionsFiltering:
    """Test session filtering by user role."""

    @pytest.fixture
    def admin_client(self):
        from fastapi import FastAPI
        app = FastAPI()
        def mock_require_auth():
            return {"sub": "admin@test.com", "is_admin": True}
        app.dependency_overrides[require_auth] = mock_require_auth
        app.include_router(router)
        return TestClient(app)

    @pytest.fixture
    def user_client(self):
        from fastapi import FastAPI
        app = FastAPI()
        def mock_require_auth():
            return {"sub": "user@test.com", "is_admin": False}
        app.dependency_overrides[require_auth] = mock_require_auth
        app.include_router(router)
        return TestClient(app)

    def test_admin_sees_all_sessions(self, admin_client, mock_websocket):
        """Admin user sees all sessions."""
        manager.sessions.clear()
        s1 = ReverseProxySession("s1", mock_websocket, "user1@test.com")
        s2 = ReverseProxySession("s2", mock_websocket, "user2@test.com")
        manager.sessions["s1"] = s1
        manager.sessions["s2"] = s2

        try:
            response = admin_client.get("/reverse-proxy/sessions")
            assert response.status_code == 200
            data = response.json()
            assert data["total"] == 2
        finally:
            manager.sessions.clear()

    def test_user_sees_own_and_anonymous(self, user_client, mock_websocket):
        """Regular user sees own sessions + anonymous ones."""
        manager.sessions.clear()
        s1 = ReverseProxySession("s1", mock_websocket, "user@test.com")
        s2 = ReverseProxySession("s2", mock_websocket, "other@test.com")
        s3 = ReverseProxySession("s3", mock_websocket, None)  # anonymous
        manager.sessions["s1"] = s1
        manager.sessions["s2"] = s2
        manager.sessions["s3"] = s3

        try:
            response = user_client.get("/reverse-proxy/sessions")
            assert response.status_code == 200
            data = response.json()
            # Should see own (s1) + anonymous (s3), not other's (s2)
            assert data["total"] == 2
            session_ids = [s["session_id"] for s in data["sessions"]]
            assert "s1" in session_ids
            assert "s3" in session_ids
            assert "s2" not in session_ids
        finally:
            manager.sessions.clear()


# ---------------------------------------------------------------------------
# Token missing subject claim tests
# ---------------------------------------------------------------------------


class TestWebSocketTokenMissingSubject:
    """Tests for token payloads missing sub/email claim."""

    @pytest.mark.asyncio
    async def test_bearer_token_missing_subject(self, mock_websocket):
        """Bearer token auth failure is rejected."""
        from mcpgateway.routers.reverse_proxy import websocket_endpoint

        mock_websocket.headers = {"Authorization": "Bearer valid-token"}
        mock_websocket.query_params = {}

        with patch("mcpgateway.routers.reverse_proxy.settings") as mock_settings:
            mock_settings.auth_required = True
            mock_settings.mcp_client_auth_enabled = False
            mock_settings.trust_proxy_auth = False

            with patch("mcpgateway.routers.reverse_proxy.get_current_user") as mock_get_user:
                mock_get_user.side_effect = HTTPException(status_code=401, detail="Invalid token")
                await websocket_endpoint(mock_websocket, Mock())

        mock_websocket.accept.assert_not_called()
        mock_websocket.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_query_token_missing_subject(self, mock_websocket):
        """Query token auth failure is rejected."""
        from mcpgateway.routers.reverse_proxy import websocket_endpoint

        mock_websocket.headers = {}
        mock_websocket.query_params = {"token": "valid-query-token"}

        with patch("mcpgateway.routers.reverse_proxy.settings") as mock_settings:
            mock_settings.auth_required = True
            mock_settings.mcp_client_auth_enabled = False
            mock_settings.trust_proxy_auth = False

            with patch("mcpgateway.routers.reverse_proxy.get_current_user") as mock_get_user:
                mock_get_user.side_effect = HTTPException(status_code=401, detail="Invalid token")
                await websocket_endpoint(mock_websocket, Mock())

        mock_websocket.accept.assert_not_called()
        mock_websocket.close.assert_called_once()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])


class TestCrossWorkerForwarding:
    """Test cross-worker session affinity forwarding via Redis Pub/Sub.

    These tests exercise the path where a tools/call HTTP request lands on a
    worker that does NOT own the WebSocket session.  The non-owner worker must:
      1. Detect it is not the owner (Redis GET returns a different WORKER_ID)
      2. Publish the message to the owner's Redis channel
      3. Wait for the response on a unique response channel

    The owner worker (via start_rpc_listener) must:
      1. Receive the ``reverse_proxy_forward`` message
      2. Dispatch to ``execute_forwarded_message()``
      3. Send the message to the local WebSocket session
      4. Wait for the agent response via ``_wait_for_response()``
      5. Publish the response back to the response channel

    Both sides are tested here with mocked Redis so no live multi-worker
    deployment is required.
    """

    @pytest.mark.asyncio
    async def test_execute_forwarded_message_success(self, mock_websocket):
        """Owner worker executes a forwarded request and publishes the response.

        The real flow: execute_forwarded_message() calls _wait_for_response() which
        registers a Future in pending_responses[request_id].  The WebSocket message
        loop resolves that future when the agent replies.  We simulate this by running
        a concurrent task that polls pending_responses until the key appears, then
        sets the result – exactly as the real message loop does.
        """
        # Standard Library
        import asyncio

        # First-Party
        from mcpgateway.routers.reverse_proxy import ReverseProxyManager, ReverseProxySession, pending_responses

        owner_manager = ReverseProxyManager()
        session = ReverseProxySession("sess-owner", mock_websocket, "user@test.com")
        await owner_manager.add_session(session)

        expected_response = {"type": "response", "payload": {"result": "ok"}, "sessionId": "sess-owner"}

        async def _simulate_agent_reply():
            """Poll pending_responses until req-001 is registered, then resolve it."""
            for _ in range(100):
                if "req-001" in pending_responses:
                    pending_responses["req-001"].set_result(expected_response)
                    return
                await asyncio.sleep(0.01)

        mock_redis = AsyncMock()

        forward_data = {
            "type": "reverse_proxy_forward",
            "session_id": "sess-owner",
            "message": {"type": "request", "payload": {"method": "tools/call", "id": "req-001"}},
            "response_channel": "mcpgw:reverse_proxy_response:abc123",
            "original_worker": "other-host:9999",
        }

        # Run both concurrently: execute_forwarded_message waits for the future;
        # _simulate_agent_reply resolves it once registered.
        await asyncio.gather(
            owner_manager.execute_forwarded_message(forward_data, mock_redis),
            _simulate_agent_reply(),
        )

        # Owner must have published the response to the response channel
        mock_redis.publish.assert_called_once()
        channel_arg, payload_arg = mock_redis.publish.call_args[0]
        assert channel_arg == "mcpgw:reverse_proxy_response:abc123"
        published = orjson.loads(payload_arg)
        assert published == expected_response

    @pytest.mark.asyncio
    async def test_execute_forwarded_message_session_not_found(self):
        """Owner worker publishes error when session is not found locally."""
        # First-Party
        from mcpgateway.routers.reverse_proxy import ReverseProxyManager

        owner_manager = ReverseProxyManager()
        # Session NOT added – simulates request arriving on wrong worker

        mock_redis = AsyncMock()

        forward_data = {
            "type": "reverse_proxy_forward",
            "session_id": "missing-session",
            "message": {"type": "request", "payload": {"method": "tools/call", "id": "req-002"}},
            "response_channel": "mcpgw:reverse_proxy_response:def456",
            "original_worker": "other-host:9999",
        }

        await owner_manager.execute_forwarded_message(forward_data, mock_redis)

        # Must publish an error response so the non-owner worker doesn't hang
        mock_redis.publish.assert_called_once()
        channel_arg, payload_arg = mock_redis.publish.call_args[0]
        assert channel_arg == "mcpgw:reverse_proxy_response:def456"
        published = orjson.loads(payload_arg)
        assert published["status"] == "error"
        assert "missing-session" in published["error"]

    @pytest.mark.asyncio
    async def test_execute_forwarded_notification_no_response_wait(self, mock_websocket):
        """Owner worker sends notification without waiting for a response."""
        # First-Party
        from mcpgateway.routers.reverse_proxy import ReverseProxyManager, ReverseProxySession

        owner_manager = ReverseProxyManager()
        session = ReverseProxySession("sess-notif", mock_websocket, "user@test.com")
        await owner_manager.add_session(session)

        mock_redis = AsyncMock()

        # Notification: no ``id`` field in payload → is_notification=True
        forward_data = {
            "type": "reverse_proxy_forward",
            "session_id": "sess-notif",
            "message": {"type": "notification", "payload": {"method": "notifications/initialized"}},
            "response_channel": "mcpgw:reverse_proxy_response:ghi789",
            "original_worker": "other-host:9999",
        }

        await owner_manager.execute_forwarded_message(forward_data, mock_redis)

        # Must publish notification_sent ack (no agent response wait)
        mock_redis.publish.assert_called_once()
        channel_arg, payload_arg = mock_redis.publish.call_args[0]
        assert channel_arg == "mcpgw:reverse_proxy_response:ghi789"
        published = orjson.loads(payload_arg)
        assert published["status"] == "notification_sent"

    @pytest.mark.asyncio
    async def test_forward_request_to_session_publishes_to_owner_channel(self, mock_websocket):
        """Non-owner worker publishes to the correct owner Redis channel via forward_message_to_owner."""
        # Standard Library
        import asyncio

        # First-Party
        import orjson as _orjson
        from mcpgateway.routers.reverse_proxy import ReverseProxyManager

        non_owner_manager = ReverseProxyManager()

        expected_response = {"type": "response", "payload": {"result": "forwarded-ok"}}

        # Build a mock Redis that:
        # - Returns the owner worker ID from GET (ownership check in get_session_owner)
        # - Simulates a pubsub that immediately delivers the response message
        mock_pubsub = AsyncMock()
        mock_pubsub.subscribe = AsyncMock()
        mock_pubsub.unsubscribe = AsyncMock()

        # get_message must yield to the event loop so asyncio.timeout() can fire.
        # Use a coroutine side_effect that includes asyncio.sleep(0).
        _responses = [{"type": "message", "data": _orjson.dumps(expected_response)}, None]
        _call_count = [0]

        async def _get_message_side_effect(**kwargs):
            await asyncio.sleep(0)  # yield to event loop
            idx = _call_count[0]
            _call_count[0] += 1
            if idx < len(_responses):
                return _responses[idx]
            return None

        mock_pubsub.get_message = _get_message_side_effect

        mock_redis = AsyncMock()
        # get_session_owner calls redis.get(owner_key) → returns owner worker ID
        mock_redis.get = AsyncMock(return_value=b"owner-host:1234")
        mock_redis.pubsub = MagicMock(return_value=mock_pubsub)

        # forward_message_to_owner(session_id, message) – the method that does Redis Pub/Sub
        message = {"type": "request", "sessionId": "sess-remote", "payload": {"method": "tools/call", "id": "req-003"}}

        with patch("mcpgateway.utils.redis_client.get_redis_client", return_value=mock_redis):
            result = await non_owner_manager.forward_message_to_owner("sess-remote", message, timeout=5.0)

        # Must have published to the owner's channel (mcpgw:reverse_proxy:{owner_worker_id})
        mock_redis.publish.assert_called_once()
        channel_arg, payload_arg = mock_redis.publish.call_args[0]
        assert channel_arg == "mcpgw:reverse_proxy:owner-host:1234"
        published = _orjson.loads(payload_arg)
        assert published["type"] == "reverse_proxy_forward"
        assert published["session_id"] == "sess-remote"
        assert published["message"] == message

        # Must return the response received from the owner via pubsub
        assert result == expected_response

    @pytest.mark.asyncio
    async def test_forward_request_to_session_timeout(self, mock_websocket):
        """Non-owner worker raises TimeoutError when owner doesn't respond."""
        # Standard Library
        import asyncio

        # First-Party
        from mcpgateway.routers.reverse_proxy import ReverseProxyManager

        non_owner_manager = ReverseProxyManager()

        mock_pubsub = AsyncMock()
        mock_pubsub.subscribe = AsyncMock()
        mock_pubsub.unsubscribe = AsyncMock()
        # Never delivers a message → timeout.
        # Must yield to the event loop so asyncio.timeout() can actually fire.
        async def _never_respond(**kwargs):
            await asyncio.sleep(0)  # yield to event loop
            return None

        mock_pubsub.get_message = _never_respond

        mock_redis = AsyncMock()
        mock_redis.get = AsyncMock(return_value=b"owner-host:1234")
        mock_redis.pubsub = MagicMock(return_value=mock_pubsub)

        message = {"type": "request", "sessionId": "sess-remote", "payload": {"method": "tools/call", "id": "req-004"}}

        with patch("mcpgateway.utils.redis_client.get_redis_client", return_value=mock_redis):
            with pytest.raises(asyncio.TimeoutError):
                await non_owner_manager.forward_message_to_owner("sess-remote", message, timeout=0.1)

        # Must have unsubscribed from the response channel even on timeout
        mock_pubsub.unsubscribe.assert_called_once()
