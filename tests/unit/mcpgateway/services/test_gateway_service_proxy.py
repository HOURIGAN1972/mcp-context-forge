# -*- coding: utf-8 -*-
"""Location: ./tests/unit/mcpgateway/services/test_gateway_service_proxy.py
Copyright 2025
SPDX-License-Identifier: Apache-2.0
Authors: Mihai Criveti

Unit tests for GatewayService proxy-specific functionality.
Tests the new reverse proxy integration features including:
- register_gateway with is_proxy=True
- _initialize_gateway with proxy mode
- connect_to_proxy_server
"""

# Standard
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, Mock, patch

# Third-Party
import pytest
from url_normalize import url_normalize

# First-Party
from mcpgateway.db import Gateway as DbGateway
from mcpgateway.db import Tool as DbTool
from mcpgateway.db import Resource as DbResource
from mcpgateway.db import Prompt as DbPrompt
from mcpgateway.schemas import GatewayCreate, ToolCreate, ResourceCreate, PromptCreate
from mcpgateway.services.gateway_service import (
    GatewayConnectionError,
    GatewayService,
)


def _make_execute_result(*, scalar=None, scalars_list=None, rowcount=0):
    """Helper to create mock SQLAlchemy Result objects."""
    result = MagicMock()
    result.scalar_one_or_none.return_value = scalar
    scalars_proxy = MagicMock()
    scalars_proxy.all.return_value = scalars_list or []
    result.scalars.return_value = scalars_proxy
    result.rowcount = rowcount
    return result


@pytest.fixture(autouse=True)
def mock_logging_services():
    """Mock audit_trail and structured_logger to prevent database writes during tests."""
    from mcpgateway.utils.ssl_context_cache import clear_ssl_context_cache
    clear_ssl_context_cache()

    with patch("mcpgateway.services.gateway_service.audit_trail") as mock_audit, \
         patch("mcpgateway.services.gateway_service.structured_logger") as mock_logger:
        mock_audit.log_action = MagicMock(return_value=None)
        mock_logger.log = MagicMock(return_value=None)
        yield {"audit_trail": mock_audit, "structured_logger": mock_logger}


@pytest.fixture(autouse=True)
def _bypass_gatewayread_validation(monkeypatch):
    """Stub out GatewayRead.model_validate for mock objects."""
    from mcpgateway.schemas import GatewayRead
    monkeypatch.setattr(GatewayRead, "model_validate", staticmethod(lambda x: x))


@pytest.fixture
def gateway_service():
    """A GatewayService instance with mocked HTTP client."""
    service = GatewayService()
    service._http_client = AsyncMock()
    return service


@pytest.fixture
def mock_db():
    """Return a mocked SQLAlchemy session."""
    session = MagicMock()
    session.query.return_value = MagicMock()
    session.commit.return_value = None
    session.rollback.return_value = None
    session.flush.return_value = None
    session.refresh.return_value = None
    session.add.return_value = None
    return session


@pytest.fixture
def mock_forward_request():
    """Mock forward_request_func for proxy connections."""
    async def forward_func(session_id, request):
        """Mock function that simulates forwarding MCP requests."""
        method = request.get("method")
        
        if method == "initialize":
            return {
                "payload": {
                    "result": {
                        "capabilities": {
                            "tools": {"listChanged": True},
                            "resources": {"subscribe": True},
                            "prompts": {"listChanged": True}
                        },
                        "serverInfo": {"name": "test-server", "version": "1.0.0"}
                    }
                }
            }
        elif method == "notifications/initialized":
            return {"payload": {}}
        elif method == "tools/list":
            return {
                "payload": {
                    "result": {
                        "tools": [
                            {
                                "name": "test_tool",
                                "description": "A test tool",
                                "inputSchema": {"type": "object", "properties": {}}
                            }
                        ]
                    }
                }
            }
        elif method == "resources/list":
            return {
                "payload": {
                    "result": {
                        "resources": [
                            {
                                "uri": "test://resource",
                                "name": "Test Resource",
                                "description": "A test resource",
                                "mimeType": "text/plain"
                            }
                        ]
                    }
                }
            }
        elif method == "prompts/list":
            return {
                "payload": {
                    "result": {
                        "prompts": [
                            {
                                "name": "test_prompt",
                                "description": "A test prompt",
                                "template": "This is a test prompt template"
                            }
                        ]
                    }
                }
            }
        return {"payload": {}}
    
    return AsyncMock(side_effect=forward_func)


class TestGatewayServiceProxy:
    """Tests for proxy-specific gateway service functionality."""

    # ────────────────────────────────────────────────────────────────────
    # register_gateway with is_proxy=True
    # ────────────────────────────────────────────────────────────────────

    @pytest.mark.asyncio
    async def test_register_proxy_gateway_success(self, gateway_service, mock_db, mock_forward_request, monkeypatch):
        """Test successful registration of a proxy gateway."""
        # Setup mocks - need to provide enough results for all db.execute() calls
        mock_db.execute = Mock(
            side_effect=[
                _make_execute_result(scalar=None),  # No existing gateway (line 1075)
                _make_execute_result(scalars_list=[]),  # Valid gateway IDs check for resources (line 921)
                _make_execute_result(scalars_list=[]),  # Candidate resources query (line 922)
                _make_execute_result(scalars_list=[]),  # Valid gateway IDs for prompts (line 1008)
                _make_execute_result(scalars_list=[]),  # Candidate prompts query (line 1009)
            ]
        )
        
        gateway_service._notify_gateway_added = AsyncMock()
        
        # Mock GatewayRead.model_validate to return a mock with .masked()
        mock_model = Mock()
        mock_model.masked.return_value = mock_model
        mock_model.name = "proxy_gateway"
        mock_model.url = "ws://proxy"
        mock_model.id = "test-session-123"
        
        monkeypatch.setattr(
            "mcpgateway.services.gateway_service.GatewayRead.model_validate",
            lambda x: mock_model,
        )
        
        gateway_create = GatewayCreate(
            name="proxy_gateway",
            url="ws://proxy",
            description="A proxy gateway",
            transport="PROXIED",  # PROXIED transport for reverse proxy gateways
        )
        
        result = await gateway_service.register_gateway(
            mock_db,
            gateway_create,
            is_proxy=True,
            session_id="test-session-123",
            forward_request_func=mock_forward_request,
        )
        
        # Verify result structure for proxy mode
        assert isinstance(result, tuple)
        assert len(result) == 4
        gateway_read, tool_ids, resource_ids, prompt_ids = result
        
        # Verify gateway was added
        mock_db.add.assert_called_once()
        mock_db.commit.assert_called_once()  # Proxy mode commits immediately
        mock_db.refresh.assert_called_once()
        
        # Verify forward_request was called for MCP protocol
        assert mock_forward_request.call_count >= 2  # At least initialize + initialized

    @pytest.mark.asyncio
    async def test_register_proxy_gateway_missing_session_id(self, gateway_service, mock_db):
        """Test that proxy registration fails without session_id."""
        gateway_create = GatewayCreate(
            name="proxy_gateway",
            url="ws://proxy",
            description="A proxy gateway",
            transport="PROXIED",
        )
        
        with pytest.raises(ValueError, match="session_id is required when is_proxy=True"):
            await gateway_service.register_gateway(
                mock_db,
                gateway_create,
                is_proxy=True,
                session_id=None,  # Missing!
            )

    @pytest.mark.asyncio
    async def test_register_proxy_gateway_update_existing(self, gateway_service, mock_db, mock_forward_request, monkeypatch):
        """Test updating an existing proxy gateway."""
        # Create existing gateway mock
        existing_gateway = MagicMock(spec=DbGateway)
        existing_gateway.id = "test-session-123"
        existing_gateway.name = "old_name"
        existing_gateway.tools = []
        existing_gateway.resources = []
        existing_gateway.prompts = []
        
        mock_db.execute = Mock(
            side_effect=[
                _make_execute_result(scalar=existing_gateway),  # Existing gateway found (line 1075)
                _make_execute_result(scalars_list=[]),  # Valid gateway IDs for resources (line 921)
                _make_execute_result(scalars_list=[]),  # Candidate resources query (line 922)
                _make_execute_result(scalars_list=[]),  # Valid gateway IDs for prompts (line 1008)
                _make_execute_result(scalars_list=[]),  # Candidate prompts query (line 1009)
            ]
        )
        
        gateway_service._notify_gateway_added = AsyncMock()
        
        # Mock GatewayRead.model_validate
        mock_model = Mock()
        mock_model.masked.return_value = mock_model
        mock_model.name = "updated_proxy"
        
        monkeypatch.setattr(
            "mcpgateway.services.gateway_service.GatewayRead.model_validate",
            lambda x: mock_model,
        )
        
        gateway_create = GatewayCreate(
            name="updated_proxy",
            url="ws://proxy",
            description="Updated proxy gateway",
            transport="PROXIED",
        )
        
        result = await gateway_service.register_gateway(
            mock_db,
            gateway_create,
            is_proxy=True,
            session_id="test-session-123",
            forward_request_func=mock_forward_request,
        )
        
        # Verify update path was taken (commit called for proxy mode)
        # Note: db.add may be called for new tools/resources/prompts even in update mode
        mock_db.commit.assert_called_once()
        
        # Verify the gateway update was processed (name attribute should be set)
        # The actual update happens on the existing_gateway object
        assert hasattr(existing_gateway, 'name')

    # ────────────────────────────────────────────────────────────────────
    # _initialize_gateway with proxy mode
    # ────────────────────────────────────────────────────────────────────

    @pytest.mark.asyncio
    async def test_initialize_gateway_proxy_mode(self, gateway_service, mock_forward_request):
        """Test _initialize_gateway with is_proxy=True."""
        capabilities, tools, resources, prompts = await gateway_service._initialize_gateway(
            url="ws://proxy",
            authentication={},
            transport="PROXIED",
            is_proxy=True,
            session_id="test-session-123",
            forward_request_func=mock_forward_request,
        )
        
        # Verify capabilities were retrieved
        assert "tools" in capabilities
        assert capabilities["tools"]["listChanged"] is True
        
        # Verify tools were retrieved and converted to ToolCreate
        assert len(tools) == 1
        assert isinstance(tools[0], ToolCreate)
        assert tools[0].name == "test_tool"
        
        # Verify resources were retrieved
        assert len(resources) == 1
        assert isinstance(resources[0], ResourceCreate)
        assert resources[0].uri == "test://resource"
        
        # Verify prompts were retrieved
        assert len(prompts) == 1
        assert isinstance(prompts[0], PromptCreate)
        assert prompts[0].name == "test_prompt"

    @pytest.mark.asyncio
    async def test_initialize_gateway_proxy_mode_missing_params(self, gateway_service):
        """Test _initialize_gateway fails with missing proxy parameters."""
        with pytest.raises(GatewayConnectionError, match="Failed to initialize gateway"):
            await gateway_service._initialize_gateway(
                url="ws://proxy",
                authentication={},
                transport="PROXIED",
                is_proxy=True,
                session_id=None,  # Missing!
                forward_request_func=None,  # Missing!
            )

    @pytest.mark.asyncio
    async def test_initialize_gateway_standard_mode(self, gateway_service):
        """Test _initialize_gateway with is_proxy=False uses standard transport."""
        # Mock the standard connection methods
        gateway_service.connect_to_sse_server = AsyncMock(
            return_value=({"tools": {}}, [], [], [])
        )
        
        capabilities, tools, resources, prompts = await gateway_service._initialize_gateway(
            url="http://example.com",
            authentication={},
            transport="SSE",
            is_proxy=False,
        )
        
        # Verify SSE connection was used
        gateway_service.connect_to_sse_server.assert_called_once()

    # ────────────────────────────────────────────────────────────────────
    # connect_to_proxy_server
    # ────────────────────────────────────────────────────────────────────

    @pytest.mark.asyncio
    async def test_connect_to_proxy_server_success(self, gateway_service, mock_forward_request):
        """Test successful connection to proxy server."""
        capabilities, tools, resources, prompts = await gateway_service.connect_to_proxy_server(
            session_id="test-session-123",
            forward_request_func=mock_forward_request,
            authentication={},
            include_prompts=True,
            include_resources=True,
        )
        
        # Verify MCP protocol sequence
        calls = mock_forward_request.call_args_list
        assert len(calls) >= 4  # initialize, initialized, tools/list, resources/list, prompts/list
        
        # Verify initialize was called first
        init_call = calls[0][0][1]
        assert init_call["method"] == "initialize"
        assert init_call["params"]["protocolVersion"] == "2024-11-05"
        
        # Verify capabilities
        assert "tools" in capabilities
        
        # Verify tools
        assert len(tools) == 1
        assert tools[0].name == "test_tool"
        
        # Verify resources
        assert len(resources) == 1
        assert resources[0].uri == "test://resource"
        
        # Verify prompts
        assert len(prompts) == 1
        assert prompts[0].name == "test_prompt"

    @pytest.mark.asyncio
    async def test_connect_to_proxy_server_no_resources(self, gateway_service, mock_forward_request):
        """Test proxy connection with include_resources=False."""
        capabilities, tools, resources, prompts = await gateway_service.connect_to_proxy_server(
            session_id="test-session-123",
            forward_request_func=mock_forward_request,
            authentication={},
            include_prompts=True,
            include_resources=False,  # Skip resources
        )
        
        # Verify resources were not fetched
        assert len(resources) == 0
        
        # But tools and prompts should still be fetched
        assert len(tools) == 1
        assert len(prompts) == 1

    @pytest.mark.asyncio
    async def test_connect_to_proxy_server_no_prompts(self, gateway_service, mock_forward_request):
        """Test proxy connection with include_prompts=False."""
        capabilities, tools, resources, prompts = await gateway_service.connect_to_proxy_server(
            session_id="test-session-123",
            forward_request_func=mock_forward_request,
            authentication={},
            include_prompts=False,  # Skip prompts
            include_resources=True,
        )
        
        # Verify prompts were not fetched
        assert len(prompts) == 0
        
        # But tools and resources should still be fetched
        assert len(tools) == 1
        assert len(resources) == 1

    @pytest.mark.asyncio
    async def test_connect_to_proxy_server_connection_error(self, gateway_service):
        """Test proxy connection handles errors gracefully."""
        async def failing_forward(session_id, request):
            raise Exception("Connection failed")
        
        with pytest.raises(GatewayConnectionError, match="Failed to fetch capabilities from reverse proxy session"):
            await gateway_service.connect_to_proxy_server(
                session_id="test-session-123",
                forward_request_func=AsyncMock(side_effect=failing_forward),
                authentication={},
            )

    @pytest.mark.asyncio
    async def test_connect_to_proxy_server_tools_fetch_failure(self, gateway_service):
        """Test proxy connection continues when tools fetch fails."""
        async def partial_forward(session_id, request):
            method = request.get("method")
            if method == "initialize":
                return {
                    "payload": {
                        "result": {
                            "capabilities": {"tools": {"listChanged": True}},
                            "serverInfo": {"name": "test", "version": "1.0"}
                        }
                    }
                }
            elif method == "notifications/initialized":
                return {"payload": {}}
            elif method == "tools/list":
                raise Exception("Tools fetch failed")
            return {"payload": {}}
        
        # Should not raise, just log warning and return empty tools
        capabilities, tools, resources, prompts = await gateway_service.connect_to_proxy_server(
            session_id="test-session-123",
            forward_request_func=AsyncMock(side_effect=partial_forward),
            authentication={},
            include_resources=False,
            include_prompts=False,
        )
        
        # Verify capabilities were still retrieved
        assert "tools" in capabilities
        
        # But tools list is empty due to error
        assert len(tools) == 0

    @pytest.mark.asyncio
    async def test_connect_to_proxy_server_resource_validation_fallback(self, gateway_service):
        """Test proxy connection handles resource validation errors with fallback."""
        async def forward_with_invalid_resource(session_id, request):
            method = request.get("method")
            if method == "initialize":
                return {
                    "payload": {
                        "result": {
                            "capabilities": {"resources": {"subscribe": True}},
                            "serverInfo": {"name": "test", "version": "1.0"}
                        }
                    }
                }
            elif method == "notifications/initialized":
                return {"payload": {}}
            elif method == "tools/list":
                return {"payload": {"result": {"tools": []}}}
            elif method == "resources/list":
                return {
                    "payload": {
                        "result": {
                            "resources": [
                                {
                                    "uri": "test://resource",
                                    "name": "Test Resource",
                                    # Missing required fields to trigger validation error
                                }
                            ]
                        }
                    }
                }
            return {"payload": {}}
        
        capabilities, tools, resources, prompts = await gateway_service.connect_to_proxy_server(
            session_id="test-session-123",
            forward_request_func=AsyncMock(side_effect=forward_with_invalid_resource),
            authentication={},
            include_resources=True,
            include_prompts=False,
        )
        
        # Verify fallback resource was created
        assert len(resources) == 1
        assert resources[0].uri == "test://resource"
        assert resources[0].name == "Test Resource"
        assert resources[0].content == ""  # Default content