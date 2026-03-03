# ruff: noqa: INP001
"""Tests for idempotency key persistence in gateway RPC calls."""

from __future__ import annotations

from uuid import uuid4

import pytest

from app.services.openclaw.gateway_rpc import GatewayConfig


@pytest.mark.asyncio
async def test_send_message_uses_provided_idempotency_key(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """send_message should use the provided idempotency_key, not generate a new one."""
    import app.services.openclaw.gateway_rpc as gateway_rpc_module

    captured_params: list[dict[str, object]] = []

    async def _mock_openclaw_call(
        method: str,
        params: dict[str, object] | None = None,
        *,
        config: GatewayConfig,
    ) -> object:
        captured_params.append({"method": method, "params": params})
        return {"ok": True}

    monkeypatch.setattr(gateway_rpc_module, "openclaw_call", _mock_openclaw_call)

    config = GatewayConfig(url="wss://gateway.example.com", token="test-token")
    idempotency_key = str(uuid4())

    await gateway_rpc_module.send_message(
        "Hello, agent!",
        session_key="session:123",
        config=config,
        idempotency_key=idempotency_key,
    )

    assert len(captured_params) == 1
    params = captured_params[0]["params"]
    assert params is not None
    assert params.get("idempotencyKey") == idempotency_key


@pytest.mark.asyncio
async def test_send_message_generates_key_when_not_provided(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """send_message should generate a new idempotency_key when not provided."""
    import app.services.openclaw.gateway_rpc as gateway_rpc_module

    captured_params: list[dict[str, object]] = []

    async def _mock_openclaw_call(
        method: str,
        params: dict[str, object] | None = None,
        *,
        config: GatewayConfig,
    ) -> object:
        captured_params.append({"method": method, "params": params})
        return {"ok": True}

    monkeypatch.setattr(gateway_rpc_module, "openclaw_call", _mock_openclaw_call)

    config = GatewayConfig(url="wss://gateway.example.com", token="test-token")

    await gateway_rpc_module.send_message(
        "Hello, agent!",
        session_key="session:123",
        config=config,
        # No idempotency_key provided
    )

    assert len(captured_params) == 1
    params = captured_params[0]["params"]
    assert params is not None
    # Should have a generated key
    assert "idempotencyKey" in params
    assert params["idempotencyKey"] is not None


@pytest.mark.asyncio
async def test_retry_with_same_idempotency_key(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Retries should use the same idempotency key to enable server-side deduplication."""
    import app.services.openclaw.gateway_rpc as gateway_rpc_module

    captured_keys: list[str] = []

    async def _mock_openclaw_call(
        method: str,
        params: dict[str, object] | None = None,
        *,
        config: GatewayConfig,
    ) -> object:
        if params:
            captured_keys.append(str(params.get("idempotencyKey", "")))
        return {"ok": True}

    monkeypatch.setattr(gateway_rpc_module, "openclaw_call", _mock_openclaw_call)

    config = GatewayConfig(url="wss://gateway.example.com", token="test-token")
    idempotency_key = str(uuid4())

    # Simulate retry scenario - call send_message multiple times with same key
    for _ in range(3):
        await gateway_rpc_module.send_message(
            "Hello, agent!",
            session_key="session:123",
            config=config,
            idempotency_key=idempotency_key,
        )

    # All calls should use the same idempotency key
    assert len(captured_keys) == 3
    assert all(key == idempotency_key for key in captured_keys)


@pytest.mark.asyncio
async def test_gateway_dispatch_service_passes_idempotency_key(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """GatewayDispatchService.send_agent_message should pass idempotency_key through."""
    import app.services.openclaw.gateway_dispatch as gateway_dispatch_module

    captured_send_params: list[dict[str, object]] = []

    async def _mock_send_message(
        message: str,
        *,
        session_key: str,
        config: GatewayConfig,
        deliver: bool = False,
        idempotency_key: str | None = None,
    ) -> object:
        captured_send_params.append({
            "message": message,
            "session_key": session_key,
            "deliver": deliver,
            "idempotency_key": idempotency_key,
        })
        return {"ok": True}

    async def _mock_ensure_session(
        session_key: str,
        *,
        config: GatewayConfig,
        label: str | None = None,
    ) -> object:
        return {"ok": True}

    # Patch at the gateway_dispatch module level since it imports these functions directly
    monkeypatch.setattr(gateway_dispatch_module, "send_message", _mock_send_message)
    monkeypatch.setattr(gateway_dispatch_module, "ensure_session", _mock_ensure_session)

    # Use the dispatch service with an idempotency key
    service = gateway_dispatch_module.GatewayDispatchService(session=None)
    config = GatewayConfig(url="wss://gateway.example.com", token="test-token")
    idempotency_key = str(uuid4())

    await service.send_agent_message(
        session_key="session:123",
        config=config,
        agent_name="Test Agent",
        message="Hello!",
        idempotency_key=idempotency_key,
    )

    assert len(captured_send_params) == 1
    assert captured_send_params[0]["idempotency_key"] == idempotency_key


@pytest.mark.asyncio
async def test_webhook_dispatch_uses_payload_based_idempotency_key(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Webhook dispatch should generate idempotency key from payload_id."""
    from types import SimpleNamespace
    from uuid import uuid4

    import app.services.webhooks.dispatch as dispatch_module

    captured_keys: list[str] = []
    payload_id = uuid4()

    class _FakeDispatchService:
        def __init__(self, session: object) -> None:
            del session

        async def optional_gateway_config_for_board(self, board: object) -> object:
            del board
            return object()

        async def try_send_agent_message(
            self,
            *,
            session_key: str,
            config: object,
            agent_name: str,
            message: str,
            deliver: bool = False,
            idempotency_key: str | None = None,
        ) -> None:
            captured_keys.append(str(idempotency_key) if idempotency_key else "NONE")

    class _FakeAgentObjects:
        def filter_by(self, **kwargs: object) -> _FakeAgentObjects:
            self._kwargs = kwargs
            return self

        async def first(self, session: object) -> object:
            del session
            return SimpleNamespace(
                id=uuid4(),
                name="Lead Agent",
                openclaw_session_id="lead:session",
            )

    monkeypatch.setattr(dispatch_module.Agent, "objects", _FakeAgentObjects())
    monkeypatch.setattr(dispatch_module, "GatewayDispatchService", _FakeDispatchService)

    webhook = SimpleNamespace(id=uuid4(), description="desc", agent_id=None)
    board = SimpleNamespace(id=uuid4(), name="Board")
    payload = SimpleNamespace(id=payload_id, payload={"event": "test"})

    await dispatch_module._notify_target_agent(
        session=SimpleNamespace(),
        board=board,
        webhook=webhook,
        payload=payload,
    )

    # Should have used an idempotency key derived from payload_id
    assert len(captured_keys) == 1
    expected_key = f"webhook:{payload_id}"
    assert captured_keys[0] == expected_key
