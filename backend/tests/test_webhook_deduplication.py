# ruff: noqa: INP001
"""Tests for webhook payload deduplication and idempotency key persistence."""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

import pytest
from fastapi import APIRouter, Depends, FastAPI, Response
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker, create_async_engine
from sqlmodel import SQLModel, col, select
from sqlmodel.ext.asyncio.session import AsyncSession

from app.api import board_webhooks
from app.api.board_webhooks import router as board_webhooks_router
from app.api.deps import get_board_or_404
from app.db.session import get_session
from app.models.agents import Agent
from app.models.board_memory import BoardMemory
from app.models.board_webhook_payloads import BoardWebhookPayload
from app.models.board_webhooks import BoardWebhook
from app.models.boards import Board
from app.models.gateways import Gateway
from app.models.organizations import Organization
from app.services.webhooks.queue import QueuedInboundDelivery


async def _make_engine() -> AsyncEngine:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.connect() as conn, conn.begin():
        await conn.run_sync(SQLModel.metadata.create_all)
    return engine


def _build_test_app(
    session_maker: async_sessionmaker[AsyncSession],
) -> FastAPI:
    app = FastAPI()
    api_v1 = APIRouter(prefix="/api/v1")
    api_v1.include_router(board_webhooks_router)
    app.include_router(api_v1)

    async def _override_get_session() -> AsyncSession:
        async with session_maker() as session:
            yield session

    async def _override_get_board_or_404(
        board_id: str,
        session: AsyncSession = Depends(get_session),
    ) -> Board:
        board = await Board.objects.by_id(UUID(board_id)).first(session)
        if board is None:
            from fastapi import HTTPException, status

            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND)
        return board

    app.dependency_overrides[get_session] = _override_get_session
    app.dependency_overrides[get_board_or_404] = _override_get_board_or_404
    return app


async def _seed_webhook(
    session: AsyncSession,
    *,
    enabled: bool,
) -> tuple[Board, BoardWebhook]:
    organization_id = uuid4()
    gateway_id = uuid4()
    board_id = uuid4()
    webhook_id = uuid4()

    session.add(Organization(id=organization_id, name=f"org-{organization_id}"))
    session.add(
        Gateway(
            id=gateway_id,
            organization_id=organization_id,
            name="gateway",
            url="https://gateway.example.local",
            workspace_root="/tmp/workspace",
        ),
    )
    board = Board(
        id=board_id,
        organization_id=organization_id,
        gateway_id=gateway_id,
        name="Launch board",
        slug="launch-board",
        description="Board for launch automation.",
    )
    session.add(board)
    session.add(
        Agent(
            id=uuid4(),
            board_id=board_id,
            gateway_id=gateway_id,
            name="Lead Agent",
            status="online",
            openclaw_session_id="lead:session:key",
            is_board_lead=True,
        ),
    )
    webhook = BoardWebhook(
        id=webhook_id,
        board_id=board_id,
        description="Triage payload and create tasks for impacted services.",
        enabled=enabled,
    )
    session.add(webhook)
    await session.commit()
    return board, webhook


@pytest.mark.asyncio
async def test_duplicate_webhook_payload_is_rejected(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Duplicate webhook POSTs with identical payload are detected and skipped."""
    engine = await _make_engine()
    session_maker = async_sessionmaker(
        engine,
        class_=AsyncSession,
        expire_on_commit=False,
    )
    app = _build_test_app(session_maker)
    enqueued: list[dict[str, object]] = []

    async with session_maker() as session:
        board, webhook = await _seed_webhook(session, enabled=True)

    def _fake_enqueue(payload: QueuedInboundDelivery) -> bool:
        enqueued.append(
            {
                "board_id": str(payload.board_id),
                "webhook_id": str(payload.webhook_id),
                "payload_id": str(payload.payload_id),
            },
        )
        return True

    monkeypatch.setattr(
        board_webhooks,
        "enqueue_webhook_delivery",
        _fake_enqueue,
    )

    try:
        async with AsyncClient(
            transport=ASGITransport(app=app),
            base_url="http://testserver",
        ) as client:
            # First request - should be processed (202 Accepted)
            payload = {"event": "deploy", "service": "api", "timestamp": "2025-01-01T00:00:00Z"}
            response1 = await client.post(
                f"/api/v1/boards/{board.id}/webhooks/{webhook.id}",
                json=payload,
            )
            assert response1.status_code == 200  # FastAPI returns 200 for successful responses
            body1 = response1.json()
            first_payload_id = body1["payload_id"]
            assert body1["duplicate"] is False

            # Second request with identical payload - should be detected as duplicate
            response2 = await client.post(
                f"/api/v1/boards/{board.id}/webhooks/{webhook.id}",
                json=payload,
            )
            # Should return 200 OK with the existing payload_id (idempotent response)
            assert response2.status_code == 200
            body2 = response2.json()
            assert body2["payload_id"] == first_payload_id
            assert body2["duplicate"] is True

        # Only one payload should be stored
        async with session_maker() as session:
            payloads = (
                await session.exec(
                    select(BoardWebhookPayload).where(
                        col(BoardWebhookPayload.board_id) == board.id
                    ),
                )
            ).all()
            assert len(payloads) == 1

        # Only one enqueue call should have been made
        assert len(enqueued) == 1
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_different_payloads_are_not_deduplicated(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Different webhook payloads should each be processed separately."""
    engine = await _make_engine()
    session_maker = async_sessionmaker(
        engine,
        class_=AsyncSession,
        expire_on_commit=False,
    )
    app = _build_test_app(session_maker)
    enqueued: list[dict[str, object]] = []

    async with session_maker() as session:
        board, webhook = await _seed_webhook(session, enabled=True)

    def _fake_enqueue(payload: QueuedInboundDelivery) -> bool:
        enqueued.append(
            {
                "board_id": str(payload.board_id),
                "webhook_id": str(payload.webhook_id),
                "payload_id": str(payload.payload_id),
            },
        )
        return True

    monkeypatch.setattr(
        board_webhooks,
        "enqueue_webhook_delivery",
        _fake_enqueue,
    )

    try:
        async with AsyncClient(
            transport=ASGITransport(app=app),
            base_url="http://testserver",
        ) as client:
            # First request
            response1 = await client.post(
                f"/api/v1/boards/{board.id}/webhooks/{webhook.id}",
                json={"event": "deploy", "service": "api"},
            )
            assert response1.status_code == 200
            body1 = response1.json()
            assert body1["duplicate"] is False

            # Second request with different payload
            response2 = await client.post(
                f"/api/v1/boards/{board.id}/webhooks/{webhook.id}",
                json={"event": "deploy", "service": "worker"},
            )
            assert response2.status_code == 200
            body2 = response2.json()
            assert body2["duplicate"] is False

            # Both should have different payload_ids
            assert body1["payload_id"] != body2["payload_id"]

        # Both payloads should be stored
        async with session_maker() as session:
            payloads = (
                await session.exec(
                    select(BoardWebhookPayload).where(
                        col(BoardWebhookPayload.board_id) == board.id
                    ),
                )
            ).all()
            assert len(payloads) == 2

        # Both should be enqueued
        assert len(enqueued) == 2
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_old_duplicate_payloads_are_processed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Duplicate payloads older than the dedup window should be processed."""
    engine = await _make_engine()
    session_maker = async_sessionmaker(
        engine,
        class_=AsyncSession,
        expire_on_commit=False,
    )
    app = _build_test_app(session_maker)
    enqueued: list[dict[str, object]] = []

    async with session_maker() as session:
        board, webhook = await _seed_webhook(session, enabled=True)

        # Create an old payload with the same fingerprint
        old_payload_content = {"event": "deploy", "service": "api"}
        fingerprint = hashlib.sha256(
            json.dumps(old_payload_content, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()

        old_payload = BoardWebhookPayload(
            board_id=board.id,
            webhook_id=webhook.id,
            payload=old_payload_content,
            fingerprint_hash=fingerprint,
            received_at=datetime.now(UTC) - timedelta(minutes=10),  # 10 minutes old
        )
        session.add(old_payload)
        await session.commit()
        old_payload_id = old_payload.id

    def _fake_enqueue(payload: QueuedInboundDelivery) -> bool:
        enqueued.append(
            {
                "board_id": str(payload.board_id),
                "webhook_id": str(payload.webhook_id),
                "payload_id": str(payload.payload_id),
            },
        )
        return True

    monkeypatch.setattr(
        board_webhooks,
        "enqueue_webhook_delivery",
        _fake_enqueue,
    )

    try:
        async with AsyncClient(
            transport=ASGITransport(app=app),
            base_url="http://testserver",
        ) as client:
            # Request with same payload as old one - should be processed
            # because it's outside the dedup window (default 5 minutes)
            response = await client.post(
                f"/api/v1/boards/{board.id}/webhooks/{webhook.id}",
                json={"event": "deploy", "service": "api"},
            )
            assert response.status_code == 200
            body = response.json()
            assert body["payload_id"] != str(old_payload_id)
            assert body["duplicate"] is False

        # New payload should be enqueued
        assert len(enqueued) == 1
    finally:
        await engine.dispose()


def test_compute_payload_fingerprint() -> None:
    """Fingerprint computation should be deterministic and order-independent."""
    from app.api.board_webhooks import compute_payload_fingerprint

    # Same content, same fingerprint
    payload1 = {"event": "deploy", "service": "api"}
    payload2 = {"service": "api", "event": "deploy"}  # Different order
    assert compute_payload_fingerprint(payload1) == compute_payload_fingerprint(payload2)

    # Different content, different fingerprint
    payload3 = {"event": "deploy", "service": "worker"}
    assert compute_payload_fingerprint(payload1) != compute_payload_fingerprint(payload3)

    # Handles non-dict payloads
    assert compute_payload_fingerprint("plain text") is not None
    assert compute_payload_fingerprint(123) is not None
    assert compute_payload_fingerprint(None) is not None


def test_compute_payload_fingerprint_lists() -> None:
    """Fingerprint handles list payloads correctly."""
    from app.api.board_webhooks import compute_payload_fingerprint

    list_payload = [{"a": 1}, {"b": 2}]
    fingerprint = compute_payload_fingerprint(list_payload)
    assert fingerprint is not None
    assert len(fingerprint) == 64  # SHA256 hex digest length

    # Same list content, same fingerprint
    same_list = [{"a": 1}, {"b": 2}]
    assert compute_payload_fingerprint(same_list) == fingerprint

    # Different list content, different fingerprint
    different_list = [{"a": 1}, {"c": 3}]
    assert compute_payload_fingerprint(different_list) != fingerprint
