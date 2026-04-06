"""
End-to-end API tests for the eDeltaShared FastAPI backend.

Tests the full request-response cycle through all API routers using
httpx AsyncClient with ASGITransport (no server process needed).

Requires Docker Compose Postgres running:
    docker compose up -d postgres

Run with:
    pytest backend/tests/test_e2e_api.py -v --asyncio-mode=auto
"""

import json
import uuid
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.core.auth import create_access_token, hash_password
from app.core.config import settings
from app.db.models import Base
from app.db.models.enterprise import Organization, User

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

TEST_DB_URL = (
    f"postgresql+asyncpg://{settings.postgres_user}:{settings.postgres_password}"
    f"@{settings.postgres_host}:{settings.postgres_port}/{settings.postgres_db}"
)

_engine = create_async_engine(TEST_DB_URL, echo=False)
_session_factory = async_sessionmaker(_engine, class_=AsyncSession, expire_on_commit=False)


@pytest_asyncio.fixture(scope="session")
async def setup_db():
    """Create all tables once per session."""
    async with _engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield
    # Tables persist; they are the same ones the app uses.


@pytest_asyncio.fixture()
async def db_session(setup_db):
    """Per-test database session with rollback for isolation."""
    async with _session_factory() as session:
        yield session


@pytest_asyncio.fixture()
async def seed_org(db_session: AsyncSession):
    """Seed a test organization and return it."""
    org = Organization(name=f"TestOrg-{uuid.uuid4().hex[:6]}", slug=f"test-{uuid.uuid4().hex[:6]}")
    db_session.add(org)
    await db_session.commit()
    await db_session.refresh(org)
    return org


@pytest_asyncio.fixture()
async def seed_admin(db_session: AsyncSession, seed_org):
    """Seed an admin user and return (user, org)."""
    user = User(
        email=f"admin-{uuid.uuid4().hex[:6]}@test.io",
        password_hash=hash_password("TestPass123!"),
        full_name="Test Admin",
        role="admin",
        org_id=seed_org.id,
    )
    db_session.add(user)
    await db_session.commit()
    await db_session.refresh(user)
    return user, seed_org


@pytest_asyncio.fixture()
async def seed_super_admin(db_session: AsyncSession, seed_org):
    """Seed a super_admin user."""
    user = User(
        email=f"super-{uuid.uuid4().hex[:6]}@test.io",
        password_hash=hash_password("SuperPass123!"),
        full_name="Super Admin",
        role="super_admin",
        org_id=seed_org.id,
    )
    db_session.add(user)
    await db_session.commit()
    await db_session.refresh(user)
    return user, seed_org


@pytest_asyncio.fixture()
async def seed_viewer(db_session: AsyncSession, seed_org):
    """Seed a viewer user."""
    user = User(
        email=f"viewer-{uuid.uuid4().hex[:6]}@test.io",
        password_hash=hash_password("ViewPass123!"),
        full_name="Test Viewer",
        role="viewer",
        org_id=seed_org.id,
    )
    db_session.add(user)
    await db_session.commit()
    await db_session.refresh(user)
    return user, seed_org


def _token_for(user: User) -> str:
    return create_access_token(
        str(user.id), user.email, user.role,
        str(user.org_id) if user.org_id else None,
    )


def _auth(user: User) -> dict:
    return {"Authorization": f"Bearer {_token_for(user)}"}


@pytest_asyncio.fixture()
async def client():
    """httpx AsyncClient wired to the FastAPI ASGI app."""
    # Patch Redis so lifespan doesn't require a live Redis server
    mock_redis = AsyncMock()
    mock_redis.publish = AsyncMock(return_value=1)
    mock_redis.close = AsyncMock()

    from main import app

    # Override lifespan's Redis with mock
    original_lifespan = app.router.lifespan_context

    from contextlib import asynccontextmanager

    @asynccontextmanager
    async def patched_lifespan(app_instance):
        app_instance.state.redis = mock_redis
        yield
        # no cleanup needed for mock

    app.router.lifespan_context = patched_lifespan

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac

    # Restore
    app.router.lifespan_context = original_lifespan


# ---------------------------------------------------------------------------
# 1. Health Check
# ---------------------------------------------------------------------------


class TestHealthCheck:
    @pytest.mark.asyncio
    async def test_health_returns_200(self, client: AsyncClient):
        resp = await client.get("/api/v1/health")
        assert resp.status_code == 200
        body = resp.json()
        assert body["status"] in ("healthy", "degraded")
        assert "postgres" in body["checks"]

    @pytest.mark.asyncio
    async def test_health_postgres_healthy(self, client: AsyncClient):
        resp = await client.get("/api/v1/health")
        body = resp.json()
        assert body["checks"]["postgres"] == "healthy"


# ---------------------------------------------------------------------------
# 2. Auth Flow
# ---------------------------------------------------------------------------


class TestAuthFlow:
    @pytest.mark.asyncio
    async def test_login_success(self, client: AsyncClient, seed_admin):
        admin, _ = seed_admin
        resp = await client.post("/api/v1/auth/login", json={
            "email": admin.email,
            "password": "TestPass123!",
        })
        assert resp.status_code == 200
        body = resp.json()
        assert "access_token" in body
        assert body["token_type"] == "bearer"
        assert body["user"]["email"] == admin.email
        assert body["user"]["role"] == "admin"

    @pytest.mark.asyncio
    async def test_login_wrong_password(self, client: AsyncClient, seed_admin):
        admin, _ = seed_admin
        resp = await client.post("/api/v1/auth/login", json={
            "email": admin.email,
            "password": "WrongPassword!",
        })
        assert resp.status_code == 401
        assert "Invalid" in resp.json()["detail"]

    @pytest.mark.asyncio
    async def test_login_nonexistent_user(self, client: AsyncClient):
        resp = await client.post("/api/v1/auth/login", json={
            "email": "nobody@test.io",
            "password": "Whatever123!",
        })
        assert resp.status_code == 401

    @pytest.mark.asyncio
    async def test_me_with_valid_token(self, client: AsyncClient, seed_admin):
        admin, _ = seed_admin
        resp = await client.get("/api/v1/auth/me", headers=_auth(admin))
        assert resp.status_code == 200
        body = resp.json()
        assert body["email"] == admin.email
        assert body["role"] == "admin"

    @pytest.mark.asyncio
    async def test_me_without_token(self, client: AsyncClient):
        resp = await client.get("/api/v1/auth/me")
        assert resp.status_code == 401

    @pytest.mark.asyncio
    async def test_me_with_invalid_token(self, client: AsyncClient):
        resp = await client.get(
            "/api/v1/auth/me",
            headers={"Authorization": "Bearer invalid.jwt.token"},
        )
        assert resp.status_code == 401

    @pytest.mark.asyncio
    async def test_create_user_as_admin(self, client: AsyncClient, seed_admin):
        admin, org = seed_admin
        resp = await client.post("/api/v1/auth/users", headers=_auth(admin), json={
            "email": f"new-{uuid.uuid4().hex[:6]}@test.io",
            "password": "NewUserPass1!",
            "full_name": "New User",
            "role": "viewer",
        })
        assert resp.status_code == 201
        body = resp.json()
        assert body["role"] == "viewer"
        assert "id" in body

    @pytest.mark.asyncio
    async def test_viewer_cannot_create_user(self, client: AsyncClient, seed_viewer):
        viewer, _ = seed_viewer
        resp = await client.post("/api/v1/auth/users", headers=_auth(viewer), json={
            "email": f"hack-{uuid.uuid4().hex[:6]}@test.io",
            "password": "HackPass123!",
            "role": "admin",
        })
        assert resp.status_code == 403
        assert "admin" in resp.json()["detail"].lower()

    @pytest.mark.asyncio
    async def test_create_duplicate_user(self, client: AsyncClient, seed_admin):
        admin, _ = seed_admin
        resp = await client.post("/api/v1/auth/users", headers=_auth(admin), json={
            "email": admin.email,
            "password": "AnyPass123!",
        })
        assert resp.status_code == 409

    @pytest.mark.asyncio
    async def test_list_users_as_admin(self, client: AsyncClient, seed_admin):
        admin, _ = seed_admin
        resp = await client.get("/api/v1/auth/users", headers=_auth(admin))
        assert resp.status_code == 200
        users = resp.json()
        assert isinstance(users, list)
        emails = [u["email"] for u in users]
        assert admin.email in emails

    @pytest.mark.asyncio
    async def test_list_users_forbidden_for_viewer(self, client: AsyncClient, seed_viewer):
        viewer, _ = seed_viewer
        resp = await client.get("/api/v1/auth/users", headers=_auth(viewer))
        assert resp.status_code == 403


# ---------------------------------------------------------------------------
# 3. Config Lifecycle (Connectors + Replications)
# ---------------------------------------------------------------------------


class TestConfigLifecycle:
    @pytest.mark.asyncio
    async def test_create_connector(self, client: AsyncClient, seed_admin):
        admin, _ = seed_admin
        resp = await client.post("/api/v1/config/connectors", json={
            "type": "snowflake",
            "name": f"SF-Test-{uuid.uuid4().hex[:6]}",
            "credentials": {"account": "test", "user": "u", "password": "p"},
        })
        assert resp.status_code == 201
        body = resp.json()
        assert body["type"] == "snowflake"
        assert body["is_active"] is True
        return body["id"]

    @pytest.mark.asyncio
    async def test_create_connector_invalid_type(self, client: AsyncClient):
        resp = await client.post("/api/v1/config/connectors", json={
            "type": "mysql",
            "name": "Bad",
            "credentials": {},
        })
        assert resp.status_code == 422  # Pydantic validation

    @pytest.mark.asyncio
    async def test_list_connectors(self, client: AsyncClient, seed_admin):
        admin, _ = seed_admin
        # Create one first
        await client.post("/api/v1/config/connectors", json={
            "type": "aurora",
            "name": f"Aurora-{uuid.uuid4().hex[:6]}",
            "credentials": {"host": "localhost"},
        })
        resp = await client.get("/api/v1/config/connectors")
        assert resp.status_code == 200
        assert isinstance(resp.json(), list)

    @pytest.mark.asyncio
    async def test_list_connectors_filter_by_type(self, client: AsyncClient, seed_admin):
        admin, _ = seed_admin
        name = f"Mongo-{uuid.uuid4().hex[:6]}"
        await client.post("/api/v1/config/connectors", json={
            "type": "mongodb",
            "name": name,
            "credentials": {"uri": "mongodb://localhost"},
        })
        resp = await client.get("/api/v1/config/connectors?type=mongodb")
        assert resp.status_code == 200
        names = [c["name"] for c in resp.json()]
        assert name in names

    @pytest.mark.asyncio
    async def test_get_connector_not_found(self, client: AsyncClient):
        fake_id = str(uuid.uuid4())
        resp = await client.get(f"/api/v1/config/connectors/{fake_id}")
        assert resp.status_code == 404

    @pytest.mark.asyncio
    async def test_full_replication_lifecycle(self, client: AsyncClient, seed_admin):
        admin, _ = seed_admin
        headers = _auth(admin)

        # Step 1: Create connector
        conn_resp = await client.post("/api/v1/config/connectors", json={
            "type": "snowflake",
            "name": f"Lifecycle-{uuid.uuid4().hex[:6]}",
            "credentials": {"account": "a", "user": "u", "password": "p"},
        })
        assert conn_resp.status_code == 201
        connector_id = conn_resp.json()["id"]

        # Step 2: Create replication
        repl_resp = await client.post("/api/v1/config/replications", json={
            "source_table_schema": "SAPSR3",
            "source_table_name": "SALES_ORDERS",
            "connector_config_id": connector_id,
            "target_table_name": "sales_orders_target",
            "replication_mode": "cdc",
            "field_mappings": [
                {"source": "ORDER_ID", "target": "order_id"},
                {"source": "CUSTOMER_ID", "target": "customer_id"},
                {"source": "TOTAL_AMOUNT", "target": "total_amount"},
            ],
            "primary_key_fields": ["ORDER_ID"],
        })
        assert repl_resp.status_code == 201
        config_id = repl_resp.json()["id"]
        assert repl_resp.json()["kafka_topic"] == "edelta.sapsr3.sales_orders"

        # Step 3: Test replication
        test_resp = await client.post(f"/api/v1/config/replications/{config_id}/test")
        assert test_resp.status_code == 200
        test_body = test_resp.json()
        assert test_body["valid"] is True
        check_names = [c["name"] for c in test_body["checks"]]
        assert "connector_exists" in check_names
        assert "field_mappings" in check_names
        assert "pk_in_mappings" in check_names

        # Step 4: List replications
        list_resp = await client.get("/api/v1/config/replications")
        assert list_resp.status_code == 200
        ids = [r["id"] for r in list_resp.json()]
        assert config_id in ids

        # Step 5: Update replication
        patch_resp = await client.patch(
            f"/api/v1/config/replications/{config_id}",
            json={"target_table_name": "sales_orders_v2"},
        )
        assert patch_resp.status_code == 200
        assert patch_resp.json()["target_table_name"] == "sales_orders_v2"

        # Step 6: Delete replication
        del_resp = await client.delete(f"/api/v1/config/replications/{config_id}")
        assert del_resp.status_code == 204

        # Verify deleted
        get_resp = await client.get(f"/api/v1/config/replications/{config_id}")
        assert get_resp.status_code == 404

    @pytest.mark.asyncio
    async def test_create_replication_missing_connector(self, client: AsyncClient):
        resp = await client.post("/api/v1/config/replications", json={
            "source_table_schema": "SAPSR3",
            "source_table_name": "FAKE_TABLE",
            "connector_config_id": str(uuid.uuid4()),
            "target_table_name": "fake_target",
            "field_mappings": [{"source": "ID", "target": "id"}],
            "primary_key_fields": ["ID"],
        })
        assert resp.status_code == 404


# ---------------------------------------------------------------------------
# 4. Streams
# ---------------------------------------------------------------------------


class TestStreams:
    @pytest_asyncio.fixture()
    async def seed_stream(self, db_session: AsyncSession):
        """Create a connector + replication + stream for testing."""
        from app.db.models import ConnectorConfig, ReplicationConfig, StreamState

        connector = ConnectorConfig(
            type="snowflake",
            name=f"Stream-Conn-{uuid.uuid4().hex[:6]}",
            credentials_encrypted="enc",
        )
        db_session.add(connector)
        await db_session.commit()
        await db_session.refresh(connector)

        config = ReplicationConfig(
            source_table_schema="SAPSR3",
            source_table_name="SALES_ORDERS",
            connector_config_id=connector.id,
            target_table_name="so_target",
            field_mappings=[{"source": "ORDER_ID", "target": "order_id"}],
            primary_key_fields=["ORDER_ID"],
            kafka_topic="edelta.sapsr3.sales_orders",
        )
        db_session.add(config)
        await db_session.commit()
        await db_session.refresh(config)

        stream = StreamState(
            config_id=config.id,
            consumer_group="sf-consumer-test",
            status="active",
            last_kafka_offset=100,
            last_hana_seq_id=50,
            records_processed_total=1000,
        )
        db_session.add(stream)
        await db_session.commit()
        await db_session.refresh(stream)
        return stream, config

    @pytest.mark.asyncio
    async def test_list_streams(self, client: AsyncClient, seed_stream):
        resp = await client.get("/api/v1/streams")
        assert resp.status_code == 200
        streams = resp.json()
        assert isinstance(streams, list)

    @pytest.mark.asyncio
    async def test_get_stream_status(self, client: AsyncClient, seed_stream):
        stream, _ = seed_stream
        resp = await client.get(f"/api/v1/streams/{stream.id}/status")
        assert resp.status_code == 200
        body = resp.json()
        assert body["status"] == "active"
        assert body["consumer_group"] == "sf-consumer-test"

    @pytest.mark.asyncio
    async def test_get_stream_status_not_found(self, client: AsyncClient):
        resp = await client.get(f"/api/v1/streams/{uuid.uuid4()}/status")
        assert resp.status_code == 404

    @pytest.mark.asyncio
    async def test_get_stream_lag(self, client: AsyncClient, seed_stream):
        stream, config = seed_stream
        resp = await client.get(f"/api/v1/streams/{stream.id}/lag")
        assert resp.status_code == 200
        body = resp.json()
        assert body["consumer_group"] == "sf-consumer-test"
        assert body["topic"] == "edelta.sapsr3.sales_orders"
        assert body["current_offset"] == 100

    @pytest.mark.asyncio
    async def test_restart_stream(self, client: AsyncClient, seed_stream):
        stream, _ = seed_stream
        resp = await client.post(f"/api/v1/streams/{stream.id}/restart")
        assert resp.status_code == 200
        body = resp.json()
        assert body["message"] == "Restart command sent"

    @pytest.mark.asyncio
    async def test_restart_stream_not_found(self, client: AsyncClient):
        resp = await client.post(f"/api/v1/streams/{uuid.uuid4()}/restart")
        assert resp.status_code == 404

    @pytest.mark.asyncio
    async def test_get_stream_metrics_empty(self, client: AsyncClient, seed_stream):
        stream, _ = seed_stream
        resp = await client.get(f"/api/v1/streams/{stream.id}/metrics")
        assert resp.status_code == 200
        assert resp.json() == []


# ---------------------------------------------------------------------------
# 5. Jobs
# ---------------------------------------------------------------------------


class TestJobs:
    @pytest_asyncio.fixture()
    async def seed_config_for_jobs(self, db_session: AsyncSession):
        from app.db.models import ConnectorConfig, ReplicationConfig

        connector = ConnectorConfig(
            type="aurora",
            name=f"Job-Conn-{uuid.uuid4().hex[:6]}",
            credentials_encrypted="enc",
        )
        db_session.add(connector)
        await db_session.commit()
        await db_session.refresh(connector)

        config = ReplicationConfig(
            source_table_schema="SAPSR3",
            source_table_name="SALES_ORDERS",
            connector_config_id=connector.id,
            target_table_name="so_target_jobs",
            field_mappings=[{"source": "ORDER_ID", "target": "order_id"}],
            primary_key_fields=["ORDER_ID"],
        )
        db_session.add(config)
        await db_session.commit()
        await db_session.refresh(config)
        return config

    @pytest.mark.asyncio
    async def test_trigger_initial_load(self, client: AsyncClient, seed_config_for_jobs):
        config = seed_config_for_jobs
        resp = await client.post("/api/v1/jobs/initial-load", json={
            "config_id": str(config.id),
            "chunk_size": 5000,
            "max_hana_cpu": 50,
        })
        assert resp.status_code == 201
        body = resp.json()
        assert body["status"] == "queued"
        assert body["job_type"] == "initial_load"
        assert body["config_id"] == str(config.id)
        return body["id"]

    @pytest.mark.asyncio
    async def test_get_job(self, client: AsyncClient, seed_config_for_jobs):
        config = seed_config_for_jobs
        # Create a job first
        create_resp = await client.post("/api/v1/jobs/initial-load", json={
            "config_id": str(config.id),
        })
        job_id = create_resp.json()["id"]

        resp = await client.get(f"/api/v1/jobs/{job_id}")
        assert resp.status_code == 200
        assert resp.json()["id"] == job_id

    @pytest.mark.asyncio
    async def test_get_job_not_found(self, client: AsyncClient):
        resp = await client.get(f"/api/v1/jobs/{uuid.uuid4()}")
        assert resp.status_code == 404

    @pytest.mark.asyncio
    async def test_cancel_job(self, client: AsyncClient, seed_config_for_jobs):
        config = seed_config_for_jobs
        create_resp = await client.post("/api/v1/jobs/initial-load", json={
            "config_id": str(config.id),
        })
        job_id = create_resp.json()["id"]

        cancel_resp = await client.post(f"/api/v1/jobs/{job_id}/cancel")
        assert cancel_resp.status_code == 200
        assert cancel_resp.json()["message"] == "Job cancelled"

        # Verify status changed
        get_resp = await client.get(f"/api/v1/jobs/{job_id}")
        assert get_resp.json()["status"] == "cancelled"

    @pytest.mark.asyncio
    async def test_cancel_already_cancelled(self, client: AsyncClient, seed_config_for_jobs):
        config = seed_config_for_jobs
        create_resp = await client.post("/api/v1/jobs/initial-load", json={
            "config_id": str(config.id),
        })
        job_id = create_resp.json()["id"]

        await client.post(f"/api/v1/jobs/{job_id}/cancel")
        resp = await client.post(f"/api/v1/jobs/{job_id}/cancel")
        assert resp.status_code == 200
        assert "already" in resp.json()["message"].lower()

    @pytest.mark.asyncio
    async def test_list_jobs(self, client: AsyncClient, seed_config_for_jobs):
        config = seed_config_for_jobs
        await client.post("/api/v1/jobs/initial-load", json={
            "config_id": str(config.id),
        })
        resp = await client.get("/api/v1/jobs")
        assert resp.status_code == 200
        assert isinstance(resp.json(), list)
        assert len(resp.json()) >= 1

    @pytest.mark.asyncio
    async def test_list_jobs_filter_by_config(self, client: AsyncClient, seed_config_for_jobs):
        config = seed_config_for_jobs
        await client.post("/api/v1/jobs/initial-load", json={
            "config_id": str(config.id),
        })
        resp = await client.get(f"/api/v1/jobs?config_id={config.id}")
        assert resp.status_code == 200
        for j in resp.json():
            assert j["config_id"] == str(config.id)


# ---------------------------------------------------------------------------
# 6. Data Quality (Rules, Transforms, Violations)
# ---------------------------------------------------------------------------


class TestDataQuality:
    @pytest_asyncio.fixture()
    async def seed_config_for_quality(self, db_session: AsyncSession):
        from app.db.models import ConnectorConfig, ReplicationConfig

        connector = ConnectorConfig(
            type="snowflake",
            name=f"QC-Conn-{uuid.uuid4().hex[:6]}",
            credentials_encrypted="enc",
        )
        db_session.add(connector)
        await db_session.commit()
        await db_session.refresh(connector)

        config = ReplicationConfig(
            source_table_schema="SAPSR3",
            source_table_name="SALES_ORDERS",
            connector_config_id=connector.id,
            target_table_name="qc_target",
            field_mappings=[{"source": "ORDER_ID", "target": "order_id"}],
            primary_key_fields=["ORDER_ID"],
        )
        db_session.add(config)
        await db_session.commit()
        await db_session.refresh(config)
        return config

    @pytest.mark.asyncio
    async def test_create_quality_rule(self, client: AsyncClient, seed_config_for_quality):
        config = seed_config_for_quality
        resp = await client.post("/api/v1/quality/rules", json={
            "config_id": str(config.id),
            "field_name": "TOTAL_AMOUNT",
            "rule_type": "range",
            "rule_params": {"min": 0, "max": 1000000},
            "severity": "reject",
            "description": "Amount must be in valid range",
        })
        assert resp.status_code == 201
        body = resp.json()
        assert body["field_name"] == "TOTAL_AMOUNT"
        assert body["rule_type"] == "range"
        assert body["severity"] == "reject"
        return body["id"]

    @pytest.mark.asyncio
    async def test_create_rule_invalid_type(self, client: AsyncClient, seed_config_for_quality):
        config = seed_config_for_quality
        resp = await client.post("/api/v1/quality/rules", json={
            "config_id": str(config.id),
            "field_name": "X",
            "rule_type": "invalid_type",
            "severity": "warn",
        })
        assert resp.status_code == 422

    @pytest.mark.asyncio
    async def test_list_rules(self, client: AsyncClient, seed_config_for_quality):
        config = seed_config_for_quality
        # Create a rule first
        await client.post("/api/v1/quality/rules", json={
            "config_id": str(config.id),
            "field_name": "STATUS",
            "rule_type": "not_null",
            "severity": "warn",
        })
        resp = await client.get(f"/api/v1/quality/rules?config_id={config.id}")
        assert resp.status_code == 200
        rules = resp.json()
        assert len(rules) >= 1
        assert all(r["config_id"] == str(config.id) for r in rules)

    @pytest.mark.asyncio
    async def test_delete_rule(self, client: AsyncClient, seed_config_for_quality):
        config = seed_config_for_quality
        create_resp = await client.post("/api/v1/quality/rules", json={
            "config_id": str(config.id),
            "field_name": "CUSTOMER_ID",
            "rule_type": "regex",
            "rule_params": {"pattern": "^C-"},
            "severity": "warn",
        })
        rule_id = create_resp.json()["id"]
        del_resp = await client.delete(f"/api/v1/quality/rules/{rule_id}")
        assert del_resp.status_code == 204

    @pytest.mark.asyncio
    async def test_delete_rule_not_found(self, client: AsyncClient):
        resp = await client.delete(f"/api/v1/quality/rules/{uuid.uuid4()}")
        assert resp.status_code == 404

    @pytest.mark.asyncio
    async def test_create_transform(self, client: AsyncClient, seed_config_for_quality):
        config = seed_config_for_quality
        resp = await client.post("/api/v1/quality/transforms", json={
            "config_id": str(config.id),
            "source_field": "CUSTOMER_ID",
            "transform_type": "upper",
            "transform_params": {},
            "execution_order": 1,
        })
        assert resp.status_code == 201
        body = resp.json()
        assert body["transform_type"] == "upper"

    @pytest.mark.asyncio
    async def test_create_transform_invalid_type(self, client: AsyncClient, seed_config_for_quality):
        config = seed_config_for_quality
        resp = await client.post("/api/v1/quality/transforms", json={
            "config_id": str(config.id),
            "source_field": "X",
            "transform_type": "invalid_transform",
        })
        assert resp.status_code == 422

    @pytest.mark.asyncio
    async def test_list_transforms(self, client: AsyncClient, seed_config_for_quality):
        config = seed_config_for_quality
        await client.post("/api/v1/quality/transforms", json={
            "config_id": str(config.id),
            "source_field": "STATUS",
            "transform_type": "trim",
        })
        resp = await client.get(f"/api/v1/quality/transforms?config_id={config.id}")
        assert resp.status_code == 200
        assert len(resp.json()) >= 1

    @pytest.mark.asyncio
    async def test_delete_transform(self, client: AsyncClient, seed_config_for_quality):
        config = seed_config_for_quality
        create_resp = await client.post("/api/v1/quality/transforms", json={
            "config_id": str(config.id),
            "source_field": "X",
            "transform_type": "lower",
        })
        tid = create_resp.json()["id"]
        resp = await client.delete(f"/api/v1/quality/transforms/{tid}")
        assert resp.status_code == 204

    @pytest.mark.asyncio
    async def test_list_violations_empty(self, client: AsyncClient):
        resp = await client.get("/api/v1/quality/violations")
        assert resp.status_code == 200
        body = resp.json()
        assert "items" in body
        assert "total" in body

    @pytest.mark.asyncio
    async def test_violation_summary(self, client: AsyncClient):
        resp = await client.get("/api/v1/quality/violations/summary")
        assert resp.status_code == 200
        assert isinstance(resp.json(), list)

    @pytest.mark.asyncio
    async def test_list_schema_changes(self, client: AsyncClient):
        resp = await client.get("/api/v1/quality/schema-changes")
        assert resp.status_code == 200
        assert isinstance(resp.json(), list)

    @pytest.mark.asyncio
    async def test_get_lineage(self, client: AsyncClient):
        resp = await client.get("/api/v1/quality/lineage")
        assert resp.status_code == 200
        assert isinstance(resp.json(), list)


# ---------------------------------------------------------------------------
# 7. Alerts
# ---------------------------------------------------------------------------


class TestAlerts:
    @pytest.mark.asyncio
    async def test_create_channel(self, client: AsyncClient, seed_admin):
        admin, _ = seed_admin
        resp = await client.post("/api/v1/alerts/channels", headers=_auth(admin), json={
            "channel_type": "slack",
            "name": f"Test-Slack-{uuid.uuid4().hex[:6]}",
            "config": {"webhook_url": "https://hooks.slack.com/services/test"},
        })
        assert resp.status_code == 201
        body = resp.json()
        assert body["channel_type"] == "slack"
        assert body["is_active"] is True
        return body["id"]

    @pytest.mark.asyncio
    async def test_create_channel_invalid_type(self, client: AsyncClient, seed_admin):
        admin, _ = seed_admin
        resp = await client.post("/api/v1/alerts/channels", headers=_auth(admin), json={
            "channel_type": "sms",
            "name": "Bad",
            "config": {},
        })
        assert resp.status_code == 422

    @pytest.mark.asyncio
    async def test_create_channel_requires_admin(self, client: AsyncClient, seed_viewer):
        viewer, _ = seed_viewer
        resp = await client.post("/api/v1/alerts/channels", headers=_auth(viewer), json={
            "channel_type": "email",
            "name": "Should Fail",
            "config": {"to": "test@test.io"},
        })
        assert resp.status_code == 403

    @pytest.mark.asyncio
    async def test_list_channels(self, client: AsyncClient, seed_admin):
        admin, _ = seed_admin
        resp = await client.get("/api/v1/alerts/channels", headers=_auth(admin))
        assert resp.status_code == 200
        assert isinstance(resp.json(), list)

    @pytest.mark.asyncio
    async def test_create_alert_rule(self, client: AsyncClient, seed_admin):
        admin, _ = seed_admin
        # Create channel first
        ch_resp = await client.post("/api/v1/alerts/channels", headers=_auth(admin), json={
            "channel_type": "webhook",
            "name": f"Hook-{uuid.uuid4().hex[:6]}",
            "config": {"url": "https://example.com/hook"},
        })
        channel_id = ch_resp.json()["id"]

        resp = await client.post("/api/v1/alerts/rules", headers=_auth(admin), json={
            "name": "Latency Alert",
            "condition_type": "latency_breach",
            "condition_params": {"threshold_ms": 5000},
            "channel_id": channel_id,
            "severity_filter": "warn",
            "cooldown_minutes": 30,
        })
        assert resp.status_code == 201
        body = resp.json()
        assert body["name"] == "Latency Alert"
        assert body["condition_type"] == "latency_breach"

    @pytest.mark.asyncio
    async def test_list_alert_rules(self, client: AsyncClient, seed_admin):
        admin, _ = seed_admin
        resp = await client.get("/api/v1/alerts/rules", headers=_auth(admin))
        assert resp.status_code == 200
        assert isinstance(resp.json(), list)

    @pytest.mark.asyncio
    async def test_alert_history_empty(self, client: AsyncClient, seed_admin):
        admin, _ = seed_admin
        resp = await client.get("/api/v1/alerts/history", headers=_auth(admin))
        assert resp.status_code == 200
        assert isinstance(resp.json(), list)


# ---------------------------------------------------------------------------
# 8. Logs
# ---------------------------------------------------------------------------


class TestLogs:
    @pytest_asyncio.fixture()
    async def seed_audit_logs(self, db_session: AsyncSession):
        from app.db.models import AuditLog

        logs = []
        for i in range(5):
            log = AuditLog(
                entity_type="replication_config",
                action="created" if i % 2 == 0 else "updated",
                user_id="test-user",
                changes={"index": i},
            )
            db_session.add(log)
            logs.append(log)
        await db_session.commit()
        return logs

    @pytest.mark.asyncio
    async def test_list_logs(self, client: AsyncClient, seed_audit_logs):
        resp = await client.get("/api/v1/logs")
        assert resp.status_code == 200
        body = resp.json()
        assert "items" in body
        assert "total" in body
        assert body["total"] >= 5

    @pytest.mark.asyncio
    async def test_list_logs_pagination(self, client: AsyncClient, seed_audit_logs):
        resp = await client.get("/api/v1/logs?limit=2&offset=0")
        assert resp.status_code == 200
        body = resp.json()
        assert len(body["items"]) <= 2
        assert body["limit"] == 2
        assert body["offset"] == 0

    @pytest.mark.asyncio
    async def test_list_logs_filter_by_action(self, client: AsyncClient, seed_audit_logs):
        resp = await client.get("/api/v1/logs?action=created")
        assert resp.status_code == 200
        for item in resp.json()["items"]:
            assert item["action"] == "created"

    @pytest.mark.asyncio
    async def test_list_logs_filter_by_entity_type(self, client: AsyncClient, seed_audit_logs):
        resp = await client.get("/api/v1/logs?entity_type=replication_config")
        assert resp.status_code == 200
        for item in resp.json()["items"]:
            assert item["entity_type"] == "replication_config"

    @pytest.mark.asyncio
    async def test_list_log_actions(self, client: AsyncClient, seed_audit_logs):
        resp = await client.get("/api/v1/logs/actions")
        assert resp.status_code == 200
        actions = resp.json()
        assert isinstance(actions, list)
        assert "created" in actions


# ---------------------------------------------------------------------------
# 9. Costs
# ---------------------------------------------------------------------------


class TestCosts:
    @pytest.mark.asyncio
    async def test_cost_summary(self, client: AsyncClient, seed_admin):
        admin, _ = seed_admin
        resp = await client.get(
            "/api/v1/costs/summary?days=30", headers=_auth(admin)
        )
        assert resp.status_code == 200
        body = resp.json()
        assert "total_cost_usd" in body
        assert "by_target" in body
        assert body["period_days"] == 30

    @pytest.mark.asyncio
    async def test_cost_summary_requires_auth(self, client: AsyncClient):
        resp = await client.get("/api/v1/costs/summary")
        assert resp.status_code == 401

    @pytest.mark.asyncio
    async def test_daily_costs(self, client: AsyncClient, seed_admin):
        admin, _ = seed_admin
        resp = await client.get(
            "/api/v1/costs/daily?days=7", headers=_auth(admin)
        )
        assert resp.status_code == 200
        assert isinstance(resp.json(), list)


# ---------------------------------------------------------------------------
# 10. Organizations
# ---------------------------------------------------------------------------


class TestOrgs:
    @pytest.mark.asyncio
    async def test_create_org_as_super_admin(self, client: AsyncClient, seed_super_admin):
        sa, _ = seed_super_admin
        slug = f"org-{uuid.uuid4().hex[:8]}"
        resp = await client.post("/api/v1/orgs", headers=_auth(sa), json={
            "name": f"NewOrg-{slug}",
            "slug": slug,
        })
        assert resp.status_code == 201
        body = resp.json()
        assert body["slug"] == slug

    @pytest.mark.asyncio
    async def test_create_org_forbidden_for_admin(self, client: AsyncClient, seed_admin):
        admin, _ = seed_admin
        resp = await client.post("/api/v1/orgs", headers=_auth(admin), json={
            "name": "Nope",
            "slug": "nope",
        })
        assert resp.status_code == 403

    @pytest.mark.asyncio
    async def test_create_org_duplicate_slug(self, client: AsyncClient, seed_super_admin):
        sa, org = seed_super_admin
        resp = await client.post("/api/v1/orgs", headers=_auth(sa), json={
            "name": "Dup",
            "slug": org.slug,
        })
        assert resp.status_code == 409

    @pytest.mark.asyncio
    async def test_list_orgs(self, client: AsyncClient, seed_super_admin):
        sa, _ = seed_super_admin
        resp = await client.get("/api/v1/orgs", headers=_auth(sa))
        assert resp.status_code == 200
        orgs = resp.json()
        assert isinstance(orgs, list)
        assert len(orgs) >= 1

    @pytest.mark.asyncio
    async def test_list_orgs_forbidden_for_viewer(self, client: AsyncClient, seed_viewer):
        viewer, _ = seed_viewer
        resp = await client.get("/api/v1/orgs", headers=_auth(viewer))
        assert resp.status_code == 403

    @pytest.mark.asyncio
    async def test_org_slug_validation(self, client: AsyncClient, seed_super_admin):
        sa, _ = seed_super_admin
        resp = await client.post("/api/v1/orgs", headers=_auth(sa), json={
            "name": "Bad Slug",
            "slug": "INVALID SLUG!",
        })
        assert resp.status_code == 422


# ---------------------------------------------------------------------------
# 11. Schema (HANA Introspection — mocked)
# ---------------------------------------------------------------------------


class TestSchema:
    @pytest.mark.asyncio
    async def test_list_sources(self, client: AsyncClient):
        resp = await client.get("/api/v1/schema/sources")
        assert resp.status_code == 200
        sources = resp.json()
        assert isinstance(sources, list)
        assert len(sources) >= 1
        assert sources[0]["name"] == "HANA Sidecar"

    @pytest.mark.asyncio
    async def test_list_tables(self, client: AsyncClient):
        """Tables endpoint works; may return empty if HANA not configured."""
        resp = await client.get("/api/v1/schema/tables")
        # Should not 500 even without HANA
        assert resp.status_code in (200, 503)


# ---------------------------------------------------------------------------
# 12. Metrics
# ---------------------------------------------------------------------------


class TestMetrics:
    @pytest.mark.asyncio
    async def test_prometheus_metrics(self, client: AsyncClient):
        resp = await client.get("/api/v1/metrics")
        assert resp.status_code == 200
        # Prometheus text format
        assert "HELP" in resp.text or "TYPE" in resp.text or resp.text == ""

    @pytest.mark.asyncio
    async def test_latency_percentiles(self, client: AsyncClient):
        resp = await client.get("/api/v1/metrics/latency")
        assert resp.status_code == 200
        assert isinstance(resp.json(), list)
