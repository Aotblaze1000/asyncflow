"""Request correlation, log formatting and health reporting."""

from __future__ import annotations

import logging

import pytest
from fastapi.testclient import TestClient

from app.core.logging import configure_logging, request_id_var


@pytest.fixture()
def isolated_logging():
    """Run a logging test without disturbing the rest of the session."""

    root = logging.getLogger()
    saved_handlers = list(root.handlers)
    saved_level = root.level
    saved_flag = getattr(root, "_asyncflow_configured", None)
    try:
        yield root
    finally:
        root.handlers[:] = saved_handlers
        root.setLevel(saved_level)
        if saved_flag is None:
            root.__dict__.pop("_asyncflow_configured", None)
        else:
            root._asyncflow_configured = saved_flag


def test_log_lines_carry_the_current_request_id(
    isolated_logging, capsys: pytest.CaptureFixture[str]
) -> None:
    configure_logging(logging.INFO)
    token = request_id_var.set("abc123")
    try:
        logging.getLogger("asyncflow.test").info("upload accepted")
    finally:
        request_id_var.reset(token)

    output = capsys.readouterr().out
    assert "[abc123]" in output
    assert "asyncflow.test: upload accepted" in output


def test_log_lines_outside_a_request_use_a_placeholder(
    isolated_logging, capsys: pytest.CaptureFixture[str]
) -> None:
    configure_logging(logging.INFO)

    logging.getLogger("asyncflow.test").info("worker started")

    assert "[-]" in capsys.readouterr().out


def test_configure_logging_is_idempotent(isolated_logging) -> None:
    configure_logging(logging.INFO)
    configure_logging(logging.INFO)

    assert len(isolated_logging.handlers) == 1


def test_uvicorn_loggers_are_routed_through_the_root_handler(
    isolated_logging,
) -> None:
    configure_logging(logging.WARNING)

    for name in ("uvicorn", "uvicorn.error", "uvicorn.access"):
        logger = logging.getLogger(name)
        assert logger.handlers == []
        assert logger.propagate is True


def test_health_reports_version_and_broker(client: TestClient, monkeypatch) -> None:
    monkeypatch.setattr("app.main._redis_reachable", lambda: True)

    response = client.get("/health")

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "ok"
    assert body["version"] == "0.1.0"
    assert body["broker"] == "up"


def test_response_carries_a_generated_request_id(client: TestClient) -> None:
    response = client.get("/health")

    request_id = response.headers.get("X-Request-ID")
    assert request_id
    assert len(request_id) == 16
    assert all(character in "0123456789abcdef" for character in request_id)


def test_client_supplied_request_id_is_echoed(client: TestClient) -> None:
    response = client.get("/health", headers={"X-Request-ID": "trace-abc-123"})

    assert response.headers["X-Request-ID"] == "trace-abc-123"


def test_absurd_request_id_is_replaced(client: TestClient) -> None:
    response = client.get("/health", headers={"X-Request-ID": "x" * 500})

    request_id = response.headers["X-Request-ID"]
    assert request_id != "x" * 500
    assert len(request_id) <= 64


def test_request_ids_differ_between_requests(client: TestClient) -> None:
    first = client.get("/health").headers["X-Request-ID"]
    second = client.get("/health").headers["X-Request-ID"]

    assert first != second


def test_error_responses_also_carry_a_request_id(client: TestClient) -> None:
    response = client.get("/api/jobs/missing", headers={"X-Request-ID": "err-1"})

    assert response.status_code == 404
    assert response.headers["X-Request-ID"] == "err-1"


def test_access_log_line_keeps_the_request_id(
    client: TestClient,
    isolated_logging,
    capsys: pytest.CaptureFixture[str],
) -> None:
    configure_logging(logging.INFO)

    client.get("/health", headers={"X-Request-ID": "trace-log-1"})

    output = capsys.readouterr().out
    assert "[trace-log-1] app.main: GET /health -> 200" in output
