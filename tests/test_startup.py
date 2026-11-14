"""Startup resilience: a database that is a moment late must not crash the app."""

from __future__ import annotations

import pytest
from sqlalchemy.exc import OperationalError

from app.database import connection


def _refused() -> OperationalError:
    return OperationalError("CREATE TABLE", {}, Exception("the database system is starting up"))


def test_schema_is_created_on_the_first_attempt(monkeypatch) -> None:
    calls: list[int] = []

    def _ok() -> None:
        calls.append(1)

    monkeypatch.setattr(connection, "init_db", _ok)

    connection.init_db_with_retry(attempts=3, delay_seconds=0)

    assert len(calls) == 1


def test_startup_waits_for_a_database_that_arrives_late(monkeypatch) -> None:
    failures = {"left": 2}
    calls: list[int] = []

    def _flaky() -> None:
        calls.append(1)
        if failures["left"]:
            failures["left"] -= 1
            raise _refused()

    monkeypatch.setattr(connection, "init_db", _flaky)

    connection.init_db_with_retry(attempts=5, delay_seconds=0)

    assert len(calls) == 3


def test_startup_gives_up_after_the_budget_is_spent(monkeypatch) -> None:
    calls: list[int] = []

    def _always_refused() -> None:
        calls.append(1)
        raise _refused()

    monkeypatch.setattr(connection, "init_db", _always_refused)

    with pytest.raises(OperationalError):
        connection.init_db_with_retry(attempts=4, delay_seconds=0)

    assert len(calls) == 4


def test_non_connection_errors_are_not_retried(monkeypatch) -> None:
    calls: list[int] = []

    def _broken_schema() -> None:
        calls.append(1)
        raise ValueError("bad migration")

    monkeypatch.setattr(connection, "init_db", _broken_schema)

    with pytest.raises(ValueError):
        connection.init_db_with_retry(attempts=5, delay_seconds=0)

    assert len(calls) == 1
