from db import is_retryable_db_error
from psycopg import InterfaceError, OperationalError
from psycopg.errors import FeatureNotSupported


def test_is_retryable_db_error_accepts_connection_errors() -> None:
    assert is_retryable_db_error(OperationalError("db down")) is True
    assert is_retryable_db_error(InterfaceError("socket closed")) is True


def test_is_retryable_db_error_accepts_cached_plan_shape_change() -> None:
    assert is_retryable_db_error(FeatureNotSupported("cached plan must not change result type")) is True


def test_is_retryable_db_error_rejects_other_sql_errors() -> None:
    assert is_retryable_db_error(ValueError("boom")) is False
    assert is_retryable_db_error(FeatureNotSupported("feature not supported")) is False
