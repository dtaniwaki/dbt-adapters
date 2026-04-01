import threading
import time
from unittest import mock

import pytest
from pyathena.model import AthenaQueryExecution

from dbt.adapters.athena.connections import AthenaCredentials, AthenaCursor
from dbt.adapters.athena.exceptions import (
    AthenaModelTimeoutError,
    AthenaQueryTimeoutError,
)
from dbt.adapters.athena.impl import AthenaAdapter

from tests.unit import constants


class TestQueryTimeout:
    def _make_cursor(self, query_timeout_seconds=None, poll_interval=0.1):
        with mock.patch("pyathena.cursor.Cursor.__init__", return_value=None):
            cursor = AthenaCursor(query_timeout_seconds=query_timeout_seconds)
        cursor._poll_interval = poll_interval
        mock_connection = mock.MagicMock()
        mock_connection.cursor_kwargs = {
            "debug_query_state": False,
            "num_iceberg_retries": 3,
            "query_timeout_seconds": None,
        }
        type(cursor).connection = mock.PropertyMock(return_value=mock_connection)
        cursor._mock_connection = mock_connection
        return cursor

    def test_poll_timeout_fires(self):
        cursor = self._make_cursor(query_timeout_seconds=1, poll_interval=0.1)
        mock_execution = mock.MagicMock()
        mock_execution.state = AthenaQueryExecution.STATE_RUNNING
        cursor._get_query_execution = mock.MagicMock(return_value=mock_execution)
        cursor._cancel = mock.MagicMock()

        with pytest.raises(AthenaQueryTimeoutError, match="timed out after 1 seconds"):
            cursor._AthenaCursor__poll("test-query-id")

        cursor._cancel.assert_called_once_with("test-query-id")

    def test_poll_succeeds_within_timeout(self):
        cursor = self._make_cursor(query_timeout_seconds=10, poll_interval=0.1)
        running = mock.MagicMock()
        running.state = AthenaQueryExecution.STATE_RUNNING
        succeeded = mock.MagicMock()
        succeeded.state = AthenaQueryExecution.STATE_SUCCEEDED
        cursor._get_query_execution = mock.MagicMock(
            side_effect=[running, succeeded]
        )

        result = cursor._AthenaCursor__poll("test-query-id")
        assert result.state == AthenaQueryExecution.STATE_SUCCEEDED

    def test_poll_no_timeout_when_none(self):
        cursor = self._make_cursor(query_timeout_seconds=None, poll_interval=0.1)
        succeeded = mock.MagicMock()
        succeeded.state = AthenaQueryExecution.STATE_SUCCEEDED
        cursor._get_query_execution = mock.MagicMock(return_value=succeeded)

        result = cursor._AthenaCursor__poll("test-query-id")
        assert result.state == AthenaQueryExecution.STATE_SUCCEEDED

    def test_poll_uses_cursor_kwargs_fallback(self):
        cursor = self._make_cursor(query_timeout_seconds=None, poll_interval=0.1)
        cursor._mock_connection.cursor_kwargs["query_timeout_seconds"] = 1

        mock_execution = mock.MagicMock()
        mock_execution.state = AthenaQueryExecution.STATE_RUNNING
        cursor._get_query_execution = mock.MagicMock(return_value=mock_execution)
        cursor._cancel = mock.MagicMock()

        with pytest.raises(AthenaQueryTimeoutError):
            cursor._AthenaCursor__poll("test-query-id")

    def test_init_pops_query_timeout(self):
        with mock.patch("pyathena.cursor.Cursor.__init__", return_value=None):
            cursor = AthenaCursor(query_timeout_seconds=300)
        assert cursor._query_timeout_seconds == 300

    def test_completed_state_takes_priority_over_timeout(self):
        cursor = self._make_cursor(query_timeout_seconds=0, poll_interval=0.1)

        succeeded = mock.MagicMock()
        succeeded.state = AthenaQueryExecution.STATE_SUCCEEDED
        cursor._get_query_execution = mock.MagicMock(return_value=succeeded)

        result = cursor._AthenaCursor__poll("test-query-id")
        assert result.state == AthenaQueryExecution.STATE_SUCCEEDED


class TestCredentialsQueryTimeout:
    def test_default_is_none(self):
        creds = AthenaCredentials(
            database=constants.DATA_CATALOG_NAME,
            schema=constants.DATABASE_NAME,
            s3_staging_dir=constants.S3_STAGING_DIR,
            region_name=constants.AWS_REGION,
        )
        assert creds.query_timeout_seconds is None

    def test_set_value(self):
        creds = AthenaCredentials(
            database=constants.DATA_CATALOG_NAME,
            schema=constants.DATABASE_NAME,
            s3_staging_dir=constants.S3_STAGING_DIR,
            region_name=constants.AWS_REGION,
            query_timeout_seconds=300,
        )
        assert creds.query_timeout_seconds == 300

    def test_in_connection_keys(self):
        creds = AthenaCredentials(
            database=constants.DATA_CATALOG_NAME,
            schema=constants.DATABASE_NAME,
            s3_staging_dir=constants.S3_STAGING_DIR,
            region_name=constants.AWS_REGION,
        )
        assert "query_timeout_seconds" in creds._connection_keys()


class TestModelTimeout:
    @pytest.fixture
    def adapter(self):
        adapter = mock.MagicMock(spec=AthenaAdapter)
        adapter._model_deadline = threading.local()
        adapter.set_model_timeout = lambda timeout_seconds: AthenaAdapter.set_model_timeout(
            adapter, timeout_seconds
        )
        adapter.clear_model_timeout = lambda: AthenaAdapter.clear_model_timeout(adapter)
        adapter.check_model_timeout = lambda: AthenaAdapter.check_model_timeout(adapter)
        return adapter

    def test_set_and_check_within_deadline(self, adapter):
        adapter.set_model_timeout(10)
        adapter.check_model_timeout()

    def test_check_raises_after_deadline(self, adapter):
        adapter.set_model_timeout(1)
        time.sleep(1.1)
        with pytest.raises(AthenaModelTimeoutError, match="model_timeout_seconds"):
            adapter.check_model_timeout()

    def test_clear_prevents_timeout(self, adapter):
        adapter.set_model_timeout(1)
        adapter.clear_model_timeout()
        time.sleep(1.1)
        adapter.check_model_timeout()

    def test_none_timeout_is_noop(self, adapter):
        adapter.set_model_timeout(None)
        adapter.check_model_timeout()

    def test_zero_timeout_is_noop(self, adapter):
        adapter.set_model_timeout(0)
        adapter.check_model_timeout()

    def test_negative_timeout_is_noop(self, adapter):
        adapter.set_model_timeout(-1)
        adapter.check_model_timeout()

    def test_check_before_set_is_noop(self, adapter):
        adapter.check_model_timeout()

    def test_thread_isolation(self, adapter):
        results = {}

        def thread_a():
            adapter.set_model_timeout(1)
            time.sleep(1.1)
            try:
                adapter.check_model_timeout()
                results["a"] = "no_timeout"
            except AthenaModelTimeoutError:
                results["a"] = "timeout"

        def thread_b():
            adapter.set_model_timeout(10)
            time.sleep(1.1)
            try:
                adapter.check_model_timeout()
                results["b"] = "no_timeout"
            except AthenaModelTimeoutError:
                results["b"] = "timeout"

        t_a = threading.Thread(target=thread_a)
        t_b = threading.Thread(target=thread_b)
        t_a.start()
        t_b.start()
        t_a.join()
        t_b.join()

        assert results["a"] == "timeout"
        assert results["b"] == "no_timeout"
