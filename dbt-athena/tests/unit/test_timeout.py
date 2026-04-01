import threading
import time
from unittest import mock

import pytest

from dbt.adapters.athena.exceptions import AthenaModelTimeoutError
from dbt.adapters.athena.impl import AthenaAdapter


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
