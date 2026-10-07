import time
from unittest.mock import MagicMock

from dbt.adapters.athena.spark_connect.keepalive import SessionKeepalive


def _wait_for(predicate, timeout=2.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.01)
    return False


def test_sends_select_until_stopped():
    spark = MagicMock()
    keepalive = SessionKeepalive(spark, "sid-1", interval=0.02)
    keepalive.start()
    assert _wait_for(lambda: spark.sql.call_count >= 2)
    keepalive.stop()

    calls_after_stop = spark.sql.call_count
    time.sleep(0.1)
    assert spark.sql.call_count == calls_after_stop
    spark.sql.assert_called_with("SELECT 1")
    spark.sql.return_value.collect.assert_called()


def test_keeps_running_after_a_failed_operation():
    spark = MagicMock()
    spark.sql.return_value.collect.side_effect = [RuntimeError("boom"), None, None]
    keepalive = SessionKeepalive(spark, "sid-1", interval=0.02)
    keepalive.start()
    assert _wait_for(lambda: spark.sql.call_count >= 3)
    keepalive.stop()


def test_waits_for_the_interval_before_the_first_operation():
    spark = MagicMock()
    keepalive = SessionKeepalive(spark, "sid-1", interval=10)
    keepalive.start()
    time.sleep(0.1)
    keepalive.stop()
    spark.sql.assert_not_called()
