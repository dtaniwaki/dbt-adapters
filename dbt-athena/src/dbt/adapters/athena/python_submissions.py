import os
import random
import threading
import time
from functools import cached_property
from typing import Any, Dict, Optional

import botocore
from dbt_common.exceptions import DbtRuntimeError

from dbt.adapters.athena.config import AthenaSparkSessionConfig
from dbt.adapters.athena.connections import AthenaCredentials
from dbt.adapters.athena.constants import LOGGER
from dbt.adapters.athena.session import AthenaSparkSessionManager
from dbt.adapters.base import PythonJobHelper

SUBMISSION_LANGUAGE = "python"

# Minimum remaining token lifetime before refreshing (seconds).
_TOKEN_REFRESH_MARGIN_SECONDS = 120

# Max retry attempts for transient Spark Connect errors.
_SPARK_CONNECT_MAX_RETRIES = 3

# Patterns that indicate a transient Spark Connect error
# where retrying with a new session is expected to succeed.
_TRANSIENT_SPARK_PATTERNS = [
    # Spark executor failed to obtain credentials from the provider chain.
    # Observed as a transient issue when many sessions start concurrently.
    "Unable to load credentials",
    # Spark executor failed to resolve the AWS region via DefaultAwsRegionProviderChain
    # (IMDS not yet available at executor startup). Same transient pattern as credentials.
    "Unable to load region",
    # gRPC connection pool was shut down (secondary failure after credentials
    # or session error). A new session creates a fresh pool.
    "Pool not running",
    # Athena terminated the Spark session (idle timeout, DPU pressure, or
    # concurrent session limit). A new session resolves this.
    "Session not active",
]


def _is_transient_spark_error(e: Exception) -> bool:
    """Return True if the exception is a transient Spark Connect error."""
    error_str = f"{type(e).__name__}: {e}"
    return any(p in error_str for p in _TRANSIENT_SPARK_PATTERNS)


_spark_connect_env_lock = threading.Lock()
_spark_connect_env_set = False


def _ensure_spark_connect_env() -> None:
    """Set SPARK_CONNECT_MODE_ENABLED=1 once for the process."""
    global _spark_connect_env_set
    if _spark_connect_env_set:
        return
    with _spark_connect_env_lock:
        if not _spark_connect_env_set:
            os.environ["SPARK_CONNECT_MODE_ENABLED"] = "1"
            _spark_connect_env_set = True


def _create_athena_channel_builder(
    athena_client: Any,
    session_id: str,
    endpoint_url: str,
    initial_auth_token: Optional[str] = None,
    initial_token_expiry: Any = None,
) -> Any:
    """Create a ChannelBuilder subclass that auto-refreshes the Athena AuthToken.

    The AuthToken from GetSessionEndpoint expires after ~29 minutes.
    This builder calls GetSessionEndpoint to obtain a fresh token
    whenever the current token is about to expire, so that long-running
    Spark Connect jobs are not interrupted by PERMISSION_DENIED errors.

    Returns a ChannelBuilder subclass instance (import deferred to avoid
    top-level pyspark dependency).
    """
    from datetime import datetime, timezone

    from pyspark.sql.connect.client.core import ChannelBuilder

    class AthenaChannelBuilder(ChannelBuilder):
        def __init__(
            self, client: Any, sid: str, url: str, auth_token: Optional[str], token_expiry: Any
        ):
            sc_url = url.replace("https://", "sc://", 1) + ":443/;use_ssl=true"
            super().__init__(sc_url)
            self._athena_client = client
            self._athena_session_id = sid
            self._auth_token = auth_token
            self._token_expiry = token_expiry

        def _refresh_token_if_needed(self) -> None:
            if self._auth_token and self._token_expiry:
                remaining = (self._token_expiry - datetime.now(timezone.utc)).total_seconds()
                if remaining > _TOKEN_REFRESH_MARGIN_SECONDS:
                    return
                LOGGER.debug(f"AuthToken expiring in {remaining:.0f}s, refreshing")

            response = self._athena_client.get_session_endpoint(SessionId=self._athena_session_id)
            auth_token = response.get("AuthToken")
            if not auth_token:
                raise DbtRuntimeError(
                    f"GetSessionEndpoint returned no AuthToken for session {self._athena_session_id}"
                )
            self._auth_token = auth_token
            self._token_expiry = response.get("AuthTokenExpirationTime")

        def metadata(self):
            self._refresh_token_if_needed()
            base = [(k, v) for k, v in super().metadata() if k != "x-aws-proxy-auth"]
            base.append(("x-aws-proxy-auth", self._auth_token))
            return base

    return AthenaChannelBuilder(
        athena_client, session_id, endpoint_url, initial_auth_token, initial_token_expiry
    )


class AthenaPythonJobHelper(PythonJobHelper):
    """
    Default helper to execute python models with Athena Spark.

    Args:
        PythonJobHelper (PythonJobHelper): The base python helper class
    """

    def __init__(self, parsed_model: Dict[Any, Any], credentials: AthenaCredentials) -> None:
        """
        Initialize spark config and connection.

        Args:
            parsed_model (Dict[Any, Any]): The parsed python model.
            credentials (AthenaCredentials): Credentials for Athena connection.
        """
        self.relation_name = parsed_model.get("relation_name", None)
        self.query_comment = parsed_model.get("query_comment", "")
        self.config = AthenaSparkSessionConfig(
            parsed_model.get("config", {}),
            polling_interval=credentials.poll_interval,
            retry_attempts=credentials.num_retries,
        )
        self.spark_connection = AthenaSparkSessionManager(
            credentials,
            self.timeout,
            self.polling_interval,
            self.engine_config,
            self.relation_name,
            spark_managed_logging=self.config.config.get("spark_managed_logging", False),
        )

    @cached_property
    def timeout(self) -> int:
        """
        Get the timeout value.

        Returns:
            int: The timeout value in seconds.
        """
        return self.config.set_timeout()

    @cached_property
    def session_id(self) -> str:
        """
        Get the session ID.

        Returns:
            str: The session ID as a string.
        """
        return str(self.spark_connection.get_session_id())

    @cached_property
    def polling_interval(self) -> float:
        """
        Get the polling interval.

        Returns:
            float: The polling interval in seconds.
        """
        return self.config.set_polling_interval()

    @cached_property
    def engine_config(self) -> Dict[str, int]:
        """
        Get the engine configuration.

        Returns:
            Dict[str, int]: A dictionary containing the engine configuration.
        """
        return self.config.set_engine_config()

    @cached_property
    def athena_client(self) -> Any:
        """
        Get the Athena client.

        Returns:
            Any: The Athena client object.
        """
        return self.spark_connection.athena_client

    def get_current_session_status(self) -> Any:
        """
        Get the current session status.

        Returns:
            Any: The status of the session
        """
        return self.spark_connection.get_session_status(self.session_id)

    def _prepend_query_comment(self, compiled_code: str) -> str:
        if self.query_comment:
            escaped = self.query_comment.replace("\\", "\\\\").replace('"', '\\"')
            return f'spark.conf.set("dbt.query_comment", "{escaped}")\n{compiled_code}'
        return compiled_code

    def submit(self, compiled_code: str) -> Any:
        """
        Submit a calculation to Athena.

        For PySpark engine version 3, uses the Calculations API
        (StartCalculationExecution).
        For Apache Spark version 3.5+, uses Spark Connect via
        GetSessionEndpoint.
        """
        if str(self.config.config.get("spark_engine_version", "")) == "3.5":
            return self._submit_spark_connect(compiled_code)
        return self._submit_calculation_api(compiled_code)

    def _wait_for_endpoint(self) -> Dict[str, Any]:
        """Poll until the session endpoint is ready and return the full response."""
        polling_interval = self.polling_interval
        timer: float = 0
        throttle_backoff: float = 0
        while True:
            try:
                response = self.athena_client.get_session_endpoint(SessionId=self.session_id)
                endpoint_url = response.get("EndpointUrl")
                if endpoint_url:
                    if not response.get("AuthToken"):
                        raise DbtRuntimeError(
                            f"GetSessionEndpoint returned no AuthToken for session {self.session_id}"
                        )
                    return response
                throttle_backoff = 0
            except botocore.exceptions.ClientError as e:
                error_code = e.response.get("Error", {}).get("Code", "")
                if error_code == "ThrottlingException":
                    throttle_backoff = min((throttle_backoff or 1) * 2, 30) + random.uniform(0, 1)
                    LOGGER.warning(
                        f"Session {self.session_id} endpoint throttled, backing off {throttle_backoff:.1f}s"
                    )
                else:
                    throttle_backoff = 0
                    LOGGER.debug(f"Waiting for session {self.session_id} endpoint: {e}")
            if timer >= self.timeout:
                raise DbtRuntimeError(
                    f"Session {self.session_id} endpoint did not become available within {self.timeout}s"
                )
            sleep_time = throttle_backoff if throttle_backoff else polling_interval
            time.sleep(sleep_time)
            timer += sleep_time

    def _submit_spark_connect(self, compiled_code: str) -> Any:
        """Submit code via Spark Connect (Apache Spark version 3.5+).

        Uses AthenaChannelBuilder to auto-refresh the AuthToken before
        it expires (~29 min TTL), enabling long-running jobs.
        Enforces self.timeout as a hard execution time limit covering both
        endpoint wait and code execution.

        Transient Spark Connect errors (credential propagation failures,
        gRPC pool shutdown) are retried with a new Athena session up to
        ``_SPARK_CONNECT_MAX_RETRIES`` times.
        """
        if not compiled_code.strip():
            return {"SparkConnect": True}

        _ensure_spark_connect_env()
        start_time = time.monotonic()

        last_error: Optional[Exception] = None
        for attempt in range(1, _SPARK_CONNECT_MAX_RETRIES + 1):
            session_id = self.session_id
            spark = None
            timer = None
            timeout_event = threading.Event()
            failed_transient = False

            try:
                elapsed = time.monotonic() - start_time
                remaining = self.timeout - elapsed
                if remaining <= 0:
                    raise DbtRuntimeError(
                        f"Spark Connect execution timed out after {self.timeout} seconds."
                    )

                response = self._wait_for_endpoint()
                channel_builder = _create_athena_channel_builder(
                    self.athena_client,
                    session_id,
                    response["EndpointUrl"],
                    initial_auth_token=response.get("AuthToken"),
                    initial_token_expiry=response.get("AuthTokenExpirationTime"),
                )

                from pyspark.sql.connect.session import SparkSession as ConnectSparkSession

                spark = ConnectSparkSession.builder.channelBuilder(channel_builder).create()

                elapsed = time.monotonic() - start_time
                remaining = self.timeout - elapsed
                if remaining <= 0:
                    raise DbtRuntimeError(
                        f"Spark Connect execution timed out after {self.timeout} seconds."
                    )

                def _on_timeout():
                    timeout_event.set()
                    LOGGER.warning(
                        f"Model {self.relation_name} - Execution timed out after {self.timeout}s"
                    )
                    spark.interruptAll()

                timer = threading.Timer(remaining, _on_timeout)
                timer.start()

                exec_globals = {"spark": spark}
                exec(compiled_code, exec_globals)
                return {"SparkConnect": True}
            except DbtRuntimeError:
                raise
            except Exception as e:
                if timeout_event.is_set():
                    raise DbtRuntimeError(
                        f"Spark Connect execution timed out after {self.timeout} seconds."
                    )
                last_error = e
                if attempt < _SPARK_CONNECT_MAX_RETRIES and _is_transient_spark_error(e):
                    import traceback

                    failed_transient = True
                    backoff = min(2**attempt, 30) + random.uniform(0, 1)
                    LOGGER.warning(
                        f"Model {self.relation_name} - Transient Spark Connect error "
                        f"(attempt {attempt}/{_SPARK_CONNECT_MAX_RETRIES}), "
                        f"retrying in {backoff:.1f}s with new session: "
                        f"{type(e).__name__}: {e}\n{traceback.format_exc()}"
                    )
                    time.sleep(backoff)
                else:
                    import traceback

                    LOGGER.error(f"Spark Connect traceback:\n{traceback.format_exc()}")
                    raise DbtRuntimeError(
                        f"Spark Connect execution failed: {type(e).__name__}: {e}"
                    ) from e
            finally:
                if timer is not None:
                    timer.cancel()
                if spark is not None:
                    spark.stop()
                if failed_transient:
                    # Terminate the failed session to free DPU resources,
                    # then clear cached session_id so the next attempt
                    # creates a fresh session.
                    self.spark_connection.terminate_and_remove_session(session_id)
                    if "session_id" in self.__dict__:
                        del self.__dict__["session_id"]
                else:
                    self.spark_connection.set_spark_session_load(session_id, -1)

        # All retries exhausted
        raise DbtRuntimeError(
            f"Spark Connect execution failed after {_SPARK_CONNECT_MAX_RETRIES} attempts: "
            f"{type(last_error).__name__}: {last_error}"
        )

    def _submit_calculation_api(self, compiled_code: str) -> Any:
        """Submit code via Calculations API (PySpark engine version 3)."""
        # Seeing an empty calculation along with main python model code calculation is submitted for almost every model
        # Also, if not returning the result json, we are getting green ERROR messages instead of OK messages.
        # And with this handling, the run model code in target folder every model under run folder seems to be empty
        # Need to fix this work around solution
        if compiled_code.strip():
            compiled_code = self._prepend_query_comment(compiled_code)
            while True:
                try:
                    LOGGER.debug(
                        f"Model {self.relation_name} - Using session: {self.session_id} to start calculation execution."
                    )
                    calculation_execution_id = self.athena_client.start_calculation_execution(
                        SessionId=self.session_id, CodeBlock=compiled_code.lstrip()
                    )["CalculationExecutionId"]
                    break
                except botocore.exceptions.ClientError as ce:
                    LOGGER.exception(f"Encountered client error: {ce}")
                    error_message = ce.response["Error"].get("Message", "")
                    if (
                        ce.response["Error"]["Code"] == "InvalidRequestException"
                        and "Session is in the BUSY state; needs to be IDLE to accept Calculations."
                        in error_message
                    ):
                        LOGGER.exception("Going to poll until session is IDLE")
                        self.poll_until_session_idle()
                    elif any(
                        state in error_message
                        for state in ["TERMINATED", "TERMINATING", "DEGRADED", "FAILED"]
                    ):
                        # Session is no longer available, clean it up and try with a new one
                        LOGGER.warning(
                            f"Model {self.relation_name} - Session {self.session_id} is no longer available. "
                            f"Error: {error_message}. Getting new session."
                        )
                        self.spark_connection.remove_terminated_session(self.session_id)
                        if "session_id" in self.__dict__:
                            del self.__dict__["session_id"]
                        # Loop continues with fresh session
                    else:
                        # Unknown client error, don't retry forever
                        raise DbtRuntimeError(
                            f"Unable to start spark python code execution. ClientError: {ce}"
                        )
                except Exception as e:
                    raise DbtRuntimeError(f"Unable to start spark python code execution. Got: {e}")
            execution_status = self.poll_until_execution_completion(calculation_execution_id)
            LOGGER.debug(
                f"Model {self.relation_name} - Received execution status {execution_status}"
            )
            result = {}
            if execution_status == "COMPLETED":
                try:
                    execution = self.athena_client.get_calculation_execution(
                        CalculationExecutionId=calculation_execution_id
                    )
                    result = execution["Result"]
                    result["Statistics"] = execution.get("Statistics", {})
                except Exception as e:
                    LOGGER.error(f"Unable to retrieve results: Got: {e}")
                    result = {}
            return result
        else:
            return {
                "ResultS3Uri": "string",
                "ResultType": "string",
                "StdErrorS3Uri": "string",
                "StdOutS3Uri": "string",
            }

    def poll_until_session_idle(self) -> None:
        """
        Polls the session status until it becomes idle or exceeds the timeout.

        Raises:
            DbtRuntimeError: If the session chosen is not available or if it does not become idle within the timeout.
        """
        polling_interval = self.polling_interval
        timer: float = 0
        while True:
            session_status = self.get_current_session_status()["State"]
            if session_status in ["TERMINATING", "TERMINATED", "DEGRADED", "FAILED"]:
                LOGGER.debug(
                    f"Model {self.relation_name} - The session: {self.session_id} was not available. "
                    f"Got status: {session_status}. Will try with a different session."
                )
                self.spark_connection.remove_terminated_session(self.session_id)
                if "session_id" in self.__dict__:
                    del self.__dict__["session_id"]
                break
            if session_status == "IDLE":
                break
            time.sleep(polling_interval)
            timer += polling_interval
            if timer > self.timeout:
                LOGGER.debug(
                    f"Model {self.relation_name} - Session {self.session_id} did not become free within {self.timeout}"
                    " seconds. Will try with a different session."
                )
                if "session_id" in self.__dict__:
                    del self.__dict__["session_id"]
                break

    def poll_until_execution_completion(self, calculation_execution_id: str) -> Any:
        """
        Poll the status of a calculation execution until it is completed, failed, or canceled.

        This function polls the status of a calculation execution identified by the given `calculation_execution_id`
        until it is completed, failed, or canceled. It uses the Athena client to retrieve the status of the execution
        and checks if the state is one of "COMPLETED", "FAILED", or "CANCELED". If the execution is not yet completed,
        the function sleeps for a certain polling interval, which starts with the value of `self.polling_interval` and
        doubles after each iteration until it reaches the `self.timeout` period. If the execution does not complete
        within the timeout period, a `DbtRuntimeError` is raised.

        Args:
            calculation_execution_id (str): The ID of the calculation execution to poll.

        Returns:
            str: The final state of the calculation execution, which can be one of "COMPLETED", "FAILED" or "CANCELED".

        Raises:
            DbtRuntimeError: If the calculation execution does not complete within the timeout period.

        """
        try:
            polling_interval = self.polling_interval
            timer: float = 0
            while True:
                execution_response = self.athena_client.get_calculation_execution(
                    CalculationExecutionId=calculation_execution_id
                )
                execution_session = execution_response.get("SessionId", None)
                execution_status = execution_response.get("Status", None)
                execution_result = execution_response.get("Result", None)
                execution_stderr_s3_path = ""
                if execution_result:
                    execution_stderr_s3_path = execution_result.get("StdErrorS3Uri", None)

                execution_status_state = ""
                execution_status_reason = ""
                if execution_status:
                    execution_status_state = execution_status.get("State", None)
                    execution_status_reason = execution_status.get("StateChangeReason", None)

                if execution_status_state in ["FAILED", "CANCELED"]:
                    raise DbtRuntimeError(
                        f"""Calculation Id:   {calculation_execution_id}
Session Id:     {execution_session}
Status:         {execution_status_state}
Reason:         {execution_status_reason}
Stderr s3 path: {execution_stderr_s3_path}
"""
                    )

                if execution_status_state == "COMPLETED":
                    return execution_status_state

                time.sleep(polling_interval)
                timer += polling_interval
                if timer > self.timeout:
                    self.athena_client.stop_calculation_execution(
                        CalculationExecutionId=calculation_execution_id
                    )
                    raise DbtRuntimeError(
                        f"Execution {calculation_execution_id} did not complete within {self.timeout} seconds."
                    )
        finally:
            self.spark_connection.set_spark_session_load(self.session_id, -1)
