import contextlib
import json
import os
import random
import threading
import time
import traceback
from functools import cached_property
from hashlib import md5
from typing import Any, Dict, Iterator, NamedTuple, Optional

import botocore
from dbt_common.exceptions import DbtRuntimeError
from dbt_common.invocation import get_invocation_id

from dbt.adapters.athena.config import AthenaSparkSessionConfig
from dbt.adapters.athena.connections import AthenaCredentials
from dbt.adapters.athena.constants import DEFAULT_SPARK_CONNECT_MAX_SESSIONS, LOGGER
from dbt.adapters.athena.session import AthenaSparkSessionManager
from dbt.adapters.athena.spark_connect_session import SparkConnectSessionPool
from dbt.adapters.base import PythonJobHelper

SUBMISSION_LANGUAGE = "python"

# GetSessionEndpoint returns an AuthToken that expires after ~29 minutes;
# refresh when fewer than this many seconds remain so a long-running gRPC
# call never attaches an expired token.
_TOKEN_REFRESH_MARGIN_SECONDS = 120

# Max retry attempts for transient Spark Connect errors.
_SPARK_CONNECT_MAX_RETRIES = 3

# Upper bound on how long to wait for GetSessionEndpoint to return a ready
# endpoint before giving up.  Kept separate from ``self.timeout`` so endpoint
# wait cannot consume the entire execution budget.
_ENDPOINT_READY_TIMEOUT_SECONDS = 180

# Patterns that indicate a transient Spark Connect error where retrying
# with a new session is expected to succeed.  String matching is fragile —
# pyspark wording may drift between versions — so ``_is_transient_spark_error``
# also inspects gRPC status codes when available.
_TRANSIENT_SPARK_PATTERNS = [
    # Spark executor failed to obtain credentials from the provider chain
    # (observed when many sessions start concurrently).
    "Unable to load credentials",
    # Spark executor failed to resolve the AWS region via
    # DefaultAwsRegionProviderChain (IMDS not yet available at executor startup).
    "Unable to load region",
    # gRPC connection pool was shut down (secondary failure after a session
    # error).  A new session creates a fresh pool.
    "Pool not running",
    # Athena terminated the Spark session (idle timeout, DPU pressure, or
    # concurrent session limit).  A new session resolves this.
    "Session not active",
    # Athena rejected start_session because the account/workgroup session
    # quota is exhausted.  Retrying after other sessions finish is expected.
    "Maximum allowed sessions",
]

# gRPC status codes (by name) that we treat as transient.  Checked
# structurally via ``grpc.RpcError.code()`` so wording changes do not defeat
# retry behavior.
_TRANSIENT_GRPC_STATUS_CODES = frozenset(
    {"UNAVAILABLE", "DEADLINE_EXCEEDED", "ABORTED", "RESOURCE_EXHAUSTED"}
)


class _AttemptResult(NamedTuple):
    """Outcome of a single Spark Connect submission attempt.

    ``done=True`` means the caller should return ``result`` immediately.
    ``done=False`` means a transient failure was captured in ``error`` and
    the caller may retry with a new session.
    """

    result: Optional[Dict[str, Any]]
    error: Optional[BaseException]
    done: bool


def _is_transient_spark_error(e: BaseException) -> bool:
    """Return True if the exception is a transient Spark Connect error.

    Prefer gRPC status code when the exception exposes one; fall back to
    substring matching against known pyspark/Athena messages.
    """
    # gRPC errors may be wrapped by pyspark; walk the chain.  Track seen
    # exception identities to handle the rare case of a self-referential
    # ``__cause__`` / ``__context__`` cycle introduced by pathological user
    # code or instrumentation wrappers.
    seen: set = set()
    current: Optional[BaseException] = e
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        code_fn = getattr(current, "code", None)
        if callable(code_fn):
            try:
                code = code_fn()
            except Exception:  # noqa: BLE001 - not a gRPC error
                code = None
            if code is not None and getattr(code, "name", None) in _TRANSIENT_GRPC_STATUS_CODES:
                return True
        current = current.__cause__ or current.__context__

    error_str = f"{type(e).__name__}: {e}"
    return any(p in error_str for p in _TRANSIENT_SPARK_PATTERNS)


# SPARK_CONNECT_MODE_ENABLED influences how ``pyspark.sql.SparkSession``
# builder resolves when user model code falls back to the generic SparkSession
# API instead of our injected ``spark`` object.  We scope this per-exec so
# the adapter never permanently pollutes the host process environment (which
# would affect unrelated pyspark usage in multi-tenant workers like dbt Cloud).
@contextlib.contextmanager
def _spark_connect_mode_enabled() -> Iterator[None]:
    previous = os.environ.get("SPARK_CONNECT_MODE_ENABLED")
    os.environ["SPARK_CONNECT_MODE_ENABLED"] = "1"
    try:
        yield
    finally:
        if previous is None:
            os.environ.pop("SPARK_CONNECT_MODE_ENABLED", None)
        else:
            os.environ["SPARK_CONNECT_MODE_ENABLED"] = previous


# Cache for the dynamically-built AthenaChannelBuilder class.  pyspark is
# imported lazily so this module stays importable without Spark 3.5.
_athena_channel_builder_cls: Any = None
_athena_channel_builder_cls_lock = threading.Lock()


def _get_athena_channel_builder_cls() -> Any:
    global _athena_channel_builder_cls
    if _athena_channel_builder_cls is not None:
        return _athena_channel_builder_cls
    with _athena_channel_builder_cls_lock:
        if _athena_channel_builder_cls is not None:
            return _athena_channel_builder_cls

        from datetime import datetime, timezone

        from pyspark.sql.connect.client.core import ChannelBuilder

        class AthenaChannelBuilder(ChannelBuilder):
            """pyspark ChannelBuilder with Athena-specific AuthToken refresh.

            ``metadata()`` is invoked from pyspark gRPC executor threads on
            every outgoing call, potentially concurrently.  Each call must
            attach a current ``x-aws-proxy-auth`` header — Athena issues
            tokens that expire in ~29 minutes, so long-running jobs must
            refresh on their own.  This class serialises refreshes with
            double-checked locking and preserves the parent-class header
            set.
            """

            def __init__(
                self,
                client: Any,
                sid: str,
                url: str,
                auth_token: Optional[str],
                token_expiry: Any,
            ) -> None:
                sc_url = url.replace("https://", "sc://", 1) + ":443/;use_ssl=true"
                super().__init__(sc_url)
                self._athena_client = client
                self._athena_session_id = sid
                self._auth_token = auth_token
                self._token_expiry = token_expiry
                # gRPC calls ``metadata()`` from executor threads; serialize
                # read-modify-write of the token to avoid racing refreshes.
                self._token_lock = threading.Lock()

            def _refresh_token_if_needed(self) -> None:
                # Double-checked locking: cheap read outside the lock, then
                # recheck under the lock so at most one thread issues the
                # refresh call.
                if self._token_is_fresh():
                    return
                with self._token_lock:
                    if self._token_is_fresh():
                        return
                    response = self._athena_client.get_session_endpoint(
                        SessionId=self._athena_session_id
                    )
                    auth_token = response.get("AuthToken")
                    if not auth_token:
                        raise DbtRuntimeError(
                            f"GetSessionEndpoint returned no AuthToken for session "
                            f"{self._athena_session_id}"
                        )
                    self._auth_token = auth_token
                    self._token_expiry = response.get("AuthTokenExpirationTime")

            def _token_is_fresh(self) -> bool:
                if not (self._auth_token and self._token_expiry):
                    return False
                remaining = (self._token_expiry - datetime.now(timezone.utc)).total_seconds()
                return remaining > _TOKEN_REFRESH_MARGIN_SECONDS

            def metadata(self) -> Any:
                self._refresh_token_if_needed()
                # Hold the lock across both the token read and the
                # super().metadata() call: pyspark's ChannelBuilder does not
                # publicly commit to metadata() being pure, so serialising
                # keeps us safe against future implementation changes.
                with self._token_lock:
                    token = self._auth_token
                    base = [(k, v) for k, v in super().metadata() if k != "x-aws-proxy-auth"]
                    base.append(("x-aws-proxy-auth", token))
                    return base

        _athena_channel_builder_cls = AthenaChannelBuilder
        return _athena_channel_builder_cls


def _create_athena_channel_builder(
    athena_client: Any,
    session_id: str,
    endpoint_url: str,
    initial_auth_token: Optional[str] = None,
    initial_token_expiry: Any = None,
) -> Any:
    """Build a ChannelBuilder that auto-refreshes the Athena AuthToken.

    The AuthToken returned by GetSessionEndpoint expires after ~29 minutes.
    This builder calls GetSessionEndpoint to obtain a fresh token whenever
    the current token is about to expire, so long-running Spark Connect
    jobs are not interrupted by PERMISSION_DENIED errors.  Imports pyspark
    lazily so the module stays importable without the Spark 3.5 runtime.
    """
    cls = _get_athena_channel_builder_cls()
    return cls(
        athena_client,
        session_id,
        endpoint_url,
        initial_auth_token,
        initial_token_expiry,
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
        self.credentials = credentials
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

    @cached_property
    def _is_spark_connect(self) -> bool:
        """True when the model requests Apache Spark 3.5+ via Spark Connect."""
        return str(self.config.config.get("spark_engine_version", "")) == "3.5"

    @cached_property
    def _spark_connect_pool(self) -> SparkConnectSessionPool:
        return SparkConnectSessionPool()

    @cached_property
    def _session_fingerprint(self) -> str:
        """md5 of engine config + workgroup + engine version.

        Sessions with matching fingerprint may be reused across models.
        Workgroup and engine version are included so two models that differ
        only in those attributes never accidentally share a session.
        """
        payload = {
            "engine_config": self.engine_config,
            "spark_work_group": self.credentials.spark_work_group,
            "spark_engine_version": str(self.config.config.get("spark_engine_version", "")),
        }
        # ``usedforsecurity=False`` is required on FIPS-enforced Python builds
        # (e.g. RHEL in FIPS mode); md5 here is purely a session-key fingerprint.
        return md5(
            json.dumps(payload, sort_keys=True, ensure_ascii=True, default=str).encode("utf-8"),
            usedforsecurity=False,
        ).hexdigest()

    @cached_property
    def _session_key(self) -> tuple:
        return (get_invocation_id(), self._session_fingerprint)

    @cached_property
    def _spark_connect_max_sessions(self) -> int:
        # AthenaCredentials.__post_init__ already validates type and range,
        # so here we only apply the default when the user didn't set it.
        configured = getattr(self.credentials, "spark_connect_max_sessions", None)
        if configured is None:
            return DEFAULT_SPARK_CONNECT_MAX_SESSIONS
        return int(configured)

    def _session_description(self) -> str:
        invocation = get_invocation_id()
        return f"dbt: {invocation} - {self._session_fingerprint}"

    def get_current_session_status(self) -> Any:
        """
        Get the current session status.

        Returns:
            Any: The status of the session
        """
        return self.spark_connection.get_session_status(self.session_id)

    def submit(self, compiled_code: str) -> Any:
        """
        Submit a calculation to Athena.

        For PySpark engine version 3, executes via the Calculations API
        (StartCalculationExecution).  For Apache Spark 3.5+, executes via
        Spark Connect over a gRPC channel obtained from GetSessionEndpoint.

        Args:
            compiled_code (str): The compiled code to submit for execution.

        Returns:
            dict: The execution result.

        Raises:
            DbtRuntimeError: If the execution ends in a state other than "COMPLETED".

        """
        if self._is_spark_connect:
            return self._submit_spark_connect(compiled_code)

        # Seeing an empty calculation along with main python model code calculation is submitted for almost every model
        # Also, if not returning the result json, we are getting green ERROR messages instead of OK messages.
        # And with this handling, the run model code in target folder every model under run folder seems to be empty
        # Need to fix this work around solution
        if compiled_code.strip():
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
            if execution_status == "COMPLETED":
                try:
                    execution_response = self.athena_client.get_calculation_execution(
                        CalculationExecutionId=calculation_execution_id
                    )
                    result = execution_response.get("Result") or {}
                    statistics = execution_response.get("Statistics")
                    if statistics is not None:
                        result["Statistics"] = statistics
                    result["SparkSessionId"] = self.session_id
                    result["SparkCalculationExecutionId"] = calculation_execution_id
                except Exception as e:
                    LOGGER.error(f"Unable to retrieve results: Got: {e}")
                    # Preserve identifiers so CloudWatch / Athena console can
                    # still be used to debug the failed fetch.
                    result = {
                        "SparkSessionId": self.session_id,
                        "SparkCalculationExecutionId": calculation_execution_id,
                    }
            return result
        else:
            # dbt submits an empty "ghost" calculation alongside every python
            # model to keep the adapter response shape consistent.  This
            # branch returns placeholder data without hitting Athena.
            return {
                "ResultS3Uri": "string",
                "ResultType": "string",
                "StdErrorS3Uri": "string",
                "StdOutS3Uri": "string",
                "SparkSessionId": self.session_id,
                "SparkCalculationExecutionId": None,
            }

    def _acquire_spark_connect_session(self) -> str:
        """Acquire a Spark Connect session from the pool."""
        spark_work_group = self.credentials.spark_work_group
        if not spark_work_group:
            # Spark Connect cannot target an Athena workgroup if none is
            # configured; fail fast with a clear message rather than letting
            # boto3 surface a less helpful validation error.
            raise DbtRuntimeError(
                "spark_work_group must be set in the Athena profile to submit "
                "python models via Spark Connect (spark_engine_version=3.5)."
            )
        return self._spark_connect_pool.acquire(
            key=self._session_key,
            athena_client=self.athena_client,
            spark_work_group=spark_work_group,
            engine_config=self.engine_config,
            session_description=self._session_description(),
            max_sessions=self._spark_connect_max_sessions,
            timeout=self.timeout,
            polling_interval=self.polling_interval,
        )

    def _wait_for_endpoint(self, session_id: str) -> Dict[str, Any]:
        """Poll GetSessionEndpoint until the endpoint is ready.

        Bounded by ``min(self.timeout, _ENDPOINT_READY_TIMEOUT_SECONDS)`` so
        slow endpoint provisioning cannot consume the full execution budget
        reserved for user code.
        """
        deadline_seconds = min(self.timeout, _ENDPOINT_READY_TIMEOUT_SECONDS)
        timer: float = 0
        # ``throttle_base`` is the exponential-backoff base (without jitter),
        # so successive throttles compute 1→2→4→…→30 rather than doubling a
        # jittered value that drifts unpredictably.
        throttle_base: float = 0
        while True:
            throttled = False
            try:
                response = self.athena_client.get_session_endpoint(SessionId=session_id)
                endpoint_url = response.get("EndpointUrl")
                if endpoint_url:
                    if not response.get("AuthToken"):
                        # Retry instead of failing fast: Athena occasionally
                        # returns an endpoint_url a moment before AuthToken
                        # is populated.
                        LOGGER.debug(
                            f"Session {session_id} endpoint returned without AuthToken, "
                            f"retrying"
                        )
                    else:
                        return response
            except botocore.exceptions.ClientError as e:
                error_code = e.response.get("Error", {}).get("Code", "")
                if error_code == "ThrottlingException":
                    throttled = True
                    throttle_base = min(max(throttle_base, 1) * 2, 30)
                    LOGGER.debug(
                        f"Session {session_id} endpoint throttled, "
                        f"backing off ~{throttle_base:.1f}s"
                    )
                else:
                    LOGGER.debug(f"Waiting for session {session_id} endpoint: {e}")

            if not throttled:
                # Non-throttle path (success-without-token or transient error):
                # reset the backoff so we don't inherit stale pressure.
                throttle_base = 0

            if timer >= deadline_seconds:
                raise DbtRuntimeError(
                    f"Session {session_id} endpoint did not become ready within "
                    f"{deadline_seconds}s (endpoint-wait deadline, not execution "
                    f"timeout)"
                )
            sleep_time = (
                throttle_base + random.uniform(0, 1) if throttled else self.polling_interval
            )
            time.sleep(sleep_time)
            timer += sleep_time

    def _submit_spark_connect(self, compiled_code: str) -> Any:
        """Submit code via Spark Connect (Apache Spark 3.5+).

        Transient Spark Connect errors (credential propagation failures,
        gRPC pool shutdown) are retried with a new session up to
        ``_SPARK_CONNECT_MAX_RETRIES`` times.  ``self.timeout`` is a hard
        execution-time limit that covers both endpoint wait and code
        execution.

        Empty ``compiled_code`` bypasses both Athena and the session pool —
        dbt submits a "ghost" empty calculation alongside every python
        model and we have nothing to run.  No session is acquired in this
        case because starting one would cost DPUs for no benefit; models
        with real code will still fingerprint-match and share a session
        via the pool.
        """
        if not compiled_code.strip():
            return {"SparkConnect": True, "SparkSessionId": None}

        start_time = time.monotonic()
        last_error: Optional[BaseException] = None

        for attempt in range(1, _SPARK_CONNECT_MAX_RETRIES + 1):
            outcome = self._spark_connect_attempt(compiled_code, attempt, start_time)
            if outcome.done:
                return outcome.result
            last_error = outcome.error

            is_last_attempt = attempt >= _SPARK_CONNECT_MAX_RETRIES
            if is_last_attempt:
                break

            backoff = min(2**attempt, 30) + random.uniform(0, 1)
            remaining = self.timeout - (time.monotonic() - start_time)
            if backoff >= remaining:
                # No budget left to retry; surface the last error as
                # "failed after N attempts" rather than silently sleeping
                # past the execution timeout.
                LOGGER.warning(
                    f"Model {self.relation_name} - Transient Spark Connect error on "
                    f"attempt {attempt}/{_SPARK_CONNECT_MAX_RETRIES}, "
                    f"but remaining budget ({remaining:.1f}s) is below backoff "
                    f"({backoff:.1f}s); giving up."
                )
                break
            LOGGER.warning(
                f"Model {self.relation_name} - Transient Spark Connect error "
                f"(attempt {attempt}/{_SPARK_CONNECT_MAX_RETRIES}), "
                f"retrying in {backoff:.1f}s with new session: "
                f"{type(last_error).__name__}: {last_error}"
            )
            time.sleep(backoff)

        # All retries exhausted — re-raise the last error with a wrapping
        # message so operators can distinguish "failed once" from "failed
        # after N attempts".
        raise DbtRuntimeError(
            f"Spark Connect execution failed after {_SPARK_CONNECT_MAX_RETRIES} "
            f"attempts: {type(last_error).__name__}: {last_error}"
        ) from last_error

    def _spark_connect_attempt(
        self,
        compiled_code: str,
        attempt: int,
        start_time: float,
    ) -> "_AttemptResult":
        """Run one Spark Connect attempt.

        Returns an ``_AttemptResult`` whose semantics are:
          * ``done=True, result=<dict>`` — success, caller should return result.
          * ``done=False, error=<exc>`` — transient failure, caller may retry.

        Non-retriable failures and timeouts raise directly (caller sees the
        exception instead of a return value).

        Session lifecycle: on transient failure the session is terminated
        (it is likely broken); on success or acquire failure it is released
        back to the pool for reuse.
        """
        session_id = self._acquire_spark_connect_session()
        spark = None
        timer: Optional[threading.Timer] = None
        timeout_event = threading.Event()
        terminate_session = False

        try:
            remaining = self.timeout - (time.monotonic() - start_time)
            if remaining <= 0:
                raise DbtRuntimeError(
                    f"Spark Connect execution timed out after {self.timeout} seconds."
                )

            response = self._wait_for_endpoint(session_id)
            channel_builder = _create_athena_channel_builder(
                self.athena_client,
                session_id,
                response["EndpointUrl"],
                initial_auth_token=response.get("AuthToken"),
                initial_token_expiry=response.get("AuthTokenExpirationTime"),
            )

            from pyspark.sql.connect.session import (
                SparkSession as ConnectSparkSession,
            )

            spark = ConnectSparkSession.builder.channelBuilder(channel_builder).create()

            remaining = self.timeout - (time.monotonic() - start_time)
            if remaining <= 0:
                raise DbtRuntimeError(
                    f"Spark Connect execution timed out after {self.timeout} seconds."
                )

            def _on_timeout() -> None:
                timeout_event.set()
                LOGGER.warning(
                    f"Model {self.relation_name} - " f"Execution timed out after {self.timeout}s"
                )
                if spark is not None:
                    spark.interruptAll()

            timer = threading.Timer(remaining, _on_timeout)
            timer.start()

            exec_globals: Dict[str, Any] = {"spark": spark}
            with _spark_connect_mode_enabled():
                exec(compiled_code, exec_globals)  # noqa: S102 - user model code
            return _AttemptResult(
                result={"SparkConnect": True, "SparkSessionId": session_id},
                error=None,
                done=True,
            )
        except DbtRuntimeError:
            raise
        except Exception as e:
            if timeout_event.is_set():
                raise DbtRuntimeError(
                    f"Spark Connect execution timed out after {self.timeout} seconds."
                ) from e

            transient = _is_transient_spark_error(e)
            is_last_attempt = attempt >= _SPARK_CONNECT_MAX_RETRIES

            if transient:
                # Terminate the broken session whether or not we retry:
                # leaving it in the pool risks a later model reusing it and
                # hitting the same failure ("Session not active" etc.).
                terminate_session = True

            if not transient or is_last_attempt:
                LOGGER.error(
                    f"Model {self.relation_name} - Spark Connect execution failed "
                    f"(attempt {attempt}/{_SPARK_CONNECT_MAX_RETRIES}): "
                    f"{type(e).__name__}: {e}\n{traceback.format_exc()}"
                )
                if not transient:
                    raise DbtRuntimeError(
                        f"Spark Connect execution failed: {type(e).__name__}: {e}"
                    ) from e

            # Transient + not last attempt → let the caller retry with a new
            # session.
            return _AttemptResult(result=None, error=e, done=False)
        finally:
            # Cancel the watchdog timer first and wait for any already-fired
            # callback to finish.  Otherwise spark.interruptAll() running in
            # the timer thread can race with spark.stop() below.
            if timer is not None:
                timer.cancel()
                timer.join(timeout=5)
            if spark is not None:
                try:
                    spark.stop()
                except Exception as e:  # noqa: BLE001 - best-effort cleanup
                    LOGGER.debug(f"Ignoring error while stopping Spark session: {e}")
            if terminate_session:
                # Terminate the failed session so DPUs are released;
                # the next attempt acquires a fresh one from the pool.
                self._spark_connect_pool.terminate_and_remove(session_id)
            else:
                self._spark_connect_pool.release(session_id)

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
