import json
import threading
import time
from functools import cached_property
from hashlib import md5
from typing import Any, Dict

import boto3
import boto3.session
from dbt_common.exceptions import DbtRuntimeError
from dbt_common.invocation import get_invocation_id

from dbt.adapters.athena.config import get_boto3_config
from dbt.adapters.athena.constants import (
    DEFAULT_THREAD_COUNT,
    LOGGER,
    SESSION_IDLE_TIMEOUT_MIN,
)
from dbt.adapters.contracts.connection import Connection

invocation_id = get_invocation_id()
spark_session_list: Dict[str, str] = {}
spark_session_load: Dict[str, int] = {}


def get_boto3_session(connection: Connection) -> boto3.session.Session:
    return boto3.session.Session(
        aws_access_key_id=connection.credentials.aws_access_key_id,
        aws_secret_access_key=connection.credentials.aws_secret_access_key,
        aws_session_token=connection.credentials.aws_session_token,
        region_name=connection.credentials.region_name,
        profile_name=connection.credentials.aws_profile_name,
    )


def get_boto3_session_from_credentials(credentials: Any) -> boto3.session.Session:
    return boto3.session.Session(
        aws_access_key_id=credentials.aws_access_key_id,
        aws_secret_access_key=credentials.aws_secret_access_key,
        aws_session_token=credentials.aws_session_token,
        region_name=credentials.region_name,
        profile_name=credentials.aws_profile_name,
    )


class AthenaSparkSessionManager:
    """
    A helper class to manage Athena Spark Sessions.
    """

    def __init__(
        self,
        credentials: Any,
        timeout: int,
        polling_interval: float,
        engine_config: Dict[str, int],
        relation_name: str = "N/A",
        spark_managed_logging: bool = False,
    ) -> None:
        """
        Initialize the AthenaSparkSessionManager instance.

        Args:
            credentials (Any): The credentials to be used.
            timeout (int): The timeout value in seconds.
            polling_interval (float): The polling interval in seconds.
            engine_config (Dict[str, int]): The engine configuration.

        """
        self.credentials = credentials
        self.timeout = timeout
        self.polling_interval = polling_interval
        self.engine_config = engine_config
        self.lock = threading.Lock()
        self.relation_name = relation_name
        self.spark_managed_logging = spark_managed_logging

    @cached_property
    def spark_threads(self) -> int:
        """Max concurrent Spark sessions.

        Resolution order:
        1. ``spark_threads`` in profile (credentials)
        2. ``DEFAULT_THREAD_COUNT`` constant (fallback)
        """
        configured = getattr(self.credentials, "spark_threads", None)
        if configured is not None:
            return int(configured)
        if not DEFAULT_THREAD_COUNT:
            return 1
        return int(DEFAULT_THREAD_COUNT)

    @cached_property
    def spark_work_group(self) -> str:
        """
        Get the Spark work group.

        Returns:
            str: The Spark work group. Raises an exception if not found in the profile.
        """
        if not self.credentials.spark_work_group:
            raise DbtRuntimeError(
                f"Expected spark_work_group in profile. Got: {self.credentials.spark_work_group}"
            )
        return str(self.credentials.spark_work_group)

    @cached_property
    def athena_client(self) -> Any:
        """
        Get the AWS Athena client.

        This function returns an AWS Athena client object that can be used to interact with the Athena service.
        The client is created using the region name and profile name provided during object instantiation.

        Returns:
            Any: The Athena client object.

        """
        return get_boto3_session_from_credentials(self.credentials).client(
            "athena", config=get_boto3_config(num_retries=self.credentials.effective_num_retries)
        )

    @cached_property
    def session_description(self) -> str:
        """
        Converts the engine configuration to md5 hash value

        Returns:
            str: A concatenated text of dbt invocation_id and engine configuration's md5 hash
        """
        hash_desc = md5(
            json.dumps(self.engine_config, sort_keys=True, ensure_ascii=True).encode("utf-8")
        ).hexdigest()
        return f"dbt: {invocation_id} - {hash_desc}"

    _DEAD_SESSION_STATES = frozenset({"FAILED", "TERMINATED", "TERMINATING", "DEGRADED"})

    def _evict_dead_sessions(self) -> int:
        """Check pool sessions and remove any that Athena has terminated.

        Returns the number of evicted sessions.
        """
        evicted = 0
        for session_id in list(spark_session_list):
            try:
                state = self.get_session_status(session_id).get("State", "")
            except Exception:
                state = "UNKNOWN"
            if state in self._DEAD_SESSION_STATES or state == "UNKNOWN":
                LOGGER.info(f"Evicting dead session {session_id} (state={state}) from pool")
                self.remove_terminated_session(session_id)
                evicted += 1
        return evicted

    def get_session_id(self, session_query_capacity: int = 1) -> str:
        """Get or wait for a Spark session within the ``spark_threads`` limit.

        * Under the limit → create a new session immediately.
        * At the limit → wait for an existing session to have capacity.
        * If all sessions are dead (idle timeout etc.) → evict and create new.
        * Raises ``DbtRuntimeError`` if no session becomes available
          within ``self.timeout`` seconds.
        """
        timer: float = 0
        eviction_done = False
        while True:
            session_list = list(spark_session_list.items())

            if len(session_list) < self.spark_threads:
                LOGGER.debug(
                    f"Within thread limit ({len(session_list)}/{self.spark_threads}), "
                    f"creating new session for model: {self.relation_name}"
                )
                return self.start_session()

            matching_session_id = next(
                (
                    session_id
                    for session_id, description in session_list
                    if description == self.session_description
                    and spark_session_load.get(session_id, 0) <= session_query_capacity
                ),
                None,
            )
            if matching_session_id:
                LOGGER.debug(
                    f"At thread limit, reusing session {matching_session_id} "
                    f"for model: {self.relation_name}"
                )
                self.set_spark_session_load(str(matching_session_id), 1)
                return matching_session_id

            # No available session. Before waiting, evict dead sessions once
            # so that slots open up immediately (e.g. after idle timeout).
            if not eviction_done:
                evicted = self._evict_dead_sessions()
                eviction_done = True
                if evicted > 0:
                    continue  # Re-check pool without sleeping

            if timer >= self.timeout:
                raise DbtRuntimeError(
                    f"No Spark session available for model {self.relation_name} "
                    f"within {self.timeout}s (spark_threads={self.spark_threads}, "
                    f"active_sessions={len(session_list)})"
                )

            LOGGER.debug(
                f"At thread limit ({len(session_list)}/{self.spark_threads}), "
                f"all sessions busy. Waiting for model: {self.relation_name}"
            )
            time.sleep(self.polling_interval)
            timer += self.polling_interval

    def start_session(self) -> str:
        """
        Start an Athena session.

        This function sends a request to the Athena service to start a session in the specified Spark workgroup.
        It configures the session with specific engine configurations. If the session state is not IDLE, the function
        polls until the session creation is complete. The response containing session information is returned.

        Returns:
            dict: The session information dictionary.

        """
        description = self.session_description
        response = self.athena_client.start_session(
            Description=description,
            WorkGroup=self.credentials.spark_work_group,
            EngineConfiguration=self.engine_config,
            SessionIdleTimeoutInMinutes=SESSION_IDLE_TIMEOUT_MIN,
        )
        session_id = response["SessionId"]
        if response["State"] != "IDLE":
            self.poll_until_session_creation(session_id)

        with self.lock:
            spark_session_list[session_id] = self.session_description
            spark_session_load[session_id] = 1

        return session_id

    def poll_until_session_creation(self, session_id: str) -> None:
        """
        Polls the status of an Athena session creation until it is completed or reaches the timeout.

        Args:
            session_id (str): The ID of the session being created.

        Returns:
            str: The final status of the session, which will be "IDLE" if the session creation is successful.

        Raises:
            DbtRuntimeError: If the session creation fails, is terminated, or degrades during polling.
            DbtRuntimeError: If the session does not become IDLE within the specified timeout.

        """
        polling_interval = self.polling_interval
        timer: float = 0
        while True:
            creation_status_response = self.get_session_status(session_id)
            creation_status_state = creation_status_response.get("State", "")
            creation_status_reason = creation_status_response.get("StateChangeReason", "")
            if creation_status_state in ["FAILED", "TERMINATED", "DEGRADED"]:
                raise DbtRuntimeError(
                    f"Unable to create session: {session_id}. Got status: {creation_status_state}"
                    f" with reason: {creation_status_reason}."
                )
            elif creation_status_state == "IDLE":
                LOGGER.debug(f"Session: {session_id} created")
                break
            time.sleep(polling_interval)
            timer += polling_interval
            if timer > self.timeout:
                self.remove_terminated_session(session_id)
                raise DbtRuntimeError(
                    f"Session {session_id} did not create within {self.timeout} seconds."
                )

    def get_session_status(self, session_id: str) -> Any:
        """
        Get the session status.

        Returns:
            Any: The status of the session
        """
        return self.athena_client.get_session_status(SessionId=session_id)["Status"]

    def remove_terminated_session(self, session_id: str) -> None:
        """
        Removes session uuid from session list variable

        Returns: None
        """
        with self.lock:
            spark_session_list.pop(session_id, "Session id not found")
            spark_session_load.pop(session_id, "Session id not found")

    def terminate_and_remove_session(self, session_id: str) -> None:
        """Terminate an Athena Spark session and remove it from the pool.

        Sends TerminateSession to Athena so the session releases its DPU
        resources immediately (instead of waiting for idle timeout), then
        removes it from the in-memory pool.  Best-effort: errors from the
        API call are logged but do not propagate.
        """
        try:
            self.athena_client.terminate_session(SessionId=session_id)
            LOGGER.info(f"Terminated Athena session {session_id}")
        except Exception as e:
            LOGGER.warning(f"Failed to terminate session {session_id}: {e}")
        self.remove_terminated_session(session_id)

    def set_spark_session_load(self, session_id: str, change: int) -> None:
        """
        Increase or decrease the session load variable

        Returns: None
        """
        with self.lock:
            spark_session_load[session_id] = spark_session_load.get(session_id, 0) + change
