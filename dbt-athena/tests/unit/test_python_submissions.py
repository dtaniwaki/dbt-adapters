import time
import uuid
from unittest.mock import Mock, patch

import botocore.exceptions
import pytest
from dbt_common.exceptions import DbtRuntimeError

from dbt.adapters.athena.python_submissions import AthenaPythonJobHelper
from dbt.adapters.athena.session import AthenaSparkSessionManager

from .constants import DATABASE_NAME


@pytest.mark.usefixtures("athena_credentials", "athena_client")
class TestAthenaPythonJobHelper:
    """
    A class to test the AthenaPythonJobHelper
    """

    @pytest.fixture
    def parsed_model(self, request):
        config: dict[str, int] = request.param.get("config", {"timeout": 1, "polling_interval": 5})

        return {
            "alias": "test_model",
            "schema": DATABASE_NAME,
            "config": {
                "timeout": config["timeout"],
                "polling_interval": config["polling_interval"],
                "engine_config": request.param.get(
                    "engine_config",
                    {"CoordinatorDpuSize": 1, "MaxConcurrentDpus": 2, "DefaultExecutorDpuSize": 1},
                ),
            },
        }

    @pytest.fixture
    def athena_spark_session_manager(self, athena_credentials, parsed_model):
        return AthenaSparkSessionManager(
            athena_credentials,
            timeout=parsed_model["config"]["timeout"],
            polling_interval=parsed_model["config"]["polling_interval"],
            engine_config=parsed_model["config"]["engine_config"],
        )

    @pytest.fixture
    def athena_job_helper(
        self,
        athena_credentials,
        athena_client,
        athena_spark_session_manager,
        parsed_model,
        monkeypatch,
    ):
        mock_job_helper = AthenaPythonJobHelper(parsed_model, athena_credentials)
        monkeypatch.setattr(mock_job_helper, "athena_client", athena_client)
        monkeypatch.setattr(mock_job_helper, "spark_connection", athena_spark_session_manager)
        return mock_job_helper

    @pytest.mark.parametrize(
        "parsed_model, session_status_response, expected_response",
        [
            (
                {"config": {"timeout": 5, "polling_interval": 5}},
                {
                    "State": "IDLE",
                },
                None,
            ),
            pytest.param(
                {"config": {"timeout": 5, "polling_interval": 5}},
                {
                    "State": "FAILED",
                },
                None,
                marks=pytest.mark.xfail,
            ),
            pytest.param(
                {"config": {"timeout": 5, "polling_interval": 5}},
                {
                    "State": "TERMINATED",
                },
                None,
                marks=pytest.mark.xfail,
            ),
            pytest.param(
                {"config": {"timeout": 1, "polling_interval": 5}},
                {
                    "State": "CREATING",
                },
                None,
                marks=pytest.mark.xfail,
            ),
        ],
        indirect=["parsed_model"],
    )
    def test_poll_session_idle(
        self,
        session_status_response,
        expected_response,
        athena_job_helper,
        athena_spark_session_manager,
        monkeypatch,
    ):
        with patch.multiple(
            athena_spark_session_manager,
            get_session_status=Mock(return_value=session_status_response),
            get_session_id=Mock(return_value="test_session_id"),
        ):

            def mock_sleep(_):
                pass

            monkeypatch.setattr(time, "sleep", mock_sleep)
            poll_response = athena_job_helper.poll_until_session_idle()
            assert poll_response == expected_response

    @pytest.mark.parametrize(
        "parsed_model, execution_status, expected_response",
        [
            (
                {"config": {"timeout": 1, "polling_interval": 5}},
                {
                    "Status": {
                        "State": "COMPLETED",
                    }
                },
                "COMPLETED",
            ),
            pytest.param(
                {"config": {"timeout": 1, "polling_interval": 5}},
                {
                    "Status": {
                        "State": "FAILED",
                    }
                },
                None,
                marks=pytest.mark.xfail,
            ),
            pytest.param(
                {"config": {"timeout": 1, "polling_interval": 5}},
                {
                    "Status": {
                        "State": "RUNNING",
                    }
                },
                "RUNNING",
                marks=pytest.mark.xfail,
            ),
        ],
        indirect=["parsed_model"],
    )
    def test_poll_execution(
        self,
        execution_status,
        expected_response,
        athena_job_helper,
        athena_spark_session_manager,
        athena_client,
        monkeypatch,
    ):
        with patch.multiple(
            athena_spark_session_manager,
            get_session_id=Mock(return_value=uuid.uuid4()),
        ):
            with patch.multiple(
                athena_client,
                get_calculation_execution=Mock(return_value=execution_status),
            ):

                def mock_sleep(_):
                    pass

                monkeypatch.setattr(time, "sleep", mock_sleep)
                poll_response = athena_job_helper.poll_until_execution_completion(
                    "test_calculation_id"
                )
                assert poll_response == expected_response

    @pytest.mark.parametrize(
        "parsed_model, test_calculation_execution_id, test_calculation_execution",
        [
            pytest.param(
                {"config": {"timeout": 1, "polling_interval": 5}},
                {"CalculationExecutionId": "test_execution_id"},
                {
                    "Result": {"ResultS3Uri": "test_results_s3_uri"},
                    "Status": {"State": "COMPLETED"},
                },
            ),
            pytest.param(
                {"config": {"timeout": 1, "polling_interval": 5}},
                {"CalculationExecutionId": "test_execution_id"},
                {"Result": {}, "Status": {"State": "FAILED"}},
                marks=pytest.mark.xfail,
            ),
            pytest.param(
                {"config": {"timeout": 1, "polling_interval": 5}},
                {},
                {"Result": {}, "Status": {"State": "FAILED"}},
                marks=pytest.mark.xfail,
            ),
        ],
        indirect=["parsed_model"],
    )
    def test_submission(
        self,
        test_calculation_execution_id,
        test_calculation_execution,
        athena_job_helper,
        athena_spark_session_manager,
        athena_client,
    ):
        with patch.multiple(
            athena_spark_session_manager, get_session_id=Mock(return_value=uuid.uuid4())
        ):
            with patch.multiple(
                athena_client,
                start_calculation_execution=Mock(return_value=test_calculation_execution_id),
                get_calculation_execution=Mock(return_value=test_calculation_execution),
            ):
                with patch.multiple(
                    athena_job_helper, poll_until_session_idle=Mock(return_value="IDLE")
                ):
                    result = athena_job_helper.submit("hello world")
                    expected = dict(test_calculation_execution["Result"])
                    expected["Statistics"] = test_calculation_execution.get("Statistics", {})
                    assert result == expected


class TestSessionStateErrorHandling:
    """
    Self-contained tests for session state error handling in submit().
    These tests don't depend on the class-scoped fixtures from conftest.py.
    """

    @pytest.fixture
    def mock_credentials(self):
        """Create a mock credentials object."""
        credentials = Mock()
        credentials.aws_access_key_id = None
        credentials.aws_secret_access_key = None
        credentials.aws_session_token = None
        credentials.region_name = "us-east-1"
        credentials.aws_profile_name = None
        credentials.spark_work_group = "test-workgroup"
        credentials.poll_interval = 1
        credentials.num_retries = 3
        credentials.effective_num_retries = 3
        return credentials

    @pytest.fixture
    def mock_parsed_model(self):
        """Create a mock parsed model."""
        return {
            "alias": "test_model",
            "relation_name": "test_relation",
            "schema": "test_schema",
            "config": {
                "timeout": 10,
                "polling_interval": 1,
                "engine_config": {
                    "CoordinatorDpuSize": 1,
                    "MaxConcurrentDpus": 2,
                    "DefaultExecutorDpuSize": 1,
                },
            },
        }

    @pytest.mark.parametrize(
        "session_state",
        ["TERMINATED", "TERMINATING", "DEGRADED", "FAILED"],
    )
    def test_submit_handles_terminated_session_states(
        self, mock_credentials, mock_parsed_model, session_state
    ):
        """Test that submit() handles terminated session states by getting a new session."""
        first_session_id = str(uuid.uuid4())
        second_session_id = str(uuid.uuid4())

        # Track session_id calls
        session_id_calls = [0]

        def mock_get_session_id():
            session_id_calls[0] += 1
            if session_id_calls[0] == 1:
                return uuid.UUID(first_session_id)
            return uuid.UUID(second_session_id)

        # First call raises ClientError, second succeeds
        error_response = {
            "Error": {
                "Code": "InvalidRequestException",
                "Message": f"Session is in the {session_state} state",
            }
        }
        client_error = botocore.exceptions.ClientError(error_response, "StartCalculationExecution")

        start_calc_calls = [0]

        def mock_start_calculation(*args, **kwargs):
            start_calc_calls[0] += 1
            if start_calc_calls[0] == 1:
                raise client_error
            return {"CalculationExecutionId": "test_execution_id"}

        with patch(
            "dbt.adapters.athena.python_submissions.AthenaSparkSessionManager"
        ) as MockSessionManager:
            mock_session_manager = Mock()
            mock_session_manager.get_session_id = mock_get_session_id
            mock_session_manager.remove_terminated_session = Mock()
            mock_session_manager.set_spark_session_load = Mock()
            MockSessionManager.return_value = mock_session_manager

            # Create the helper
            helper = AthenaPythonJobHelper(mock_parsed_model, mock_credentials)

            # Mock the athena_client
            mock_athena_client = Mock()
            mock_athena_client.start_calculation_execution = mock_start_calculation
            mock_athena_client.get_calculation_execution = Mock(
                return_value={
                    "Result": {"ResultS3Uri": "test_results_s3_uri"},
                    "Status": {"State": "COMPLETED"},
                }
            )
            helper.__dict__["athena_client"] = mock_athena_client

            result = helper.submit("print('hello')")

            # Verify session was cleaned up
            mock_session_manager.remove_terminated_session.assert_called_once_with(
                first_session_id
            )
            # Verify we got a result (meaning retry worked)
            assert result == {"ResultS3Uri": "test_results_s3_uri", "Statistics": {}}
            # Verify we made two attempts
            assert start_calc_calls[0] == 2

    def test_submit_raises_for_unknown_client_error(self, mock_credentials, mock_parsed_model):
        """Test that submit() raises DbtRuntimeError for unknown ClientErrors."""
        session_id = uuid.uuid4()

        error_response = {
            "Error": {
                "Code": "UnknownException",
                "Message": "Some unexpected error occurred",
            }
        }
        client_error = botocore.exceptions.ClientError(error_response, "StartCalculationExecution")

        with patch(
            "dbt.adapters.athena.python_submissions.AthenaSparkSessionManager"
        ) as MockSessionManager:
            mock_session_manager = Mock()
            mock_session_manager.get_session_id = Mock(return_value=session_id)
            MockSessionManager.return_value = mock_session_manager

            helper = AthenaPythonJobHelper(mock_parsed_model, mock_credentials)

            mock_athena_client = Mock()
            mock_athena_client.start_calculation_execution = Mock(side_effect=client_error)
            helper.__dict__["athena_client"] = mock_athena_client

            with pytest.raises(DbtRuntimeError) as exc_info:
                helper.submit("print('hello')")

            assert "Unable to start spark python code execution" in str(exc_info.value)
            assert "ClientError" in str(exc_info.value)

    def test_submit_routes_to_spark_connect_for_35(self, mock_credentials):
        """submit() delegates to _submit_spark_connect when spark_engine_version is 3.5."""
        parsed_model = {
            "alias": "test_model",
            "relation_name": "test_relation",
            "schema": "test_schema",
            "config": {
                "timeout": 10,
                "polling_interval": 1,
                "spark_engine_version": "3.5",
                "engine_config": {"MaxConcurrentDpus": 2},
            },
        }
        with patch(
            "dbt.adapters.athena.python_submissions.AthenaSparkSessionManager"
        ) as MockSessionManager:
            mock_session_manager = Mock()
            mock_session_manager.get_session_id = Mock(return_value="session-id")
            MockSessionManager.return_value = mock_session_manager

            helper = AthenaPythonJobHelper(parsed_model, mock_credentials)
            with patch.object(helper, "_submit_spark_connect", return_value={}) as mock_sc:
                helper.submit("print('hello')")
                mock_sc.assert_called_once_with("print('hello')")

    def test_submit_routes_to_calculation_api_without_35(
        self, mock_credentials, mock_parsed_model
    ):
        """submit() delegates to _submit_calculation_api when spark_engine_version is not 3.5."""
        with patch(
            "dbt.adapters.athena.python_submissions.AthenaSparkSessionManager"
        ) as MockSessionManager:
            mock_session_manager = Mock()
            mock_session_manager.get_session_id = Mock(return_value="session-id")
            MockSessionManager.return_value = mock_session_manager

            helper = AthenaPythonJobHelper(mock_parsed_model, mock_credentials)
            with patch.object(helper, "_submit_calculation_api", return_value={}) as mock_api:
                helper.submit("print('hello')")
                mock_api.assert_called_once_with("print('hello')")

    def test_submit_spark_connect_empty_code(self, mock_credentials):
        """_submit_spark_connect returns truthy dict for blank code."""
        parsed_model = {
            "alias": "test_model",
            "relation_name": "test_relation",
            "schema": "test_schema",
            "config": {
                "timeout": 10,
                "polling_interval": 1,
                "spark_engine_version": "3.5",
                "engine_config": {"MaxConcurrentDpus": 2},
            },
        }
        with patch(
            "dbt.adapters.athena.python_submissions.AthenaSparkSessionManager"
        ) as MockSessionManager:
            mock_session_manager = Mock()
            mock_session_manager.get_session_id = Mock(return_value="session-id")
            MockSessionManager.return_value = mock_session_manager

            helper = AthenaPythonJobHelper(parsed_model, mock_credentials)
            result = helper._submit_spark_connect("   ")
            assert result == {"SparkConnect": True}

    def test_submit_spark_connect_timeout(self, mock_credentials):
        """_submit_spark_connect raises DbtRuntimeError if endpoint never becomes available."""
        parsed_model = {
            "alias": "test_model",
            "relation_name": "test_relation",
            "schema": "test_schema",
            "config": {
                "timeout": 1,
                "polling_interval": 0.5,
                "spark_engine_version": "3.5",
                "engine_config": {"MaxConcurrentDpus": 2},
            },
        }
        with patch(
            "dbt.adapters.athena.python_submissions.AthenaSparkSessionManager"
        ) as MockSessionManager:
            mock_session_manager = Mock()
            mock_session_manager.get_session_id = Mock(return_value="session-id")
            MockSessionManager.return_value = mock_session_manager

            helper = AthenaPythonJobHelper(parsed_model, mock_credentials)
            mock_athena_client = Mock()
            mock_athena_client.get_session_endpoint = Mock(return_value={})
            helper.__dict__["athena_client"] = mock_athena_client

            with patch("dbt.adapters.athena.python_submissions.time.sleep"):
                with pytest.raises(DbtRuntimeError, match="endpoint did not become available"):
                    helper._submit_spark_connect("print('hello')")

    def test_submit_spark_connect_happy_path(self, mock_credentials):
        """_submit_spark_connect executes code and decrements session load."""
        parsed_model = {
            "alias": "test_model",
            "relation_name": "test_relation",
            "schema": "test_schema",
            "config": {
                "timeout": 10,
                "polling_interval": 1,
                "spark_engine_version": "3.5",
                "engine_config": {"MaxConcurrentDpus": 2},
            },
        }
        with patch(
            "dbt.adapters.athena.python_submissions.AthenaSparkSessionManager"
        ) as MockSessionManager:
            mock_session_manager = Mock()
            mock_session_manager.get_session_id = Mock(return_value="session-id")
            mock_session_manager.set_spark_session_load = Mock()
            MockSessionManager.return_value = mock_session_manager

            helper = AthenaPythonJobHelper(parsed_model, mock_credentials)
            mock_athena_client = Mock()
            mock_athena_client.get_session_endpoint = Mock(
                return_value={
                    "EndpointUrl": "https://spark.athena.example.com",
                    "AuthToken": "test-token",
                }
            )
            helper.__dict__["athena_client"] = mock_athena_client

            mock_spark = Mock()
            mock_connect_session_cls = Mock()
            mock_connect_session_cls.builder.channelBuilder.return_value.create.return_value = (
                mock_spark
            )

            with patch("dbt.adapters.athena.python_submissions._create_athena_channel_builder"):
                with patch.dict(
                    "sys.modules",
                    {
                        "pyspark": Mock(),
                        "pyspark.sql": Mock(),
                        "pyspark.sql.connect": Mock(),
                        "pyspark.sql.connect.session": Mock(SparkSession=mock_connect_session_cls),
                    },
                ):
                    result = helper._submit_spark_connect("x = 1")

            assert result == {"SparkConnect": True}
            mock_spark.stop.assert_called_once()
            mock_session_manager.set_spark_session_load.assert_called_once_with("session-id", -1)

    def test_submit_spark_connect_no_auth_token(self, mock_credentials):
        """_submit_spark_connect raises DbtRuntimeError when AuthToken is missing."""
        parsed_model = {
            "alias": "test_model",
            "relation_name": "test_relation",
            "schema": "test_schema",
            "config": {
                "timeout": 10,
                "polling_interval": 1,
                "spark_engine_version": "3.5",
                "engine_config": {"MaxConcurrentDpus": 2},
            },
        }
        with patch(
            "dbt.adapters.athena.python_submissions.AthenaSparkSessionManager"
        ) as MockSessionManager:
            mock_session_manager = Mock()
            mock_session_manager.get_session_id = Mock(return_value="session-id")
            MockSessionManager.return_value = mock_session_manager

            helper = AthenaPythonJobHelper(parsed_model, mock_credentials)
            mock_athena_client = Mock()
            mock_athena_client.get_session_endpoint = Mock(
                return_value={
                    "EndpointUrl": "https://spark.athena.example.com",
                }
            )
            helper.__dict__["athena_client"] = mock_athena_client

            with pytest.raises(DbtRuntimeError, match="returned no AuthToken"):
                helper._submit_spark_connect("print('hello')")

    @pytest.mark.parametrize(
        "error_message",
        [
            "Unable to load credentials from any of the providers",
            "Pool not running",
        ],
    )
    def test_spark_connect_retries_transient_error(self, mock_credentials, error_message):
        """_submit_spark_connect retries on transient errors with a new session."""
        parsed_model = {
            "alias": "test_model",
            "relation_name": "test_relation",
            "schema": "test_schema",
            "config": {
                "timeout": 30,
                "polling_interval": 1,
                "spark_engine_version": "3.5",
                "engine_config": {"MaxConcurrentDpus": 2},
            },
        }
        with patch(
            "dbt.adapters.athena.python_submissions.AthenaSparkSessionManager"
        ) as MockSessionManager:
            mock_session_manager = Mock()
            session_ids = iter(["session-1", "session-2"])
            mock_session_manager.get_session_id = Mock(side_effect=lambda: next(session_ids))
            mock_session_manager.set_spark_session_load = Mock()
            mock_session_manager.terminate_and_remove_session = Mock()
            MockSessionManager.return_value = mock_session_manager

            helper = AthenaPythonJobHelper(parsed_model, mock_credentials)
            mock_athena_client = Mock()
            mock_athena_client.get_session_endpoint = Mock(
                return_value={
                    "EndpointUrl": "https://spark.athena.example.com",
                    "AuthToken": "test-token",
                }
            )
            helper.__dict__["athena_client"] = mock_athena_client

            call_count = [0]
            mock_spark = Mock()

            def mock_exec(code, globals_dict):
                call_count[0] += 1
                if call_count[0] == 1:
                    raise ValueError(error_message)

            mock_connect_session_cls = Mock()
            mock_connect_session_cls.builder.channelBuilder.return_value.create.return_value = (
                mock_spark
            )

            with patch("dbt.adapters.athena.python_submissions._create_athena_channel_builder"):
                with patch.dict(
                    "sys.modules",
                    {
                        "pyspark": Mock(),
                        "pyspark.sql": Mock(),
                        "pyspark.sql.connect": Mock(),
                        "pyspark.sql.connect.session": Mock(SparkSession=mock_connect_session_cls),
                    },
                ):
                    with patch("dbt.adapters.athena.python_submissions.time.sleep"):
                        with patch("builtins.exec", side_effect=mock_exec):
                            result = helper._submit_spark_connect("x = 1")

            assert result == {"SparkConnect": True}
            assert call_count[0] == 2
            mock_session_manager.terminate_and_remove_session.assert_called_once_with("session-1")

    def test_spark_connect_no_retry_on_code_error(self, mock_credentials):
        """_submit_spark_connect does NOT retry on non-transient code errors."""
        parsed_model = {
            "alias": "test_model",
            "relation_name": "test_relation",
            "schema": "test_schema",
            "config": {
                "timeout": 30,
                "polling_interval": 1,
                "spark_engine_version": "3.5",
                "engine_config": {"MaxConcurrentDpus": 2},
            },
        }
        with patch(
            "dbt.adapters.athena.python_submissions.AthenaSparkSessionManager"
        ) as MockSessionManager:
            mock_session_manager = Mock()
            mock_session_manager.get_session_id = Mock(return_value="session-1")
            mock_session_manager.set_spark_session_load = Mock()
            MockSessionManager.return_value = mock_session_manager

            helper = AthenaPythonJobHelper(parsed_model, mock_credentials)
            mock_athena_client = Mock()
            mock_athena_client.get_session_endpoint = Mock(
                return_value={
                    "EndpointUrl": "https://spark.athena.example.com",
                    "AuthToken": "test-token",
                }
            )
            helper.__dict__["athena_client"] = mock_athena_client

            mock_spark = Mock()
            mock_connect_session_cls = Mock()
            mock_connect_session_cls.builder.channelBuilder.return_value.create.return_value = (
                mock_spark
            )

            with patch("dbt.adapters.athena.python_submissions._create_athena_channel_builder"):
                with patch.dict(
                    "sys.modules",
                    {
                        "pyspark": Mock(),
                        "pyspark.sql": Mock(),
                        "pyspark.sql.connect": Mock(),
                        "pyspark.sql.connect.session": Mock(SparkSession=mock_connect_session_cls),
                    },
                ):
                    with patch("builtins.exec", side_effect=TypeError("some code error")):
                        with pytest.raises(DbtRuntimeError, match="some code error"):
                            helper._submit_spark_connect("x = 1")

            # Session should NOT be terminated (not a transient error)
            mock_session_manager.terminate_and_remove_session.assert_not_called()
            mock_session_manager.set_spark_session_load.assert_called_once_with("session-1", -1)

    def test_submit_handles_busy_session_state(self, mock_credentials, mock_parsed_model):
        """Test that submit() continues to poll when session is BUSY."""
        session_id = uuid.uuid4()

        # First call raises BUSY error, second succeeds
        error_response = {
            "Error": {
                "Code": "InvalidRequestException",
                "Message": "Session is in the BUSY state; needs to be IDLE to accept Calculations.",
            }
        }
        client_error = botocore.exceptions.ClientError(error_response, "StartCalculationExecution")

        call_count = [0]

        def mock_start_calculation(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] == 1:
                raise client_error
            return {"CalculationExecutionId": "test_execution_id"}

        with patch(
            "dbt.adapters.athena.python_submissions.AthenaSparkSessionManager"
        ) as MockSessionManager:
            mock_session_manager = Mock()
            mock_session_manager.get_session_id = Mock(return_value=session_id)
            mock_session_manager.get_session_status = Mock(return_value={"State": "IDLE"})
            mock_session_manager.set_spark_session_load = Mock()
            MockSessionManager.return_value = mock_session_manager

            helper = AthenaPythonJobHelper(mock_parsed_model, mock_credentials)

            mock_athena_client = Mock()
            mock_athena_client.start_calculation_execution = mock_start_calculation
            mock_athena_client.get_calculation_execution = Mock(
                return_value={
                    "Result": {"ResultS3Uri": "test_results_s3_uri"},
                    "Status": {"State": "COMPLETED"},
                }
            )
            helper.__dict__["athena_client"] = mock_athena_client

            result = helper.submit("print('hello')")

            # Verify we got a result
            assert result == {"ResultS3Uri": "test_results_s3_uri", "Statistics": {}}
            # Verify we made two attempts (once failed with BUSY, then succeeded)
            assert call_count[0] == 2


class TestPrependQueryComment:
    """Tests for _prepend_query_comment and query comment propagation through submit()."""

    @pytest.fixture
    def mock_credentials(self):
        credentials = Mock()
        credentials.aws_access_key_id = None
        credentials.aws_secret_access_key = None
        credentials.aws_session_token = None
        credentials.region_name = "us-east-1"
        credentials.aws_profile_name = None
        credentials.spark_work_group = "test-workgroup"
        credentials.poll_interval = 1
        credentials.num_retries = 3
        credentials.effective_num_retries = 3
        return credentials

    def _make_helper(self, mock_credentials, query_comment=""):
        parsed_model = {
            "alias": "test_model",
            "relation_name": "test_relation",
            "schema": "test_schema",
            "query_comment": query_comment,
            "config": {
                "timeout": 10,
                "polling_interval": 1,
                "engine_config": {
                    "CoordinatorDpuSize": 1,
                    "MaxConcurrentDpus": 2,
                    "DefaultExecutorDpuSize": 1,
                },
            },
        }
        with patch("dbt.adapters.athena.python_submissions.AthenaSparkSessionManager"):
            return AthenaPythonJobHelper(parsed_model, mock_credentials)

    def test_no_query_comment(self, mock_credentials):
        helper = self._make_helper(mock_credentials, query_comment="")
        code = "print('hello')"
        assert helper._prepend_query_comment(code) == code

    def test_prepends_query_comment(self, mock_credentials):
        helper = self._make_helper(mock_credentials, query_comment="my_comment")
        result = helper._prepend_query_comment("print('hello')")
        assert result == 'spark.conf.set("dbt.query_comment", "my_comment")\nprint(\'hello\')'

    def test_escapes_special_characters(self, mock_credentials):
        helper = self._make_helper(mock_credentials, query_comment='has "quotes" and \\backslash')
        result = helper._prepend_query_comment("x = 1")
        assert (
            result
            == 'spark.conf.set("dbt.query_comment", "has \\"quotes\\" and \\\\backslash")\nx = 1'
        )

    def test_submit_passes_query_comment_to_calculation_api(self, mock_credentials):
        """submit() prepends query comment to the code sent to StartCalculationExecution."""
        helper = self._make_helper(mock_credentials, query_comment="test_comment")

        mock_athena_client = Mock()
        mock_athena_client.start_calculation_execution = Mock(
            return_value={"CalculationExecutionId": "exec-id"}
        )
        mock_athena_client.get_calculation_execution = Mock(
            return_value={
                "Result": {"ResultS3Uri": "s3://bucket/result"},
                "Status": {"State": "COMPLETED"},
                "Statistics": {},
            }
        )
        helper.__dict__["athena_client"] = mock_athena_client
        helper.__dict__["session_id"] = "session-id"

        helper.submit("x = 1")

        submitted_code = mock_athena_client.start_calculation_execution.call_args[1]["CodeBlock"]
        assert submitted_code.startswith('spark.conf.set("dbt.query_comment", "test_comment")')
        assert "x = 1" in submitted_code

    def test_submit_without_query_comment_sends_original_code(self, mock_credentials):
        """submit() sends original code when no query comment is set."""
        helper = self._make_helper(mock_credentials, query_comment="")

        mock_athena_client = Mock()
        mock_athena_client.start_calculation_execution = Mock(
            return_value={"CalculationExecutionId": "exec-id"}
        )
        mock_athena_client.get_calculation_execution = Mock(
            return_value={
                "Result": {"ResultS3Uri": "s3://bucket/result"},
                "Status": {"State": "COMPLETED"},
                "Statistics": {},
            }
        )
        helper.__dict__["athena_client"] = mock_athena_client
        helper.__dict__["session_id"] = "session-id"

        helper.submit("x = 1")

        submitted_code = mock_athena_client.start_calculation_execution.call_args[1]["CodeBlock"]
        assert submitted_code == "x = 1"

    def test_submit_passes_query_comment_to_spark_connect(self, mock_credentials):
        """submit() prepends query comment to the code executed via Spark Connect."""
        helper = self._make_helper(mock_credentials, query_comment="test_comment")
        helper.config.config["spark_engine_version"] = "3.5"

        with patch.object(helper, "_submit_spark_connect") as mock_sc:
            helper.submit("x = 1")

        submitted_code = mock_sc.call_args[0][0]
        assert submitted_code.startswith('spark.conf.set("dbt.query_comment", "test_comment")')
        assert "x = 1" in submitted_code
