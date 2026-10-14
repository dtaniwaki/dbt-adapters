from types import SimpleNamespace
from unittest.mock import MagicMock

import jinja2
import pytest
from moto import mock_aws

from dbt.adapters.athena.config import (
    AthenaSparkSessionConfig,
    build_iceberg_cross_account_catalog_properties,
    get_spark_cross_account_catalog_mode,
)
from dbt.adapters.athena.python_submissions import AthenaPythonJobHelper
from dbt.adapters.athena.utils import AthenaCatalogType

from .constants import FEDERATED_QUERY_CATALOG_NAME
from .test_spark_dbt_obj import _MACRO_DIR

SEPARATOR_KEY = "spark.hadoop.aws.glue.catalog.separator"
# AWS documentation sample account IDs
SHARED_ACCOUNT_ID = "123456789012"
OTHER_ACCOUNT_ID = "111122223333"


class TestSparkCrossAccountCatalogMode:
    @pytest.mark.parametrize(
        "value, expected",
        [
            (None, "none"),
            (False, "none"),
            (True, "hive"),
            ("none", "none"),
            ("hive", "hive"),
            ("iceberg", "iceberg"),
            (" Hive ", "hive"),
            ("true", "hive"),
            ("false", "none"),
        ],
    )
    def test_normalizes_booleans_and_mode_names(self, value, expected):
        assert get_spark_cross_account_catalog_mode(value) == expected

    @pytest.mark.parametrize("value", ["unknown", "", 1, ["hive"]])
    def test_rejects_unknown_values(self, value):
        with pytest.raises(ValueError, match="spark_cross_account_catalog"):
            get_spark_cross_account_catalog_mode(value)


class TestSetEngineConfigHiveMode:
    @pytest.mark.parametrize(
        "value, has_separator",
        [
            (None, False),
            (False, False),
            ("none", False),
            (True, True),
            ("hive", True),
        ],
    )
    def test_separator_set_only_for_hive(self, value, has_separator):
        config = {} if value is None else {"spark_cross_account_catalog": value}
        engine_config = AthenaSparkSessionConfig(config).set_engine_config()
        spark_properties = engine_config["SparkProperties"]
        assert (spark_properties.get(SEPARATOR_KEY) == "/") is has_separator

    def test_separator_set_for_hive_on_spark_connect(self):
        config = {"spark_cross_account_catalog": "hive", "spark_engine_version": "3.5"}
        engine_config = AthenaSparkSessionConfig(config).set_engine_config()
        spark_defaults = next(
            c for c in engine_config["Classifications"] if c["Name"] == "spark-defaults"
        )
        assert spark_defaults["Properties"][SEPARATOR_KEY] == "/"


EXPECTED_SHARED_CATALOG_PROPERTIES = {
    "spark.sql.catalog.shared_catalog": "org.apache.iceberg.spark.SparkCatalog",
    "spark.sql.catalog.shared_catalog.catalog-impl": "org.apache.iceberg.aws.glue.GlueCatalog",
    "spark.sql.catalog.shared_catalog.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
    "spark.sql.catalog.shared_catalog.glue.id": SHARED_ACCOUNT_ID,
    "spark.sql.catalog.shared_catalog.glue.account-id": SHARED_ACCOUNT_ID,
    "spark.sql.catalog.shared_catalog.client.region": "us-east-1",
}


class TestIcebergCrossAccountCatalogProperties:
    def test_one_named_catalog_per_registered_catalog(self):
        properties = build_iceberg_cross_account_catalog_properties(
            {"shared_catalog": SHARED_ACCOUNT_ID, "other_catalog": OTHER_ACCOUNT_ID},
            "us-east-1",
        )
        assert {k: v for k, v in properties.items() if "shared_catalog" in k} == (
            EXPECTED_SHARED_CATALOG_PROPERTIES
        )
        assert properties["spark.sql.catalog.other_catalog.glue.id"] == OTHER_ACCOUNT_ID
        assert len(properties) == 2 * len(EXPECTED_SHARED_CATALOG_PROPERTIES)

    def test_does_not_set_lakeformation_flag(self):
        properties = build_iceberg_cross_account_catalog_properties(
            {"shared_catalog": SHARED_ACCOUNT_ID}, "us-east-1"
        )
        assert not any(key.endswith("glue.lakeformation-enabled") for key in properties)

    def test_region_omitted_when_unknown(self):
        properties = build_iceberg_cross_account_catalog_properties(
            {"shared_catalog": SHARED_ACCOUNT_ID}, None
        )
        assert "spark.sql.catalog.shared_catalog.client.region" not in properties

    def test_empty_when_no_catalog(self):
        assert build_iceberg_cross_account_catalog_properties({}, "us-east-1") == {}


class TestSetEngineConfigIcebergMode:
    catalogs = {"shared_catalog": SHARED_ACCOUNT_ID}

    def test_named_catalog_in_spark_properties(self):
        engine_config = AthenaSparkSessionConfig(
            {"spark_cross_account_catalog": "iceberg"},
            cross_account_catalogs=self.catalogs,
            region_name="us-east-1",
        ).set_engine_config()
        spark_properties = engine_config["SparkProperties"]
        for key, value in EXPECTED_SHARED_CATALOG_PROPERTIES.items():
            assert spark_properties[key] == value
        assert SEPARATOR_KEY not in spark_properties

    def test_named_catalog_in_classifications_on_spark_connect(self):
        engine_config = AthenaSparkSessionConfig(
            {
                "spark_cross_account_catalog": "iceberg",
                "spark_engine_version": "3.5",
                "table_type": "iceberg",
                "engine_config": {
                    "MaxConcurrentDpus": 4,
                    "Classifications": [
                        {
                            "Name": "spark-defaults",
                            "Properties": {"spark.dynamicAllocation.maxExecutors": "2"},
                        }
                    ],
                },
            },
            cross_account_catalogs=self.catalogs,
            region_name="us-east-1",
        ).set_engine_config()
        spark_defaults = next(
            c for c in engine_config["Classifications"] if c["Name"] == "spark-defaults"
        )
        properties = spark_defaults["Properties"]
        for key, value in EXPECTED_SHARED_CATALOG_PROPERTIES.items():
            assert properties[key] == value
        # user-provided and table_type properties are kept; spark_catalog is unchanged
        assert properties["spark.dynamicAllocation.maxExecutors"] == "2"
        assert (
            properties["spark.sql.catalog.spark_catalog"]
            == "org.apache.iceberg.spark.SparkSessionCatalog"
        )
        assert engine_config["MaxConcurrentDpus"] == 4

    def test_no_catalog_registered_adds_nothing(self):
        engine_config = AthenaSparkSessionConfig(
            {"spark_cross_account_catalog": "iceberg"}, region_name="us-east-1"
        ).set_engine_config()
        assert not any(
            key.startswith("spark.sql.catalog.") for key in engine_config["SparkProperties"]
        )

    def test_hive_mode_ignores_catalogs(self):
        engine_config = AthenaSparkSessionConfig(
            {"spark_cross_account_catalog": "hive"},
            cross_account_catalogs=self.catalogs,
            region_name="us-east-1",
        ).set_engine_config()
        spark_properties = engine_config["SparkProperties"]
        assert spark_properties[SEPARATOR_KEY] == "/"
        assert not any(key.startswith("spark.sql.catalog.") for key in spark_properties)


@pytest.mark.usefixtures("athena_credentials")
class TestPythonJobHelperCrossAccountCatalogs:
    @mock_aws
    def test_iceberg_mode_looks_up_registered_catalogs(self, mock_aws_service, athena_credentials):
        mock_aws_service.create_data_catalog()
        mock_aws_service.create_data_catalog(
            catalog_name="shared_catalog",
            catalog_type=AthenaCatalogType.GLUE,
            catalog_id=SHARED_ACCOUNT_ID,
        )
        mock_aws_service.create_data_catalog(
            catalog_name=FEDERATED_QUERY_CATALOG_NAME,
            catalog_type=AthenaCatalogType.LAMBDA,
        )
        helper = AthenaPythonJobHelper(
            {"config": {"spark_cross_account_catalog": "iceberg"}}, athena_credentials
        )
        assert helper.config.cross_account_catalogs == {"shared_catalog": SHARED_ACCOUNT_ID}
        properties = helper.engine_config["SparkProperties"]
        assert properties["spark.sql.catalog.shared_catalog.glue.id"] == SHARED_ACCOUNT_ID
        assert (
            properties["spark.sql.catalog.shared_catalog.client.region"]
            == athena_credentials.region_name
        )

    @mock_aws
    def test_other_modes_do_not_look_up_catalogs(self, mock_aws_service, athena_credentials):
        mock_aws_service.create_data_catalog(
            catalog_name="shared_catalog",
            catalog_type=AthenaCatalogType.GLUE,
            catalog_id=SHARED_ACCOUNT_ID,
        )
        for value in (None, "hive"):
            config = {} if value is None else {"spark_cross_account_catalog": value}
            helper = AthenaPythonJobHelper({"config": config}, athena_credentials)
            assert helper.config.cross_account_catalogs == {}


def _render_get_spark_df(mode, catalogs, spark_stub):
    env = jinja2.Environment(
        loader=jinja2.FileSystemLoader(_MACRO_DIR),
        extensions=["jinja2.ext.do"],
    )
    env.globals["config"] = SimpleNamespace(get=lambda *_, **__: mode)
    env.globals["adapter"] = SimpleNamespace(get_spark_cross_account_catalog_map=lambda: catalogs)
    rendered = env.get_template("python_submissions.sql").module.athena__py_get_spark_dbt_object()

    class _StubDbtObj:
        def __init__(self, load_df_function):
            self.load_df_function = load_df_function

    namespace = {
        "dbtObj": _StubDbtObj,
        "source": MagicMock(),
        "ref": MagicMock(),
        "spark": spark_stub,
    }
    exec(rendered, namespace)
    return namespace["get_spark_df"]


class TestGetSparkDfCrossAccountIdentifiers:
    catalogs = {"shared_catalog": SHARED_ACCOUNT_ID}
    identifier = '"shared_catalog"."analytics"."events"'

    @pytest.mark.parametrize(
        "mode, expected",
        [
            ("hive", f"`{SHARED_ACCOUNT_ID}/analytics`.events"),
            (True, f"`{SHARED_ACCOUNT_ID}/analytics`.events"),
            ("iceberg", "`shared_catalog`.analytics.events"),
        ],
    )
    def test_cross_account_catalog_is_translated_per_mode(self, mode, expected):
        spark = MagicMock()
        _render_get_spark_df(mode, self.catalogs, spark)(self.identifier)
        spark.table.assert_called_once_with(expected)

    @pytest.mark.parametrize("mode", [False, "none"])
    def test_disabled_mode_falls_back_to_two_part_name(self, mode):
        spark = MagicMock()
        _render_get_spark_df(mode, self.catalogs, spark)(self.identifier)
        spark.table.assert_called_once_with("analytics.events")

    def test_local_catalog_is_two_part_in_iceberg_mode(self):
        spark = MagicMock()
        _render_get_spark_df("iceberg", self.catalogs, spark)(
            '"awsdatacatalog"."analytics"."events"'
        )
        spark.table.assert_called_once_with("analytics.events")

    def test_unregistered_catalog_is_two_part_in_iceberg_mode(self):
        spark = MagicMock()
        _render_get_spark_df("iceberg", self.catalogs, spark)('"other"."analytics"."events"')
        spark.table.assert_called_once_with("analytics.events")
