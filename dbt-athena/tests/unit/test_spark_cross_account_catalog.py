import pytest

from dbt.adapters.athena.config import (
    AthenaSparkSessionConfig,
    get_spark_cross_account_catalog_mode,
)

SEPARATOR_KEY = "spark.hadoop.aws.glue.catalog.separator"


class TestSparkCrossAccountCatalogMode:
    @pytest.mark.parametrize(
        "value, expected",
        [
            (None, "none"),
            (False, "none"),
            (True, "hive"),
            ("none", "none"),
            ("hive", "hive"),
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
