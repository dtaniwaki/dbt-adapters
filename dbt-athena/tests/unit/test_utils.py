import pytest

from dbt.adapters.athena.utils import (
    clean_sql_comment,
    ellipsis_comment,
    get_chunks,
    is_valid_table_parameter_key,
    resolve_spark_catalog_overrides,
    stringify_table_parameter_value,
)


def test_clean_comment():
    assert (
        clean_sql_comment(
            """
       my long comment
         on several lines
        with weird spaces and indents.
    """
        )
        == "my long comment on several lines with weird spaces and indents."
    )


def test_stringify_table_parameter_value():
    class NonStringifiableObject:
        def __str__(self):
            raise ValueError("Non-stringifiable object")

    assert stringify_table_parameter_value(True) == "True"
    assert stringify_table_parameter_value(123) == "123"
    assert stringify_table_parameter_value("dbt-athena") == "dbt-athena"
    assert stringify_table_parameter_value(["a", "b", 3]) == '["a", "b", 3]'
    assert stringify_table_parameter_value({"a": 1, "b": "c"}) == '{"a": 1, "b": "c"}'
    assert len(stringify_table_parameter_value("a" * 512001)) == 512000
    assert stringify_table_parameter_value(NonStringifiableObject()) is None
    assert stringify_table_parameter_value([NonStringifiableObject()]) is None


def test_is_valid_table_parameter_key():
    assert is_valid_table_parameter_key("valid_key") is True
    assert is_valid_table_parameter_key("Valid Key 123*!") is True
    assert is_valid_table_parameter_key("invalid \n key") is False
    assert is_valid_table_parameter_key("long_key" * 100) is False


def test_get_chunks_empty():
    assert len(list(get_chunks([], 5))) == 0


def test_get_chunks_uneven():
    chunks = list(get_chunks([1, 2, 3], 2))
    assert chunks[0] == [1, 2]
    assert chunks[1] == [3]
    assert len(chunks) == 2


def test_get_chunks_more_elements_than_chunk():
    chunks = list(get_chunks([1, 2, 3], 4))
    assert chunks[0] == [1, 2, 3]
    assert len(chunks) == 1


@pytest.mark.parametrize(
    ("max_len", "expected"),
    (
        pytest.param(12, "abc def ghi", id="ok string"),
        pytest.param(6, "abc...", id="ellipsis"),
    ),
)
def test_ellipsis_comment(max_len, expected):
    assert expected == ellipsis_comment("abc def ghi", max_len=max_len)


GLUE_ID = "123456789012"


class TestResolveSparkCatalogOverrides:
    @staticmethod
    def _no_lookup():
        raise AssertionError("catalog IDs must not be looked up")

    def test_empty(self):
        assert resolve_spark_catalog_overrides({}, self._no_lookup) == {}
        assert resolve_spark_catalog_overrides(None, self._no_lookup) == {}

    def test_iceberg_with_glue_id_and_region(self):
        res = resolve_spark_catalog_overrides(
            {"shared": {"type": "iceberg", "glue_id": GLUE_ID, "region": "eu-west-1"}},
            self._no_lookup,
        )
        assert res == {
            "shared": {
                "spark.sql.catalog.shared": "org.apache.iceberg.spark.SparkCatalog",
                "spark.sql.catalog.shared.catalog-impl": "org.apache.iceberg.aws.glue.GlueCatalog",
                "spark.sql.catalog.shared.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
                "spark.sql.catalog.shared.glue.id": "123456789012",
                "spark.sql.catalog.shared.glue.account-id": "123456789012",
                "spark.sql.catalog.shared.client.region": "eu-west-1",
            }
        }

    def test_region_is_optional(self):
        res = resolve_spark_catalog_overrides(
            {"shared": {"type": "iceberg", "glue_id": GLUE_ID}}, self._no_lookup
        )
        assert "spark.sql.catalog.shared.client.region" not in res["shared"]

    def test_glue_id_defaults_to_registered_data_catalog(self):
        calls = []

        def lookup():
            calls.append(1)
            return {"shared": "123456789012", "other": "210987654321"}

        res = resolve_spark_catalog_overrides(
            {"shared": {"type": "iceberg"}, "other": {"type": "iceberg"}}, lookup
        )
        assert res["shared"]["spark.sql.catalog.shared.glue.id"] == "123456789012"
        assert res["other"]["spark.sql.catalog.other.glue.id"] == "210987654321"
        assert len(calls) == 1

    @pytest.mark.parametrize(
        "overrides",
        [
            ["shared"],
            [],
            "",
            0,
            {"shared": "iceberg"},
            {"shared": {}},
            {"shared": {"type": "hive", "glue_id": GLUE_ID}},
            {"shared": {"type": ["iceberg"], "glue_id": GLUE_ID}},
            {"shared": {"type": "iceberg"}},
            {"shared": {"type": "iceberg", "glue-id": GLUE_ID}},
            {"shared": {"type": "iceberg", "glue_id": 123456789012}},
            {"shared": {"type": "iceberg", "glue_id": 0}},
            {"shared": {"type": "iceberg", "glue_id": ""}},
            {"shared": {"type": "iceberg", "glue_id": "12345"}},
            {"awsdatacatalog": {"type": "iceberg", "glue_id": GLUE_ID}},
            {"spark_catalog": {"type": "iceberg", "glue_id": GLUE_ID}},
            {"a.b": {"type": "iceberg", "glue_id": GLUE_ID}},
            {"a`b": {"type": "iceberg", "glue_id": GLUE_ID}},
        ],
    )
    def test_invalid(self, overrides):
        with pytest.raises(ValueError):
            resolve_spark_catalog_overrides(overrides, lambda: {})

    @pytest.mark.parametrize(
        "spec",
        [
            {"type": "iceberg", "glue-id": GLUE_ID},
            {"type": "iceberg", "regoin": "eu-west-1"},
            {"type": "iceberg", "glue_id": 0},
            {"type": "iceberg", "glue_id": ""},
        ],
    )
    def test_invalid_even_when_registered_in_athena(self, spec):
        with pytest.raises(ValueError):
            resolve_spark_catalog_overrides({"shared": spec}, lambda: {"shared": GLUE_ID})
