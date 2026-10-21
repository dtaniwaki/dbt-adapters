"""
Unit tests for the athena__py_save_table_as macro, covering both the
``use_iceberg_write_to`` writeTo branch and the ``model() returns None``
skip-adapter-materialize branch.

Renders the macro end-to-end with jinja2.FileSystemLoader, following the
pattern in test_get_partition_batches.py.
"""

import os
from types import SimpleNamespace
from unittest import mock

import jinja2
import pytest

_MACRO_DIR = os.path.normpath(
    os.path.join(
        os.path.dirname(__file__),
        os.pardir,
        os.pardir,
        "src",
        "dbt",
        "include",
        "athena",
        "macros",
        "adapters",
    )
)


def _render(
    optional_args,
    *,
    compiled_code="def model(dbt, spark):\n    return spark",
    target_identifier="my_table",
    this_identifier="my_table",
    schema="my_schema",
):
    """Render athena__py_save_table_as with stubbed dbt context.

    ``target_identifier`` defaults to the final relation (``materialized='table'``
    behaviour); pass ``"my_table__dbt_tmp"`` to mirror the incremental Python
    flow, where create_table_as is invoked with the intermediate temp relation
    while ``this`` still points to the final one.
    """
    target_relation = SimpleNamespace(schema=schema, identifier=target_identifier)
    context = {
        "target": SimpleNamespace(
            assume_role_arn=None,
            assume_role_external_id=None,
            assume_role_session_name=None,
            region_name="us-east-1",
        ),
        "this": SimpleNamespace(schema=schema, identifier=this_identifier),
    }
    env = jinja2.Environment(
        loader=jinja2.FileSystemLoader(_MACRO_DIR),
        extensions=["jinja2.ext.do"],
    )
    template = env.get_template("python_submissions.sql", globals=context)
    return template.module.athena__py_save_table_as(compiled_code, target_relation, optional_args)


class TestUseIcebergWriteToRendering:
    """Verify the use_iceberg_write_to branch generates correct Python."""

    def test_renders_writeto_with_native_partition_transforms(self):
        rendered = _render(
            {
                "location": "s3://bucket/path",
                "use_iceberg_write_to": True,
                "partitioned_by": ["day(created_at)", "bucket(user_id, 4)"],
            }
        )
        assert '.writeTo("my_schema.my_table")' in rendered
        assert '.using("iceberg")' in rendered
        assert ".createOrReplace()" in rendered
        # partitioned_by values are passed through tojson, so they appear
        # as Python string literals consumed by _parse_iceberg_partition.
        assert '_parse_iceberg_partition("day(created_at)")' in rendered
        assert '_parse_iceberg_partition("bucket(user_id, 4)")' in rendered
        # spark_ctas branch must NOT be reached.
        assert "spark_session.sql" not in rendered

    def test_renders_writeto_without_partitions(self):
        rendered = _render({"location": "s3://bucket/path", "use_iceberg_write_to": True})
        assert ".writeTo(" in rendered
        assert ".createOrReplace()" in rendered
        # No partitionedBy call when partitioned_by is omitted.
        assert ".partitionedBy(" not in rendered

    def test_extra_table_properties_use_tojson_escaping(self):
        rendered = _render(
            {
                "location": "s3://bucket/path",
                "use_iceberg_write_to": True,
                "extra_table_properties": {
                    "format-version": "2",
                    'key"with"quotes': "value\\backslash",
                },
            }
        )
        # location goes through tojson together with the trailing slash.
        assert '_writer.tableProperty("location", "s3://bucket/path/")' in rendered
        # Plain property: emitted as JSON-escaped Python string literals.
        assert '_writer.tableProperty("format-version", "2")' in rendered
        # Special characters are escaped (no raw injection).
        assert 'key\\"with\\"quotes' in rendered
        assert "value\\\\backslash" in rendered

    def test_extra_table_properties_coerces_non_string_values(self):
        # Booleans / ints sometimes appear in user configs; |string|tojson
        # must coerce them to a Python string literal rather than blow up.
        rendered = _render(
            {
                "location": "s3://bucket/path",
                "use_iceberg_write_to": True,
                "extra_table_properties": {"format-version": 2},
            }
        )
        assert '_writer.tableProperty("format-version", "2")' in rendered

    def test_falls_back_to_spark_ctas_when_disabled(self):
        rendered = _render(
            {
                "location": "s3://bucket/path",
                "use_iceberg_write_to": False,
                "spark_ctas": "create table foo using iceberg as",
            }
        )
        assert "writeTo" not in rendered
        assert "spark_session.sql" in rendered
        assert "create table foo using iceberg as" in rendered


class TestParseIcebergPartition:
    """Execute the inline _parse_iceberg_partition function from the
    rendered Python code against a stub pyspark.sql.functions module to
    cover its dispatch and arity validation."""

    @pytest.fixture
    def parse_fn(self):
        rendered = _render({"location": "s3://b/p", "use_iceberg_write_to": True})

        # Pull just the _parse_iceberg_partition definition out of the
        # rendered template and exec it with a stubbed F (pyspark.sql.functions)
        # injected directly, so we don't need a real Spark context.
        marker = "def _parse_iceberg_partition(expr_str):"
        start = rendered.index(marker)
        end = rendered.index("_writer = df.writeTo", start)
        # Strip the leading 4-space indent from each line (the function
        # lives inside materialize()).
        body = "\n".join(
            line[4:] if line.startswith("    ") else line
            for line in rendered[start:end].splitlines()
        )

        F_stub = SimpleNamespace(
            col=lambda c: ("col", c),
            days=lambda c: ("days", c),
            months=lambda c: ("months", c),
            years=lambda c: ("years", c),
            hours=lambda c: ("hours", c),
            bucket=lambda n, c: ("bucket", n, c),
            truncate=lambda n, c: ("truncate", n, c),
        )
        import re as re_module

        ns = {"re": re_module, "F": F_stub}
        exec(body, ns)
        return ns["_parse_iceberg_partition"]

    @pytest.mark.parametrize(
        "expr,expected",
        [
            ("day(created_at)", ("days", ("col", "created_at"))),
            ("days(created_at)", ("days", ("col", "created_at"))),
            ("month(ts)", ("months", ("col", "ts"))),
            ("year(ts)", ("years", ("col", "ts"))),
            ("hour(ts)", ("hours", ("col", "ts"))),
            ("bucket(user_id, 256)", ("bucket", 256, ("col", "user_id"))),
            ("truncate(name, 10)", ("truncate", 10, ("col", "name"))),
            ("plain_col", ("col", "plain_col")),
        ],
    )
    def test_dispatch(self, parse_fn, expr, expected):
        assert parse_fn(expr) == expected

    def test_bucket_with_missing_arg_raises_clear_error(self, parse_fn):
        # bucket and truncate share the arity-validation branch, so this
        # exercise covers both.
        with pytest.raises(ValueError, match="requires 2 arguments"):
            parse_fn("bucket(user_id)")

    def test_unknown_transform_raises_value_error(self, parse_fn):
        with pytest.raises(ValueError, match="Unknown Iceberg partition transform"):
            parse_fn("md5(name)")


class TestSkipMaterializeWhenModelReturnsNone:
    """Rendered template must branch on ``df is None`` and only call
    materialize() when the user returned a DataFrame."""

    def test_renders_none_branch_with_glue_guard(self):
        rendered = _render({"location": "s3://b/p"})
        assert "if df is None:" in rendered
        assert "glue.get_table(" in rendered
        assert "else:" in rendered
        assert "materialize(spark, df, dbt.this)" in rendered

    def test_guard_uses_final_relation_not_tmp(self):
        """The guard must look up the final (``this``) relation, never the
        ``__dbt_tmp`` intermediate. Otherwise incremental Python models
        that write the final relation directly always raise on first run."""
        rendered = _render(
            {"location": "s3://b/p"},
            target_identifier="my_table__dbt_tmp",
            this_identifier="my_table",
        )
        assert 'Name="my_table"' in rendered
        assert "my_table__dbt_tmp" not in rendered.split("if df is None:", 1)[1]

    def _exec_trailing_block(self, *, table_exists, model_returns):
        """Execute the rendered ``if df is None:`` ... ``else: materialize(...)``
        block with stubbed dbt / model / spark / boto3."""
        rendered = _render({"location": "s3://b/p"})
        marker = "dbt = SparkdbtObj()"
        body = rendered[rendered.index(marker) :]

        materialize_calls = []

        def fake_materialize(spark_session, df, target):
            materialize_calls.append((df, target))

        glue_client = mock.Mock()
        if table_exists:
            glue_client.get_table.return_value = {}
        else:
            from botocore.exceptions import ClientError

            glue_client.get_table.side_effect = ClientError(
                error_response={"Error": {"Code": "EntityNotFoundException"}},
                operation_name="GetTable",
            )

        ns = {
            "SparkdbtObj": lambda: SimpleNamespace(
                this=SimpleNamespace(schema="my_schema", identifier="my_table"),
            ),
            "model": lambda dbt, spark: model_returns,
            "spark": mock.Mock(),
            "materialize": fake_materialize,
        }
        with mock.patch("boto3.client", return_value=glue_client):
            exec(compile(body, "<rendered>", "exec"), ns)
        return materialize_calls

    def test_none_with_existing_target_skips_materialize(self):
        calls = self._exec_trailing_block(table_exists=True, model_returns=None)
        assert calls == []

    def test_none_with_missing_target_raises(self):
        with pytest.raises(Exception, match="model\\(\\) returned None"):
            self._exec_trailing_block(table_exists=False, model_returns=None)

    def test_dataframe_return_still_materializes(self):
        sentinel = object()
        calls = self._exec_trailing_block(table_exists=True, model_returns=sentinel)
        assert len(calls) == 1
        assert calls[0][0] is sentinel
