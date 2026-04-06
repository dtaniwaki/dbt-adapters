import pytest

from dbt.tests.util import run_dbt


models__build_with_subquery_sql = """
{{ config(
    materialized = 'incremental',
    unique_key = 'id',
    incremental_strategy = 'merge',
    table_type = 'iceberg',
    build_with_subquery = True,
) }}

{% if not is_incremental() %}

select 1 as id, 'hello' as msg, 'blue' as color
union all
select 2 as id, 'goodbye' as msg, 'red' as color

{% else %}

select 1 as id, 'hey' as msg, 'blue' as color
union all
select 2 as id, 'yo' as msg, 'green' as color
union all
select 3 as id, 'anyway' as msg, 'purple' as color

{% endif %}
"""

models__build_with_subquery_force_batch_sql = """
{{ config(
    materialized = 'incremental',
    unique_key = 'id',
    incremental_strategy = 'merge',
    table_type = 'iceberg',
    build_with_subquery = True,
    force_batch = True,
) }}

select 1 as id, 'hello' as msg
"""

models__append_with_subquery_sql = """
{{ config(
    materialized = 'incremental',
    incremental_strategy = 'append',
    table_type = 'iceberg',
    build_with_subquery = True,
) }}

select 1 as id, 'hello' as msg, 'blue' as color
"""

models__append_with_subquery_force_batch_sql = """
{{ config(
    materialized = 'incremental',
    incremental_strategy = 'append',
    table_type = 'iceberg',
    build_with_subquery = True,
    force_batch = True,
) }}

select 1 as id, 'hello' as msg
"""

models__hive_append_with_subquery_sql = """
{{ config(
    materialized = 'incremental',
    incremental_strategy = 'append',
    table_type = 'hive',
    build_with_subquery = True,
) }}

select 1 as id, 'hello' as msg, 'blue' as color
"""

seeds__expected_csv = """id,msg,color
1,hello,blue
2,goodbye,red
3,anyway,purple
"""


class TestIcebergMergeWithSubquery:
    @pytest.fixture(scope="class")
    def models(self):
        return {"build_with_subquery.sql": models__build_with_subquery_sql}

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"expected_build_with_subquery.csv": seeds__expected_csv}

    def test__build_with_subquery_full_refresh(self, project):
        """Full refresh should create the table via CTAS regardless of build_with_subquery"""
        results = run_dbt(["run", "--select", "build_with_subquery", "--full-refresh"])
        assert len(results) == 1

    def test__build_with_subquery_incremental(self, project):
        """Incremental run should use subquery MERGE with empty tmp table for schema"""
        run_dbt(["run", "--select", "build_with_subquery", "--full-refresh"])
        results = run_dbt(["run", "--select", "build_with_subquery"])
        assert len(results) == 1


class TestIcebergMergeWithSubqueryIncompatibleForceBatch:
    @pytest.fixture(scope="class")
    def models(self):
        return {"merge_subquery_force_batch.sql": models__build_with_subquery_force_batch_sql}

    def test__build_with_subquery_force_batch_fails(self, project):
        """build_with_subquery + force_batch should raise a compiler error"""
        run_dbt(["run", "--select", "merge_subquery_force_batch", "--full-refresh"])
        results = run_dbt(
            ["run", "--select", "merge_subquery_force_batch"],
            expect_pass=False,
        )
        assert len(results) == 1
        assert "incompatible with force_batch" in results[0].message


class TestIcebergAppendWithSubquery:
    @pytest.fixture(scope="class")
    def models(self):
        return {"append_with_subquery.sql": models__append_with_subquery_sql}

    def test__append_with_subquery_full_refresh(self, project):
        """Full refresh should create the table via CTAS regardless of build_with_subquery"""
        results = run_dbt(["run", "--select", "append_with_subquery", "--full-refresh"])
        assert len(results) == 1

    def test__append_with_subquery_incremental(self, project):
        """Incremental append should use subquery INSERT with empty tmp table for schema"""
        run_dbt(["run", "--select", "append_with_subquery", "--full-refresh"])
        results = run_dbt(["run", "--select", "append_with_subquery"])
        assert len(results) == 1


class TestIcebergAppendWithSubqueryIncompatibleForceBatch:
    @pytest.fixture(scope="class")
    def models(self):
        return {"append_subquery_force_batch.sql": models__append_with_subquery_force_batch_sql}

    def test__append_with_subquery_force_batch_fails(self, project):
        """build_with_subquery + force_batch should raise a compiler error for append"""
        run_dbt(["run", "--select", "append_subquery_force_batch", "--full-refresh"])
        results = run_dbt(
            ["run", "--select", "append_subquery_force_batch"],
            expect_pass=False,
        )
        assert len(results) == 1
        assert "incompatible with force_batch" in results[0].message


class TestHiveAppendWithSubquery:
    @pytest.fixture(scope="class")
    def models(self):
        return {"hive_append_with_subquery.sql": models__hive_append_with_subquery_sql}

    def test__hive_append_with_subquery_full_refresh(self, project):
        """Full refresh should create the table via CTAS regardless of build_with_subquery"""
        results = run_dbt(["run", "--select", "hive_append_with_subquery", "--full-refresh"])
        assert len(results) == 1

    def test__hive_append_with_subquery_incremental(self, project):
        """Incremental hive append should use subquery INSERT with empty tmp table for schema"""
        run_dbt(["run", "--select", "hive_append_with_subquery", "--full-refresh"])
        results = run_dbt(["run", "--select", "hive_append_with_subquery"])
        assert len(results) == 1
