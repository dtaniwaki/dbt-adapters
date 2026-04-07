{%- macro athena__py_save_table_as(compiled_code, target_relation, optional_args={}) -%}
    {% set location = optional_args.get("location") %}
    {% set format = optional_args.get("format", "parquet") %}
    {% set mode = optional_args.get("mode", "overwrite") %}
    {% set write_compression = optional_args.get("write_compression", "snappy") %}
    {% set partitioned_by = optional_args.get("partitioned_by") %}
    {% set bucketed_by = optional_args.get("bucketed_by") %}
    {% set sorted_by = optional_args.get("sorted_by") %}
    {% set merge_schema = optional_args.get("merge_schema", true) %}
    {% set bucket_count = optional_args.get("bucket_count") %}
    {% set field_delimiter = optional_args.get("field_delimiter") %}
    {% set spark_ctas = optional_args.get("spark_ctas", "") %}

import pyspark


{{ compiled_code }}
def materialize(spark_session, df, target_relation):
    import pandas
    if isinstance(df, pyspark.sql.dataframe.DataFrame):
        pass
    elif isinstance(df, pandas.core.frame.DataFrame):
        df = spark_session.createDataFrame(df)
    else:
        msg = f"{type(df)} is not a supported type for dbt Python materialization"
        raise Exception(msg)

{% if spark_ctas|length > 0 %}
    df.createOrReplaceTempView("{{ target_relation.schema}}_{{ target_relation.identifier }}_tmpvw")
    spark_session.sql("""
    {{ spark_ctas }}
    select * from {{ target_relation.schema}}_{{ target_relation.identifier }}_tmpvw
    """)
{% else %}
    writer = df.write \
    .format("{{ format }}") \
    .mode("{{ mode }}") \
    .option("path", "{{ location }}") \
    .option("compression", "{{ write_compression }}") \
    .option("mergeSchema", "{{ merge_schema }}") \
    .option("delimiter", "{{ field_delimiter }}")
    if {{ partitioned_by }} is not None:
        writer = writer.partitionBy({{ partitioned_by }})
    if {{ bucketed_by }} is not None:
        writer = writer.bucketBy({{ bucket_count }},{{ bucketed_by }})
    if {{ sorted_by }} is not None:
        writer = writer.sortBy({{ sorted_by }})

    writer.saveAsTable(
        name="{{ target_relation.schema}}.{{ target_relation.identifier }}",
    )
{% endif %}

    return "Success: {{ target_relation.schema}}.{{ target_relation.identifier }}"

{{ athena__py_get_spark_dbt_object() }}

dbt = SparkdbtObj()
df = model(dbt, spark)
materialize(spark, df, dbt.this)
{%- endmacro -%}

{%- macro athena__py_execute_query(query) -%}
{{ athena__py_get_spark_dbt_object() }}

def execute_query(spark_session):
    spark_session.sql("""{{ query }}""")
    return "OK"

dbt = SparkdbtObj()
execute_query(spark)
{%- endmacro -%}

{%- macro athena__py_get_spark_dbt_object() -%}
{#-
  Cross-account GLUE catalogs registered in Athena are resolved at compile
  time so that Spark Python models can transparently reference them via
  dbt.source() / dbt.ref() without hard-coding account IDs.

  The mapping is injected as a Python dict. When combined with the
  `spark_cross_account_catalog` profile option (which sets
  `spark.hadoop.aws.glue.catalog.separator` to `/`), Spark can resolve
  `<account_id>/<schema>.<table>` identifiers against a remote Glue catalog.
-#}
{%- set cross_account_catalogs = adapter.get_spark_cross_account_catalog_map() -%}
_CROSS_ACCOUNT_CATALOGS = {{ cross_account_catalogs | tojson }}

def get_spark_df(identifier):
    """
    Override the arguments to ref and source dynamically.

    dbt passes a fully-qualified identifier like
    '"awsdatacatalog"."analytics_dev"."model"'.

    spark.table('awsdatacatalog.analytics_dev.model') raises
    pyspark.sql.utils.AnalysisException:
      spark_catalog requires a single-part namespace,
      but got [awsdatacatalog, analytics_dev]

    For local catalog (`awsdatacatalog`) references we therefore strip the
    catalog component and call spark.table('schema.table').

    For cross-account GLUE catalogs registered in Athena, we translate the
    catalog name to the corresponding AWS account ID (resolved at compile
    time by the adapter) and emit 'account_id/schema.table' — which Spark's
    Glue Catalog Client can resolve when the
    `spark.hadoop.aws.glue.catalog.separator` property is set to '/'
    (enabled via the `spark_cross_account_catalog` profile option).
    """
    parts = identifier.replace('"', '').split(".")
    if len(parts) < 3:
        return spark.table(identifier.replace('"', ''))
    catalog, schema, table = parts[0], parts[1], parts[2]
    if catalog.lower() == "awsdatacatalog":
        return spark.table(f"{schema}.{table}")
    account_id = _CROSS_ACCOUNT_CATALOGS.get(catalog)
    if account_id is None:
        return spark.table(f"{schema}.{table}")
    return spark.table(f"{account_id}/{schema}.{table}")

class SparkdbtObj(dbtObj):
    def __init__(self):
        super().__init__(load_df_function=get_spark_df)
        self.source = lambda *args: source(*args, dbt_load_df_function=get_spark_df)
        self.ref = lambda *args: ref(*args, dbt_load_df_function=get_spark_df)

{%- endmacro -%}
