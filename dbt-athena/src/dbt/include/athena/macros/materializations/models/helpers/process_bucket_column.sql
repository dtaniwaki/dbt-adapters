{% macro process_bucket_column(col, partition_key, table, ns, col_index) %}
    {# Extract bucket information from the partition key #}
    {# Iceberg format: bucket(N, col) — first arg is bucket count #}
    {%- set iceberg_bucket_match = modules.re.search('bucket\(\s*(\d+)\s*,\s*(.+?)\s*\)', partition_key) -%}
    {# Hive format: bucket(col, N) — second arg is bucket count #}
    {%- set hive_bucket_match = modules.re.search('bucket\((.+?),\s*(\d+)\)', partition_key) -%}

    {%- if iceberg_bucket_match -%}
        {%- set column_type = adapter.convert_type(table, col_index) -%}
        {%- set ns.is_bucketed = true -%}
        {%- set ns.bucket_column = iceberg_bucket_match[2] -%}
        {%- set bucket_num = adapter.murmur3_hash(col, iceberg_bucket_match[1] | int) -%}
    {%- elif hive_bucket_match -%}
        {%- set column_type = adapter.convert_type(table, col_index) -%}
        {%- set ns.is_bucketed = true -%}
        {%- set ns.bucket_column = hive_bucket_match[1] -%}
        {%- set bucket_num = adapter.murmur3_hash(col, hive_bucket_match[2] | int) -%}
    {%- endif -%}

    {%- if iceberg_bucket_match or hive_bucket_match -%}
        {%- set formatted_value, comp_func = adapter.format_value_for_partition(col, column_type) -%}

        {%- if bucket_num not in ns.bucket_numbers %}
            {%- do ns.bucket_numbers.append(bucket_num) %}
            {%- do ns.bucket_conditions.update({bucket_num: [formatted_value]}) -%}
        {%- elif formatted_value not in ns.bucket_conditions[bucket_num] %}
            {%- do ns.bucket_conditions[bucket_num].append(formatted_value) -%}
        {%- endif -%}
    {%- endif -%}
{% endmacro %}
