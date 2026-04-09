# Custom Build

Base: v1.17.3
Date: 2026-04-09

## Included PRs

- https://github.com/dbt-labs/dbt-adapters/pull/1211 — Fix a debug log about the Athena workgroup
- https://github.com/dbt-labs/dbt-adapters/pull/1657 — feat(athena): Add STS AssumeRole support for cross-account access
- https://github.com/dbt-labs/dbt-adapters/pull/1704 — perf(athena): cache `_get_data_catalog()` result to avoid repeated STS calls
- https://github.com/dbt-labs/dbt-adapters/pull/1705 — fix(athena): fix "connection never acquired" with `--no-populate-cache` and `threads > 1`
- https://github.com/dbt-labs/dbt-adapters/pull/1740 — fix(athena): exclude ICEBERG_FILESYSTEM_ERROR from outer retry
- https://github.com/dbt-labs/dbt-adapters/pull/1743 — fix(athena): handle unpartitioned models in create_table_as_with_partitions
- https://github.com/dbt-labs/dbt-adapters/pull/1749 — fix(athena): create empty target table when no partition batches found
- https://github.com/dbt-labs/dbt-adapters/pull/1784 — fix(athena): Coordinate chunk sizes in get_partition_batches to respect partition limit
- https://github.com/dbt-labs/dbt-adapters/pull/1814 — fix(athena): skip retry of deterministic errors with configurable timeout handling
- https://github.com/dtaniwaki/dbt-adapters/pull/3 — feat(athena): add disable_batch_fallback config option
- https://github.com/dtaniwaki/dbt-adapters/pull/4 — feat(athena): add model-level timeout support
- https://github.com/dbt-labs/dbt-adapters/pull/1830 — feat(athena): add build_with_subquery config for incremental merge and append
- https://github.com/dbt-labs/dbt-adapters/pull/1832 — feat(athena): add merge_exclude_source_columns config
- https://github.com/dtaniwaki/dbt-adapters/pull/7 — feat(athena): resolve cross-account Glue catalogs in Spark Python models

## Conflict Resolutions

- `dbt-athena/tests/unit/test_adapter.py`: PR #1705 のテスト追加が #1704 のテスト追加と衝突（rerere自動解決）
- `dbt-athena/src/dbt/adapters/athena/connections.py`: PR #1814 のリトライ制御が #1740 のエラー除外と衝突（rerere自動解決）
- `dbt-athena/src/dbt/include/athena/macros/materializations/models/table/create_table_as.sql`: fork #4 のmodel_timeout対応が fork #3 のdisable_batch_fallback対応と衝突（rerere自動解決）
- `dbt-athena/src/dbt/adapters/athena/impl.py`: upstream #1830 の build_with_subquery フィールドと fork #4 の model_timeout_seconds / fork #3 の disable_batch_fallback が同位置に追加（rerere自動解決）
- `dbt-athena/src/dbt/include/athena/macros/materializations/models/incremental/helpers.sql`: upstream #1830 の source_sql パラメータと fork #3 の disable_batch_fallback パラメータが同位置に追加（rerere自動解決）
- `dbt-athena/src/dbt/include/athena/macros/materializations/models/incremental/incremental.sql`: upstream #1830 の build_with_subquery ロジック分岐と fork #3 の disable_batch_fallback パラメータが衝突（rerere自動解決）
- `dbt-athena/src/dbt/include/athena/macros/materializations/models/incremental/merge.sql`: upstream #1830 の source_sql パラメータと fork #3 の disable_batch_fallback パラメータが同位置に追加（rerere自動解決）
- `dbt-athena/tests/unit/test_adapter.py`: fork #7 の Spark クロスアカウントカタログテストと PR #1704/#1705 のテスト追加が同位置に追加（rerere自動解決）

## Post-merge Fixes

- `dbt-athena/src/dbt/include/athena/macros/materializations/models/table/create_table_as.sql`: PR #1749 の空バッチチェックが PR #1743 の `partitioned_by` 分岐の外に配置されてしまい、未パーティションモデルで余分なCTASが実行される問題を修正（`partitioned_by is not none` ガード内に移動）
- `dbt-athena/tests/unit/test_create_table_as_with_partitions.py`: 上記修正に合わせてテストに `partitioned_by` パラメータを追加し、未パーティション経路のテストも追加
- `dbt-athena/tests/unit/test_build_with_subquery.py`: PR #1830 の MockAdapter に fork #3 の `disable_batch_fallback` 引数を追加
- `dbt-athena/tests/unit/test_merge_exclude_source_columns.py`: PR #1832 の MockAdapter に fork #3 の `disable_batch_fallback` 引数を追加
