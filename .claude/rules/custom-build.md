# Custom Build

Base: `e6747c5b3` (unchanged since rebuild 23)
Date: 2026-10-29 (rebuild 28)

## Changes since rebuild 27

- **dbt-labs #1830 (`feat/athena-build-with-subquery`) を更新** (`3dc2505e6`)。`build_strategy='subquery'` で `TOO_MANY_OPEN_PARTITIONS` から空テーブル作成にフォールバックするとき、`contract: enforced` が有効だと契約検査が `WITH NO DATA` 付きの SQL に対して走り、`mismatched input 'WITH'` で失敗していた。`create_table_as` に `contract_sql` 引数を足し、空テーブル作成の 4 か所（table 1 / incremental 3）は実クエリに対して契約を検査する。
- ベースは rebuild 27 と同一。PR 構成も同一 (14本)。

## Included PRs

- https://github.com/dbt-labs/dbt-adapters/pull/1211 — Fix a debug log about the Athena workgroup (branch: `origin/patch-1`)
- https://github.com/dbt-labs/dbt-adapters/pull/1704 — perf(athena): cache `_get_data_catalog()` result to avoid repeated STS calls (branch: `origin/fix/lru-cache-data-catalog`)
- https://github.com/dbt-labs/dbt-adapters/pull/1705 — fix(athena): fix "connection never acquired" with `--no-populate-cache` and `threads > 1` (branch: `origin/fix/no-populate-cache-thread-connection`)
- https://github.com/dbt-labs/dbt-adapters/pull/1743 — fix(athena): handle unpartitioned models in create_table_as_with_partitions (branch: `origin/fix/athena-unpartitioned-too-many-open-partitions`)
- https://github.com/dbt-labs/dbt-adapters/pull/1749 — fix(athena): create empty target table when no partition batches found (branch: `origin/fix/athena-empty-batch-target-table`)
- https://github.com/dtaniwaki/dbt-adapters/pull/3 — feat(athena): add disable_batch_fallback config option (branch: `origin/worktree-soft-chasing-ullman`)
- https://github.com/dtaniwaki/dbt-adapters/pull/4 — feat(athena): add model-level timeout support (branch: `origin/feat/athena-model-timeout` `ed580a81d`)
- https://github.com/dbt-labs/dbt-adapters/pull/1830 — feat(athena): add build_strategy config for incremental and table materializations (branch: `origin/feat/athena-build-with-subquery`)
- https://github.com/dbt-labs/dbt-adapters/pull/1832 — feat(athena): add merge_exclude_source_columns config (branch: `origin/feat/athena-merge-select-exclude-columns`)
- https://github.com/dtaniwaki/dbt-adapters/pull/7 — feat(athena): resolve cross-account Glue catalogs in Spark Python models (branch: `origin/feat/athena-spark-cross-account-catalog` `b15bfd586`)
- https://github.com/dbt-labs/dbt-adapters/pull/1874 — feat(athena): add Apache Spark 3.5 support via Spark Connect (branch: `origin/feat/spark-3.5-support` `180097716`。session pool の StartSession throttle backoff・shared-session drain fix・python model の assume_role_arn identity fix (boto3.DEFAULT_SESSION 方式)・Spark Connect PERMISSION_DENIED の再試行・fork #15 の Athena セッションごとのクライアント共有・fork #16 の共有クライアントでの artifact 再送抑止・fork #17 のセッション終了時の再試行・fork #18〜#23 を内包)
- https://github.com/dbt-labs/dbt-adapters/pull/1881 — feat(athena): add use_iceberg_write_to config for Iceberg Python models (branch: `origin/fix/iceberg-python-writeto`)
- https://github.com/dbt-labs/dbt-adapters/pull/1990 — feat(athena): let Python models self-materialize by returning None (branch: `origin/feat/athena-python-model-skip-materialize`)
- https://github.com/dbt-labs/dbt-adapters/pull/1998 — fix(athena): use RefreshableCredentials for AssumeRole sessions (branch: `origin/fix/athena-refreshable-assume-role-credentials`)

## Already in base (no explicit merge needed)

- https://github.com/dbt-labs/dbt-adapters/pull/1221 — Add a debug log about an Athena execution error
- https://github.com/dbt-labs/dbt-adapters/pull/1636 — Fix a bucket partitioning error against many partitions
- https://github.com/dbt-labs/dbt-adapters/pull/1637 — feat(athena): Migrate dbt-athena from PyAthena to direct boto3 calls
- https://github.com/dbt-labs/dbt-adapters/pull/1650 — Override check_schema_exists in Athena adapter to use Glue API
- https://github.com/dbt-labs/dbt-adapters/pull/1657 — feat(athena): Add STS AssumeRole support for cross-account access
- https://github.com/dbt-labs/dbt-adapters/pull/1784 — fix(athena): Coordinate chunk sizes in get_partition_batches
- https://github.com/dbt-labs/dbt-adapters/pull/1984 — fix(athena): cancel in-flight Athena queries when a dbt invocation is cancelled
- https://github.com/dbt-labs/dbt-adapters/pull/2000 — pin core to <2.0
- https://github.com/dbt-labs/dbt-adapters/pull/2047 — feat(athena): add S3 Tables catalog support

## Closed / Excluded PRs

- https://github.com/dbt-labs/dbt-adapters/pull/1740 — fix(athena): exclude ICEBERG_FILESYSTEM_ERROR from outer retry (**closed**: #1637 に機能包含)
- https://github.com/dbt-labs/dbt-adapters/pull/1814 — fix(athena): skip retry of deterministic errors with configurable timeout handling (**closed**: #1637 に機能包含)

## Conflict Resolutions

- `dbt-athena/src/dbt/adapters/athena/utils.py` / `dbt-athena/tests/unit/test_adapter.py`（`feat/athena-spark-cross-account-catalog`）: 手動解決。`typing` の import は両側の名前の和集合、テストは両側の追加分を残す。
- rerere 自動解決: `fix/lru-cache-data-catalog` / `fix/no-populate-cache-thread-connection` / `worktree-soft-chasing-ullman` /
  `feat/athena-model-timeout` / `feat/athena-build-with-subquery` /
  `feat/spark-3.5-support`（`AthenaModelTimeoutError` と `SparkSessionTerminatedError` は両方残る）/ `fix/iceberg-python-writeto` / `feat/athena-python-model-skip-materialize`。
  `fix/iceberg-python-writeto` の解決後も `python_submissions.sql` の 3.5 判定が `adapter.is_spark_connect_engine` のままであること、全マージ後に `spark_connect/` が `origin/feat/spark-3.5-support` と一致することを確認済み。
- それ以外はコンフリクトなし。

## Post-merge fixes

複数 PR の組み合わせで生じるテスト破綻を補正する単一コミット
`fix(athena): post-merge fixes for custom-build rebuild` (前回 `d55e6d7b3` を cherry-pick し、下記を追加)。
補正内容は rebuild 26 に以下を追加: `test_py_save_table_as` / `test_spark_connect_engine_version` / `test_spark_dbt_obj` の `adapter` stub に `get_spark_catalog_overrides`（fork #7 で macro が呼ぶため）。rebuild 26 は rebuild 25 に以下を追加していた: `python_submissions.sql` footer の 3.5 判定を `adapter.is_spark_connect_engine` に揃える、`test_py_save_table_as` に `adapter` stub、`test_spark_connect_engine_version` に `config` / `target` / `this` stub（#22 で macro が adapter を呼ぶため）。既存 (create_table_as_with_partitions の空バッチ guard、`test_adapter` の `AthenaError` 追従、
MockAdapter の `disable_batch_fallback` kwarg、`test_python_submissions` の Spark フィールド、`test_spark_dbt_obj` /
`test_py_save_table_as` の `config` stub、`python_submissions.sql` footer の 3.5 経路での self-assume 回避 等)。

全 unit tests: **846 passed** (14 xfailed / 1 xpassed)。ほかに 7 件が `athena_credentials` fixture の収集エラーになるが、手元の pytest の版による環境差でコード起因ではない。収集数は 853 件で、rebuild 27 の 850 件に #1830 の契約検査のテスト 3 件（`test_build_strategy.py`）が増えた分。post-merge fixes には `test_create_table_as_with_partitions` の `create_table_as` stub に `contract_sql` 引数を足す補正と、`test_build_strategy` の `resolve_table_type` stub を含む。
注: ローカルで `test_session.py::...[no_profile_in_credentials]` が 1 件 fail することがあるが、
これは実行環境に `AWS_PROFILE` が漏れている場合のみで、unset すれば pass する (コード起因ではない)。
