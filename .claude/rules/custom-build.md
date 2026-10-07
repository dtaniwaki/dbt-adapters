# Custom Build

Base: `e6747c5b3` (unchanged since rebuild 23)
Date: 2026-10-07 (rebuild 26)

## Changes since rebuild 25

- **#1874 (`feat/spark-3.5-support`) を `16146b6b3` → `180097716` に更新**。fork PR を 6 本内包:
  - #18 `spark_connect_retry_on`: 再試行する transient 失敗の種類 (session_ended / capacity / executor_environment / connection) を選べる。省略時は全種類。
  - #19 keepalive: モデル実行中だけ `spark_connect_keepalive_interval` (既定 300s、0 で無効) ごとに `SELECT 1` を送り、ドライバ側処理が idle timeout を超えてもセッションを落とさない。
  - #20 Calculations 経路の空 submission が Athena に触れないようにする。共有クライアントの artifact RPC 前に AuthToken を取り直す。
  - #21 プール: 状態不明は保持（idle timeout 超過で終了）、判定は各セッションの client、前 invocation の使用中セッションは drain、DPU 予算不足時は他キーのアイドルを回収。
  - #22 振る舞いを変えないリファクタ（3.5 判定の集約、`effective_spark_connect_*`、attempt と acquire の分割、import の一方向化）。
  - #23 コメント整理。
- ベースは rebuild 25 と同一（upstream/main の前進分 3 コミットは bigquery / snowflake / CI のみ）。
- PR 構成は rebuild 25 と同一 (14本)。

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
- https://github.com/dtaniwaki/dbt-adapters/pull/7 — feat(athena): resolve cross-account Glue catalogs in Spark Python models (branch: `origin/feat/athena-spark-cross-account-catalog`)
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

- `dbt-athena/src/dbt/adapters/athena/exceptions.py`（`feat/spark-3.5-support`）: 手動解決。`AthenaModelTimeoutError` と `SparkSessionTerminatedError` を両方残す（#23 で後者の docstring が `pass` になり rerere が当たらなくなったため）。
- rerere 自動解決: `fix/lru-cache-data-catalog` / `fix/no-populate-cache-thread-connection` / `worktree-soft-chasing-ullman` /
  `feat/athena-model-timeout` / `feat/athena-build-with-subquery` / `feat/athena-spark-cross-account-catalog` /
  `feat/spark-3.5-support`（exceptions.py 以外）/ `fix/iceberg-python-writeto` / `feat/athena-python-model-skip-materialize`。
  `fix/iceberg-python-writeto` の解決後も `python_submissions.sql` の 3.5 判定が `adapter.is_spark_connect_engine` のままであること、全マージ後に `spark_connect/` が `origin/feat/spark-3.5-support` と一致することを確認済み。
- それ以外はコンフリクトなし。

## Post-merge fixes

複数 PR の組み合わせで生じるテスト破綻を補正する単一コミット
`fix(athena): post-merge fixes for custom-build rebuild` (前回 `823ca3ec3` を cherry-pick し、下記を追加)。
補正内容は rebuild 25 に以下を追加: `python_submissions.sql` footer の 3.5 判定を `adapter.is_spark_connect_engine` に揃える、`test_py_save_table_as` に `adapter` stub、`test_spark_connect_engine_version` に `config` / `target` / `this` stub（#22 で macro が adapter を呼ぶため）。既存 (create_table_as_with_partitions の空バッチ guard、`test_adapter` の `AthenaError` 追従、
MockAdapter の `disable_batch_fallback` kwarg、`test_python_submissions` の Spark フィールド、`test_spark_dbt_obj` /
`test_py_save_table_as` の `config` stub、`python_submissions.sql` footer の 3.5 経路での self-assume 回避 等)。

全 unit tests: **822 passed** (13 xfailed / 2 xpassed)。630 からの増分は fork #18〜#22 のテスト。
注: ローカルで `test_session.py::...[no_profile_in_credentials]` が 1 件 fail することがあるが、
これは実行環境に `AWS_PROFILE` が漏れている場合のみで、unset すれば pass する (コード起因ではない)。
