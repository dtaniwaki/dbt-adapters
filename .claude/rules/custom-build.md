# Custom Build

Base: `e6747c5b3` (upstream/main HEAD at rebuild 23)
Date: 2026-10-02 (rebuild 23)

## Changes since rebuild 22

- **Base を `860da8922` → `e6747c5b3` (upstream/main HEAD) に前進**: 間の 27 コミットに dbt-athena を触るものは無い
  (dbt-adapters 共通部分・テスト・他アダプタの修正のみ)。
- **#1874 (`feat/spark-3.5-support`) を `d8a79b446` → `962a92501` に更新**:
  - Spark Connect の PERMISSION_DENIED を、pyspark の `Retrying` ループごとに最初の拒否から 600 秒まで backoff で
    再試行する。Athena の Spark Connect プロキシは、リクエスト量がセッション横断の上限を超えると、新規セッション
    を含む全 RPC を数分間 PERMISSION_DENIED で拒否するため、従来の即時 1 回の再試行では回復できなかった。
    最初の ExecutePlan が拒否されたクライアントの reattach が `INVALID_HANDLE.SESSION_NOT_FOUND` になる場合は、
    応答 0 件に限り ExecutePlan を送り直す。実機（5 セッションのバースト）で、途中拒否・拒否中に作った新規
    クライアントとも 250 秒前後待って成功を確認。
  - fork #15 (merged into `feat/spark-3.5-support`): Athena セッションごとに Spark Connect クライアントを 1 つ共有する。
    pyspark 3.5 の `stop()` はサーバー側セッションを解放せず、Athena セッションあたり 25 本で
    `RESOURCE_EXHAUSTED: Maximum allowed sessions (25) exceeded` になる（実機で 26 本目から拒否を確認）。
    共有クライアントは最初のモデルの athena client でトークンを更新し続けるため、#1998 (RefreshableCredentials)
    と組み合わせて使う前提。
- **fork #4 (`feat/athena-model-timeout`) を `606b3bd3d` → `ed580a81d` に更新**: base 前進で `impl.py` がコンフリクト
  対象になり、pre-commit の black が PR ブランチ自体の整形違反（`raise AthenaModelTimeoutError(...)` の折り返し）
  を検出したため、PR ブランチ側で black を適用。
- PR 構成は rebuild 22 と同一 (14本)。新規追加・除外なし。

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
- https://github.com/dbt-labs/dbt-adapters/pull/1874 — feat(athena): add Apache Spark 3.5 support via Spark Connect (branch: `origin/feat/spark-3.5-support` `962a92501`。session pool の StartSession throttle backoff・shared-session drain fix・python model の assume_role_arn identity fix (boto3.DEFAULT_SESSION 方式)・Spark Connect PERMISSION_DENIED の再試行・fork #15 の Athena セッションごとのクライアント共有を内包)
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

すべて rerere 自動解決（手動解決なし）:
`fix/lru-cache-data-catalog` / `fix/no-populate-cache-thread-connection` / `worktree-soft-chasing-ullman` /
`feat/athena-model-timeout` / `feat/athena-build-with-subquery` / `feat/athena-spark-cross-account-catalog` /
`feat/spark-3.5-support` / `fix/iceberg-python-writeto` / `feat/athena-python-model-skip-materialize`。
それ以外はコンフリクトなし。

## Post-merge fixes

複数 PR の組み合わせで生じるテスト破綻を補正する単一コミット
`fix(athena): post-merge fixes for custom-build rebuild` (前回 `e0e37ece7` を cherry-pick、クリーン適用)。
補正内容は rebuild 22 と同一 (create_table_as_with_partitions の空バッチ guard、`test_adapter` の `AthenaError` 追従、
MockAdapter の `disable_batch_fallback` kwarg、`test_python_submissions` の Spark フィールド、`test_spark_dbt_obj` /
`test_py_save_table_as` の `config` stub、`python_submissions.sql` footer の 3.5 経路での self-assume 回避 等)。

全 unit tests: **611 passed** (13 xfailed / 2 xpassed)。594 からの増分は #1874 の PERMISSION_DENIED 再試行と
fork #15 のテスト。
注: ローカルで `test_session.py::...[no_profile_in_credentials]` が 1 件 fail することがあるが、
これは実行環境に `AWS_PROFILE` が漏れている場合のみで、unset すれば pass する (コード起因ではない)。
