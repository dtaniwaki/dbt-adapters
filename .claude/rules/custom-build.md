# Custom Build

Base: `860da8922` (upstream/main HEAD at rebuild 22)
Date: 2026-07-31 (rebuild 22)

## Changes since rebuild 21

- **Base を `fec9d2d96` → `860da8922` (upstream/main HEAD) に前進**: rebuild 19-21 は base 据え置きだったが、
  今回 #1874 (`feat/spark-3.5-support`) の新 HEAD `d8a79b446` が upstream/main HEAD を取り込んだため、
  据え置きの意味が薄れた。base を前進させ、S3 Tables catalog サポート (#2047) 等 upstream の新規変更を取り込む。
- **#1874 (`feat/spark-3.5-support`) を `07e953f55` → `d8a79b446` に更新**: Spark Connect python model の
  `assume_role_arn` 対応で、既定 session の据え方を `boto3.setup_default_session(botocore_session=assumed._session)`
  から `boto3.DEFAULT_SESSION = assumed` に変更。旧実装は既に boto3 Session でラップ済みの botocore session を
  再ラップし、`creating-client-class.s3` ハンドラを二重登録するため、モデル最初の `boto3.client("s3")` が
  `Cannot inject class attribute "upload_file"` で落ちていた。ただの代入は再構築を伴わないため二重登録が
  構造的に起きず、`DeferredRefreshableCredentials` の refresh 能力も保持する（frozen 化しないので長時間 run でも
  失効しない）。s3 client 生成を叩く unit regression + functional probe (`boto3.client("s3")`) を追加し、
  assume あり/なしとも実機 (udwh-development) で pass 済み。`origin/feat/spark-3.5-support` へ squash+force push 済み。
- 既存 PR 構成は rebuild 21 と同一 (14本)。新規追加・除外なし。

## Included PRs

- https://github.com/dbt-labs/dbt-adapters/pull/1211 — Fix a debug log about the Athena workgroup (branch: `origin/patch-1`)
- https://github.com/dbt-labs/dbt-adapters/pull/1704 — perf(athena): cache `_get_data_catalog()` result to avoid repeated STS calls (branch: `origin/fix/lru-cache-data-catalog`)
- https://github.com/dbt-labs/dbt-adapters/pull/1705 — fix(athena): fix "connection never acquired" with `--no-populate-cache` and `threads > 1` (branch: `origin/fix/no-populate-cache-thread-connection`)
- https://github.com/dbt-labs/dbt-adapters/pull/1743 — fix(athena): handle unpartitioned models in create_table_as_with_partitions (branch: `origin/fix/athena-unpartitioned-too-many-open-partitions`)
- https://github.com/dbt-labs/dbt-adapters/pull/1749 — fix(athena): create empty target table when no partition batches found (branch: `origin/fix/athena-empty-batch-target-table`)
- https://github.com/dtaniwaki/dbt-adapters/pull/3 — feat(athena): add disable_batch_fallback config option (branch: `origin/worktree-soft-chasing-ullman`)
- https://github.com/dtaniwaki/dbt-adapters/pull/4 — feat(athena): add model-level timeout support (branch: `origin/feat/athena-model-timeout`)
- https://github.com/dbt-labs/dbt-adapters/pull/1830 — feat(athena): add build_strategy config for incremental and table materializations (branch: `origin/feat/athena-build-with-subquery`)
- https://github.com/dbt-labs/dbt-adapters/pull/1832 — feat(athena): add merge_exclude_source_columns config (branch: `origin/feat/athena-merge-select-exclude-columns`)
- https://github.com/dtaniwaki/dbt-adapters/pull/7 — feat(athena): resolve cross-account Glue catalogs in Spark Python models (branch: `origin/feat/athena-spark-cross-account-catalog`)
- https://github.com/dbt-labs/dbt-adapters/pull/1874 — feat(athena): add Apache Spark 3.5 support via Spark Connect (branch: `origin/feat/spark-3.5-support` `d8a79b446`。session pool の StartSession throttle backoff・shared-session drain fix・python model の assume_role_arn identity fix (boto3.DEFAULT_SESSION 方式) を内包)
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
- https://github.com/dbt-labs/dbt-adapters/pull/2047 — feat(athena): add S3 Tables catalog support (base 前進で新規に取り込み)

## Closed / Excluded PRs

- https://github.com/dbt-labs/dbt-adapters/pull/1740 — fix(athena): exclude ICEBERG_FILESYSTEM_ERROR from outer retry (**closed**: #1637 に機能包含)
- https://github.com/dbt-labs/dbt-adapters/pull/1814 — fix(athena): skip retry of deterministic errors with configurable timeout handling (**closed**: #1637 に機能包含)

## Conflict Resolutions

base 前進 (`fec9d2d96` → `860da8922`) で `impl.py` に新規 conflict が発生（新 base の S3 Tables 対応 #2047 が
`_get_data_catalog` を `_get_aws_account_id` + `_get_data_catalog` に分割したため）。以下は手動解決、残りは rerere 自動解決:

- `#1704` (lru-cache): `impl.py` — 新 base の `_get_aws_account_id` を保持しつつ `_get_data_catalog` に
  `@lru_cache(maxsize=32)` を付与（手動解決）。
- `#1705` (no-populate-cache): `impl.py` — import に `get_boto3_session_from_credentials` を追加し、
  `_get_aws_account_id` / `_get_data_catalog` を `get_thread_connection()` から
  `self.connections.profile.credentials` + `get_boto3_session_from_credentials` に置換（手動解決）。
- `#1881` (iceberg-writeto): `create_table_as.sql` — S3 Tables python guard (新 base) と `use_iceberg_write_to`
  config (PR) を併存させ、`athena__py_save_table_as` の optional_args に `extra_table_properties` /
  `use_iceberg_write_to` / `spark_engine_version` を統合（手動解決）。
- 上記以外 (`worktree-soft-chasing-ullman` / `feat/athena-model-timeout` / `feat/athena-build-with-subquery` /
  `feat/athena-spark-cross-account-catalog` / `feat/spark-3.5-support` / `feat/athena-python-model-skip-materialize`)
  は rerere 自動解決。

## Post-merge fixes

複数 PR の組み合わせで生じるテスト破綻を補正する単一コミット
`fix(athena): post-merge fixes for custom-build rebuild` (前回 `be914bfa3` を cherry-pick、クリーン適用)。
補正内容は rebuild 21 と同一 (create_table_as_with_partitions の空バッチ guard、`test_adapter` の
`AthenaError` 追従、MockAdapter の `disable_batch_fallback` kwarg、`test_python_submissions` の
Spark フィールド、`test_spark_dbt_obj` / `test_py_save_table_as` の `config` stub 等)。加えて base 前進で
`impl.py` を触ったため black 自動整形 + import 並べ替え (`exceptions` を `session` より前) を同コミットに含む。

- **機能 (rebuild 22 上で追加)**: `python_submissions.sql` の `if df is None:` footer で、
  `assume_role_arn` 設定時の materialize チェック用 glue クライアント生成条件を
  `assume_role_arn` → `assume_role_arn and spark_engine_version|string != "3.5"` に変更。#1990 (footer の
  明示 assume) と #1874 (Spark 3.5 の `boto3.DEFAULT_SESSION = assumed`) の組み合わせで、3.5 経路では
  default session が既に exec role のため footer の明示 `sts.assume_role(exec role)` が self-assume
  AccessDenied になり `return None` する python model が全滅していた。3.5 経路は default session の
  plain glue クライアントを使う。非 3.5 (レガシー) 経路は従来の明示 assume を維持し #1990 単体を退行させない。
  regression unit (`test_py_save_table_as.py::TestSkipMaterializeGlueSession`) 追加。実機 (udwh-development)
  で `return None` python model の materialize チェック通過を確認済み。

全 unit tests: **594 passed** (13 xfailed / 2 xpassed)。591 に対し +3 は上記 footer self-assume regression。
注: ローカルで `test_session.py::...[no_profile_in_credentials]` が 1 件 fail することがあるが、
これは実行環境に `AWS_PROFILE=udwh-development` が漏れている場合のみで、unset すれば pass する
(コード起因ではない)。
