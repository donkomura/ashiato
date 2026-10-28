# S3 のソースのテスト

S3 のソース（`source.kind: s3`）のテストのうち、S3 互換のストレージを使うものは、MinIO を Docker で動かして実行する。所要時間が長く、Docker を必要とするので、`#[ignore]` を付けて `cargo test` から外している。

`503 SlowDown` に対する再試行のテスト（`crates/ashiato-ingest/tests/s3_retry.rs`）は、テストの中で小さな HTTP サーバーを動かすので、Docker を使わず、通常の `cargo test` で実行される。

## 必要なもの

- Docker。テストは `docker` コマンドでコンテナを起動・停止する。
- MinIO のイメージ `bitnamilegacy/minio:2025.7.23`。MinIO の公式のイメージ（`minio/minio`）は配布が止まっているので、MinIO のリリース（RELEASE.2025-07-23）を Bitnami がビルドしたイメージを使う。初回は次のコマンドで取得しておく。

```sh
docker pull bitnamilegacy/minio:2025.7.23
```

## 実行

```sh
cargo test -p ashiato-ingest --test s3 -- --ignored
cargo test -p ashiato-cli --test run_s3 -- --ignored
```

テストは、テストごとに MinIO のコンテナを 1 つ起動し、テストが終わるとき（失敗した場合を含む）にコンテナを削除する。コンテナはボリュームを使わず、`--rm` を付けて起動するので、削除すればデータは残らない。MinIO のポートは `127.0.0.1` の空いているポートに割り当てるので、すでに動いている MinIO や他のテストとぶつからない。

テストのプロセスを強制終了した場合は、コンテナが残ることがある。残ったコンテナは、ラベルを指定して削除する。

```sh
docker rm --force $(docker ps --quiet --filter label=ashiato-test=minio)
```

## 確かめていること

- `crates/ashiato-ingest/tests/s3.rs`
  - 組織の証跡とアカウントの証跡のファイルが、接頭辞の列挙で求めたアカウントとリージョンの組ごとに、ファイル名の時刻順にまとめられる。`CloudTrail-Digest/` は一覧に入らない。
  - バージョニングが有効なバケットでは、一覧の後に同じキーに新しいバージョンを置いても、一覧のときのバージョンを読む。
  - バージョニングが無効なバケットでは、一覧の後にオブジェクトを置き換えると ETag が一致しなくなり、ワーカーのエラーになる。一覧の後にオブジェクトを消した場合も、ワーカーのエラーになる。
- `crates/ashiato-cli/tests/run_s3.rs`
  - 同じログを MinIO に置いて S3 のソースで `run` を実行した出力が、ローカルのソースで実行した出力と一致する。
