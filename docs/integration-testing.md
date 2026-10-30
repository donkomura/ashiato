# 分散配置の統合テスト

分散配置の統合テスト（`streaming-pipeline.md` の 14.1 節）は、`xtask` のコマンドで実行する。取り込みワーカー、NATS サーバー、分析コンシューマー、MinIO を Docker Compose で別々のコンテナとして立てるので、所要時間が長く、Docker を必要とする。`cargo test` には含めない。

## 必要なもの

- Docker と Docker Compose v2。
- 物理コアが 6 つ以上ある CPU。物理コア 0 を MinIO と NATS サーバー、1 を分析コンシューマー、2〜5 を取り込みワーカーに割り当てる。
- 次のイメージ。初回は取得しておく。

```sh
docker pull bitnamilegacy/minio:2025.7.23
docker pull nats:2.10.29
docker pull debian:bookworm-20251229-slim
```

`ashiato` はホストで `cargo build --release -p ashiato-cli` でビルドし、バイナリと `libduckdb.so` を `debian:bookworm-20251229-slim` のコンテナに読み取り専用でマウントして動かす。ビルドは `xtask` が行う。

## 実行

```sh
cargo xtask integration correctness --dataset a
cargo xtask integration correctness --dataset a-no-data-events
```

`--scale` でデータセットを小さくできる。`--scale 0.01` のように指定すると、手早く手順を確かめられる。

`xtask` は、次の順に処理する。

1. CPU のトポロジーと、Docker、Docker Compose、NATS サーバー、MinIO の版を記録する。
2. データセットが `target/integration/datasets/<データセット>-<scale>/` になければ生成する。
3. データセットを MinIO のボリューム `ashiato-integration-minio-<データセット>-<scale>-seed<種>` のバケット `ashiato-integration` に置く。バケットのバージョニングは有効にする。バケットに同じ `dataset.json` がすでにあれば、置き直さない。
4. R0、D1〜D4、F1〜F4 を順に実行する。各実行は `target/integration/<実行>/` を作り直し、Docker Compose のプロジェクト `ashiato-integration-<データセット>-<実行>` でコンテナを立て、分析コンシューマーが終わったらコンテナを削除する。
5. 各実行を R0 と比べ、`target/integration/report.md` に報告を書く。1 つでも一致しなければ、コマンドは 0 以外で終わる。

## 速度の確認

速度の確認（14.1.4 節）は、データセット B で次のコマンドを実行する。

```sh
cargo xtask integration scaling --dataset b --workers 1,2,3,4 --repeat 3
```

`--workers` には計測するワーカーの数をカンマで区切って指定し、`--repeat` には各ワーカーの数での計測の回数を指定する。`--scale` は一致の確認と同じく、データセットを小さくする。`--scale 0.005 --workers 1,2 --repeat 1` のように指定すると、手早く手順を確かめられる。データセットが小さいと取り込み時間が短く、E(N) は基準を満たさないことがある。

`xtask` は、CPU のトポロジーと版の記録、データセットの生成と MinIO への配置、`ashiato` のビルドを一致の確認と同じく行ってから、次の順に処理する。

1. R0 を `single_process` の配置で 1 回実行する。
2. `--workers` の各 N について、小さい順に、ウォームアップの実行 `N<N>-WARMUP` を 1 回、計測の実行 `N<N>-1`〜`N<N>-<repeat>` を行う。配置は `distributed` で、障害は注入しない。各実行は `target/integration/scaling/<実行>/` を作り直し、Docker Compose のプロジェクト `ashiato-integration-b-<実行>` でコンテナを立てる。
3. 各実行の間、`docker stats --no-stream --format '{{json .}}'` を 1 秒ごとに実行し、時刻とともに `target/integration/scaling/<実行>/logs/docker-stats.log` に残す。`docker stats --no-stream` はコンテナの 2 回の読み取りを待ってから返るので、標本の間隔は 1 秒より長くなることがある。ログには他の実行のコンテナも残るので、CPU を取り合っていなかったかを後から確かめられる。
4. 計測の実行の分析コンシューマーとワーカーの標準エラー出力から最終の計測値を読み、N ごとに T(N) の中央値、取り込みの速度、S(N)、E(N) を求める。CPU 使用率は、各回の取り込み時間 T(N) の間に取った標本だけを使う。
5. ウォームアップを含むすべての実行を R0 と比べ、`target/integration/scaling-report.md` に報告を書く。R0 と一致しない実行がある場合、計測値を読めない実行がある場合、または N = 2、3、4 のいずれかで E(N) が基準を下回った場合は、コマンドは 0 以外で終わる。

## 残るもの

- `target/integration/<実行>/` に、Compose のファイル、実行の定義、ワークスペースの DuckDB ファイル、JetStream の保存先、ログが残る。`logs/analyzer.stdout` が分析の出力、`logs/*.stderr` が計測値、`logs/compose.log` が MinIO と NATS サーバーを含むコンテナのログである。
- 速度の確認の実行は `target/integration/scaling/<実行>/` に、一致の確認と同じものと `logs/docker-stats.log` が残る。
- データセットを置いた MinIO のボリュームは、次の実行で使うために残す。不要になったら削除する。

```sh
docker volume rm $(docker volume ls --quiet --filter label=ashiato-integration=minio)
```

コマンドを強制終了した場合は、コンテナが残ることがある。残ったコンテナは、プロジェクトごとに削除する。

```sh
docker compose --file target/integration/<実行>/compose.yaml --project-name ashiato-integration-<データセット>-<実行> down --volumes
docker rm --force $(docker ps --all --quiet --filter label=ashiato-integration=setup)
```
