# 分散配置でのパイプラインの実行例

この例では、`ashiato run` を分散配置（`placement.mode: distributed`）で動かす。分析コンシューマー（`ashiato run`）と 2 つの取り込みワーカー（`ashiato ingest-worker`）を別々のプロセスとして起動し、NATS JetStream でイベントを受け渡す。ログは MinIO のバケットから読む。分散配置はローカルのディレクトリをソースにできないので、S3 互換のストレージが必要になる。

## 必要なもの

- Rust のツールチェーン（リポジトリのビルドに使う）
- Docker と Docker Compose v2
- 次のイメージ。初回は取得しておく。

```sh
docker pull nats:2.10.29
docker pull bitnamilegacy/minio:2025.7.23
```

NATS サーバーは 2.9.1 以降でなければならない。理由は `docs/nats-testing.md` に書いた。

## ファイル

| ファイル | 内容 |
|---|---|
| `records/` | アカウント `111111111111` の 2 つのリージョン（`us-east-1` と `ap-northeast-1`）の CloudTrail のレコード |
| `make-logs.sh` | `records/` のレコードを、CloudTrail と同じキーの階層とファイル名の `.json.gz` にして書き出すスクリプト |
| `compose.yaml` | NATS サーバー（`127.0.0.1:4222`）と MinIO（`127.0.0.1:9000`）、ログをバケットに置く `upload` サービス |
| `nats.conf` | JetStream を有効にした NATS サーバーの設定 |
| `workspace/run.yaml` | 実行の定義。`ashiato run` は、このファイルのあるディレクトリをワークスペースにして `ashiato.duckdb` を作る |

`records/` のファイル名は `<アカウント>_<リージョン>_<YYYYMMDDTHHmmZ>.json` である。`make-logs.sh` は、このファイル名からアカウント、リージョン、時刻を取り出す。ローカルのソースもワーカーも、`<アカウント>_CloudTrail_<リージョン>_<YYYYMMDDTHHmmZ>_<任意>.json.gz` という名前のファイルだけをログとして読む。

## 実行の定義

```yaml
run_id: example
source:
  kind: s3
  uri: s3://ashiato-example/AWSLogs/
  endpoint_url: http://127.0.0.1:9000
analysis_window:
  since: 2026-10-01T00:00:00Z
  until: 2026-10-02T00:00:00Z
rules: [builtin]
pipeline:
  batch: 1h
  allowed_lateness: 5m
placement:
  mode: distributed
  nats_url: nats://127.0.0.1:4222
  ingest_workers: 2
```

| キー | 意味 |
|---|---|
| `run_id` | 実行の ID。取り込みワーカーは `--run` にこの ID を指定して、どの実行に加わるかを決める。英数字、`-`、`_` だけを使える |
| `source.uri` | ログを読むバケットと、`AWSLogs/`（組織の証跡では `AWSLogs/<組織 ID>/`）までの接頭辞 |
| `source.endpoint_url` | S3 互換のストレージの URL。AWS の S3 を使う場合は書かない |
| `analysis_window` | 分析する期間。この前の 30 日（コンテキストの `baseline.lookback`）はベースラインの期間として読む |
| `rules` | 使うルール。`builtin` は組み込みのルールを指す。ディレクトリを並べると、そのルールも読む |
| `pipeline.batch` | 兆候を求める時間の区切り |
| `pipeline.allowed_lateness` | ログファイルの名前の時刻より、どれだけ前のイベントまでを分析に入れるか。それより前のイベントは分析に入れず、遅れて届いたイベントとしてワークスペースの `late_events` 表に記録する |
| `placement.ingest_workers` | 取り込みワーカーの数。パーティション（アカウントとリージョンの組）をこの数に分けて割り当てる |

この例ではパーティションが 2 つあるので、2 つのワーカーがそれぞれ 1 つのパーティションを受け持つ。

## 手順

このディレクトリで実行する。

### 1. ashiato を呼べるようにする

```sh
cargo build --release -p ashiato-cli
ashiato() { cargo run --release --quiet -p ashiato-cli -- "$@"; }
```

`target/release/ashiato` を直接実行すると、`libduckdb.so` を見つけられずに終わる。DuckDB の共有ライブラリはビルドの過程で `target/` の下に置かれ、バイナリにはその場所が記録されないためである。`cargo run` は、その場所を `LD_LIBRARY_PATH` に加えてから起動する。

### 2. ログを作り、サーバーを起動してバケットに置く

```sh
./make-logs.sh logs
docker compose up --detach --wait nats minio
docker compose run --rm upload
```

`upload` は、バケット `ashiato-example` を作ってバージョニングを有効にし、`logs/AWSLogs/` をバケットに置く。バージョニングを有効にすると、ワーカーは一覧を取ったときの版のオブジェクトを読む。

### 3. 分析コンシューマーと取り込みワーカーを起動する

MinIO の認証情報は、AWS SDK の既定の方法で環境変数から読ませる。

```sh
export AWS_ACCESS_KEY_ID=ashiato-example
export AWS_SECRET_ACCESS_KEY=ashiato-example-secret
export AWS_REGION=us-east-1

ashiato run --plan workspace/run.yaml --emit-signals > workspace/output.jsonl 2> workspace/run.log &
ashiato ingest-worker --nats nats://127.0.0.1:4222 --run example --worker-id 0 2> workspace/worker-0.log &
ashiato ingest-worker --nats nats://127.0.0.1:4222 --run example --worker-id 1 2> workspace/worker-1.log &
wait
```

起動の順は問わない。取り込みワーカーは、分析コンシューマーが答えるまで、受け持つパーティションの問い合わせを送り直す。`--worker-id` は 0 から `ingest_workers` − 1 までの値で、範囲外の値を指定したワーカーはエラーで終わる。ワーカーは、受け持ったパーティションを送り終えると終わる。分析コンシューマーは、すべてのパーティションを取り込んで分析を終えると終わる。

各プロセスは、1 秒ごとの計測値を標準エラー出力に JSON で書く。

### 4. 出力を確かめる

```sh
jq -c 'select(.type == "signal") | {signal_id, severity, region, start_time}' workspace/output.jsonl
```

```json
{"signal_id":"console_login_without_mfa","severity":"medium","region":"us-east-1","start_time":"2026-10-01T10:01:00Z"}
{"signal_id":"cloudtrail_logging_disabled","severity":"high","region":"ap-northeast-1","start_time":"2026-10-01T10:07:00Z"}
```

`us-east-1` のレコードは MFA を使わないコンソールへのログイン、`ap-northeast-1` のレコードは証跡の停止（`StopLogging`）であり、それぞれが兆候になる。2 つのワーカーが別々に取り込んだイベントが、分析コンシューマーで 1 つの分析にまとまる。

出力には、兆候のほかに次の記録も入る。

- `run`：実行の版、分析期間、取り込んだファイルとイベントの数
- `rule_skipped`：実行しなかったルール。`reason` が `not_streamable` のルールは、`batch` の区切りごとには計算できない集計や、ベースラインの期間の参照を使うので、パイプラインでは実行しない（同じルールは `ashiato analyze` では実行できる）。`missing_data` のルールは、この例のログにない S3 のデータイベントを必要とする
- `origin_score`：起点の主体（この例では `alice`）ごとの点数と、兆候の戦術。点数が閾値（既定値は 10）以上で、戦術が既定値で 3 種類以上あると `above_threshold` が `true` になる

出力の読み方と絞り込みは、`docs/analysis-method.md` に書いた。

## 片付け

```sh
docker compose down --volumes
```

同じワークスペースで実行し直すときは、`workspace/` の `run.yaml` 以外のファイルを消してから実行する。

## この例の試験

`crates/ashiato-cli/tests/example_pipeline.rs` が、この例の実行の定義を読めること、`make-logs.sh` が 2 つのパーティションのログを作ること、分散配置で実行すると上の 2 つの兆候が出ることを確かめる。最後の試験は Docker を使うので、`--ignored` を付けたときだけ実行する。

```sh
cargo test -p ashiato-cli --test example_pipeline -- --ignored
```
