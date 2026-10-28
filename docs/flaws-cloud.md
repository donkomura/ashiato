# flaws.cloud のデータセットによる結合テスト

結合テストでは、flaws.cloud と flaws2.cloud を運用している AWS アカウントの CloudTrail ログを MinIO に置き、S3 のソース（`source.kind: s3`）で `ashiato run` を最後まで実行する。このデータセットは Summit Route が公開しているものであり、リポジトリには含めない。

- 公開元の説明：<https://summitroute.com/blog/2020/10/09/public_dataset_of_cloudtrail_logs_from_flaws_cloud/>

## データセットの取得

```sh
mkdir -p ~/datasets/flaws && cd ~/datasets/flaws
curl -LO https://summitroute.com/downloads/flaws_cloudtrail_logs.tar
sha256sum flaws_cloudtrail_logs.tar
tar -xf flaws_cloudtrail_logs.tar
```

2026-10-26 に取得したファイルは次のとおりだった。

- サイズは 251,688,960 バイトで、SHA-256 は `f26c2b8da56bb00d695a57f00f86208117582b9216bb52424d68e718ca2a96aa` である。
- 展開すると、`flaws_cloudtrail_logs/` の下に `flaws_cloudtrail00.json.gz` から `flaws_cloudtrail19.json.gz` までの 20 ファイルができる。各ファイルは CloudTrail の `{"Records": [...]}` 形式である。
- イベントの期間は 2017-02-12 から 2020-10-07 までである。

## MinIO の準備

テストは MinIO のコンテナを自分で起動し、終わるときに削除する。Docker と MinIO のイメージが必要である。イメージと、残ったコンテナの削除の方法は `docs/s3-testing.md` に書いた。

```sh
docker pull bitnamilegacy/minio:2025.7.23
```

## キーの階層への並べ直し

データセットのファイルは、CloudTrail のキーの階層（`AWSLogs/<account>/CloudTrail/...`）に置かれておらず、1 つのファイルに複数のリージョンのイベントが入っている。テストは、MinIO に置く前に、レコードを次の規則で並べ直す。

- データセットのファイルごとに、レコードを `recipientAccountId`、`awsRegion`、`eventTime` を 5 分で切り捨てた時刻の組でまとめ、組ごとに 1 つのログファイルにする。
- ログファイルのキーは `AWSLogs/<recipientAccountId>/CloudTrail/<awsRegion>/YYYY/MM/DD/<recipientAccountId>_CloudTrail_<awsRegion>_<YYYYMMDDTHHmmZ>_flaws<NN>.json.gz` とする。ファイル名の時刻と日付のディレクトリは、5 分の区切りの終わりの時刻にする。`<NN>` は元のデータセットのファイルの番号である。
  - 5 分の区切りが 2 つのデータセットのファイルにまたがる場合は、同じ時刻で `<NN>` の異なる 2 つのログファイルになる。
- どのイベントも、ファイル名の時刻より前の 5 分以内にあるので、実行の定義の `allowed_lateness` を `5m` にすれば、遅れて届いたイベント（`late_events`）にならない。
- バケットはバージョニングを有効にして作る。

## 実行

```sh
ASHIATO_FLAWS_DIR=~/datasets/flaws/flaws_cloudtrail_logs \
  cargo test --release -p ashiato-cli --test flaws -- --ignored --nocapture
```

テストは一時ディレクトリでレコードを並べ直し、MinIO に置いてから、一時ディレクトリのワークスペースに次の実行の定義を書き、`ashiato run --plan run.yaml --emit-signals` を実行する。認証情報は、AWS SDK の既定の方法で環境変数から読ませる。

```yaml
run_id: flaws
source:
  kind: s3
  uri: s3://flaws-trail/AWSLogs/
  endpoint_url: http://127.0.0.1:<MinIO のポート>
analysis_window:
  since: 2017-03-14T00:00:00Z
  until: 2020-10-08T00:00:00Z
rules: [builtin]
pipeline:
  batch: 1h
  allowed_lateness: 5m
placement:
  mode: single_process
```

`accounts` と `regions` は指定しないので、アカウントとリージョンの組は接頭辞の列挙で求める。

調査期間は、既定で 2017-03-14 から 2020-10-08 までとする。開始日をデータの始まりから 30 日後にしたのは、ベースライン期間（既定 30 日）にイベントがあるようにするためである。調査期間は環境変数 `ASHIATO_FLAWS_SINCE` と `ASHIATO_FLAWS_UNTIL` で変えられる。

## 出力される値

テストは、`run` が成功し、`run` レコードの `file_errors` と `late_events` が 0 であり、`ingested_files` が並べ直したログファイルの数と一致することを確かめる。そのうえで、次の値を標準エラー出力に書く。

| 値 | 内容 |
|---|---|
| `dataset` | 並べ直したレコードの件数とログファイルの数 |
| `preparing MinIO` | 並べ直しと MinIO への配置にかかった時間 |
| `run` | `ashiato run` の処理時間と最大メモリ使用量 |
| `workspace` | ワークスペースのディスク使用量。実行中の最大と、終了時の値 |
| `pending_max_rows` | 分析コンシューマーの `pending` の最大行数 |
| `run record` | `run` レコードの取り込んだファイル数、壊れたファイル数、遅れて届いたイベント数、データの有無 |
| `run stderr` | `ashiato run` の標準エラー出力（警告と計測値の行） |
| `signal`、`scenario_match`、`rule_skipped` | 兆候、シナリオの照合結果、外したルールの、ルールごとの件数 |

最大メモリ使用量は、Linux の `/proc/<pid>/status` の `VmHWM` を 20 ミリ秒ごとに読んで求める。Linux 以外では `unknown` になる。ワークスペースのディスク使用量も、同じ間隔でワークスペースのファイルの大きさを合計して求める。

`pending_max_rows` は、`ashiato run` が終了時に標準エラー出力へ書く計測値の行から読む。この行は次の形の 1 行の JSON であり、標準出力には書かない。

```json
{"metrics":"run","pending_max_rows":12345}
```

`pending_max_rows` は、受け取ったイベントを確定するたびに数えた `pending` の行数の最大値である。バッチが確定してイベントが `pending` から消えた後も、最大値は残る。
