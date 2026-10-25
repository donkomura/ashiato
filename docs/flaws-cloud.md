# flaws.cloud のデータセットによる結合テスト

結合テストでは、flaws.cloud と flaws2.cloud を運用している AWS アカウントの CloudTrail ログを、取り込みから `analyze` まで通して実行する。このデータセットは Summit Route が公開しているものであり、リポジトリには含めない。

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

ファイルは S3 のキーの階層（`AWSLogs/<account>/CloudTrail/...`）に置かれていないが、ローカルのディレクトリを取り込むときは `.json.gz` を再帰的に列挙するだけなので、そのまま取り込める。

## 実行

```sh
ASHIATO_FLAWS_DIR=~/datasets/flaws/flaws_cloudtrail_logs \
  cargo test --release -p ashiato-cli --test flaws -- --ignored --nocapture
```

テストは一時ディレクトリにワークスペースを作り、`ashiato ingest` と `ashiato analyze --emit-signals` を実行する。次の値を標準エラーに出す。

- 取り込んだイベントの件数
- `ingest` と `analyze` のそれぞれの処理時間と最大メモリ使用量
- 兆候とシナリオの、ルールごとの件数

最大メモリ使用量は、Linux の `/proc/<pid>/status` の `VmHWM` を 20 ミリ秒ごとに読んで求める。Linux 以外では `unknown` になる。

調査期間は、既定で 2017-03-14 から 2020-10-08 までとする。開始日をデータの始まりから 30 日後にしたのは、ベースライン期間（既定 30 日）にイベントがあるようにするためである。調査期間は環境変数 `ASHIATO_FLAWS_SINCE` と `ASHIATO_FLAWS_UNTIL` で変えられる。

## 記録

2026-10-26 に、release ビルドで既定の調査期間に対して実行した結果は次のとおりだった。

- データセットには 1,939,207 件のレコードがあり、`eventID` の重複を除いた 1,225,784 件が `events` に入った。
- `ingest` は 192.7 秒かかり、最大メモリ使用量は 3,245 MiB だった。
- `analyze` は 0.7 秒かかり、最大メモリ使用量は 437 MiB だった。
- 兆候は 12,847 件出た。内訳は次のとおりである。
  - `first_seen_identity_probe`：12,212 件
  - `access_denied_enumeration`：599 件
  - `iam_privilege_modification`：15 件
  - `iam_persistence`：13 件
  - `console_login_without_mfa`：6 件
  - `compute_resource_launch`：2 件
- シナリオは 1 件も成立しなかった。

この時点の標準ルールは、シナリオ `console_takeover_mining`、`role_chain_escalation`、`leaked_key_recon_persistence` と、それらの兆候だけである。文脈ファイルによる除外とスコアリングはまだない。
