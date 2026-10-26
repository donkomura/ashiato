# 検知結果の分析の手順

`ashiato analyze` は、兆候とシナリオ一致を 1 件ずつ JSON Lines で出力する。攻撃の多いアカウントでは、1 件ずつ読んでも全体像をつかめないほど件数が多くなる。そのため、まず集計で全体像をつかみ、重大度の高いものから順に個々の記録へ絞り込んでいく。この文書では、そのための 2 つのオプションと、それを使った分析の手順を説明する。

## 出力を絞り込むオプション

`--severity` は、列挙した重大度の兆候とシナリオ一致だけを出力する。重大度は `low`、`medium`、`high`、`critical` のいずれかで、複数をカンマ区切りで指定できる（`--severity critical,high`）。指定しなければ、すべての重大度を出力する。

`--summary` は、兆候とシナリオ一致を（種類、ルール ID、起点の主体）ごとに 1 行の `summary` 記録へまとめ、個々の記録の代わりに出力する。兆候も集計に含めるので、`--emit-signals` を併せて指定する必要はない。`--severity` と併用すると、指定した重大度だけを集計する。`summary` 記録には次の値が入る。

- `kind`：`signal` か `scenario_match` のいずれか
- `rule_id`、`severity`、`origin`：集計したルールと起点の主体
- `count`：まとめた兆候またはシナリオ一致の件数
- `first_seen`、`last_seen`：根拠のイベントの最初と最後の時刻
- `source_ips`：根拠の送信元 IP の数
- `events`：根拠のイベントの数（重複を除く）

`summary` 記録は、重大度の高い順に並び、同じ重大度の中では件数の多い順に並ぶ。

## 実行しなかったルール

S3 のデータイベントのように、証跡の設定によっては記録されないデータを必要とするルールがある。ルールは、必要なデータを `requires` に宣言する。分析期間に必要なデータがない場合、`analyze` はそのルールと、そのルールをステップに含むシナリオを実行しない。そして、実行しなかったルールごとに `rule_skipped` 記録を出力する。データがないのに結果が空になると、攻撃がなかったと読み違えるおそれがあるので、実行しなかったことを明示している。

```json
{"type":"rule_skipped","rule_id":"cross_account_s3_destruction","reason":"missing_data","requires":["s3_data_events"]}
```

`rule_skipped` 記録は、`--summary` や `--severity` を指定しても出力する。`run` 記録の `data` には、分析期間のイベントの件数（`events`）と、S3 のデータイベントの有無（`s3_data_events`）が入る。

## 分析の手順

最初に、重大度の高い検知だけを集計して、攻撃の全体像をつかむ。

```sh
ashiato analyze --workspace case --since 2026-09-01 --until 2026-10-01 \
  --summary --severity critical,high
```

集計は起点の主体ごとにまとめるので、同じ主体に対する別々の攻撃が 1 行にまとまることがある。`first_seen` から `last_seen` までの期間が長い行や、`source_ips` が多い行は、複数の攻撃を含んでいる可能性がある。

次に、気になる行のルールと主体で個々の記録を絞り込み、根拠のイベント ID を確認する。兆候まで見るには `--emit-signals` を付ける。

```sh
ashiato analyze --workspace case --since 2026-09-01 --until 2026-10-01 \
  --emit-signals --severity critical,high \
  | jq -c 'select(.type == "signal" and .signal_id == "compute_launch_attempts"
                  and .origin.id == "iam_user:arn:aws:iam::111111111111:user/alice")
           | {start_time, source_ip, attrs}'
```

送信元 IP と時刻を並べると、攻撃ごとに切り分けられる。根拠のイベントは、ワークスペースの `ashiato.duckdb` の `events` テーブルから `event_id` で引ける。

重大度の高い検知を確認し終えたら、`--severity medium` や `--severity low` で同じように集計する。重大度の低い兆候は件数が多いので、件数の多い主体や、重大度の高い検知と同じ主体の行から確認する。

最後に、良性の検知を見分ける。アカウントの所有者による作業（MFA を使った root の操作、監査用ロールによる一斉の参照など）でも、兆候は出る。所有者の作業かどうかは、送信元 IP がベースライン期間に使われていたか、同じ時刻に所有者のほかの操作があるかで判断する。
