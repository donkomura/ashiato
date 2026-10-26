# ルールセット

ルールセットは、ルールの集まり（`rules/`）から実行するルールを選んで列挙したマニフェストである。`analyze` はルールセットに含まれるルールだけを実行する。ATT&CK のようなフレームワークに沿って検査したいときは、そのフレームワークのルールセットを指定する。

## ルールセットの指定

ルールセットもルールも指定しなければ、標準のルールを列挙した `default` ルールセットを実行する。

```sh
ashiato analyze --workspace case --since 2026-09-01 --until 2026-10-01 \
  --ruleset default
```

| オプション | 意味 |
|---|---|
| `--ruleset <id>[,<id>...]` | 実行するルールセット。複数を指定すると、含まれるルールの和集合を実行する |
| `--rule <id>[,<id>...]` | 指定したルールセットの中から、さらに実行するルールを絞り込む。シナリオを指定すると、そのステップの兆候も実行する |
| `--rulesets <dir>` | 埋め込みのルールセットに加えて読むマニフェストのディレクトリ。同じ ID のルールセットは置き換える |
| `--rules <dir>` | ルールの集まりにルールを追加し、同じ ID のルールを置き換える。どのルールセットにも含まれない追加のルールは、`default` に加える |

指定したルールセットに含まれないルールを `--rule` で指定すると、エラーになる。

`run` レコードの `rulesets` には、実行したルールセットの ID とマニフェストのダイジェストが入る。`rule_sources` には、ルールの出どころ（埋め込みのルールと `--rules` のディレクトリ）の ID とダイジェストが入る。

フレームワークを持つルールセットで実行すると、`signal` と `scenario_match` のレコードに、そのルールに対応するフレームワークの項目の ID が `refs` として入る。

```json
{"type":"signal","signal_id":"cloudtrail_logging_disabled","refs":[{"ruleset":"attack-v20","ids":["T1685.002"]}],...}
```

## ルールセットの中身の確認

```sh
ashiato rulesets list
ashiato rulesets show default
```

`rulesets list` はルールセットごとに、ID、説明、フレームワーク、ルールの数を出力する。`rulesets show` は、ルールセットのルールごとに、ID、種類、重大度、戦術、`refs` を出力する。どちらも JSON Lines で出力する。

## マニフェストの記法

```yaml
id: attack-v20
title: MITRE ATT&CK v20 Enterprise（IaaS、Identity Provider）
framework:
  kind: mitre_attack
  version: "20.0"
  url: https://attack.mitre.org/versions/v20/
rules:
  - id: cloudtrail_logging_disabled
    refs: [T1685.002]
```

| キー | 意味 |
|---|---|
| `id` | ルールセットの ID |
| `title` | ルールセットの説明 |
| `framework` | 省略できる。対応付けるフレームワークの種類（`kind`）、版（`version`）、URL（`url`、省略できる） |
| `rules[].id` | ルールの集まりにあるルールの ID |
| `rules[].refs` | 省略できる。フレームワークの項目の ID。`framework` がないルールセットでは書けない |

`framework.kind` は、現時点では `mitre_attack` だけを受け付ける。`mitre_attack` の `refs` は、`T1078` や `T1078.004` のような技術の ID でなければならない。

ルールセットに含めたシナリオのステップが参照する兆候は、同じルールセットに含めなければならない。`rules validate` は、ルールセットが存在しないルールを列挙していないか、シナリオの兆候が欠けていないかも検査する。`rules test --ruleset <id>` は、そのルールセットに含まれるルールのフィクスチャだけを試験する。
