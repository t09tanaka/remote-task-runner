# remote-task-runner 実装計画

`docs/spec.md`（仕様版 1.3）の実装を、PR 単位に分けた計画である。仕様の受入条件 A01〜A49 は、この表のいずれかの行が担当する。

## 前提

- lease 更新の実プロセス試験は、準備開始・更新の durable commit・seed の更新キーと単一 monotonic 測定期限・コマンド開始・supervisor の更新キー観測を順に確認する。短い実時間枠内での起動速度や旧 watcher を期待せず、期限境界は純粋な LeaseClock 試験で検証する。観測 journal は best-effort 診断であり、lease の判定やコマンドの継続条件にしない。

- 全 PR を `main` 宛てにし、単体で CI が通る形で都度マージする。依存列の「#N」は「#N が main にマージされた後、main から起点を切る」。
- 共通の検証コマンド（RTR = remote-task-runner）: `cargo fmt --all --check`、`cargo clippy --all-targets -- -D warnings`、`cargo test`、GitHub Actions の緑。純粋モジュールは local-ci と同じく行カバレッジ 100% のゲートと cargo-schemata の変異テストに登録する。
- 略称: RTR = remote-task-runner、LC = local-ci、TTC = task-traffic-controller。

## 分割表

| # | repo | タスク | スコープ | 完了条件 | 非スコープ | 依存 |
|---|---|---|---|---|---|---|
| S0 | RTR | 仕様 v1.2 | `docs/spec.md`、`docs/plan.md`（この表と A 対応表） | 仕様 v1.2 を反映（workspace は fast-worktree、基準版を local-ci 0.9.0 / task-traffic-controller 0.5.0 へ、Linux と一時 worker、`.worktreeinclude`、A45〜A48）。公開禁止語の grep が 0 件 | コード | なし |
| S1 | RTR | crate の骨組みと CI | `Cargo.toml`（local-ci 0.9・task-traffic-controller 0.5 を crates.io から）、§18 の module を空で全部作る、clap で §4 の全サブコマンド（未実装は `NOT_IMPLEMENTED` の構造化 error）、`--json` の外枠（schema_version・error code）、`.github/workflows/ci.yml`（fmt・clippy・test を macOS と Linux）、coverage gate と cargo-schemata の枠 | 共通の検証コマンドが緑。`remote-task-runner <各サブコマンド> --json` が schema_version 付きの error を返すテスト | 各機能の中身 | なし |
| X1 | LC・TTC | core の不足調査（読むだけ・完了） | §16 の core API・§17 の公開 API と local-ci 0.9.0 / task-traffic-controller 0.5.0 の差分。結果は仕様 §16・§17 の「0.5.0 / 0.9.0 での状態」列に反映済み | 各 API が有る／名前違いで有る／無いと、必要な変更の一覧。T1・L1a・L1b・L2・L3 のスコープがこれで確定した | 実装 | なし |
| S2 | RTR | 契約（ID・状態・wire） | §10 の ID 型、状態機械と terminal outcome・観測状態・receipt、error code、§11 の全操作の request/response DTO（schema_version・digest） | 純粋モジュールとしてカバレッジ 100%・変異テスト。状態遷移表のテスト | 通信・保存の実装 | S1 |
| S3 | RTR | 設定 | §5 のユーザー設定・プロジェクト設定、優先順位、未知キーの error、`.local-ci.toml` の fallback と矛盾の出所表示、`[toolchain]`・`[source] secret_files`・`[[outputs]]` の型 | 設定例 2 つが通る、未知キー・矛盾のテスト | 設定の利用側 | S1 |
| S4 | RTR | storage | SQLite（WAL）、migration と互換表、親 DB・worker DB の schema、条件付き terminal 確定、event journal、retire tombstone の永続保持 | 未知 version を書き換えないテスト、terminal が 1 回だけ確定する競合テスト | 配置・通信 | S2 |
| S5 | RTR | protocol 転送層 | 長さ prefix JSON frame、`hello` の version・feature 照合、control を log が塞がない多重化、chunk 転送の offset・digest 再開、`worker --stdio` の Unix socket bridge、SSH 起動（argv をシェル文字列にしない） | 切断・再開・非互換 version のテスト | 各操作の業務処理 | S2 |
| S6 | RTR | source capture と object store | `local_ci::capture_source` を使った worktree / commit / directory、private object store、§6 の SourceManifest、SOURCE_BUSY、未 push objects の bundle、submodule・LFS、展開時の path 安全性 | A11〜A14 相当のテスト（一時 Git repo の自作 fixture） | worker での展開 | S2（X1 で capture に不足があれば L 行も） |
| S7 | RTR | worker の workspace | repo ごとの canonical clone、attempt workspace を fast-worktree で作る・消す（プロジェクトの `.fast-worktree.toml` と `.worktreeinclude` をそのまま使う）、directory snapshot、`exec --in-place`（親 local worker 限定） | macOS と Linux で add / remove が通るテスト、元 worktree を書き換えないテスト（A48） | setup template（S17） | S6 |
| S8 | RTR | secret_files | §22.3 の SecretBundle（cluster 鍵の HMAC）、親での 0600 保持と削除、転送操作、0600 配置、terminal 時と reconcile 時の消去、ログの mask | A43・A47 のテスト | toolchain | S5・S6 |
| S9a | RTR | supervisor（寿命・lease・停止） | attempt ごとの別プロセス、task-traffic-controller の managed `register` / `try_admit` / `run_command`、launch intent、process identity（boot ID・開始時刻）、suspend を含む単調時計の lease、renew の冪等性、cancel の TERM→KILL と停止確認、3 種の timeout、実行中の sleep 抑止 | A20・A21・A22・A25・A26（時計）のテスト。OS 依存部分（単調時計・boot ID・process identity・sleep 抑止）の macOS 版と Linux 版（A45） | log spool（S9b） | S4 |
| S9b | RTR | log spool と結果記録 | stdout / stderr の disk spool、stream ID・offset・sequence、非 UTF-8、上限 1 GiB と `log_truncated`、結果と artifact の attempt 別保存 | A29 のテスト | 回収（S14） | S4 |
| S10 | RTR | worker agent | §11 の worker 側操作すべて、`attempt_id + payload_hash` の冪等受理、retire tombstone、再起動時の supervisor adopt、boot ID 変化で interrupted、`ack_collected` までの GC 保護 | A18・A19・A23・A24（擬似）のテスト | forecast（S11）・導入（S22） | S4・S5・S9a・S9b |
| S11 | RTR | worker forecast | task-traffic-controller の forecast に受理済み未登録の overlay を重ねる、coverage（attempt・plan_digest・expansion_seq・node）、読み取り専用、TTL と epoch 失効、measured / fallback / unknown | A06〜A10 のテスト | 配置の判断（S13） | S10・X1 |
| S12 | RTR | coordinator の中核 | durable queue（run・task・attempt）、prefetch / max_active を守る dispatch、未 ACK 需要を同じ transaction に記録、retire してから再配置、unknown の保持、epoch と再起動時の reconcile・adopt、cancel の伝達 | A19・A21・A22・A23 のテスト（親側） | 予測による選択（S13）・回収（S14） | S4・S5・S10 |
| S13 | RTR | scheduler | 1 actor で配置を直列化、転送＋setup＋実行＋回収の予測完了時間で比較、同点規則、途中参加 worker の取り込み、配置理由の表示 | A01〜A04 のテスト（1 台上の複数 worker） | CI 固有の処理 | S11・S12 |
| S14 | RTR | 結果の回収 | collecting 状態、artifact の digest 検証と atomic 公開、`ack_collected`、回収だけの再開、完全性検査と `no_work`、保持 7 日の GC | A30・A31 のテスト | GitHub status（S19） | S9b・S12 |
| S15 | RTR | 利用者コマンド | `init`・`exec`・`submit`・`ls`・`show`・`logs --follow`・`wait`・`cancel`（Ctrl-C）・`retry`・`outputs`・`estimate`、JSON と exit code | 各コマンドの統合テスト、`exec` の exit code 透過 | `ci`（S16）・`workers`（S21〜S23） | S12・S14 |
| S16 | RTR | CI 統合 | `ci` / `ci plan`、FrozenPlan を 1 回だけ作る、Job DAG の配置と needs output、dynamic matrix の展開 event、host をまたぐ中間 artifact の receipt（workflow 単位）、matrix の global permit・max-parallel・fail-fast・join・決定的な集約、分割できない理由の表示 | A10・A27・A28・A38 のテスト | test sharding（S18） | S13・S14・L1a・L1b |
| S17 | RTR | setup と template | SetupRecipe / PreparedTemplate receipt、worker 内 singleflight の builder（builder_id・consumer 参照・独自 lease）、immutable template と書き込み分離の複製、`--no-cache`、`[prepare]`。fast-worktree の `[[clone]]` との境界は仕様 v1.2 に従う | A15〜A17 のテスト | toolchain の導入（S20） | S7・S9a・S10 |
| L1a | LC | step 実行の差し込み口 | `exec::Opts` に `CommandExecutor` 相当の trait（`StepRequest` を受けて `StepExit` を返す）を足す。executor が無いときは従来どおりバイナリを子プロセスで起動する。pipe fd（token の受け渡し）と process group の扱いを trait の契約に明記する。local-ci は task-traffic-controller の crate に依存しない | local-ci の CI（coverage・変異）緑、executor 未設定の挙動がバイト単位で不変（A36）、`cargo publish` まで | L1b・L2・L3 | なし |
| L1b | LC | task demand 列挙と wire DTO | `plan_task_demand`（task-traffic-controller の型に依存しない DTO。条件式・動的 matrix は `conditional`）、`SetupRecipe`・`PreparedReceipt`・`TaskResult`・`TaskInputs`・`ExecutionTask`・`JobGraph` の schema_version と digest、wire から host path を除く（`StepResult.log`・`RunResult.run_dir`）、`RunResult` の `Deserialize` | local-ci の CI 緑、既存 CLI の JSON 出力は不変（A36）、`cargo publish` まで | test sharding（L2・L3） | L1a・L2 |
| L2 | LC | test sharding（土台＋cargo test・Vitest・Jest） | `partition_tests`、adapter の discover / partition / invoke / collect / merge、履歴による長い順の割り当てと決定的な fallback、全件 1 回の集合検証 | A39・A40 のテスト（同梱 example） | Playwright・Cypress・coverage merge | X1 |
| L3 | LC | test sharding（Playwright・Cypress・coverage merge・分割禁止条件） | 2 adapter、runner 対応の coverage merge、DB / port / container を隔離できない E2E の分割禁止 | A41 のテスト、`cargo publish` まで | RTR 側の配置 | L2 |
| T1 | TTC | 分散用 API の不足分 | `exec::Opts` にenv map（`env_clear` 後に設定）とstdio（stdout・stderrの追記先log path、stdinはnull）を足し、`managed::run_command` まで通す。`None` 既定で既存CLIの挙動を変えず、履歴keyにenvを混ぜない。任意: `legacy_clock`、inspect の history quality | task-traffic-controller の CI 緑、env 隔離・log 追記・lease と marker が残るテスト、既存 CLI の挙動不変、`cargo publish` まで | RTR 側 | なし |
| S18 | RTR | sharding の配置 | ShardGroup の配置と集約、fail-fast、失敗 Shard の retry、通過済み結果の保持 | A39〜A41 の統合テスト | adapter 本体 | S16・L3 |
| S19 | RTR | GitHub status | 親だけが投稿、公開 commit と検査入力の一致検証、dirty / synthetic は success にしない、計算結果と投稿失敗を分けて報告 | A32 のテスト | local-ci 単体の status の変更（必要なら L 行を追加） | S16 |
| S20 | RTR | toolchain | §22.1 の版指定の読み取り、baseline、match 規則、adapter（nodenv・rustup・mise・fvm・corepack）の detect / resolve / install / activate、setup と同じ経路の導入、`blocked: environment_mismatch` | A33・A42 のテスト | diff / sync（S21） | S10・S17 |
| S21 | RTR | `workers diff` / `sync` | §22.2 の差分項目と 5 分類、sync は自動導入できる差だけ、`doctor` へ含める | A44 のテスト | 導入・常駐（S22） | S20・S22 |
| S22 | RTR | 導入と常駐（macOS） | `workers add --install`（SSH bootstrap、版別ディレクトリ）、LaunchAgent / LaunchDaemon（`UserName`）、start / stop / drain / resume / `stop --cancel-running` | 1 台上での install → start → stop の統合テスト | upgrade・doctor（S23）・Mac 以外（S24） | S10 |
| S23 | RTR | upgrade・rollback・doctor | drain → 配置 → 互換検査 → 切替 → health → resume、失敗時の rollback、doctor（task-traffic-controller runtime の互換・policy digest・state path・Docker の resource domain・service 環境での profile 検査） | A05・A34・A36 のテスト | Mac 以外 | S22 |
| S24 | RTR | Linux の常駐と Colab の一時 worker | Linux の systemd user service（install / start / stop / upgrade）、Colab の一時 worker（`worker --ephemeral` をノートブックから起動、利用者が用意したトンネル経由で親が SSH、起動ごとに新しい identity、`workers forget` で消滅を宣言したときだけ interrupted に確定） | Linux（CI の ubuntu）での install → start → stop の統合テスト、ephemeral の identity と forget のテスト | macOS の常駐（S22）、トンネルの提供 | S22・S23 |
| S25 | RTR | 1 台上の試験ハーネスと障害注入 | 複数 worker（別 state dir）を 1 台で立てる、kill 点・ACK 消失・時計のずれの注入口、A 対応表のうち実機不要のものを自動試験にまとめる | 実機不要の A がすべて自動で緑 | 実機の試験（S26） | S15・S16・S18・S23 |
| S26 | RTR | 実機での受入と初回リリース | 手元の Mac ＋ 2 台目の Mac（＋ S24 の Linux・一時 worker）で A01・A02・A24・A26・A34・A35・A37・A45・A46、結果の報告、0.1.0 の `cargo publish` とタグ・Release | §19 の全 A が緑、§20 の完了報告 | なし（最終行） | 全行、実機（2 台目の Mac）の用意 |

## 現在の進捗と追加作業（2026-11-14）

この計画を進捗と追加変更の基準とする。完成に必要な不足が判明したら、理由・担当・検証条件・依存を追記する。実装済みの部品と、利用者が実際に使う経路への接続は区別し、接続と受入検証が終わるまで担当行を完了にしない。

- S0〜S14 と S17（PR #38、merge `9a9abc8`）は main に統合済み。S17 の検証対象 `279449b` はローカル実行 9 ジョブとリモート 9 ジョブが成功。S15 以降の接続と、追加検証で見つかった回復・テストの問題を別変更で進める。
- S15 は以下の小さい PR に分ける。S15a は S17 を起点に実装を先行できるが、公開・統合前に S17 の統合結果へ載せ直す。S15b の library 常駐基盤も実装・独立レビュー済みであり、別 process の bootstrap と CLI 接続は S15c で検証する。独立した修正・調査は担当を分け、設計と統合は親担当が行う。
- 検証はコミット済み HEAD に対する `local-ci run --include-disabled` を基準とし、対象 SHA、コードの失敗、環境エラー、未再現の範囲、リモート CI の結果を分ける。共有ツールが原因なら、そのツールで修正・検証・公開した後、RTR の利用版を別変更で追従する。

| 追加行 | 担当 | 必要になった理由・スコープ | 完了条件 | 依存・状態 |
|---|---|---|---|---|
| F1 | TTC | 同じ秒に開始した無関係な session leader が、孤児の候補から外れない。同 boot の単調時刻で子より古いと証明できる場合だけ除外する | 修正前に失敗する決定的な回帰テスト。unknown・equal・newer・別 boot・矛盾する証拠は保持。関連統合テストと CI、修正版の公開 | PR #29 統合、0.8.1 公開済み。ローカル実行ジョブ 5 件・リモート 5 件成功。ローカルの installer action 未再現は別記 |
| F2 | fast-worktree | 並行する Git worktree 登録が、別の登録途中の空 `commondir` を読む競合 | Git ネイティブの登録操作と関連する読取・削除を repository 共通の排他で保護。hook は抑止せず、同 repo への同期再入は lock を迂回せず即時エラーにして deadlock を防ぐ。並行登録・hook の実行と拒否・再入・失敗時の状態の回帰テストと CI、制限を明記した修正版の公開 | PR #11 統合、0.4.0 公開済み。ローカル 3 件・リモート 3 件成功。native Git hook 中は同 repo の操作が直列化される。依存準備と fast-worktree の設定 hook は排他外。独自の排他を使わない raw Git は保護範囲外 |
| F3 | RTR | 修正済み共有ツールと実行環境への追従 | TTC・fast-worktree の修正版を利用し、必要版・表示・テスト・文書を一致させる。local-ci の実行失敗を解消し、PR #38 をマージ | F1・F2 完了、PR #38 統合済み。追加の型付き回復 API は F8 で扱う |
| F4 | 検証環境 | CI の Rust 1.100 と古いローカル formatter・coverage ツールの差 | 同じ Rust 版と対応する coverage ツールで検証。新しい compiler で判明した未カバー分岐をテストし、ゲートを維持 | formatter と forecast の不足テストは PR #38 に反映。coverage ツールを CI と同版へ更新済み |
| F5 | RTR | protocol flood のテストが無受信 1 秒を全 request 到着とみなし、遅い RPC を取り残す | 全 caller の応答と受理数を照合して終了。RPC が同期の各 phase より先に timeout しない階層予算を持ち、遅延到着を sleep なしで再現する回帰テストを追加 | 関連 3 テストを各 20 回、計 60 回成功。HEAD `279449b` の全体ローカル実行ジョブ成功。Overloaded/Internal の分類と件数の検査は維持 |
| F6 | RTR CI | 同一ホストの mutation 4 shard がそれぞれ多数の Rust/Tokio integration test を並列起動し、基準実行の lease・deadline 検証を過負荷にする | mutation job に `RUST_TEST_THREADS=2` を設定し、4 shard・全テスト・全 mutant・判定基準は維持。cargo-schemata の基準実行と mutant 実行への継承を確認 | 既知失敗 5 件×4 プロセスと、関連 module 114 件×4 プロセス、計 456 件の module 検証が成功。HEAD `279449b` の全体ローカル実行 9 ジョブ成功。導入 action 未対応による incomplete は保持。リモート 9 ジョブも成功 |

| F7 | RTR test | 稼働中 SQLite DB と SHM を同一 process で raw open/close すると POSIX lock が解除され、別 writer 終了時の WAL 消失と readonly エラー 522 を起こす | 独立 process で DB/WAL/SHM の物理バイトを検査し、論理削除済みデータの漏洩検査も維持する。readonly 接続を弱めず、最小再現と WAL 回帰で検証 | 通常 cleanup の関連 3 件成功。追加の物理残存データ回帰を検証中。VACUUM による圧縮 snapshot は検査範囲が狭くなるため採用しない |

| F8 | TTC・RTR | 回復時の TTC `ProbeUnknown` を通常の失敗に数え、identity の不確定と他の失敗が混在した場合も分類できていなかった | 同じ ledger lock 内の解決判定を型付き API で返し、文字列解析・再 scan を避ける。ProbeUnknown だけの不確定は再確認し、混在原因は区別する。新 launch や偽 terminal を作らず回復する決定的テストと全 CI | TTC PR #30 の観測同期修正と PR #31 の API を統合し 0.8.2 公開済み。ローカル・リモート各 5 ジョブ成功、配布 8 asset の checksum 一致。RTR の関連 unit 31 件・実回復 1 件・pure mutation 2 caught 成功、公開版追従後の全体 CI 待ち。負荷下の既存時間依存テストは別件として残し、未証明の CPU guard 設定案は採用しない |

F2 は登録と hook を分離する案も調査したが、`core.hooksPath` の抑止は reference-transaction hook まで消し、`--no-checkout` でも同 hook が登録中に発火する。hook の契約を削らず単純な排他を採用するため、長い native hook の並行性と同期再入の制限を明示する方針へ変更した。Git の最低版を上げる案は採用しない。

### S15 の実装順

| 行 | スコープ | 完了条件 | 依存・状態 |
|---|---|---|---|
| S15a | 設定から Coordinator・Scheduler・Agent・Setup への checked 変換、検証済み worker identity と priority、`TaskSpec.setup` の転送、`Machine::with_setup` 必須の共通 factory | 非既定値・単位変換の境界・lease/budget/TTL・identity の不一致・旧 task JSON・setup payload 転送・factory の関連テスト | 実装・独立レビュー・関連検証済み。S17 統合後の載せ直しと全体 CI 待ち |
| S15b | 親の常駐 loop、local worker、hello 検証済み remote link、再接続、独立した回収 loop、単一の ResultStore、ローカル IPC | 呼出元 CLI 終了後も durable task が実行・回収され、再起動で同じ attempt を引き継ぐ。遅い回収が lease 更新を止めない | S15a |
| S15c | `init`・source/secret capture と durable `submit`・`ls`・`show`、run/task の問い合わせ API | 同時 CLI でも coordinator epoch が増殖しない。capture 失敗時は配置せず、完了済み run も正しく列挙する。JSON と stderr の契約を検証 | S15b、起動方式は S22 と整合 |
| S15d | `exec`・`wait`・`cancel`・Ctrl-C・明示 `retry`・local-only `--in-place` | exit code を透過し、停止不明を cancelled にしない。unknown の retry を拒否し、元 snapshot と履歴を保つ。in-place を remote に配置しない | S15c |
| S15e | `logs --follow`・再接続・`outputs`・retention の常駐 loop への接続 | 非 UTF-8・offset・切断再開、出力 path 衝突と部分失敗、回収失敗、実行中・回収中・保持対象を消さない GC を検証。自動削除に件数上限と時計跳躍の保護を付ける | S15d・S14 |
| S15f | 読み取り専用 `estimate`、配置理由、転送・setup・回収の実測値の接続 | estimate が queue・attempt・ledger 需要を増やさない。measured/fallback/unknown・TTL・priority の説明を保つ。S15 の受入条件を利用者経路で閉じる | S15b〜S15e、S11・S13 |

S15c の常駐入力は既存 user config の隣にある private `coordinator.json` に保存し、既存 bootstrap input を再利用する。`config.toml` の文法や暗黙の環境変数は追加しない。未初期化は明示エラーとし、`ls` / `show` は停止中でも読み取り専用 DB を使い、起動・epoch 更新・migration を行わない。`submit` だけが必要な owner 起動を要求する。

S15c の残件には、file/reader ベースのメモリ上限付き source 転送、active attempt のページ走査、requires/profile の配置条件、success/failure/always と glob・複数 path の出力計画を含める。現在の限定的な submit 経路でこれらを明示拒否していても、完成条件からは外さない。終了済み worker の spec・workspace は回収 ACK 前にも削除し、保持対象の結果・ログは残す。親側 source/Vault の retention は S15e で retry と回収条件を保護して接続する。

S15c は source/secret capture、CLI 終了後も動く local durable submit、bounded な offline query を実装・独立レビュー済み。blob/bundle は同じ検証済み FD から最大 64 KiB ずつ送り、転送停止時は owner を保持せず専用 lease で残る IO と cleanup を保護する。関連 7 件・strict clippy が成功。統合差分の全体検証をこれから行い、remote worker の lifecycle は未完了として残す。

S15d1 は `exec` / `wait` / `cancel` を先に接続する。受理前 Ctrl-C でも取消が失われないよう durable run cancellation intent を enqueue transaction で反映する。取消受理と停止確定、計算終了と回収成功・回収閉鎖を区別する。明示 retry は仕様どおり同 run の新 attempt とし、取消 intent を消して古い投入を復活させない generation authorization を次の slice で設計する。`--in-place` も必須残件として維持する。

S22 の native 所有確認は、Apple 標準 service の状態を読み取り専用で調べた実際の形式に基づいて実装する。print は安定 API ではないため未知・曖昧な形式は拒否する。GUI domain、system-user の名前と UID、実行 argv と managed file receipt、認証済み worker を照合し、直前にも再確認する。cooperative lifecycle lock による通常の取り違え防止を目的とし、OS に無い原子的 CAS や同 UID/root の任意差替え防止を保証したとは記載しない。テストは fake と private temp dir のみで、実 launchd の変更は行わない。

S16 は、S16a（入力確定済み FrozenPlan の再利用と安全な summary adapter）、S16b（S15c と共有する source 確定から公開 `ci plan` への接続）、その後の DAG 実行・動的 matrix・permit・artifact 集約の順に進める。S16a の時点で CLI の既定 snapshot や未実装状態は変えず、公開 `ci plan` 完成とは扱わない。

S16・S18〜S25 は元の分割表の必須範囲を維持する。新しい不足はこの追加表へ登録し、未実装の必須項目を「将来対応」として完成から外さない。

### 受入環境と S26 の進め方

- 当面の追加 worker と Linux 検証には Docker を使う。別 state dir と portool で分離し、複数 worker、親の再起動、接続断、ACK 消失、worker 消失、cold/warm と親単独との差を確認する。
- Docker 上の Linux worker と macOS ホストの結果は区別する。Docker を別の Mac 実機と記載せず、独立した Mac の sleep・boot・launchd・実機間転送など、代替できない項目は実機確認待ちとして A 対応表と最終報告へ残す。
- 公開する版には Docker で確認した範囲と未検証範囲を明記する。実機未確認の項目を、A01〜A49 全件確認済みと報告しない。

## A01〜A49 の対応

| A | 担当行 | 実機要 |
|---|---|---|
| A01〜A04 | S13（A01 は S26 でも） | A01・A02 |
| A05 | S9a・S23 | |
| A06〜A10 | S11（A10 は S16 も） | |
| A11〜A14 | S6・S7（A13 は S5 も） | |
| A15〜A17 | S17 | |
| A18〜A19 | S10・S12 | |
| A20〜A23 | S9a・S10・S12 | |
| A24 | S10（擬似）・S26 | ○ |
| A25 | S9a | |
| A26 | S9a・S26 | ○ |
| A27・A28・A38 | S16 | |
| A29 | S9b・S5 | |
| A30・A31 | S14 | |
| A32 | S19 | |
| A33 | S20・S8 | |
| A34・A35 | S22・S23・S26 | ○ |
| A36 | L1a・L1b・L2・L3・T1・S23 | |
| A37 | S26 | ○ |
| A39〜A41 | L2・L3・S18 | |
| A42 | S20 | |
| A43 | S8 | |
| A44 | S21 | |
| A45 | S9a・S10（OS 依存部分）・S24 | ○（S26 の Linux） |
| A46 | S24・S26 | ○ |
| A47 | S8 | |
| A48 | S7 | |
| A49 | S13（配置）・S27（設定） | |

## 確定事項（仕様 v1.2 に反映済み）

- Linux（x86_64・arm64）を正式対応する（systemd user service）。Google Colab は常駐なしの一時 worker とする。OS 依存部分（suspend を含む単調時計、boot ID、process identity、sleep 抑止）は platform module に macOS と Linux の実装を置き、S9a・S10 から両方で動かす。
- `.worktreeinclude` に一致する ignored ファイルは、自動で secret_files と同じ経路で送る。`secret_files` には追加分だけを書く。
- 追加実機が未準備のため、当面は Docker の複数 worker と Linux・障害注入で受入を進める。Mac 実機固有の未検証項目は対応表に残す。

### PR #42 の baseline fixture 補正

lease/lock 修正を接続した全体検証では通常テスト・coverage と mutation 3 shard が成功したが、
残る shard の unmutated baseline で setup 4件・hook 1件・captured submit 1件が失敗した。
setup の目的は共有・再構築・停止であり、lease expiry の試験ではないため、
全 fixture に与えていた人工的な3秒 leaseを外し production default を使う。
1秒の step timeout、本番 lease・更新間隔・CPU guard は変更しない。
hook の descendant 試験は実 shell と child の起動予算を10秒とし、実際の timeout・stderr・child停止の検査と30秒上限は維持する。
captured submit は fixture が所有する daemon の Tokio thread 数だけを制限し、
失敗時の bounded TTC/worker metadata を残す。実行中との推測に Ready を使わない。

fixture 補正 `72b3f09` で setup 全30件、hook 1件、captured submit 1件、
strict all-target/all-feature clippy と fmt が成功した。
以前の interrupted が lease expiry だったことは追跡されておらず、起動・スケジュール感度という推定と区別する。
補正後 HEAD の全体 CI とリモート確認が終わるまで PR #42 を統合しない。
