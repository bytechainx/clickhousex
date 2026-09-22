# Changelog — clickhousex

本文件记录 `clickhousex` 的用户可见变更，遵循 [Keep a Changelog](https://keepachangelog.com/)
与 [Semantic Versioning](https://semver.org/)。

本仓库代码自 `xhyper.rs` 的 `crates/platform/drivers/clickhouse` 抽取而来（抽取时点为 `0.4.6`）。
该工程内的版本线不在本文件中延续，本仓库从 `0.1.0` 重新起算。

## [Unreleased]

## [0.1.3] - 2026-09-22

### 变更

- **内部结构改写（公开 API 与可观察契约均不变）**：按 `docs/module-rules.md` §5.5 的手法，把
  `src/config.rs` 的三块职责下沉为 `src/config/` 子模块 —— 链式构建器实现
  （`impl Default for ClickHouseConfigBuilder` + `impl ClickHouseConfigBuilder`）→
  `src/config/builder.rs`（144 行）；环境变量加载层（`from_env`、`apply_env_overrides` 与
  `env_non_empty` / `env_trimmed` / `env_parsed` / `env_bool` / `resolve_http_port`）→
  `src/config/envvars.rs`（135 行）；配置校验（`validate`）与明文 HTTP 放行判定
  （`host_is_loopback` / `host_allows_plain_http` / `host_allows_plain_http_with_list`）→
  `src/config/validate.rs`（85 行）。门面 `src/config.rs` 保留模块文档、全部 `ENV_*` / `DEFAULT_*`
  常量、`ClickHouseConfig` 与 `ClickHouseConfigBuilder` 的**类型定义与字段**、`Default` / `Debug`、
  `from_toml` / `from_toml_file`（连同 `de_millis` / `de_optional_millis` 两个 serde 解析器）、
  `builder` / `base_url` 与**原有内联测试**。
  搬走的 15 个构建方法与 `validate` / `from_env` 原本就是 `pub`，故**公开路径与签名一字未改**；
  只有被门面内联测试直接驱动的 `env_parsed` / `resolve_http_port` /
  `host_allows_plain_http_with_list` 提为 `pub(super)`，其余辅助（`apply_env_overrides`、
  `env_non_empty`、`env_trimmed`、`env_bool`、`host_is_loopback`、`host_allows_plain_http`）
  **保持私有**。`de_millis` / `de_optional_millis` 留在门面，是因为 `#[serde(deserialize_with = …)]`
  的路径按**结构体所在模块**解析，随结构体留在一起最稳。
  子模块名用 `envvars` 而非 `env`，避免 edition 2018 的 uniform path 遮蔽 `std::env`
  （与 `ossx` / `s3x` / `postgresx` 的处理一致）。
  `src/config.rs` 生产段 **569 → 250** 行。
  动机：`module-rules` 是元仓库必需检查，且它审计各仓**默认分支**，故当 `config.rs` 的生产段距
  `MR-STRUCT-007` 的 800 行 ERROR 阈值只剩 231 行时，任一仓的任意改动都可能卡住元仓库的全部 PR。
  属**纯搬移**（行多重集比对确认零代码行丢失，内联测试段除新增两条 `use` 外逐字节一致），
  87 项测试与 2 项 doctest 结果不变。

## [0.1.2] - 2026-09-22

### 变更

- **内部结构改写（公开 API 与可观察契约均不变）**：按 `docs/module-rules.md` §5.5 的手法，把
  `src/client.rs` 的 `impl Inner` 整块（构造、背压获取、关闭等待与查询收发共 10 个方法）下沉为
  `src/client/inner.rs`。门面 `src/client.rs` 保留模块文档、`BatchInsertOptions` /
  `ClickHousePoolStats` / `ClickHouseHealth` / `ClickHouseClient` / `ClickHousePool` 的定义、
  `Inner` 的**结构与字段**、`impl ClickHouseClient` / `impl ClickHousePool`、
  `impl_connection_api!` 宏及其两次展开、两个 `Debug` 实现、三个公开纯函数
  （`parse_tab_separated_rows` / `chunk_ranges` / `build_query_url`）与**原有内联测试**。
  `Inner` 是私有结构，其方法原本就不可见于 crate 之外，故**公开路径与签名一字未改**；
  门面与其宏实际调用的 7 个方法提为 `pub(super)`，只在子模块内使用的
  `ensure_open` / `acquire` / `post_query_inner` **保持私有**。
  子模块名 `inner` 取自它承载实现的那个类型名，与现有 `client/transport.rs` 并列。
  `src/client.rs` 生产段 **577 → 393** 行，`src/client/inner.rs` 为 204 行。
  动机：`module-rules` 是元仓库必需检查，且它审计各仓**默认分支**，故当 `client.rs` 的生产段距
  `MR-STRUCT-007` 的 800 行 ERROR 阈值只剩 223 行时，任一仓的任意改动都可能卡住元仓库的全部 PR。
  属**纯搬移**（行多重集比对确认零代码行丢失，内联测试段逐字节一致），87 项测试与 doctest 结果不变。

## [0.1.1] - 2026-09-22

### 新增

- 三类测试基线（特性 002）：`tests/tdd_contracts.rs`（逐公开入口的行为契约，头部
  `TDD-PROBE` 表登记「入口 / 变异 / 红 / 绿」）、`tests/sdd_spec.rs`（`docs/标准.md`
  全部 `##` 章节 1:1 对照的可执行断言）、`tests/aidd_boundary.rs`（AI 生成、人工复核
  后保留的边界用例）。三者全部离线运行，不依赖真实服务。
- live 真连服用例 `tests/live_clickhouse.rs`（全部 `#[ignore]`，默认不参与 CI；
  凭据只读 `FOUNDATIONX_CLICKHOUSEX_*`，流程为建连 → 探活 → 唯一化建表 → 批量写 →
  查询 → 删表并断言清理生效 → close）。
- `tests/close_drain.rs`：关闭排空契约（在途等待 + 幂等）的常驻回归用例。

### 修正

- **`close()` 现在等待在途操作结束**（`docs/标准.md` §4「`close()` 拒绝新请求并等待
  在途操作」）。此前 `close()` 只置关闭位并立即返回，未等待在途请求，与标准不符：
  修复方式为置关闭位后取回全部并发额度（在途操作各持 1 个、完成时释放，故取回全部
  等价于在途已排空），且不再关闭信号量（拒绝新请求由关闭位在 `ensure_open` 处完成）。
  等待上界由在途请求自身的超时 `config.timeout` 隐式给出；无在途操作时立即返回，
  重复调用仍成功（幂等）。属「实现向契约靠拢」的行为收紧，未变更任何公开签名。
  先红后绿证据见 PR 描述。

## [0.1.0] - 2026-09-21

### 新增

- 两种并发入口首次以独立 crate 形式提供：`ClickHousePool`（`max_in_flight` 信号量做
  全局背压）与 `ClickHouseClient`（单连接语义，请求串行化）。
- 数据面方法：`execute`（DDL / DML）、`query` / `query_with_params`（`TabSeparated` 返回行）、
  `query_text`（原始响应文本）、`insert_json_each_row` 与 `insert_batch`（按行数 / 字节分块）。
- `ClickHouseConfig` / `ClickHouseConfigBuilder`：`from_env()`（前缀
  `FOUNDATIONX_CLICKHOUSEX_`）、`from_toml()`、`from_toml_file()`、`validate()` 与 `builder()`。
- 认证：默认 `Authorization: Basic` 头；`auth_in_url` 打开后改用 URL 查询参数；
  支持追加 PEM CA 与 mTLS 客户端证书。
- 三段超时分离可配：单次请求超时、连接（TCP / TLS 握手）超时、等待 in-flight 许可超时。
- 错误分类 `ClickHouseError::is_retryable()`；运行时计数 `ClickHousePoolStats` 与结构化
  健康检查 `ClickHouseHealth`。
- 纯函数工具 `parse_tab_separated_rows` / `chunk_ranges` / `build_query_url` 首次公开，
  以及 `ENV_*` / `DEFAULT_*` 常量，避免调用方硬编码字符串。

### 变更

- **解耦**：错误模型从主工程的 `kernel` 错误体系下沉为 crate 内 `src/error.rs` 的
  `ClickHouseError`，`Cargo.toml` 不再声明任何内部 crate 依赖。
- **破坏性变更**：移除 `ClickHouseConfig::load_layered()`（主工程的分层 TOML 加载）；
  配置面只保留 `from_env()` / `from_toml()` / `from_toml_file()`，源模块的
  `from_toml_str()` 更名为 `from_toml()`。
- **破坏性变更**：移除源模块的 `time_storage`（精度声明与损失策略）：本仓库不再提供
  `DateTime64` 精度能力声明或 `PrecisionLossPolicy`，时间类型语义由调用方与表结构自行承担。
- 新增构建器方法 `connect_timeout` 与 `auth_in_url`：源模块只有单一 `timeout`，
  也没有「用 URL 查询参数传凭据」的开关。

### 说明

- 只覆盖 ClickHouse HTTP 协议（默认端口 8123）与连接生命周期；**不包含**领域模型、
  业务表结构、调度编排，也不提供 native TCP（9000）支持。
- 查询返回 `TabSeparated` 文本（`Vec<Vec<String>>`），**不做**类型反序列化，类型转换由
  调用方处理。
- `insert_batch` 的分块之间**不承诺原子性**：每个分块是一次独立 HTTP 请求，失败时已写入的
  分块不回滚。
- 不提供隐式重试或自动重连；是否重试由调用方按 `ClickHouseError::is_retryable()` 决定。
- 密码不是 TOML 字段：只能经环境变量或构建器注入，`Debug` 输出固定脱敏为 `***`。
- 本 crate **不发布到 crates.io**，仅以 GitHub 源码 / git 依赖形式复用，安装方式见 `README.md`。
