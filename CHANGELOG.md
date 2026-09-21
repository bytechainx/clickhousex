# Changelog — clickhousex

本文件记录 `clickhousex` 的用户可见变更，遵循 [Keep a Changelog](https://keepachangelog.com/)
与 [Semantic Versioning](https://semver.org/)。

本仓库代码自 `xhyper.rs` 的 `crates/platform/drivers/clickhouse` 抽取而来（抽取时点为 `0.4.6`）。
该工程内的版本线不在本文件中延续，本仓库从 `0.1.0` 重新起算。

## [Unreleased]

### 新增

- 三类测试基线（特性 002）：`tests/tdd_contracts.rs`（逐公开入口的行为契约，头部
  `TDD-PROBE` 表登记「入口 / 变异 / 红 / 绿」）、`tests/sdd_spec.rs`（`docs/标准.md`
  全部 `##` 章节 1:1 对照的可执行断言）、`tests/aidd_boundary.rs`（AI 生成、人工复核
  后保留的边界用例）。三者全部离线运行，不依赖真实服务。
- live 真连服用例 `tests/live_clickhouse.rs`（全部 `#[ignore]`，默认不参与 CI；
  凭据只读 `FOUNDATIONX_CLICKHOUSEX_*`，流程为建连 → 探活 → 唯一化建表 → 批量写 →
  查询 → 删表并断言清理生效 → close）。

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
