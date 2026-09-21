//! `clickhousex` — ClickHouse HTTP 适配器（生产客户端，默认端口 8123）。
//!
//! 只提供 ClickHouse HTTP 协议、连接、查询、写入与健康检查原语；不包含任何
//! 领域模型或业务表结构，零内部耦合。
//!
//! # 特性
//!
//! - **HTTP 客户端与连接池**：基于 `reqwest`（rustls）复用同一个连接池；
//!   [`ClickHousePool`] 以 `max_in_flight` 信号量做全局背压，[`ClickHouseClient`]
//!   为单连接语义（请求串行化）。
//! - **数据面**：[`ClickHousePool::execute`]（DDL/DML）、[`ClickHousePool::query`] /
//!   [`ClickHousePool::query_with_params`]（返回行）、[`ClickHousePool::insert_batch`]
//!   （按行数/字节分块写入 `JSONEachRow`）。
//! - **认证**：`Authorization: Basic` 头（默认）或 URL 查询参数
//!   （`ClickHouseConfig::auth_in_url`）；支持 PEM CA 与 mTLS 客户端证书。
//! - **超时**：请求超时、连接（TCP/TLS 握手）超时与获取 in-flight 许可的超时
//!   分离，均可配置。
//! - **错误分类**：[`ClickHouseError::is_retryable`] 区分可重试（网络、429、
//!   5xx、超时）与不可重试（配置、参数、远端业务错误）。错误消息不回显服务端
//!   响应正文、SQL 片段或凭据。
//! - **可观测**：[`ClickHousePool::stats`] 暴露 `open` / `in_flight` / `waiters` /
//!   `ok` / `error` 等计数；[`ClickHousePool::health_check`] 返回结构化健康结果。
//!
//! # 快速开始
//!
//! ```no_run
//! use clickhousex::{BatchInsertOptions, ClickHouseConfig, ClickHousePool};
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let config = ClickHouseConfig::builder()
//!         .host("127.0.0.1")
//!         .http_port(8123)
//!         .database("default")
//!         .user("default")
//!         .password(std::env::var("FOUNDATIONX_CLICKHOUSEX_PASSWORD").unwrap_or_default())
//!         .build()?;
//!
//!     let pool = ClickHousePool::connect(config).await?;
//!     pool.ping().await?;
//!     pool.execute("CREATE TABLE IF NOT EXISTS demo (id UInt64) ENGINE = Memory").await?;
//!
//!     let rows = vec![serde_json::json!({ "id": 1 }), serde_json::json!({ "id": 2 })];
//!     pool.insert_batch("demo", &rows, BatchInsertOptions::default()).await?;
//!
//!     let selected = pool.query("SELECT id FROM demo ORDER BY id").await?;
//!     assert_eq!(selected.len(), 2);
//!
//!     pool.close().await?;
//!     Ok(())
//! }
//! ```
//!
//! # 公开 API 一览
//!
//! | 类型 / 函数 | 用途 |
//! | --- | --- |
//! | [`ClickHouseConfig`] / [`ClickHouseConfigBuilder`] | 配置（`from_env` / `from_toml` / `validate` / `builder`） |
//! | [`ClickHouseClient`] | 单连接客户端（`new` 同步构造） |
//! | [`ClickHousePool`] | 并发受控连接池（`connect` 异步构造 + `ping` 校验） |
//! | [`ClickHousePoolStats`] | 运行时计数快照 |
//! | [`ClickHouseHealth`] | 健康检查结果 |
//! | [`BatchInsertOptions`] | 批量写入分块选项 |
//! | [`ClickHouseError`] / [`ClickHouseResult`] | 错误类型与 `Result` 别名 |
//! | [`parse_tab_separated_rows`] / [`chunk_ranges`] / [`build_query_url`] | 纯函数工具 |
//!
//! # 配置
//!
//! 环境变量前缀为 `FOUNDATIONX_CLICKHOUSEX_`，可用常量（如
//! [`ENV_HOST`](crate::ENV_HOST)）避免硬编码字符串。

#![deny(missing_docs)]
#![forbid(unsafe_code)]

mod client;
mod config;
mod error;

pub use client::{
    build_query_url, chunk_ranges, parse_tab_separated_rows, BatchInsertOptions, ClickHouseClient,
    ClickHouseHealth, ClickHousePool, ClickHousePoolStats,
};
pub use config::{
    ClickHouseConfig, ClickHouseConfigBuilder, DEFAULT_DATABASE, DEFAULT_HTTP_PORT, DEFAULT_USER,
    ENV_ACQUIRE_TIMEOUT_MS, ENV_AUTH_IN_URL, ENV_CONNECT_TIMEOUT_MS, ENV_DATABASE, ENV_HOST,
    ENV_HTTP_PORT, ENV_MAX_IDLE_PER_HOST, ENV_MAX_IN_FLIGHT, ENV_PASSWORD, ENV_PLAIN_HTTP_HOSTS,
    ENV_PORT, ENV_PREFIX, ENV_TIMEOUT_MS, ENV_TLS, ENV_TLS_CA_FILE, ENV_TLS_CLIENT_CERT_FILE,
    ENV_TLS_CLIENT_KEY_FILE, ENV_USER,
};
pub use error::{ClickHouseError, ClickHouseResult};
