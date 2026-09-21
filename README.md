# clickhousex

[![License](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](#license)

`clickhousex` 是一个零内部耦合的 **ClickHouse HTTP 客户端库**（默认端口 `8123`），
基于 `reqwest`（rustls）实现，提供连接复用、并发背压、查询、批量写入、认证、
超时与健康检查等基础设施原语。

它只负责 ClickHouse 协议与连接生命周期，**不包含**任何领域模型、业务表结构或
调度编排，因此可以零耦合地嵌入任意 Rust 服务。

- 零内部依赖，仅使用 crates.io 公开依赖
- `ClickHousePool`（并发受控）与 `ClickHouseClient`（单连接语义）两种入口
- 统一错误分类：`ClickHouseError::is_retryable()` 区分瞬时故障与永久故障
- 错误消息不回显服务端响应正文、SQL 片段或凭据

## 安装

本 crate **不发布到 crates.io**，通过 git 依赖引入：

```toml
[dependencies]
clickhousex = { git = "https://github.com/bytechainx/clickhousex" }
```

## 最小可运行示例

```rust,no_run
use clickhousex::{BatchInsertOptions, ClickHouseConfig, ClickHousePool};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 凭据只从环境变量或构建器注入，不会出现在 Debug / TOML 中。
    let config = ClickHouseConfig::builder()
        .host("127.0.0.1")
        .http_port(8123)
        .database("default")
        .user("default")
        .password(std::env::var("FOUNDATIONX_CLICKHOUSEX_PASSWORD").unwrap_or_default())
        .build()?;

    // 连接 → connect 内部已执行一次 ping
    let pool = ClickHousePool::connect(config).await?;
    pool.ping().await?;

    // DDL / DML
    pool.execute("CREATE TABLE IF NOT EXISTS demo (id UInt64) ENGINE = Memory").await?;

    // 批量写入（JSONEachRow，按行数/字节自动分块）
    let rows = vec![serde_json::json!({ "id": 1 }), serde_json::json!({ "id": 2 })];
    pool.insert_batch("demo", &rows, BatchInsertOptions::default()).await?;

    // 查询（TabSeparated → Vec<Vec<String>>）
    let selected = pool.query("SELECT id FROM demo ORDER BY id").await?;
    assert_eq!(selected.len(), 2);

    // 结构化健康检查
    let health = pool.health_check().await;
    println!("healthy={} version={:?} latency={}ms", health.healthy, health.version, health.latency_ms);

    pool.close().await?;
    Ok(())
}
```

需要单连接语义（请求串行化）时改用同步构造的 `ClickHouseClient`：

```rust,no_run
use clickhousex::{ClickHouseClient, ClickHouseConfig};

let client = ClickHouseClient::new(ClickHouseConfig::from_env()?)?;
client.ping().await?;
```

## 公开 API

| 类型 / 函数 | 说明 |
| --- | --- |
| `ClickHouseConfig` / `ClickHouseConfigBuilder` | 配置：`from_env()`、`from_toml()`、`from_toml_file()`、`validate()`、`builder()` |
| `ClickHouseClient` | 单连接客户端：`new(config)`（同步，仅校验 + 构造，不发请求） |
| `ClickHousePool` | 并发受控连接池：`connect(config)` / `connect_from_env()`（异步） |
| `ClickHousePoolStats` | 运行时计数：`total` / `open` / `in_flight` / `waiters` / `ok` / `error` / `closed` |
| `ClickHouseHealth` | 健康检查结果：`healthy` / `version` / `latency_ms` |
| `BatchInsertOptions` | 批量写入分块：`max_rows_per_chunk` / `max_bytes_per_chunk` / `batch_size` |
| `ClickHouseError` / `ClickHouseResult` | 统一错误类型与 `Result` 别名 |
| `parse_tab_separated_rows` / `chunk_ranges` / `build_query_url` | 纯函数工具（便于复用与测试） |

数据面方法（`ClickHouseClient` 与 `ClickHousePool` 均提供）：

| 方法 | 说明 |
| --- | --- |
| `ping()` | `SELECT 1`，成功返回 `Ok(())` |
| `health_check()` | 返回 `ClickHouseHealth`（失败不返回 `Err`） |
| `execute(sql)` | 执行 DDL / DML，不读取结果集 |
| `query(sql)` / `query_with_params(sql, params)` | 返回 `Vec<Vec<String>>`（`TabSeparated`） |
| `query_text(sql)` | 返回原始响应文本 |
| `insert_json_each_row(table, rows)` | 写入 `JSONEachRow`（每行必须是 JSON object） |
| `insert_batch(table, rows, options)` | 分块批量写入，每个分块一次独立 HTTP 请求 |
| `stats()` / `is_closed()` / `close()` / `config()` | 运行时状态与生命周期 |

## 配置项

环境变量前缀为 `FOUNDATIONX_CLICKHOUSEX_`；`from_toml()` 使用同名扁平字段
（`schema_version = 1` 必填，`timeout`/`acquire_timeout` 对应 `timeout_ms` /
`acquire_timeout_ms`）。

| 环境变量 | TOML 字段 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `FOUNDATIONX_CLICKHOUSEX_HOST` | `host` | `127.0.0.1` | 主机名或 IP |
| `FOUNDATIONX_CLICKHOUSEX_HTTP_PORT` | `http_port` | `8123` | HTTP 端口 |
| `FOUNDATIONX_CLICKHOUSEX_PORT` | `port` | — | 端口兼容别名；与主变量冲突时立即报错 |
| `FOUNDATIONX_CLICKHOUSEX_TLS` | `tls` | `false` | 是否 HTTPS；非 loopback 主机必须为 `true` |
| `FOUNDATIONX_CLICKHOUSEX_TLS_CA_FILE` | `tls_ca_file` | — | 追加 PEM CA（仅在 `tls = true` 时允许） |
| `FOUNDATIONX_CLICKHOUSEX_TLS_CLIENT_CERT_FILE` | `tls_client_cert_file` | — | mTLS 客户端证书（须与私钥同时配置） |
| `FOUNDATIONX_CLICKHOUSEX_TLS_CLIENT_KEY_FILE` | `tls_client_key_file` | — | mTLS 客户端私钥 |
| `FOUNDATIONX_CLICKHOUSEX_USER` | `user` | `default` | 用户名 |
| `FOUNDATIONX_CLICKHOUSEX_PASSWORD` | — | 空 | 密码；**不允许**写入 TOML，只能经环境变量/构建器注入 |
| `FOUNDATIONX_CLICKHOUSEX_DATABASE` | `database` | `default` | 默认数据库查询参数 |
| `FOUNDATIONX_CLICKHOUSEX_TIMEOUT_MS` | `timeout_ms` | `10000` | 单次请求超时（毫秒） |
| `FOUNDATIONX_CLICKHOUSEX_CONNECT_TIMEOUT_MS` | `connect_timeout_ms` | — | 连接（TCP/TLS 握手）超时；缺省时只受请求超时约束 |
| `FOUNDATIONX_CLICKHOUSEX_ACQUIRE_TIMEOUT_MS` | `acquire_timeout_ms` | `5000` | 等待 in-flight 许可超时（毫秒） |
| `FOUNDATIONX_CLICKHOUSEX_MAX_IDLE_PER_HOST` | `max_idle_per_host` | `8` | 每主机最大空闲连接数 |
| `FOUNDATIONX_CLICKHOUSEX_MAX_IN_FLIGHT` | `max_in_flight` | `64` | 全局并发上限（≥1） |
| `FOUNDATIONX_CLICKHOUSEX_AUTH_IN_URL` | `auth_in_url` | `false` | `true` 时用 URL 查询参数传递 `user`/`password`，否则用 `Authorization: Basic` |
| `FOUNDATIONX_CLICKHOUSEX_PLAIN_HTTP_HOSTS` | — | — | 逗号分隔的明文 HTTP 放行名单（仅内网/CI 显式放行） |

### 安全约定

- 密码仅能经 `FOUNDATIONX_CLICKHOUSEX_PASSWORD` 或 `ClickHouseConfigBuilder::password`
  注入；`Debug` 输出固定脱敏为 `***`，`from_toml()` 拒绝非空 `password`。
- 非 loopback 主机使用明文 HTTP 会在建立连接前被拒绝，除非显式配置放行名单。
- 错误映射只保留 HTTP 状态码与 ClickHouse 数字错误码（如 `server_code=60`），
  不回显响应正文、SQL 片段或凭据。
- 可重试判定：网络 / IO / 超时 / HTTP 429 / HTTP 5xx / ClickHouse `159` 为可重试；
  配置错误、参数错误、序列化错误、认证失败与业务错误为不可重试。

## 测试

```bash
cargo test
```

集成测试全部离线运行：纯函数与配置用例不触碰网络，HTTP 用例使用本地一次性
TCP 服务；失败路径统一用 `127.0.0.1:1`（必然拒绝连接）验证。

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or
[MIT license](LICENSE-MIT) at your option.

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in this crate by you, as defined in the Apache-2.0 license, shall
be dual licensed as above, without any additional terms or conditions.
