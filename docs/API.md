# clickhousex 公开 API

**版本 / 角色**：`clickhousex 0.1.0` · ClickHouse HTTP 客户端适配器（连接池 + 背压 + 批量写入 + 健康检查）

## 公开消费面

| 类型 / 函数 | 说明 |
| --- | --- |
| `ClickHouseConfig` / `ClickHouseConfigBuilder` | 配置：`from_env()` / `from_toml()` / `from_toml_file()` / `validate()` / `builder()` |
| `ClickHouseClient` | 单连接客户端：`new(config)`（同步，仅校验 + 构造，不发请求），请求串行化 |
| `ClickHousePool` | 并发受控连接池：`connect(config)` / `connect_from_env()`（异步，内部先 ping） |
| `ClickHousePoolStats` | 运行时计数：`total` / `open` / `in_flight` / `waiters` / `ok` / `error` / `closed` |
| `ClickHouseHealth` | 健康检查结果：`healthy` / `version` / `latency_ms` |
| `BatchInsertOptions` | 批量写入分块：`max_rows_per_chunk` / `max_bytes_per_chunk` / `batch_size` |
| `ClickHouseError` / `ClickHouseResult` | 统一错误类型（`#[non_exhaustive]`）与 `Result` 别名 |
| `parse_tab_separated_rows` / `chunk_ranges` / `build_query_url` | 纯函数工具（便于复用与测试） |
| `ENV_*` / `DEFAULT_*` 常量 | 环境变量名与默认值，避免硬编码字符串 |

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
| `stats()` / `is_closed()` / `close()` / `config()` | 运行时状态与生命周期；`close()` 拒绝新请求并等待在途操作结束后返回（幂等） |

## 最小用法

```rust,no_run
use clickhousex::{BatchInsertOptions, ClickHouseConfig, ClickHousePool};

# async fn demo() -> Result<(), Box<dyn std::error::Error>> {
let config = ClickHouseConfig::builder()
    .host("127.0.0.1")
    .http_port(8123)
    .database("default")
    .user("default")
    .build()?;

let pool = ClickHousePool::connect(config).await?;
pool.execute("CREATE TABLE IF NOT EXISTS demo (id UInt64) ENGINE = Memory").await?;

let rows = vec![serde_json::json!({ "id": 1 })];
pool.insert_batch("demo", &rows, BatchInsertOptions::default()).await?;
let selected = pool.query("SELECT id FROM demo").await?;
assert_eq!(selected.len(), 1);

pool.close().await?;
# Ok(())
# }
```

## 能力边界

- 只覆盖 ClickHouse HTTP 协议与连接生命周期；**不包含**领域模型、业务表结构、调度编排。
- 查询返回 `Vec<Vec<String>>`（`TabSeparated` 文本），不做类型反序列化；类型转换由调用方处理。
- `insert_batch` 的分块之间**不承诺原子性**：每个分块是一次独立 HTTP 请求，失败时已写入分块不回滚。
- 不提供隐式重试或自动重连；是否重试由调用方按 `ClickHouseError::is_retryable()` 决定
  （网络 / IO / 超时 / HTTP 429 / 5xx / ClickHouse `159` 可重试；配置、参数、序列化、认证与业务错误不可重试）。
- 不提供 native TCP 协议（9000 端口）支持，仅 HTTP（默认 8123）。
