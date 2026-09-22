//! ClickHouse HTTP 客户端与连接池（默认端口 8123）。
//!
//! - [`ClickHouseClient`]：单连接语义，同步构造，内置并发额度为 1（请求串行化）。
//! - [`ClickHousePool`]：并发受控，异步 [`ClickHousePool::connect`] 构造，
//!   以 `max_in_flight` 作为全局背压额度。
//!
//! 两者共享同一套请求原语：`execute`（DDL/DML）、`query` /
//! `query_with_params`（返回行）、`insert_json_each_row` / `insert_batch`（写入）。

use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize};
use std::sync::Arc;

use serde_json::Value;
use tokio::sync::Semaphore;

use crate::config::ClickHouseConfig;
use crate::error::{ClickHouseError, ClickHouseResult};

/// 错误响应最多读取的字节数（超限部分直接丢弃，避免无界读取）。
const ERROR_RESPONSE_CAPTURE_LIMIT: usize = 4096;
/// 单连接客户端的并发额度。
const CLIENT_PERMITS: usize = 1;
/// SQL 标识符最大长度。
const MAX_IDENT_LEN: usize = 192;

/// 批量插入选项。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BatchInsertOptions {
    /// 每个 HTTP 请求最大行数（0 会被抬升为 1）。
    pub max_rows_per_chunk: usize,
    /// 每个 HTTP 请求最大字节数（0 表示不限制；单行超限时仍单独成块）。
    pub max_bytes_per_chunk: usize,
    /// 单次 [`ClickHousePool::insert_batch`] 允许的最大总行数（0 表示不限制）。
    pub batch_size: usize,
}

impl Default for BatchInsertOptions {
    fn default() -> Self {
        Self {
            max_rows_per_chunk: 1000,
            max_bytes_per_chunk: 0,
            batch_size: 0,
        }
    }
}

impl BatchInsertOptions {
    /// 设置每个 HTTP 请求的最大行数。
    #[must_use]
    pub fn max_rows_per_chunk(mut self, max_rows_per_chunk: usize) -> Self {
        self.max_rows_per_chunk = max_rows_per_chunk;
        self
    }

    /// 设置每个 HTTP 请求的最大字节数（0 表示不限制）。
    #[must_use]
    pub fn max_bytes_per_chunk(mut self, max_bytes_per_chunk: usize) -> Self {
        self.max_bytes_per_chunk = max_bytes_per_chunk;
        self
    }

    /// 设置单次批量插入允许的最大总行数（0 表示不限制）。
    #[must_use]
    pub fn batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }
}

/// 连接池 / 客户端运行时快照。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClickHousePoolStats {
    /// 并发额度上限（连接池为 `max_in_flight`，单连接客户端为 1）。
    pub total: usize,
    /// 当前可立即使用的额度。
    pub open: usize,
    /// 正在执行的请求数。
    pub in_flight: usize,
    /// 正在等待额度的请求数。
    pub waiters: usize,
    /// 累计成功完成的请求数。
    pub ok: u64,
    /// 累计失败的请求数。
    pub error: u64,
    /// 是否已关闭。
    pub closed: bool,
}

/// 健康检查结果。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClickHouseHealth {
    /// `SELECT 1` 是否成功。
    pub healthy: bool,
    /// 服务端版本（`SELECT version()`；失败时为 `None`）。
    pub version: Option<String>,
    /// `SELECT 1` 往返耗时（毫秒）。
    pub latency_ms: u64,
}

/// 单连接 ClickHouse HTTP 客户端。
///
/// 内部为 `Arc`，克隆廉价；请求串行化（并发额度 1）。
#[derive(Clone)]
pub struct ClickHouseClient {
    inner: Arc<Inner>,
}

/// 并发受控的 ClickHouse HTTP 连接池。
///
/// 复用同一个 `reqwest::Client` 连接池，并以信号量限制全局 in-flight 请求数。
/// 内部为 `Arc`，克隆廉价；[`ClickHousePool::close`] 后拒绝新请求。
#[derive(Clone)]
pub struct ClickHousePool {
    inner: Arc<Inner>,
}

impl ClickHouseClient {
    /// 同步构造客户端：仅做配置校验与 HTTP 客户端构造，**不发起网络请求**。
    ///
    /// TLS CA / 客户端证书文件在此时读取，因此缺文件会立即返回
    /// [`ClickHouseError::Config`]。
    pub fn new(config: ClickHouseConfig) -> ClickHouseResult<Self> {
        Ok(Self {
            inner: Arc::new(Inner::new(config, CLIENT_PERMITS)?),
        })
    }
}

impl ClickHousePool {
    /// 按配置建立连接池，并执行一次 `ping` 验证连通性。
    pub async fn connect(config: ClickHouseConfig) -> ClickHouseResult<Self> {
        let permits = config.max_in_flight;
        let build_config = config.clone();
        // 构造过程会同步读取 PEM 文件，放到阻塞线程池避免卡住异步运行时。
        let inner = tokio::task::spawn_blocking(move || Inner::new(build_config, permits))
            .await
            .map_err(|error| {
                ClickHouseError::Connection(format!("HTTP 客户端构建任务失败: {error}"))
            })??;
        let pool = Self {
            inner: Arc::new(inner),
        };
        pool.ping().await?;
        Ok(pool)
    }

    /// 从环境变量加载配置并建立连接池。
    pub async fn connect_from_env() -> ClickHouseResult<Self> {
        Self::connect(ClickHouseConfig::from_env()?).await
    }
}

/// 为 [`ClickHouseClient`] 与 [`ClickHousePool`] 生成同一套数据面 API。
macro_rules! impl_connection_api {
    ($ty:ident) => {
        impl $ty {
            /// 健康检查：执行 `SELECT 1`，成功返回 `Ok(())`。
            pub async fn ping(&self) -> ClickHouseResult<()> {
                self.inner.ping().await
            }

            /// 健康检查：返回结构化结果（不因失败返回 `Err`）。
            ///
            /// `healthy` 为 `SELECT 1` 是否成功；`version` 取自 `SELECT version()`，
            /// 失败时为 `None`；`latency_ms` 为 `SELECT 1` 的往返耗时。
            pub async fn health_check(&self) -> ClickHouseHealth {
                self.inner.health_check().await
            }

            /// 执行不返回结果集的 SQL（DDL / DML）。
            pub async fn execute(&self, sql: &str) -> ClickHouseResult<()> {
                let _ = self.inner.post_query(sql, None, &[]).await?;
                Ok(())
            }

            /// 执行查询并返回原始响应文本（ClickHouse 默认 `TabSeparated`）。
            ///
            /// 瞬时错误按 [`RetryConfig`](crate::RetryConfig) 自动重试；
            /// 永久错误立即上抛。
            pub async fn query_text(&self, sql: &str) -> ClickHouseResult<String> {
                self.inner.post_query_read(sql, &[]).await
            }

            /// 执行查询并按行返回结果（`TabSeparated`）。
            ///
            /// 重试语义同 [`query_text`](Self::query_text)。
            pub async fn query(&self, sql: &str) -> ClickHouseResult<Vec<Vec<String>>> {
                let text = self.inner.post_query_read(sql, &[]).await?;
                Ok(parse_tab_separated_rows(&text))
            }

            /// 带查询参数执行查询并按行返回结果。
            ///
            /// 参数以 `param_<name>=<value>` 形式传入，SQL 中用 `{name:Type}` 引用；
            /// 参数名必须是字母/数字/下划线组合。
            ///
            /// 重试语义同 [`query_text`](Self::query_text)。
            pub async fn query_with_params(
                &self,
                sql: &str,
                params: &[(&str, &str)],
            ) -> ClickHouseResult<Vec<Vec<String>>> {
                validate_params(params)?;
                let text = self.inner.post_query_read(sql, params).await?;
                Ok(parse_tab_separated_rows(&text))
            }

            /// 以 `JSONEachRow` 写入若干行；`rows` 中每个 `Value` 必须是 object。
            ///
            /// 空 `rows` 直接返回 `Ok(())`（不发起请求）。
            pub async fn insert_json_each_row(
                &self,
                table: &str,
                rows: &[Value],
            ) -> ClickHouseResult<()> {
                validate_ident(table)?;
                if rows.is_empty() {
                    return Ok(());
                }
                let body = join_lines(&encode_rows(rows)?);
                let sql = format!("INSERT INTO {table} FORMAT JSONEachRow");
                let _ = self
                    .inner
                    .post_query(&sql, Some(body.as_str()), &[])
                    .await?;
                Ok(())
            }

            /// 分块批量写入：按 `max_rows_per_chunk` / `max_bytes_per_chunk` 切分，
            /// 每个分块发出一次独立的 HTTP 请求。
            ///
            /// 空 `rows` 直接返回 `Ok(())`；总行数超过 `batch_size`（非 0 时）返回
            /// [`ClickHouseError::Invalid`]。
            pub async fn insert_batch(
                &self,
                table: &str,
                rows: &[Value],
                options: BatchInsertOptions,
            ) -> ClickHouseResult<()> {
                validate_ident(table)?;
                if rows.is_empty() {
                    return Ok(());
                }
                if options.batch_size > 0 && rows.len() > options.batch_size {
                    return Err(ClickHouseError::Invalid(format!(
                        "批量插入行数 {} 超过 batch_size={}",
                        rows.len(),
                        options.batch_size
                    )));
                }
                let encoded = encode_rows(rows)?;
                let sql = format!("INSERT INTO {table} FORMAT JSONEachRow");
                for (start, end) in chunk_ranges(encoded.len(), options.max_rows_per_chunk) {
                    for (chunk_start, chunk_end) in
                        split_by_bytes(&encoded, start, end, options.max_bytes_per_chunk)
                    {
                        let body = join_lines(&encoded[chunk_start..chunk_end]);
                        let _ = self
                            .inner
                            .post_query(&sql, Some(body.as_str()), &[])
                            .await?;
                    }
                }
                Ok(())
            }

            /// 当前统计快照。
            #[must_use]
            pub fn stats(&self) -> ClickHousePoolStats {
                self.inner.stats()
            }

            /// 是否已关闭。
            #[must_use]
            pub fn is_closed(&self) -> bool {
                self.inner.is_closed()
            }

            /// 关闭：拒绝后续请求，等待在途操作结束后返回（连接由 `Drop` 回收）。
            pub async fn close(&self) -> ClickHouseResult<()> {
                self.inner.close().await
            }

            /// 当前配置（密码已脱敏，仅 `Debug` 可见 `***`）。
            #[must_use]
            pub fn config(&self) -> &ClickHouseConfig {
                &self.inner.config
            }
        }
    };
}

impl_connection_api!(ClickHouseClient);
impl_connection_api!(ClickHousePool);

impl std::fmt::Debug for ClickHouseClient {
    /// 只打印句柄名与统计快照（不含配置与凭据）。
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClickHouseClient")
            .field("stats", &self.inner.stats())
            .finish()
    }
}

impl std::fmt::Debug for ClickHousePool {
    /// 只打印句柄名与统计快照（不含配置与凭据）。
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClickHousePool")
            .field("stats", &self.inner.stats())
            .finish()
    }
}

/// 共享的连接状态：HTTP 客户端、配置、背压信号量与统计计数。
struct Inner {
    http: reqwest::Client,
    config: ClickHouseConfig,
    sem: Arc<Semaphore>,
    total: usize,
    in_flight: AtomicUsize,
    waiters: AtomicUsize,
    ok: AtomicU64,
    error: AtomicU64,
    closed: AtomicBool,
}

/// 解析 ClickHouse 默认 `TabSeparated` 文本为行列。
///
/// 跳过空行；按 tab 分列。纯函数。
///
/// # Examples
///
/// ```
/// use clickhousex::parse_tab_separated_rows;
///
/// let rows = parse_tab_separated_rows("id\tname\n1\tfoo\n\n");
/// assert_eq!(
///     rows,
///     vec![
///         vec!["id".to_owned(), "name".to_owned()],
///         vec!["1".to_owned(), "foo".to_owned()],
///     ]
/// );
/// ```
#[must_use]
pub fn parse_tab_separated_rows(text: &str) -> Vec<Vec<String>> {
    let mut rows = Vec::new();
    for line in text.lines() {
        if line.is_empty() {
            continue;
        }
        rows.push(line.split('\t').map(str::to_owned).collect());
    }
    rows
}

/// 计算分块范围：`(start, end)` 半开区间。
///
/// `max_per_chunk` 为 0 时抬升为 1；`total` 为 0 时返回空。纯函数。
#[must_use]
pub fn chunk_ranges(total: usize, max_per_chunk: usize) -> Vec<(usize, usize)> {
    if total == 0 {
        return Vec::new();
    }
    let size = max_per_chunk.max(1);
    let mut ranges = Vec::new();
    let mut start = 0;
    while start < total {
        let end = (start + size).min(total);
        ranges.push((start, end));
        start = end;
    }
    ranges
}

/// 构造查询 URL（纯函数，不含网络 IO）。
///
/// 始终附带 `database` 查询参数；当 [`ClickHouseConfig::auth_in_url`] 为 `true`
/// 时附带 `user` / `password`；`params` 以 `param_<name>` 追加。
///
/// # Errors
///
/// 配置的 host/port 无法构成合法 URL，或参数名非法时返回
/// [`ClickHouseError::Config`] / [`ClickHouseError::Invalid`]。
pub fn build_query_url(
    config: &ClickHouseConfig,
    params: &[(&str, &str)],
) -> ClickHouseResult<String> {
    validate_params(params)?;
    Ok(build_url(config, params)?.to_string())
}

mod inner;
mod transport;

use transport::{
    build_url, encode_rows, join_lines, split_by_bytes, validate_ident, validate_params,
};

// 仅测试段使用（`mtLS` 与 HTTP 错误映射的用例）。
#[cfg(test)]
use transport::{build_http_client, map_http_error};

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    use reqwest::StatusCode;

    use super::transport::clickhouse_server_code;

    fn local_config(max_in_flight: usize) -> ClickHouseConfig {
        ClickHouseConfig::builder()
            .host("127.0.0.1")
            .http_port(1)
            .timeout(Duration::from_millis(200))
            .acquire_timeout(Duration::from_millis(200))
            .max_in_flight(max_in_flight)
            .build()
            .expect("测试配置必须有效")
    }

    #[test]
    fn batch_options_default_and_builders() {
        let options = BatchInsertOptions::default();
        assert_eq!(options.max_rows_per_chunk, 1000);
        assert_eq!(options.max_bytes_per_chunk, 0);
        assert_eq!(options.batch_size, 0);

        let tuned = BatchInsertOptions::default()
            .max_rows_per_chunk(2)
            .max_bytes_per_chunk(64)
            .batch_size(10);
        assert_eq!(tuned.max_rows_per_chunk, 2);
        assert_eq!(tuned.max_bytes_per_chunk, 64);
        assert_eq!(tuned.batch_size, 10);
    }

    #[test]
    fn chunk_ranges_concrete_sizes() {
        assert!(chunk_ranges(0, 10).is_empty());
        assert_eq!(chunk_ranges(5, 10), vec![(0, 5)]);
        assert_eq!(chunk_ranges(5, 2), vec![(0, 2), (2, 4), (4, 5)]);
        assert_eq!(chunk_ranges(3, 1), vec![(0, 1), (1, 2), (2, 3)]);
        assert_eq!(chunk_ranges(2, 0), vec![(0, 1), (1, 2)]);
        assert_eq!(chunk_ranges(7, 3), vec![(0, 3), (3, 6), (6, 7)]);
    }

    #[test]
    fn parse_tab_separated_rows_skips_blank_lines() {
        let rows = parse_tab_separated_rows("a\tb\n\nc\td\te\n");
        assert_eq!(
            rows,
            vec![
                vec!["a".to_owned(), "b".to_owned()],
                vec!["c".to_owned(), "d".to_owned(), "e".to_owned()]
            ]
        );
        assert!(parse_tab_separated_rows("").is_empty());
        assert!(parse_tab_separated_rows("\n\n\n").is_empty());
    }

    #[test]
    fn split_by_bytes_keeps_progress_for_oversized_line() {
        let encoded = vec!["a".repeat(10), "b".repeat(4), "c".repeat(4)];
        assert_eq!(split_by_bytes(&encoded, 0, 3, 0), vec![(0, 3)]);
        // 单行 11 字节已超过上限，仍须单独成块。
        assert_eq!(split_by_bytes(&encoded, 0, 3, 12), vec![(0, 1), (1, 3)]);
        assert_eq!(split_by_bytes(&encoded, 1, 3, 100), vec![(1, 3)]);
        assert_eq!(split_by_bytes(&encoded, 2, 2, 5), vec![(2, 2)]);
    }

    #[test]
    fn build_query_url_contains_database_and_params() {
        let config = local_config(4);
        let url = build_query_url(&config, &[("id", "42")]).expect("URL 构造");
        assert!(url.starts_with("http://127.0.0.1:1/?"), "url={url}");
        assert!(url.contains("database=default"));
        assert!(url.contains("param_id=42"));
        assert!(!url.contains("password"), "默认不把凭据放进 URL");

        let auth_in_url = ClickHouseConfig::builder()
            .host("127.0.0.1")
            .user("writer")
            .password("s3cret")
            .auth_in_url(true)
            .build()
            .expect("配置有效");
        let url = build_query_url(&auth_in_url, &[]).expect("URL 构造");
        assert!(url.contains("user=writer"));
        assert!(url.contains("password=s3cret"));

        assert!(build_query_url(&config, &[("bad-name", "1")]).is_err());
    }

    #[test]
    fn ident_and_param_validation() {
        assert!(validate_ident("infra_draft_smoke").is_ok());
        assert!(validate_ident("1bad").is_err());
        assert!(validate_ident("a;drop").is_err());
        assert!(validate_ident("").is_err());
        assert!(validate_ident(&"a".repeat(MAX_IDENT_LEN + 1)).is_err());
        assert!(validate_params(&[("id", "1")]).is_ok());
        assert!(validate_params(&[("1id", "1")]).is_err());
        assert!(validate_params(&[("", "1")]).is_err());
    }

    #[test]
    fn http_error_mapping_hides_response_body() {
        let secret = "SELECT private_column; payload=secret-value";
        let body = format!("Code: 60. DB::Exception: UNKNOWN_TABLE; {secret}");

        let missing = map_http_error(StatusCode::BAD_REQUEST, body.as_bytes());
        assert!(matches!(missing, ClickHouseError::Backend(_)));
        assert!(missing.to_string().contains("server_code=60"));
        assert!(
            !missing.to_string().contains(secret),
            "错误不得回显响应正文"
        );
        assert!(!missing.is_retryable());

        let conflict = map_http_error(StatusCode::BAD_REQUEST, b"Code: 57. DB::Exception: exists");
        assert!(!conflict.is_retryable());

        let unknown_db = map_http_error(StatusCode::BAD_REQUEST, b"Code: 81. DB::Exception");
        assert!(!unknown_db.is_retryable());
    }

    #[test]
    fn http_error_mapping_classifies_retryable_statuses() {
        let too_many = map_http_error(StatusCode::TOO_MANY_REQUESTS, b"");
        assert!(matches!(too_many, ClickHouseError::Unavailable(_)));
        assert!(too_many.is_retryable());

        let server_error = map_http_error(StatusCode::INTERNAL_SERVER_ERROR, b"boom");
        assert!(server_error.is_retryable());

        let unavailable = map_http_error(StatusCode::SERVICE_UNAVAILABLE, b"");
        assert!(unavailable.is_retryable());

        let server_timeout = map_http_error(StatusCode::BAD_REQUEST, b"Code: 159. DB::Exception");
        assert!(server_timeout.is_retryable());

        let unauthorized = map_http_error(StatusCode::UNAUTHORIZED, b"");
        assert!(!unauthorized.is_retryable());
        let forbidden = map_http_error(StatusCode::FORBIDDEN, b"denied");
        assert!(!forbidden.is_retryable());
    }

    #[test]
    fn server_code_parser_is_bounded_to_prefix() {
        assert_eq!(clickhouse_server_code(b"Code: 81. DB::Exception"), Some(81));
        assert_eq!(clickhouse_server_code(b"not a ClickHouse exception"), None);
        assert_eq!(clickhouse_server_code(&[0xff, 0xfe]), None);
        assert_eq!(clickhouse_server_code(b"Code: abc"), None);
    }

    #[test]
    fn encode_rows_rejects_non_object() {
        let error = encode_rows(&[serde_json::json!(["not", "an", "object"])])
            .expect_err("非 object 行必须拒绝");
        assert!(matches!(error, ClickHouseError::Serialization(_)));

        let encoded = encode_rows(&[serde_json::json!({ "a": 1 }), serde_json::json!({ "b": 2 })])
            .expect("object 行应可序列化");
        assert_eq!(encoded.len(), 2);
        assert_eq!(join_lines(&encoded), "{\"a\":1}\n{\"b\":2}\n");
    }

    #[test]
    fn mtls_reads_reject_missing_files_without_echoing_contents() {
        let config = ClickHouseConfig::builder()
            .tls_client_cert_file("/nonexistent/cert.pem")
            .tls_client_key_file("/nonexistent/key.pem")
            .build()
            .expect("配置有效");
        let error = build_http_client(&config).expect_err("缺失证书必须失败");
        assert!(error.to_string().contains("客户端证书"));
        assert!(!error.is_retryable());

        let ca_config = ClickHouseConfig::builder()
            .tls(true)
            .tls_ca_file("/nonexistent/ca.pem")
            .build()
            .expect("配置有效");
        let error = build_http_client(&ca_config).expect_err("缺失 CA 必须失败");
        assert!(error.to_string().contains("TLS CA"));
    }

    #[test]
    fn client_new_is_synchronous_and_rejects_zero_permits_config() {
        let client = ClickHouseClient::new(local_config(1)).expect("同步构造必须成功");
        assert!(!client.is_closed());
        assert_eq!(client.stats().total, 1);
        assert_eq!(client.stats().open, 1);
        assert_eq!(client.stats().in_flight, 0);
        assert_eq!(client.stats().waiters, 0);
        assert_eq!(client.stats().ok, 0);
        assert_eq!(client.stats().error, 0);
        assert_eq!(client.config().host, "127.0.0.1");

        let mut invalid = local_config(1);
        invalid.max_in_flight = 0;
        assert!(ClickHouseClient::new(invalid).is_err());
    }

    #[tokio::test]
    async fn closed_client_rejects_requests_and_stats_flip() {
        let client = ClickHouseClient::new(local_config(2)).expect("构造");
        client.close().await.expect("关闭");
        assert!(client.is_closed());
        assert!(client.stats().closed);
        assert_eq!(client.stats().open, 0);

        let error = client
            .execute("SELECT 1")
            .await
            .expect_err("关闭后必须拒绝");
        assert!(matches!(error, ClickHouseError::Closed(_)));
        assert!(!error.is_retryable());
    }

    #[tokio::test]
    async fn insert_batch_validates_ident_and_size_before_network() {
        let client = ClickHouseClient::new(local_config(1)).expect("构造");

        let error = client
            .insert_batch(
                "1bad",
                &[serde_json::json!({ "a": 1 })],
                BatchInsertOptions::default(),
            )
            .await
            .expect_err("非法表名必须拒绝");
        assert!(matches!(error, ClickHouseError::Invalid(_)));

        let rows = vec![serde_json::json!({ "a": 1 }); 3];
        let error = client
            .insert_batch(
                "valid_table",
                &rows,
                BatchInsertOptions::default().batch_size(2),
            )
            .await
            .expect_err("超过 batch_size 必须拒绝");
        assert!(matches!(error, ClickHouseError::Invalid(_)));

        // 空 rows 在发出任何请求之前短路返回。
        client
            .insert_batch("valid_table", &[], BatchInsertOptions::default())
            .await
            .expect("空 rows 必须成功");
        client
            .insert_json_each_row("valid_table", &[])
            .await
            .expect("空 rows 必须成功");
    }

    #[tokio::test]
    async fn unreachable_endpoint_maps_to_retryable_error() {
        let client = ClickHouseClient::new(local_config(1)).expect("构造");
        let error = client.ping().await.expect_err("127.0.0.1:1 必须失败");
        assert!(
            matches!(
                error,
                ClickHouseError::Connection(_) | ClickHouseError::Timeout(_)
            ),
            "error={error:?}"
        );
        assert!(error.is_retryable());
        // ping 走只读重试路径：1 次首发 + 3 次重试（默认 RetryConfig）= 4 次失败计数
        assert_eq!(client.stats().error, 4);
        assert_eq!(client.stats().ok, 0);
    }

    #[tokio::test]
    async fn health_check_reports_unhealthy_without_error() {
        let client = ClickHouseClient::new(local_config(1)).expect("构造");
        let health = client.health_check().await;
        assert!(!health.healthy);
        assert!(health.version.is_none());
    }
}
