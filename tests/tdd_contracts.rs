#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unreachable
)]
//! TDD 行为契约（特性 002）。
//!
//! 逐公开入口的「变异必红、原树必绿」对照：下表每行对应 `src/` 上的一处最小语义变异，
//! 变异副本上该行标注的红用例必须失败、本工作树上必须通过。变异与复现命令见 PR 描述。
//!
//! // TDD-PROBE: ClickHouseConfig::from_env | 变异：from_env 不再读取 ENV_DATABASE | 红=config_from_env_reads_prefixed_env | 绿=config_from_env_reads_prefixed_env
//! // TDD-PROBE: ClickHouseConfig::from_toml | 变异：from_toml 接受非空 password（去掉拒绝分支） | 红=config_from_toml_rejects_non_empty_password | 绿=config_from_toml_rejects_non_empty_password
//! // TDD-PROBE: ClickHouseConfig::validate | 变异：max_in_flight 下界由 1 放宽为 0 | 红=config_validate_rejects_boundary_violations | 绿=config_validate_rejects_boundary_violations
//! // TDD-PROBE: ClickHouseClient::new | 变异：单连接并发额度 CLIENT_PERMITS 由 1 改为 2 | 红=client_new_does_not_connect_and_has_single_permit | 绿=client_new_does_not_connect_and_has_single_permit
//! // TDD-PROBE: ClickHousePool::connect | 变异：connect 去掉返回前的 ping 冒烟 | 红=pool_connect_pings_before_returning | 绿=pool_connect_pings_before_returning
//! // TDD-PROBE: ClickHouseClient::ping | 变异：ping 去掉响应体必须为 "1" 的协议校验 | 红=client_ping_requires_protocol_body | 绿=client_ping_requires_protocol_body
//! // TDD-PROBE: ClickHouseClient::health_check | 变异：health_check 的 healthy 恒为 true | 红=health_check_reports_unreachable_as_unhealthy | 绿=health_check_reports_unreachable_as_unhealthy
//! // TDD-PROBE: ClickHouseClient::execute | 变异：execute 忽略入参 SQL，改写发送 "SELECT 1" | 红=execute_sends_the_given_sql | 绿=execute_sends_the_given_sql
//! // TDD-PROBE: ClickHouseClient::query | 变异：query 丢弃响应文本（解析结果恒为空） | 红=query_parses_tab_separated_rows | 绿=query_parses_tab_separated_rows
//! // TDD-PROBE: ClickHouseClient::insert_batch | 变异：insert_batch 忽略分块选项，整批单请求发送 | 红=insert_batch_chunks_by_rows | 绿=insert_batch_chunks_by_rows
//! // TDD-PROBE: ClickHouseError::is_retryable | 变异：Timeout 由可重试改为不可重试 | 红=is_retryable_classifies_transient_errors | 绿=is_retryable_classifies_transient_errors

use std::sync::Mutex;
use std::time::Duration;

use clickhousex::{
    BatchInsertOptions, ClickHouseClient, ClickHouseConfig, ClickHouseError, ClickHousePool,
    ENV_DATABASE, ENV_HOST, ENV_HTTP_PORT, ENV_PASSWORD, ENV_USER,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

/// 环境变量是进程级全局状态，串行化所有会读写 env 的用例。
static ENV_LOCK: Mutex<()> = Mutex::new(());

/// 清空本 crate 关心的环境变量，使判定不受宿主机环境影响。
fn clear_env() {
    for name in [
        ENV_HOST,
        ENV_HTTP_PORT,
        clickhousex::ENV_PORT,
        ENV_USER,
        ENV_PASSWORD,
        ENV_DATABASE,
    ] {
        std::env::remove_var(name);
    }
}

/// 指向必然拒绝连接的本机端口：用于「不触碰网络」的离线用例。
fn offline_config(max_in_flight: usize) -> ClickHouseConfig {
    local_config(1, max_in_flight)
}

fn local_config(port: u16, max_in_flight: usize) -> ClickHouseConfig {
    ClickHouseConfig::builder()
        .host("127.0.0.1")
        .http_port(port)
        .database("analytics")
        .timeout(Duration::from_secs(5))
        .acquire_timeout(Duration::from_secs(5))
        .max_in_flight(max_in_flight)
        .build()
        .expect("测试配置必须有效")
}

/// 一次性 HTTP 响应。
struct MockResponse {
    status: u16,
    reason: &'static str,
    body: String,
}

impl MockResponse {
    fn ok(body: impl Into<String>) -> Self {
        Self {
            status: 200,
            reason: "OK",
            body: body.into(),
        }
    }
}

/// 起一个接收 `responses.len()` 次请求的本地一次性 HTTP 服务。
async fn spawn_mock(responses: Vec<MockResponse>) -> (u16, tokio::task::JoinHandle<Vec<String>>) {
    let listener = TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("绑定临时端口");
    let port = listener.local_addr().expect("读取临时端口").port();
    let handle = tokio::spawn(async move {
        let mut received = Vec::new();
        for response in responses {
            let (mut stream, _) = listener.accept().await.expect("接受连接");
            received.push(read_request(&mut stream).await);
            let payload = format!(
                "HTTP/1.1 {} {}\r\nContent-Type: text/plain; charset=utf-8\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                response.status,
                response.reason,
                response.body.len(),
                response.body
            );
            stream.write_all(payload.as_bytes()).await.expect("写响应");
            stream.shutdown().await.expect("关闭连接");
        }
        received
    });
    (port, handle)
}

/// 读完一个完整 HTTP 请求（含 `Content-Length` 指定的正文）。
async fn read_request(stream: &mut TcpStream) -> String {
    let mut buffer = Vec::new();
    let mut chunk = [0_u8; 1024];
    loop {
        let read = tokio::time::timeout(Duration::from_secs(5), stream.read(&mut chunk))
            .await
            .expect("读取请求不得超时")
            .expect("读取请求");
        if read == 0 {
            break;
        }
        buffer.extend_from_slice(&chunk[..read]);
        let text = String::from_utf8_lossy(&buffer).into_owned();
        if let Some(header_end) = text.find("\r\n\r\n") {
            let content_length = text[..header_end]
                .lines()
                .find_map(|line| {
                    line.to_ascii_lowercase()
                        .strip_prefix("content-length:")
                        .map(|value| value.trim().parse::<usize>().unwrap_or(0))
                })
                .unwrap_or(0);
            if buffer.len() >= header_end + 4 + content_length {
                break;
            }
        }
    }
    String::from_utf8_lossy(&buffer).into_owned()
}

/// 收齐 mock 服务的结果；请求数少于预期时以超时暴露，而不是静默挂起。
async fn collect_requests(server: tokio::task::JoinHandle<Vec<String>>) -> Vec<String> {
    tokio::time::timeout(Duration::from_secs(5), server)
        .await
        .expect("mock 服务必须在超时内收齐预期请求数")
        .expect("mock 服务任务不应 panic")
}

/// 入口 1：`ClickHouseConfig::from_env` 读取契约前缀并做默认值兜底。
#[test]
fn config_from_env_reads_prefixed_env() {
    let guard = ENV_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    clear_env();
    std::env::set_var(ENV_HOST, "127.0.0.2");
    std::env::set_var(ENV_HTTP_PORT, "9440");
    std::env::set_var(ENV_USER, "writer");
    std::env::set_var(ENV_DATABASE, "analytics");
    std::env::set_var(ENV_PASSWORD, "pw-from-env");

    let config = ClickHouseConfig::from_env().expect("环境加载必须成功");
    let observed = (
        config.host.clone(),
        config.http_port,
        config.user.clone(),
        config.database.clone(),
        config.password.clone(),
    );

    clear_env();
    drop(guard);

    assert_eq!(observed.0, "127.0.0.2", "ENV_HOST 必须生效");
    assert_eq!(observed.1, 9440, "ENV_HTTP_PORT 必须生效");
    assert_eq!(observed.2, "writer", "ENV_USER 必须生效");
    assert_eq!(observed.3, "analytics", "ENV_DATABASE 必须生效");
    assert_eq!(observed.4, "pw-from-env", "密码只能经环境变量注入");
}

/// 入口 2：`ClickHouseConfig::from_toml` 拒绝非空 password，且不回显取值。
#[test]
fn config_from_toml_rejects_non_empty_password() {
    let error = ClickHouseConfig::from_toml(
        "schema_version = 1\nhost = \"127.0.0.1\"\npassword = \"hunter2\"\n",
    )
    .expect_err("TOML 中的非空 password 必须拒绝");
    assert!(error.to_string().contains("password"));
    assert!(
        !error.to_string().contains("hunter2"),
        "错误不得回显 password"
    );

    let parsed = ClickHouseConfig::from_toml("schema_version = 1\nhost = \"127.0.0.1\"\n")
        .expect("扁平字段应可解析");
    assert!(parsed.password.is_empty(), "TOML 不参与密码注入");
}

/// 入口 3：`ClickHouseConfig::validate` 在建立连接前 fail-fast。
#[test]
fn config_validate_rejects_boundary_violations() {
    let zero_in_flight = ClickHouseConfig {
        max_in_flight: 0,
        ..Default::default()
    };
    assert!(zero_in_flight.validate().is_err(), "0 并发必须拒绝");

    let zero_timeout = ClickHouseConfig {
        timeout: Duration::ZERO,
        ..Default::default()
    };
    assert!(zero_timeout.validate().is_err(), "0 超时必须拒绝");

    let remote_plain_http = ClickHouseConfig {
        host: "clickhouse.example.com".into(),
        ..Default::default()
    };
    assert!(
        remote_plain_http.validate().is_err(),
        "远程明文 HTTP 必须 fail-closed"
    );

    ClickHouseConfig::default()
        .validate()
        .expect("默认（loopback 明文）配置必须有效");
}

/// 入口 4：`ClickHouseClient::new` 同步构造、不联网、并发额度恒为 1。
#[test]
fn client_new_does_not_connect_and_has_single_permit() {
    let client = ClickHouseClient::new(offline_config(8)).expect("同步构造必须成功");
    let stats = client.stats();
    assert_eq!(stats.total, 1, "单连接客户端并发额度恒为 1");
    assert_eq!(stats.open, 1);
    assert_eq!(stats.in_flight, 0);
    assert_eq!(stats.ok, 0, "构造阶段不得发起请求");
    assert_eq!(stats.error, 0, "构造阶段不得发起请求");
    assert!(!client.is_closed());
}

/// 入口 5：`ClickHousePool::connect` 返回前完成一次 ping 冒烟。
#[tokio::test]
async fn pool_connect_pings_before_returning() {
    let (port, server) = spawn_mock(vec![MockResponse::ok("1\n")]).await;
    let pool = ClickHousePool::connect(local_config(port, 4))
        .await
        .expect("连接池必须建成");

    let stats = pool.stats();
    assert_eq!(stats.total, 4, "并发额度来自 max_in_flight");
    assert_eq!(stats.ok, 1, "connect 内部必须先 ping 成功");
    assert_eq!(stats.in_flight, 0);

    pool.close().await.expect("关闭");
    let requests = collect_requests(server).await;
    assert_eq!(requests.len(), 1);
    assert!(requests[0].contains("SELECT 1"));
}

/// 入口 6：`ClickHouseClient::ping` 校验响应体必须是协议约定的 `1`。
#[tokio::test]
async fn client_ping_requires_protocol_body() {
    let (port, server) = spawn_mock(vec![MockResponse::ok("1\n")]).await;
    let client = ClickHouseClient::new(local_config(port, 1)).expect("构造客户端");
    client.ping().await.expect("响应为 1 时 ping 必须成功");
    let requests = collect_requests(server).await;
    assert_eq!(requests.len(), 1);
    assert!(requests[0].contains("SELECT 1"));
    assert_eq!(client.stats().ok, 1);

    let (port, server) = spawn_mock(vec![MockResponse::ok("0\n")]).await;
    let client = ClickHouseClient::new(local_config(port, 1)).expect("构造客户端");
    let error = client.ping().await.expect_err("响应非 1 必须失败");
    assert!(
        matches!(error, ClickHouseError::Backend(_)),
        "协议不符应映射为 Backend，实际 {error:?}"
    );
    assert!(!error.is_retryable());
    let _ = collect_requests(server).await;
}

/// 入口 7：`ClickHouseClient::health_check` 失败不返回 `Err`，以 `healthy = false` 表达。
#[tokio::test]
async fn health_check_reports_unreachable_as_unhealthy() {
    let client = ClickHouseClient::new(offline_config(1)).expect("构造客户端");
    let health = client.health_check().await;
    assert!(!health.healthy, "不可达时 healthy 必须为 false");
    assert!(health.version.is_none(), "不健康时不得报告版本");
}

/// 入口 8：`ClickHouseClient::execute` 原样发送调用方 SQL，且不计入结果集。
#[tokio::test]
async fn execute_sends_the_given_sql() {
    let (port, server) = spawn_mock(vec![MockResponse::ok("")]).await;
    let client = ClickHouseClient::new(local_config(port, 1)).expect("构造客户端");

    client
        .execute("CREATE TABLE demo (id UInt64) ENGINE = Memory")
        .await
        .expect("DDL 必须成功");

    let requests = collect_requests(server).await;
    assert_eq!(requests.len(), 1);
    assert!(
        requests[0].contains("CREATE TABLE demo (id UInt64) ENGINE = Memory"),
        "请求体必须原样携带调用方 SQL: {}",
        requests[0]
    );
    assert_eq!(client.stats().ok, 1);
}

/// 入口 9：`ClickHouseClient::query` 解析 `TabSeparated` 行列。
#[tokio::test]
async fn query_parses_tab_separated_rows() {
    let (port, server) = spawn_mock(vec![MockResponse::ok("1\talpha\n2\tbeta\n")]).await;
    let client = ClickHouseClient::new(local_config(port, 1)).expect("构造客户端");

    let rows = client
        .query("SELECT id, name FROM demo")
        .await
        .expect("查询必须成功");

    assert_eq!(
        rows,
        vec![
            vec!["1".to_owned(), "alpha".to_owned()],
            vec!["2".to_owned(), "beta".to_owned()],
        ]
    );
    let requests = collect_requests(server).await;
    assert!(requests[0].contains("SELECT id, name FROM demo"));
}

/// 入口 10：`ClickHouseClient::insert_batch` 按 `max_rows_per_chunk` 分块独立请求。
#[tokio::test]
async fn insert_batch_chunks_by_rows() {
    let (port, server) = spawn_mock(vec![
        MockResponse::ok(""),
        MockResponse::ok(""),
        MockResponse::ok(""),
    ])
    .await;
    let client = ClickHouseClient::new(local_config(port, 1)).expect("构造客户端");
    let rows: Vec<serde_json::Value> = (0..5)
        .map(|index| serde_json::json!({ "id": index }))
        .collect();

    client
        .insert_batch(
            "demo",
            &rows,
            BatchInsertOptions::default().max_rows_per_chunk(2),
        )
        .await
        .expect("分块写入必须成功");

    let requests = collect_requests(server).await;
    assert_eq!(requests.len(), 3, "5 行 / 每块 2 行 = 3 次独立请求");
    assert_eq!(client.stats().ok, 3);
}

/// 入口 11：`ClickHouseError::is_retryable` 只把瞬时错误归为可重试。
#[test]
fn is_retryable_classifies_transient_errors() {
    let retryable = [
        ClickHouseError::Connection("x".into()),
        ClickHouseError::Unavailable("x".into()),
        ClickHouseError::Timeout("x".into()),
        ClickHouseError::Io(std::io::Error::other("x")),
    ];
    for error in retryable {
        assert!(error.is_retryable(), "{error:?} 应可重试");
    }

    let permanent = [
        ClickHouseError::Config("x".into()),
        ClickHouseError::Backend("x".into()),
        ClickHouseError::Serialization("x".into()),
        ClickHouseError::Invalid("x".into()),
        ClickHouseError::Closed("x".into()),
        ClickHouseError::Unsupported("x".into()),
    ];
    for error in permanent {
        assert!(!error.is_retryable(), "{error:?} 不应可重试");
    }
}
