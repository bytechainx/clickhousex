#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unreachable
)]
//! 基于本地一次性 HTTP 服务的数据面行为：请求次数、认证方式、错误映射与统计计数。
//!
//! 这些用例不依赖真实 ClickHouse，只驱动 HTTP 协议层面可验证的合同。

use std::time::Duration;

use clickhousex::{
    BatchInsertOptions, ClickHouseClient, ClickHouseConfig, ClickHouseError, ClickHousePool,
    ClickHousePoolStats,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

/// 一次性 HTTP 响应。
struct MockResponse {
    status: u16,
    reason: &'static str,
    body: String,
}

impl MockResponse {
    fn ok_text(body: impl Into<String>) -> Self {
        Self {
            status: 200,
            reason: "OK",
            body: body.into(),
        }
    }

    fn error(status: u16, reason: &'static str, body: impl Into<String>) -> Self {
        Self {
            status,
            reason,
            body: body.into(),
        }
    }
}

/// 起一个接收 `responses.len()` 次请求的服务，返回端口与收集到的请求文本。
async fn spawn_mock(responses: Vec<MockResponse>) -> (u16, tokio::task::JoinHandle<Vec<String>>) {
    let listener = TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("绑定临时端口");
    let port = listener.local_addr().expect("读取临时端口").port();
    let handle = tokio::spawn(async move {
        let mut received = Vec::new();
        for response in responses {
            let (mut stream, _) = listener.accept().await.expect("接受连接");
            let request = read_request(&mut stream).await;
            received.push(request);
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

fn config_for(port: u16) -> ClickHouseConfig {
    ClickHouseConfig::builder()
        .host("127.0.0.1")
        .http_port(port)
        .database("analytics")
        .timeout(Duration::from_secs(5))
        .acquire_timeout(Duration::from_secs(5))
        .max_in_flight(4)
        .build()
        .expect("测试配置必须有效")
}

fn rows(count: usize) -> Vec<serde_json::Value> {
    (0..count)
        .map(|index| serde_json::json!({ "id": index }))
        .collect()
}

#[tokio::test]
async fn ping_and_query_roundtrip_carry_database_and_sql() {
    let (port, server) = spawn_mock(vec![
        MockResponse::ok_text("1\n"),
        MockResponse::ok_text("1\tfoo\n2\tbar\n"),
    ])
    .await;

    let client = ClickHouseClient::new(config_for(port)).expect("构造客户端");
    client.ping().await.expect("ping 必须成功");

    let selected = client
        .query("SELECT id, name FROM demo")
        .await
        .expect("查询必须成功");
    assert_eq!(
        selected,
        vec![
            vec!["1".to_owned(), "foo".to_owned()],
            vec!["2".to_owned(), "bar".to_owned()],
        ]
    );

    let requests = server.await.expect("mock 服务任务");
    assert_eq!(requests.len(), 2);
    for request in &requests {
        assert!(
            request.starts_with("POST /?database=analytics"),
            "request={request}"
        );
        assert!(
            !request.to_ascii_lowercase().contains("authorization:"),
            "空密码不得发送认证头"
        );
    }
    assert!(requests[0].contains("SELECT 1"));
    assert!(requests[1].contains("SELECT id, name FROM demo"));

    let stats = client.stats();
    assert_eq!(stats.ok, 2);
    assert_eq!(stats.error, 0);
    assert_eq!(stats.in_flight, 0);
    assert_eq!(stats.waiters, 0);
    assert_eq!(stats.total, 1, "单连接客户端的并发额度为 1");
}

#[tokio::test]
async fn query_with_params_appends_param_pairs() {
    let (port, server) = spawn_mock(vec![MockResponse::ok_text("7\n")]).await;
    let client = ClickHouseClient::new(config_for(port)).expect("构造客户端");

    let selected = client
        .query_with_params("SELECT {id:UInt64}\n", &[("id", "7")])
        .await
        .expect("带参查询必须成功");
    assert_eq!(selected, vec![vec!["7".to_owned()]]);

    let requests = server.await.expect("mock 服务任务");
    assert!(
        requests[0].contains("param_id=7"),
        "request={}",
        requests[0]
    );
    assert!(requests[0].contains("SELECT {id:UInt64}"));
}

#[tokio::test]
async fn credentials_follow_configured_transport() {
    // 1) 密码非空 → Basic 认证头。
    let (port, server) = spawn_mock(vec![MockResponse::ok_text("1\n")]).await;
    let client = ClickHouseClient::new(
        ClickHouseConfig::builder()
            .host("127.0.0.1")
            .http_port(port)
            .user("writer")
            .password("s3cret")
            .build()
            .expect("配置有效"),
    )
    .expect("构造客户端");
    client.ping().await.expect("ping 必须成功");
    let requests = server.await.expect("mock 服务任务");
    assert!(requests[0]
        .to_ascii_lowercase()
        .contains("authorization: basic "));
    assert!(
        !requests[0].contains("s3cret"),
        "Basic 认证不得明文出现在请求行/头以外的位置"
    );

    // 2) auth_in_url → 凭据进入 URL 查询参数，且不发认证头。
    let (port, server) = spawn_mock(vec![MockResponse::ok_text("1\n")]).await;
    let client = ClickHouseClient::new(
        ClickHouseConfig::builder()
            .host("127.0.0.1")
            .http_port(port)
            .user("writer")
            .password("s3cret")
            .auth_in_url(true)
            .build()
            .expect("配置有效"),
    )
    .expect("构造客户端");
    client.ping().await.expect("ping 必须成功");
    let requests = server.await.expect("mock 服务任务");
    assert!(requests[0].contains("user=writer"));
    assert!(requests[0].contains("password=s3cret"));
    assert!(!requests[0].to_ascii_lowercase().contains("authorization:"));
}

#[tokio::test]
async fn insert_batch_issues_one_request_per_chunk() {
    let (port, server) = spawn_mock(vec![
        MockResponse::ok_text(""),
        MockResponse::ok_text(""),
        MockResponse::ok_text(""),
    ])
    .await;
    let client = ClickHouseClient::new(config_for(port)).expect("构造客户端");

    client
        .insert_batch(
            "demo",
            &rows(5),
            BatchInsertOptions::default().max_rows_per_chunk(2),
        )
        .await
        .expect("分块插入必须成功");

    let requests = server.await.expect("mock 服务任务");
    assert_eq!(requests.len(), 3, "5 行 / 每块 2 行 = 3 次独立请求");
    assert_eq!(
        requests[0]
            .lines()
            .filter(|line| line.starts_with("{\"id\":"))
            .count(),
        2
    );
    assert_eq!(
        requests[2]
            .lines()
            .filter(|line| line.starts_with("{\"id\":"))
            .count(),
        1
    );
    assert!(requests[0].contains("INSERT INTO demo FORMAT JSONEachRow"));
    assert_eq!(client.stats().ok, 3);
}

#[tokio::test]
async fn insert_batch_splits_by_byte_budget() {
    let (port, server) = spawn_mock(vec![
        MockResponse::ok_text(""),
        MockResponse::ok_text(""),
        MockResponse::ok_text(""),
    ])
    .await;
    let client = ClickHouseClient::new(config_for(port)).expect("构造客户端");

    // 每行 9 字节；20 字节预算下每块最多容纳 2 行。
    client
        .insert_batch(
            "demo",
            &rows(5),
            BatchInsertOptions::default()
                .max_rows_per_chunk(100)
                .max_bytes_per_chunk(20),
        )
        .await
        .expect("按字节分块必须成功");

    let requests = server.await.expect("mock 服务任务");
    assert_eq!(requests.len(), 3, "字节预算应产生更小的分块");
    assert_eq!(
        requests[2]
            .lines()
            .filter(|line| line.starts_with("{\"id\":"))
            .count(),
        1
    );
}

#[tokio::test]
async fn insert_json_each_row_rejects_bad_input_before_network() {
    let client = ClickHouseClient::new(config_for(1)).expect("构造客户端");

    let error = client
        .insert_json_each_row("1bad", &rows(1))
        .await
        .expect_err("非法表名必须拒绝");
    assert!(matches!(error, ClickHouseError::Invalid(_)));

    let error = client
        .insert_json_each_row("demo", &[serde_json::json!(["not", "an", "object"])])
        .await
        .expect_err("非 object 行必须拒绝");
    assert!(matches!(error, ClickHouseError::Serialization(_)));

    client
        .insert_json_each_row("demo", &[])
        .await
        .expect("空 rows 必须短路成功");
    assert_eq!(client.stats().ok, 0, "未发出的请求不计入统计");
}

#[tokio::test]
async fn http_status_mapping_decides_retryability() {
    let error_cases = [
        (
            MockResponse::error(429, "Too Many Requests", "Code: 159. DB::Exception"),
            true,
        ),
        (MockResponse::error(503, "Service Unavailable", ""), true),
        (
            MockResponse::error(500, "Internal Server Error", "boom"),
            true,
        ),
        (MockResponse::error(404, "Not Found", ""), false),
        (
            MockResponse::error(401, "Unauthorized", "secret payload"),
            false,
        ),
        (MockResponse::error(403, "Forbidden", "denied"), false),
        (
            MockResponse::error(400, "Bad Request", "Code: 60. DB::Exception: UNKNOWN_TABLE"),
            false,
        ),
        (
            MockResponse::error(
                400,
                "Bad Request",
                "Code: 57. DB::Exception: already exists",
            ),
            false,
        ),
    ];

    for (response, retryable) in error_cases {
        let status = response.status;
        let (port, server) = spawn_mock(vec![response]).await;
        let client = ClickHouseClient::new(config_for(port)).expect("构造客户端");
        let error = client.query("SELECT 1").await.expect_err("非 2xx 必须失败");
        assert_eq!(
            error.is_retryable(),
            retryable,
            "status={status} error={error:?}"
        );
        assert!(
            !error.to_string().contains("secret payload"),
            "错误消息不得回显响应正文: {error}"
        );
        assert!(
            !error.to_string().contains("SELECT 1"),
            "错误消息不得回显 SQL"
        );
        assert_eq!(client.stats().error, 1);
        server.await.expect("mock 服务任务");
    }
}

#[tokio::test]
async fn pool_connect_pings_and_reports_backpressure_limits() {
    let (port, server) = spawn_mock(vec![
        MockResponse::ok_text("1\n"),
        MockResponse::ok_text("1\n"),
        MockResponse::ok_text("24.3.1.1\n"),
    ])
    .await;
    let pool = ClickHousePool::connect(config_for(port))
        .await
        .expect("连接池必须建成");

    let stats: ClickHousePoolStats = pool.stats();
    assert_eq!(stats.total, 4);
    assert_eq!(stats.open, 4);
    assert_eq!(stats.in_flight, 0);
    assert_eq!(stats.waiters, 0);
    assert_eq!(stats.ok, 1, "connect 内部的 ping 计入成功数");
    assert!(!stats.closed);
    assert!(format!("{pool:?}").starts_with("ClickHousePool"));

    let health = pool.health_check().await;
    assert!(health.healthy, "mock 返回 1 时应健康");
    assert_eq!(health.version.as_deref(), Some("24.3.1.1"));
    assert!(health.latency_ms < 5_000);

    let cloned = pool.clone();
    assert_eq!(cloned.stats(), pool.stats(), "克隆共享同一份内部状态");

    pool.close().await.expect("关闭");
    assert!(pool.is_closed());
    assert_eq!(pool.stats().open, 0);

    let requests = server.await.expect("mock 服务任务");
    assert_eq!(
        requests.len(),
        3,
        "connect 的 ping + health_check 的 SELECT 1 / version"
    );
    assert!(requests[2].contains("SELECT version()"));
}
