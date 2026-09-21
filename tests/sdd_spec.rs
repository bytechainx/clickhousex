#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unreachable
)]
//! SDD 规格对照（特性 002）：把 `docs/标准.md` 的章节条款转成可执行断言。
//!
//! 章节与断言一一对应，`SPEC-MAP` 表即映射清单。
//!
//! // SPEC-MAP: S-1 | 1. 定位 | assert_positioning
//! // SPEC-MAP: S-2 | 2. 字段治理 | assert_field_governance
//! // SPEC-MAP: S-3 | 3. 安全约定 | assert_security_conventions
//! // SPEC-MAP: S-4 | 4. 失败与并发 | assert_failure_and_concurrency
//! // SPEC-MAP: S-5 | 5. 验收 | assert_acceptance

use std::sync::Mutex;
use std::time::Duration;

use clickhousex::{
    build_query_url, chunk_ranges, parse_tab_separated_rows, ClickHouseClient, ClickHouseConfig,
    ClickHouseConfigBuilder, ClickHouseError, DEFAULT_DATABASE, DEFAULT_HTTP_PORT, DEFAULT_USER,
    ENV_DATABASE, ENV_HOST, ENV_HTTP_PORT, ENV_PASSWORD, ENV_PORT, ENV_PREFIX, ENV_USER,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

/// 环境变量是进程级全局状态，串行化所有会读写 env 的用例。
static ENV_LOCK: Mutex<()> = Mutex::new(());

/// 清空本 crate 关心的环境变量。
fn clear_env() {
    for name in [
        ENV_HOST,
        ENV_HTTP_PORT,
        ENV_PORT,
        ENV_USER,
        ENV_PASSWORD,
        ENV_DATABASE,
    ] {
        std::env::remove_var(name);
    }
}

/// 远程 TLS 端点（本地辅助，避免在断言中重复字面量）。
fn remote_with_tls() -> ClickHouseConfig {
    ClickHouseConfig {
        host: "clickhouse.example.com".into(),
        http_port: 8443,
        tls: true,
        ..Default::default()
    }
}

/// 指向本地一次性桩的配置。
fn local_config(port: u16) -> ClickHouseConfig {
    ClickHouseConfig::builder()
        .host("127.0.0.1")
        .http_port(port)
        .database("analytics")
        .timeout(Duration::from_secs(5))
        .acquire_timeout(Duration::from_secs(5))
        .max_in_flight(1)
        .build()
        .expect("测试配置必须有效")
}

/// 一次性本地 HTTP 桩：对 1 次请求返回指定状态码与正文。
async fn spawn_error_mock(
    status: u16,
    reason: &'static str,
    body: &'static str,
) -> (u16, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("绑定临时端口");
    let port = listener.local_addr().expect("读取临时端口").port();
    let handle = tokio::spawn(async move {
        if let Ok((mut stream, _)) = listener.accept().await {
            let mut buffer = [0_u8; 2048];
            let _ = stream.read(&mut buffer).await;
            let response = format!(
                "HTTP/1.1 {status} {reason}\r\nContent-Type: text/plain; charset=utf-8\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            let _ = stream.write_all(response.as_bytes()).await;
            let _ = stream.shutdown().await;
        }
    });
    (port, handle)
}

/// S-1：只覆盖 HTTP 协议与连接生命周期；两种并发入口语义分明，零领域模型。
#[test]
fn assert_positioning() {
    // 默认端点即 HTTP 8123（不提供 native TCP 9000）。
    let config = ClickHouseConfig::default();
    assert_eq!(config.base_url(), "http://127.0.0.1:8123");
    assert_eq!(config.http_port, DEFAULT_HTTP_PORT);
    assert_eq!(config.user, DEFAULT_USER);
    assert_eq!(config.database, DEFAULT_DATABASE);

    // 单连接客户端：请求串行化（并发额度恒为 1），与 max_in_flight 无关。
    let single = ClickHouseClient::new(config.clone()).expect("同步构造");
    assert_eq!(single.stats().total, 1);

    let tuned = ClickHouseConfigBuilder::from_config(config)
        .max_in_flight(8)
        .build()
        .expect("配置有效");
    assert_eq!(tuned.max_in_flight, 8, "池的背压额度来自 max_in_flight");
    let still_single = ClickHouseClient::new(tuned).expect("构造");
    assert_eq!(
        still_single.stats().total,
        1,
        "客户端不因 max_in_flight 而并发化"
    );
}

/// S-2：环境变量前缀统一；TOML 扁平 + `schema_version` + 拒未知键；端口别名冲突 fail-closed。
#[test]
fn assert_field_governance() {
    assert_eq!(ENV_PREFIX, "FOUNDATIONX_CLICKHOUSEX_");
    for name in [
        ENV_HOST,
        ENV_HTTP_PORT,
        ENV_USER,
        ENV_PASSWORD,
        ENV_DATABASE,
    ] {
        assert!(name.starts_with(ENV_PREFIX), "{name} 必须带统一前缀");
    }

    // 毫秒整数映射到 Duration。
    let parsed = ClickHouseConfig::from_toml(
        "schema_version = 1\ntimeout_ms = 1500\nacquire_timeout_ms = 250\n",
    )
    .expect("扁平 TOML 应可解析");
    assert_eq!(parsed.timeout, Duration::from_millis(1500));
    assert_eq!(parsed.acquire_timeout, Duration::from_millis(250));

    // schema_version 必填且只支持 1；未知键拒绝。
    assert!(ClickHouseConfig::from_toml("host = \"127.0.0.1\"\n").is_err());
    assert!(ClickHouseConfig::from_toml("schema_version = 99\n").is_err());
    assert!(ClickHouseConfig::from_toml("schema_version = 1\nsink_id = \"x\"\n").is_err());

    // password 不是 TOML 字段：非空值直接拒绝；Debug 固定脱敏。
    assert!(ClickHouseConfig::from_toml("schema_version = 1\npassword = \"p\"\n").is_err());
    let redacted = ClickHouseConfig::builder()
        .password("secret-value")
        .build()
        .expect("配置有效");
    let rendered = format!("{redacted:?}");
    assert!(rendered.contains("***"));
    assert!(!rendered.contains("secret-value"));

    // 端口协议：两变量取值冲突时立即报错，而非静默取其一，且不回显取值。
    let guard = ENV_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    clear_env();
    std::env::set_var(ENV_HTTP_PORT, "8123");
    std::env::set_var(ENV_PORT, "8443");
    let message = ClickHouseConfig::from_env()
        .expect_err("端口冲突必须拒绝")
        .to_string();
    clear_env();
    drop(guard);
    assert!(message.contains("冲突"), "{message}");
    assert!(!message.contains("8443"), "错误不得回显取值");
}

/// S-3：远程明文 HTTP fail-closed；错误不回显正文；TLS 与 mTLS 约束成对生效。
#[tokio::test]
async fn assert_security_conventions() {
    // 非 loopback 明文 HTTP 在建立连接前被拒绝。
    let remote = ClickHouseConfig {
        host: "clickhouse.example.com".into(),
        ..Default::default()
    };
    let error = remote.validate().expect_err("远程明文必须拒绝");
    assert!(error.to_string().contains("HTTPS"));

    // 错误只保留 HTTP 状态码与 ClickHouse 数字错误码，不回显正文 / SQL / 凭据。
    // 这里走真实映射路径（本地一次性 HTTP 桩返回 400 + 夹带正文），而非手工构造错误值。
    let leaked_body =
        "Code: 60. DB::Exception: UNKNOWN_TABLE; payload=secret-value SELECT private_column";
    let (port, server) = spawn_error_mock(400, "Bad Request", leaked_body).await;
    let client = ClickHouseClient::new(local_config(port)).expect("构造客户端");
    let error = client
        .query("SELECT private_column FROM missing")
        .await
        .expect_err("非 2xx 必须失败");
    let message = error.to_string();
    assert!(
        matches!(error, ClickHouseError::Backend(_)),
        "业务错误应映射为 Backend，实际 {error:?}"
    );
    assert!(
        message.contains("server_code=60"),
        "错误应保留 ClickHouse 数字错误码: {message}"
    );
    assert!(
        !message.contains("secret-value"),
        "错误不得回显响应正文: {message}"
    );
    assert!(
        !message.contains("private_column"),
        "错误不得回显 SQL 片段: {message}"
    );
    assert!(!error.is_retryable());
    server.await.expect("mock 服务任务");

    // 自定义 CA 仅在 tls = true 时允许；mTLS 证书与私钥必须成对提供。
    let ca_without_tls = ClickHouseConfig {
        tls_ca_file: Some("/tmp/ca.pem".into()),
        ..Default::default()
    };
    assert!(ca_without_tls.validate().is_err());
    let cert_only = ClickHouseConfig {
        tls_client_cert_file: Some("/tmp/cert.pem".into()),
        ..Default::default()
    };
    assert!(cert_only.validate().is_err());
    let key_only = ClickHouseConfig {
        tls_client_key_file: Some("/tmp/key.pem".into()),
        ..Default::default()
    };
    assert!(key_only.validate().is_err());

    // 默认（auth_in_url = false）不把凭据放进 URL。
    let url = build_query_url(&remote_with_tls(), &[]).expect("URL 构造");
    assert!(!url.contains("password"), "url={url}");
    assert!(url.starts_with("https://clickhouse.example.com:8443/"));
}

/// S-4：关闭拒绝新请求并翻转统计；健康检查失败不返回 `Err`；可重试判定统一入口。
#[tokio::test]
async fn assert_failure_and_concurrency() {
    let config = ClickHouseConfig {
        host: "127.0.0.1".into(),
        http_port: 1,
        timeout: Duration::from_millis(200),
        acquire_timeout: Duration::from_millis(200),
        ..Default::default()
    };
    let client = ClickHouseClient::new(config).expect("构造客户端");

    // 关闭：拒绝新请求，统计翻转。
    client.close().await.expect("关闭");
    assert!(client.is_closed());
    let stats = client.stats();
    assert!(stats.closed);
    assert_eq!(stats.open, 0, "关闭后不再对外提供额度");
    let error = client
        .execute("SELECT 1")
        .await
        .expect_err("关闭后必须拒绝新请求");
    assert!(matches!(error, ClickHouseError::Closed(_)));
    assert!(!error.is_retryable(), "已关闭资源不可重试");

    // health_check 失败不返回 Err，以 healthy = false 表达。
    let health = client.health_check().await;
    assert!(!health.healthy);
    assert!(health.version.is_none());

    // 可重试判定统一走 is_retryable：网络 / 超时 / IO / 429 / 5xx / 159 可重试。
    for retryable in [
        ClickHouseError::Connection("x".into()),
        ClickHouseError::Timeout("x".into()),
        ClickHouseError::Unavailable("x".into()),
        ClickHouseError::Io(std::io::Error::other("x")),
    ] {
        assert!(retryable.is_retryable(), "{retryable:?} 应可重试");
    }
    for permanent in [
        ClickHouseError::Config("x".into()),
        ClickHouseError::Invalid("x".into()),
        ClickHouseError::Serialization("x".into()),
        ClickHouseError::Backend("x".into()),
    ] {
        assert!(!permanent.is_retryable(), "{permanent:?} 不应可重试");
    }
}

/// S-5：验收命令可在无网络条件下执行——纯函数、配置面与同步构造均不触碰网络。
#[test]
fn assert_acceptance() {
    // 纯函数（TabSeparated 解析、分块计算）离线可执行。
    assert_eq!(
        parse_tab_separated_rows("1\tfoo\n"),
        vec![vec!["1".to_owned(), "foo".to_owned()]]
    );
    assert_eq!(chunk_ranges(5, 2), vec![(0, 2), (2, 4), (4, 5)]);

    // 同步构造只做校验与 HTTP 客户端装配：不发请求、不计入成功数。
    let client = ClickHouseClient::new(ClickHouseConfig::default()).expect("离线构造");
    let stats = client.stats();
    assert_eq!(stats.ok, 0);
    assert_eq!(stats.error, 0);
    assert!(!client.is_closed());

    // 配置四入口中非 env 的两个可离线驱动。
    ClickHouseConfig::default()
        .validate()
        .expect("默认配置有效");
    ClickHouseConfig::from_toml("schema_version = 1\n").expect("最小 TOML 有效");
}
