//! 不可达地址必须返回 `Err`（127.0.0.1:1 必然拒绝连接）。

use std::time::Duration;

use clickhousex::{ClickHouseClient, ClickHouseConfig, ClickHouseError, ClickHousePool};

fn unreachable_config() -> ClickHouseConfig {
    ClickHouseConfig::builder()
        .host("127.0.0.1")
        .http_port(1)
        .timeout(Duration::from_millis(300))
        .acquire_timeout(Duration::from_millis(300))
        .max_in_flight(2)
        .build()
        .expect("测试配置必须有效")
}

#[tokio::test]
async fn ping_on_unreachable_endpoint_returns_err() {
    let client = ClickHouseClient::new(unreachable_config()).expect("同步构造必须成功");

    let error = client.ping().await.expect_err("127.0.0.1:1 必须 ping 失败");
    assert!(
        matches!(
            error,
            ClickHouseError::Connection(_) | ClickHouseError::Timeout(_)
        ),
        "error={error:?}"
    );
    assert!(error.is_retryable(), "连接类错误应可重试");

    let stats = client.stats();
    assert_eq!(stats.ok, 0);
    assert_eq!(stats.error, 1);
    assert_eq!(stats.in_flight, 0, "失败后必须归还 in-flight 额度");
    assert_eq!(stats.waiters, 0);
}

#[tokio::test]
async fn pool_connect_propagates_ping_failure() {
    let error = ClickHousePool::connect(unreachable_config())
        .await
        .expect_err("connect 内部 ping 失败必须向上传播");
    assert!(
        matches!(
            error,
            ClickHouseError::Connection(_) | ClickHouseError::Timeout(_)
        ),
        "error={error:?}"
    );
}

#[tokio::test]
async fn health_check_reports_unhealthy_instead_of_failing() {
    let client = ClickHouseClient::new(unreachable_config()).expect("同步构造必须成功");

    let health = client.health_check().await;
    assert!(!health.healthy);
    assert!(health.version.is_none());

    // 失败请求计入 error 计数，且不破坏后续请求（背压额度已归还）。
    let error = client
        .query("SELECT 1")
        .await
        .expect_err("不可达地址必须失败");
    assert!(error.is_retryable());
    assert_eq!(client.stats().error, 2);
}

#[tokio::test]
async fn closed_client_short_circuits_before_network() {
    let client = ClickHouseClient::new(unreachable_config()).expect("同步构造必须成功");
    client.close().await.expect("关闭");

    let error = client
        .execute("SELECT 1")
        .await
        .expect_err("关闭后必须拒绝");
    assert!(matches!(error, ClickHouseError::Closed(_)));
    assert!(!error.is_retryable());
    assert_eq!(client.stats().error, 0, "未执行的请求不计入 error 计数");
}
