#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unreachable
)]
//! 关闭排空契约：`close()` 拒绝新请求并**等待在途操作**（`docs/标准.md` §4）。
//!
//! 本文件是缺陷修复的先红后绿证据：修复前 `close()` 只置关闭位就立即返回，
//! 断言「必须等待在途操作」为红；修复后同一断言为绿。过程见 PR 描述。

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use clickhousex::{ClickHouseClient, ClickHouseConfig, ClickHouseError};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

/// 慢响应桩：读到请求后置位 `arrived`，再延迟 `delay` 才应答。
async fn spawn_slow_mock(delay: Duration) -> (u16, Arc<AtomicBool>) {
    let listener = TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("绑定临时端口");
    let port = listener.local_addr().expect("读取临时端口").port();
    let arrived = Arc::new(AtomicBool::new(false));
    let signal = Arc::clone(&arrived);
    tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.expect("接受连接");
        let mut buffer = [0_u8; 2048];
        let _ = stream.read(&mut buffer).await;
        signal.store(true, Ordering::SeqCst);
        tokio::time::sleep(delay).await;
        let response = "HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
        let _ = stream.write_all(response.as_bytes()).await;
        let _ = stream.shutdown().await;
    });
    (port, arrived)
}

/// 等待桩确已收到在途请求（避免用固定 sleep 猜测时序）。
async fn wait_until_arrived(arrived: &AtomicBool) {
    let deadline = Instant::now() + Duration::from_secs(2);
    while !arrived.load(Ordering::SeqCst) {
        assert!(Instant::now() < deadline, "在途请求未在 2s 内到达桩");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

/// `close()` 必须等待在途操作结束：返回时在途数为 0，且在途请求不被中断。
#[tokio::test]
async fn close_waits_for_in_flight_requests() {
    let delay = Duration::from_millis(300);
    let (port, arrived) = spawn_slow_mock(delay).await;
    let config = ClickHouseConfig::builder()
        .host("127.0.0.1")
        .http_port(port)
        .database("analytics")
        .timeout(Duration::from_secs(5))
        .acquire_timeout(Duration::from_secs(5))
        .max_in_flight(1)
        .build()
        .expect("测试配置必须有效");
    let client = ClickHouseClient::new(config).expect("构造客户端");

    let worker = {
        let client = client.clone();
        tokio::spawn(async move { client.execute("SELECT 1").await })
    };
    wait_until_arrived(&arrived).await;
    assert_eq!(client.stats().in_flight, 1, "请求应处于在途状态");

    let started = Instant::now();
    client.close().await.expect("close 必须成功");
    let waited = started.elapsed();

    assert!(
        waited >= Duration::from_millis(200),
        "close 必须等待在途操作，实际仅等待 {waited:?}"
    );
    assert_eq!(client.stats().in_flight, 0, "close 返回时在途数必须为 0");
    assert!(client.is_closed());
    assert_eq!(client.stats().open, 0, "关闭后不得再提供额度");

    // 在途请求不被 close 打断，正常拿到响应。
    worker
        .await
        .expect("工作线程不应 panic")
        .expect("在途请求必须成功完成");

    // 关闭后拒绝新请求。
    let error = client
        .execute("SELECT 2")
        .await
        .expect_err("close 后必须拒绝新请求");
    assert!(matches!(error, ClickHouseError::Closed(_)), "{error:?}");
    assert!(!error.is_retryable());
}

/// `close()` 在无在途操作时立即返回，且可重复调用（幂等）。
#[tokio::test]
async fn close_is_idempotent_and_immediate_when_idle() {
    let config = ClickHouseConfig::builder()
        .host("127.0.0.1")
        .http_port(1)
        .timeout(Duration::from_millis(200))
        .acquire_timeout(Duration::from_millis(200))
        .build()
        .expect("测试配置必须有效");
    let client = ClickHouseClient::new(config).expect("构造客户端");

    let started = Instant::now();
    client.close().await.expect("首次 close");
    client.close().await.expect("重复 close 必须成功");
    assert!(
        started.elapsed() < Duration::from_secs(1),
        "空池关闭应立即返回"
    );
    assert!(client.is_closed());
}
