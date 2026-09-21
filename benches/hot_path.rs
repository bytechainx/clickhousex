#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unreachable
)]
//! clickhousex 热路径基准测试：配置构造 + 校验（无网络）。
use std::hint::black_box;
use std::time::Instant;

use clickhousex::ClickHouseConfig;

fn iters() -> u32 {
    if std::env::args().any(|a| a == "--quick") {
        1_000
    } else {
        50_000
    }
}

fn build_once() -> ClickHouseConfig {
    ClickHouseConfig::builder()
        .host("127.0.0.1")
        .http_port(8123)
        .database("default")
        .user("default")
        .build()
        .expect("配置构造失败")
}

fn main() {
    let n = iters();
    // 预热
    for _ in 0..n.min(50) {
        let config = build_once();
        config.validate().expect("配置校验失败");
        black_box(&config);
    }
    let start = Instant::now();
    for _ in 0..n {
        let config = build_once();
        config.validate().expect("配置校验失败");
        black_box(&config);
    }
    let elapsed = start.elapsed();
    println!(
        "bench_clickhousex_hot_path: iters={n} total={elapsed:?} per_iter={:?}",
        elapsed / n
    );
}
