#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unreachable
)]
//! live 真连服：ClickHouse HTTP（默认 8123）。
//!
//! 全部用例 `#[ignore]`，默认不参与 CI；凭据**只**从环境变量
//! `FOUNDATIONX_CLICKHOUSEX_*` 读取，不硬编码。
//!
//! 运行方式见 `scripts/live/README.md`：
//!
//! ```text
//! set -a; source /home/workspace/sre/secrets/env/clickhousex.env; set +a
//! cd /home/workspace/bytechainx/clickhousex
//! CARGO_TARGET_DIR=/home/workspace/bytechainx/.cargo/target \
//!   cargo test --test live_clickhouse -- --ignored --test-threads=1
//! ```

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use clickhousex::{BatchInsertOptions, ClickHouseConfig, ClickHousePool};
use serde_json::json;

/// 唯一化表名：`<前缀>_<pid>_<纳秒时间戳>`，避免与并发运行或历史数据相互干扰。
fn unique_table(prefix: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("系统时钟应晚于 UNIX_EPOCH")
        .as_nanos();
    format!("{prefix}_{}_{}", std::process::id(), nanos)
}

/// 建连 → 探活 → 建表 → 批量写 → 查询 → 删表 → close 的完整往返。
#[tokio::test]
#[ignore = "需要真实 ClickHouse（HTTP 8123）与 FOUNDATIONX_CLICKHOUSEX_* 环境变量"]
async fn live_clickhouse_roundtrip() {
    // 配置只来自环境变量；缺失或非法时给出可操作的提示，不打印任何取值。
    let config = ClickHouseConfig::from_env()
        .expect("必须能从 FOUNDATIONX_CLICKHOUSEX_* 读取配置：请先 source clickhousex.env");
    let database = config.database.clone();
    let endpoint = config.base_url();

    let pool = ClickHousePool::connect(config)
        .await
        .unwrap_or_else(|error| {
            panic!("连接 {endpoint} 失败（检查服务可达性与 FOUNDATIONX_CLICKHOUSEX_*）: {error}")
        });

    // 建连断言：connect 内部已完成 ping 冒烟。
    assert_eq!(pool.stats().ok, 1, "connect 必须先成功 ping 一次");

    // 探活：结构化健康断言。
    pool.ping().await.expect("ping 必须成功");
    let health = pool.health_check().await;
    assert!(health.healthy, "health_check 应为健康: {health:?}");
    assert!(
        health.version.as_deref().is_some_and(|v| !v.is_empty()),
        "health_check 应返回服务端版本: {health:?}"
    );
    assert!(health.latency_ms < Duration::from_secs(10).as_millis() as u64);

    // 数据面往返：唯一化表名，建表 → 批量写 → 查询 → 删表。
    let table = unique_table("infra_draft_ch");
    pool.execute(&format!(
        "CREATE TABLE IF NOT EXISTS {table} (id UInt64, name String) ENGINE = Memory"
    ))
    .await
    .expect("建表必须成功");

    let rows = vec![
        json!({ "id": 1, "name": "alpha" }),
        json!({ "id": 2, "name": "beta" }),
    ];
    pool.insert_batch(&table, &rows, BatchInsertOptions::default())
        .await
        .expect("批量写入必须成功");

    let selected = pool
        .query(&format!("SELECT id, name FROM {table} ORDER BY id"))
        .await
        .expect("查询必须成功");
    assert_eq!(
        selected,
        vec![
            vec!["1".to_owned(), "alpha".to_owned()],
            vec!["2".to_owned(), "beta".to_owned()],
        ],
        "写回的 2 行必须原样可查"
    );

    // 清理并断言清理生效（只触碰本次唯一化生成的表）。
    pool.execute(&format!("DROP TABLE IF EXISTS {table}"))
        .await
        .expect("删表必须成功");
    let remaining = pool
        .query(&format!(
            "SELECT count() FROM system.tables WHERE database = '{database}' AND name = '{table}'"
        ))
        .await
        .expect("清理确认查询必须成功");
    assert_eq!(
        remaining,
        vec![vec!["0".to_owned()]],
        "清理后不得残留 {table}"
    );

    // 收尾：关闭并断言状态翻转。
    pool.close().await.expect("close 必须成功");
    assert!(pool.is_closed(), "close 后 is_closed 必须为 true");
    assert_eq!(pool.stats().open, 0, "close 后不得再提供额度");
}
