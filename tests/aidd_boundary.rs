#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unreachable
)]
//! AIDD 对抗 / 边界用例（特性 002）。
//!
//! 候选由 AI 生成，逐条人工复核后仅保留「结论=保留」项；丢弃项登记于 PR 描述。
//!
//! // AIDD: 标识符长度 192/193 边界 | 来源=AI | 复核=ZoneCNH/2026-09-22 | 依据=标准.md §3 注入防护标识符白名单 | 结论=保留
//! // AIDD: batch_size 与行数相等/超出 1 行 | 来源=AI | 复核=ZoneCNH/2026-09-22 | 依据=标准.md §4 失败与并发（拒绝越界批量） | 结论=保留
//! // AIDD: max_rows_per_chunk = 0 | 来源=AI | 复核=ZoneCNH/2026-09-22 | 依据=契约 chunk_ranges 抬升为 1 | 结论=保留
//! // AIDD: 查询参数名以数字开头 / 为空 / 下划线开头 | 来源=AI | 复核=ZoneCNH/2026-09-22 | 依据=标准.md §3 SQL 参数名白名单 | 结论=保留
//! // AIDD: TabSeparated 尾随空列与 CRLF | 来源=AI | 复核=ZoneCNH/2026-09-22 | 依据=标准.md §5 TabSeparated 解析边界 | 结论=保留
//! // AIDD: chunk_ranges 总数为 0 / 上限为 0 / 上限大于总数 | 来源=AI | 复核=ZoneCNH/2026-09-22 | 依据=标准.md §5 分块计算覆盖 | 结论=保留

use std::time::Duration;

use clickhousex::{
    chunk_ranges, parse_tab_separated_rows, BatchInsertOptions, ClickHouseClient, ClickHouseConfig,
    ClickHouseError,
};

/// 指向必然拒绝连接的本机端口；用例只观测「是否在触网前被拒」。
fn offline_client() -> ClickHouseClient {
    let config = ClickHouseConfig::builder()
        .host("127.0.0.1")
        .http_port(1)
        .database("analytics")
        .timeout(Duration::from_millis(300))
        .acquire_timeout(Duration::from_millis(300))
        .max_in_flight(1)
        .build()
        .expect("测试配置必须有效");
    ClickHouseClient::new(config).expect("构造客户端")
}

fn one_row() -> Vec<serde_json::Value> {
    vec![serde_json::json!({ "id": 1 })]
}

/// 边界：标识符长度恰好 192 字节应通过白名单（随后才因触网失败），193 字节立即拒绝。
#[tokio::test]
async fn identifier_length_boundary_is_exclusive() {
    let client = offline_client();

    let at_limit = "a".repeat(192);
    let error = client
        .insert_batch(&at_limit, &one_row(), BatchInsertOptions::default())
        .await
        .expect_err("不可达端点必须报错");
    assert!(
        !matches!(error, ClickHouseError::Invalid(_)),
        "192 字节应通过白名单校验，实际 {error:?}"
    );

    let over_limit = "a".repeat(193);
    let error = client
        .insert_batch(&over_limit, &one_row(), BatchInsertOptions::default())
        .await
        .expect_err("超长标识符必须拒绝");
    assert!(matches!(error, ClickHouseError::Invalid(_)), "{error:?}");
}

/// 边界：`batch_size` 等于行数时放行，超出 1 行即拒绝（下界与上界都不得静默放宽）。
#[tokio::test]
async fn batch_size_boundary_is_inclusive() {
    let client = offline_client();
    let rows = vec![
        serde_json::json!({ "id": 1 }),
        serde_json::json!({ "id": 2 }),
        serde_json::json!({ "id": 3 }),
    ];

    let error = client
        .insert_batch("demo", &rows, BatchInsertOptions::default().batch_size(3))
        .await
        .expect_err("不可达端点必须报错");
    assert!(
        !matches!(error, ClickHouseError::Invalid(_)),
        "行数 == batch_size 必须放行，实际 {error:?}"
    );

    let mut four = rows.clone();
    four.push(serde_json::json!({ "id": 4 }));
    let error = client
        .insert_batch("demo", &four, BatchInsertOptions::default().batch_size(3))
        .await
        .expect_err("越界批量必须拒绝");
    assert!(matches!(error, ClickHouseError::Invalid(_)), "{error:?}");
}

/// 边界：`max_rows_per_chunk = 0` 被抬升为 1，而不是产生零宽或死循环分块。
#[test]
fn zero_rows_per_chunk_is_raised_to_one() {
    assert_eq!(chunk_ranges(2, 0), vec![(0, 1), (1, 2)]);
    assert_eq!(chunk_ranges(1, 0), vec![(0, 1)]);
}

/// 边界：查询参数名以数字开头 / 为空 → 拒绝；下划线开头（非数字）→ 放行。
#[tokio::test]
async fn query_param_name_boundaries() {
    let client = offline_client();

    for bad in ["1id", "", "bad-name"] {
        let error = client
            .query_with_params("SELECT {v:UInt64}", &[(bad, "1")])
            .await
            .expect_err("非法参数名必须拒绝");
        assert!(
            matches!(error, ClickHouseError::Invalid(_)),
            "{bad}: {error:?}"
        );
    }

    let error = client
        .query_with_params("SELECT {v:UInt64}", &[("_id1", "1")])
        .await
        .expect_err("不可达端点必须报错");
    assert!(
        !matches!(error, ClickHouseError::Invalid(_)),
        "下划线开头应通过校验，实际 {error:?}"
    );
}

/// 边界：`TabSeparated` 的尾随空列、CRLF 行尾与纯空行。
#[test]
fn tab_separated_boundaries() {
    assert_eq!(
        parse_tab_separated_rows("a\t\n"),
        vec![vec!["a".to_owned(), String::new()]],
        "尾随空列必须保留为第二列"
    );
    assert_eq!(
        parse_tab_separated_rows("a\tb\r\n"),
        vec![vec!["a".to_owned(), "b".to_owned()]],
        "CRLF 行尾不得把 \\r 带进单元格"
    );
    assert!(parse_tab_separated_rows("\n\n").is_empty(), "纯空行跳过");
    assert!(parse_tab_separated_rows("").is_empty());
}

/// 边界：`chunk_ranges` 在总数为 0、上限为 0、上限大于总数与恰好整除时的形态。
#[test]
fn chunk_ranges_boundaries() {
    assert!(chunk_ranges(0, 10).is_empty(), "空输入不得产出分块");
    assert_eq!(chunk_ranges(5, 5), vec![(0, 5)], "恰好整除不产生空尾块");
    assert_eq!(chunk_ranges(5, 100), vec![(0, 5)], "上限大于总数时单块");
    assert_eq!(chunk_ranges(6, 3), vec![(0, 3), (3, 6)]);
    assert_eq!(chunk_ranges(7, 3), vec![(0, 3), (3, 6), (6, 7)]);
}
