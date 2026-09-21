//! 纯函数行为：TabSeparated 解析、分块、URL 构造与错误可重试判定。

use clickhousex::{
    build_query_url, chunk_ranges, parse_tab_separated_rows, BatchInsertOptions, ClickHouseConfig,
    ClickHouseError,
};

#[test]
fn parse_tab_separated_rows_splits_columns_and_skips_blank_lines() {
    let rows = parse_tab_separated_rows("1\tfoo\n\n2\tbar\tbaz\n");
    assert_eq!(
        rows,
        vec![
            vec!["1".to_owned(), "foo".to_owned()],
            vec!["2".to_owned(), "bar".to_owned(), "baz".to_owned()],
        ]
    );
    assert!(parse_tab_separated_rows("").is_empty());
    assert!(parse_tab_separated_rows("\n\n").is_empty());
    // 单列（无 tab）同样成行。
    assert_eq!(
        parse_tab_separated_rows("42\n"),
        vec![vec!["42".to_owned()]]
    );
}

#[test]
fn chunk_ranges_covers_boundaries() {
    assert!(chunk_ranges(0, 4).is_empty());
    assert_eq!(chunk_ranges(4, 4), vec![(0, 4)]);
    assert_eq!(chunk_ranges(4, 5), vec![(0, 4)]);
    assert_eq!(chunk_ranges(5, 2), vec![(0, 2), (2, 4), (4, 5)]);
    assert_eq!(
        chunk_ranges(3, 0),
        vec![(0, 1), (1, 2), (2, 3)],
        "0 抬升为 1"
    );

    // 分块必须无重叠、无遗漏地覆盖全部下标。
    let covered: usize = chunk_ranges(97, 7)
        .iter()
        .map(|(start, end)| end - start)
        .sum();
    assert_eq!(covered, 97);
}

#[test]
fn build_query_url_selects_database_and_arguments() {
    let config = ClickHouseConfig::builder()
        .host("127.0.0.1")
        .http_port(8123)
        .database("analytics")
        .build()
        .expect("配置有效");
    let url = build_query_url(&config, &[("id", "7"), ("name", "a b")]).expect("URL 构造");
    assert!(url.starts_with("http://127.0.0.1:8123/?"), "url={url}");
    assert!(url.contains("database=analytics"));
    assert!(url.contains("param_id=7"));
    assert!(
        url.contains("param_name=a+b") || url.contains("param_name=a%20b"),
        "url={url}"
    );
    assert!(!url.contains("password"));

    let url_auth = build_query_url(
        &ClickHouseConfig::builder()
            .host("127.0.0.1")
            .user("writer")
            .password("s3cret")
            .auth_in_url(true)
            .build()
            .expect("配置有效"),
        &[],
    )
    .expect("URL 构造");
    assert!(url_auth.contains("user=writer"));
    assert!(url_auth.contains("password=s3cret"));

    let error = build_query_url(&config, &[("bad name", "1")]).expect_err("非法参数名必须拒绝");
    assert!(matches!(error, ClickHouseError::Invalid(_)));
}

#[test]
fn error_retryability_classifies_transient_failures() {
    let retryable = [
        ClickHouseError::Connection("drop".to_owned()),
        ClickHouseError::Unavailable("429".to_owned()),
        ClickHouseError::Timeout("t".to_owned()),
        ClickHouseError::Io(std::io::Error::other("reset")),
    ];
    for error in retryable {
        assert!(error.is_retryable(), "{error:?} 应可重试");
    }

    let permanent = [
        ClickHouseError::Config("bad".to_owned()),
        ClickHouseError::Backend("syntax error".to_owned()),
        ClickHouseError::Serialization("not object".to_owned()),
        ClickHouseError::Invalid("bad ident".to_owned()),
        ClickHouseError::Closed("closed".to_owned()),
        ClickHouseError::Unsupported("native".to_owned()),
    ];
    for error in permanent {
        assert!(!error.is_retryable(), "{error:?} 不可重试");
    }
}

#[test]
fn batch_insert_options_expose_chunking_knobs() {
    let options = BatchInsertOptions::default();
    assert_eq!(options.max_rows_per_chunk, 1000);
    assert_eq!(options.max_bytes_per_chunk, 0);
    assert_eq!(options.batch_size, 0);

    let tuned = BatchInsertOptions::default()
        .max_rows_per_chunk(500)
        .max_bytes_per_chunk(4096);
    assert_eq!(tuned.max_rows_per_chunk, 500);
    assert_eq!(tuned.max_bytes_per_chunk, 4096);
    assert_eq!(BatchInsertOptions::default().batch_size(10).batch_size, 10);
}
