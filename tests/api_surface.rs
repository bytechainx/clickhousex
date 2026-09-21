#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unreachable
)]
//! 公共 API 表面：类型存在性、`Send + Sync + Clone`、`serde::Deserialize` 与同步构造。

use clickhousex::{
    BatchInsertOptions, ClickHouseClient, ClickHouseConfig, ClickHouseConfigBuilder,
    ClickHouseError, ClickHouseHealth, ClickHousePool, ClickHousePoolStats, DEFAULT_DATABASE,
    DEFAULT_HTTP_PORT, DEFAULT_USER, ENV_PREFIX,
};

fn assert_send_sync<T: Send + Sync>() {}
fn assert_clone<T: Clone>() {}
fn assert_deserialize<'de, T: serde::Deserialize<'de>>() {}

#[test]
fn public_types_are_send_sync_clone() {
    assert_send_sync::<ClickHouseClient>();
    assert_send_sync::<ClickHousePool>();
    assert_send_sync::<ClickHouseConfig>();
    assert_send_sync::<ClickHouseConfigBuilder>();
    assert_send_sync::<ClickHousePoolStats>();
    assert_send_sync::<ClickHouseHealth>();
    assert_send_sync::<BatchInsertOptions>();
    assert_send_sync::<ClickHouseError>();

    assert_clone::<ClickHouseClient>();
    assert_clone::<ClickHousePool>();
    assert_clone::<ClickHouseConfig>();
    assert_clone::<ClickHouseConfigBuilder>();
    assert_clone::<ClickHousePoolStats>();
    assert_clone::<ClickHouseHealth>();
    assert_clone::<BatchInsertOptions>();

    // 克隆后的句柄共享同一份内部状态。
    let client = ClickHouseClient::new(ClickHouseConfig::default()).expect("同步构造必须成功");
    let cloned = client.clone();
    assert_eq!(cloned.stats(), client.stats());
    assert!(format!("{cloned:?}").contains("ClickHouseClient"));
}

#[test]
fn config_derives_deserialize_and_rejects_unknown_fields() {
    assert_deserialize::<ClickHouseConfig>();

    let config: ClickHouseConfig = toml::from_str(
        r#"
host = "127.0.0.1"
http_port = 9000
database = "analytics"
timeout_ms = 1234
"#,
    )
    .expect("扁平 TOML 必须可反序列化");
    assert_eq!(config.http_port, 9000);
    assert_eq!(config.database, "analytics");
    assert_eq!(config.timeout.as_millis(), 1234);

    // 密码不允许从反序列化通道进入。
    assert!(toml::from_str::<ClickHouseConfig>("password = \"hunter2\"").is_err());
    // 未知字段 fail-closed。
    assert!(toml::from_str::<ClickHouseConfig>("sink_id = \"x\"").is_err());
}

#[test]
fn public_constants_are_stable() {
    assert_eq!(ENV_PREFIX, "FOUNDATIONX_CLICKHOUSEX_");
    assert_eq!(DEFAULT_USER, "default");
    assert_eq!(DEFAULT_DATABASE, "default");
    assert_eq!(DEFAULT_HTTP_PORT, 8123);
    assert!(clickhousex::ENV_HOST.starts_with(ENV_PREFIX));
    assert!(clickhousex::ENV_PASSWORD.starts_with(ENV_PREFIX));
    assert!(clickhousex::ENV_PLAIN_HTTP_HOSTS.starts_with(ENV_PREFIX));
}

#[test]
fn config_builder_default_and_error_types_are_exposed() {
    let config = ClickHouseConfigBuilder::new()
        .host("127.0.0.1")
        .http_port(8123)
        .max_in_flight(8)
        .build()
        .expect("构建必须成功");
    assert_eq!(config.base_url(), "http://127.0.0.1:8123");

    let error: ClickHouseError = ClickHouseConfigBuilder::new().host("").build().unwrap_err();
    assert!(!error.is_retryable());
    assert!(std::error::Error::source(&error).is_none());

    let result: clickhousex::ClickHouseResult<()> = Err(error);
    assert!(result.is_err());
}
