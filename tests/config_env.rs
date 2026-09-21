//! 配置校验、环境变量解析与密码脱敏。

use std::sync::Mutex;
use std::time::Duration;

use clickhousex::{
    ClickHouseConfig, ClickHouseConfigBuilder, ClickHouseError, ENV_ACQUIRE_TIMEOUT_MS,
    ENV_AUTH_IN_URL, ENV_CONNECT_TIMEOUT_MS, ENV_DATABASE, ENV_HOST, ENV_MAX_IN_FLIGHT,
    ENV_PASSWORD, ENV_PLAIN_HTTP_HOSTS, ENV_PORT, ENV_TIMEOUT_MS, ENV_TLS, ENV_USER,
};

/// 环境变量是进程级全局状态，串行化所有会读写 env 的用例。
static ENV_LOCK: Mutex<()> = Mutex::new(());

/// 清空本 crate 关心的全部环境变量。
fn clear_env() {
    for name in [
        ENV_HOST,
        clickhousex::ENV_HTTP_PORT,
        ENV_PORT,
        ENV_TLS,
        clickhousex::ENV_TLS_CA_FILE,
        clickhousex::ENV_TLS_CLIENT_CERT_FILE,
        clickhousex::ENV_TLS_CLIENT_KEY_FILE,
        ENV_USER,
        ENV_PASSWORD,
        ENV_DATABASE,
        ENV_CONNECT_TIMEOUT_MS,
        ENV_TIMEOUT_MS,
        clickhousex::ENV_MAX_IDLE_PER_HOST,
        ENV_MAX_IN_FLIGHT,
        ENV_ACQUIRE_TIMEOUT_MS,
        ENV_AUTH_IN_URL,
        ENV_PLAIN_HTTP_HOSTS,
    ] {
        std::env::remove_var(name);
    }
}

#[test]
fn from_env_uses_defaults_and_overrides() {
    let _guard = ENV_LOCK.lock().expect("env 锁");
    clear_env();

    let defaults = ClickHouseConfig::from_env().expect("默认 env 配置必须有效");
    assert_eq!(defaults.host, "127.0.0.1");
    assert_eq!(defaults.http_port, 8123);
    assert_eq!(defaults.user, "default");
    assert_eq!(defaults.max_in_flight, 64);

    std::env::set_var(ENV_HOST, "localhost");
    std::env::set_var(ENV_PORT, "9000");
    std::env::set_var(ENV_USER, "writer");
    std::env::set_var(ENV_DATABASE, "analytics");
    std::env::set_var(ENV_TIMEOUT_MS, "1500");
    std::env::set_var(ENV_CONNECT_TIMEOUT_MS, "900");
    std::env::set_var(ENV_MAX_IN_FLIGHT, "16");
    std::env::set_var(ENV_ACQUIRE_TIMEOUT_MS, "700");
    std::env::set_var(ENV_TLS, "no");
    std::env::set_var(ENV_AUTH_IN_URL, "1");

    let config = ClickHouseConfig::from_env().expect("env 覆盖必须有效");
    assert_eq!(config.host, "localhost");
    assert_eq!(config.http_port, 9000);
    assert_eq!(config.user, "writer");
    assert_eq!(config.database, "analytics");
    assert_eq!(config.timeout, Duration::from_millis(1500));
    assert_eq!(config.connect_timeout, Some(Duration::from_millis(900)));
    assert_eq!(config.max_in_flight, 16);
    assert_eq!(config.acquire_timeout, Duration::from_millis(700));
    assert!(!config.tls);
    assert!(config.auth_in_url);

    clear_env();
}

#[test]
fn from_env_rejects_conflicting_port_variables() {
    let _guard = ENV_LOCK.lock().expect("env 锁");
    clear_env();

    std::env::set_var(ENV_HOST, "localhost");
    std::env::set_var(clickhousex::ENV_HTTP_PORT, "8123");
    std::env::set_var(ENV_PORT, "8443");
    let error = ClickHouseConfig::from_env().expect_err("端口冲突必须拒绝");
    assert!(matches!(error, ClickHouseError::Config(_)));
    assert!(error.to_string().contains("冲突"));

    std::env::set_var(clickhousex::ENV_HTTP_PORT, "8443");
    assert_eq!(
        ClickHouseConfig::from_env().expect("同值不冲突").http_port,
        8443
    );

    clear_env();
}

#[test]
fn from_env_rejects_illegal_values_without_echoing_them() {
    let _guard = ENV_LOCK.lock().expect("env 锁");
    clear_env();

    std::env::set_var(ENV_TIMEOUT_MS, "not-a-number");
    let error = ClickHouseConfig::from_env().expect_err("非法超时必须拒绝");
    assert!(error.to_string().contains(ENV_TIMEOUT_MS));
    assert!(!error.to_string().contains("not-a-number"));

    clear_env();
    std::env::set_var(ENV_MAX_IN_FLIGHT, "0");
    assert!(ClickHouseConfig::from_env().is_err(), "0 并发必须拒绝");
    clear_env();

    std::env::set_var(ENV_HOST, "clickhouse.example.com");
    let error = ClickHouseConfig::from_env().expect_err("远程明文 HTTP 必须拒绝");
    assert!(error.to_string().contains("HTTPS"));

    // 显式放行名单后允许明文 HTTP。
    std::env::set_var(ENV_PLAIN_HTTP_HOSTS, "clickhouse.example.com");
    ClickHouseConfig::from_env().expect("显式放行的明文 HTTP 必须通过");

    clear_env();
}

#[test]
fn password_is_redacted_in_debug_and_env_only() {
    let _guard = ENV_LOCK.lock().expect("env 锁");
    clear_env();

    std::env::set_var(ENV_PASSWORD, "super-secret-value");
    let config = ClickHouseConfig::from_env().expect("env 配置");
    let rendered = format!("{config:?}");
    assert!(rendered.contains("***"), "Debug 必须脱敏: {rendered}");
    assert!(
        !rendered.contains("super-secret-value"),
        "Debug 不得包含密码: {rendered}"
    );

    // 构建器注入的密码同样脱敏。
    let built = ClickHouseConfigBuilder::new()
        .password("another-secret")
        .build()
        .expect("构建成功");
    let rendered = format!("{built:?}");
    assert!(!rendered.contains("another-secret"));
    assert!(rendered.contains("***"));

    clear_env();
}

#[test]
fn toml_rejects_password_and_unknown_fields() {
    let error = ClickHouseConfig::from_toml(
        "schema_version = 1\nhost = \"127.0.0.1\"\npassword = \"super-secret-value\"\n",
    )
    .expect_err("TOML 中的非空密码必须拒绝");
    assert!(error.to_string().contains("password"));
    assert!(
        !error.to_string().contains("super-secret-value"),
        "错误不得回显密码"
    );

    assert!(ClickHouseConfig::from_toml("schema_version = 1\nunknown_field = 1\n").is_err());
    assert!(
        ClickHouseConfig::from_toml("host = \"127.0.0.1\"\n").is_err(),
        "缺少 schema_version"
    );

    let config = ClickHouseConfig::from_toml(
        "schema_version = 1\nhost = \"127.0.0.1\"\npassword = \"\"\nhttp_port = 8124\n",
    )
    .expect("空密码占位必须通过");
    assert_eq!(config.http_port, 8124);
}

#[test]
fn validate_reports_config_errors_for_illegal_shapes() {
    let cases = [
        ClickHouseConfig {
            host: String::new(),
            ..Default::default()
        },
        ClickHouseConfig {
            http_port: 0,
            ..Default::default()
        },
        ClickHouseConfig {
            max_in_flight: 0,
            ..Default::default()
        },
        ClickHouseConfig {
            timeout: Duration::ZERO,
            ..Default::default()
        },
        ClickHouseConfig {
            acquire_timeout: Duration::ZERO,
            ..Default::default()
        },
        ClickHouseConfig {
            connect_timeout: Some(Duration::ZERO),
            ..Default::default()
        },
        ClickHouseConfig {
            host: "db.internal".into(),
            ..Default::default()
        },
        ClickHouseConfig {
            tls_ca_file: Some("/tmp/ca.pem".into()),
            ..Default::default()
        },
        ClickHouseConfig {
            tls_client_cert_file: Some("/tmp/cert.pem".into()),
            ..Default::default()
        },
    ];
    for config in cases {
        let error = config.validate().expect_err("非法配置必须拒绝");
        assert!(matches!(error, ClickHouseError::Config(_)));
        assert!(!error.is_retryable(), "配置错误不可重试");
    }

    ClickHouseConfig::default()
        .validate()
        .expect("默认配置必须有效");
    ClickHouseConfig {
        host: "cluster.internal".into(),
        tls: true,
        ..Default::default()
    }
    .validate()
    .expect("远程 HTTPS 必须有效");
}
