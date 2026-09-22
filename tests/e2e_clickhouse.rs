#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unreachable
)]
//! E2E（clickhousex）：在**真实** ClickHouse（HTTP 8123）上端到端执行**全部**公开接口。
//!
//! 与 `live_clickhouse.rs`（单条冒烟往返）不同，本文件的对齐对象是
//! `cargo +nightly public-api --simplified` 导出的完整公开面：
//! `fn` / `type` / `field` / `const` / `variant` 五类逐条登记在 [`E2E_MANIFEST`]，
//! 运行期由 `cover` 登记表核对「声明 = 实际执行」（缺一即失败）。
//!
//! **独立核对**：`scripts/verify-e2e-coverage.mjs` 会重新派生公开面与清单双向 diff，
//! 并用 `-C instrument-coverage` + `llvm-cov report --show-functions` 断言每条公开
//! 函数执行次数 > 0；本文件内的登记表只是**声明**，不是唯一证据。
//!
//! 凭据只从环境变量 `FOUNDATIONX_CLICKHOUSEX_*` 读取，不硬编码；表名唯一化并在收尾删除并断言删除生效。
//!
//! ```text
//! set -a; source /home/zone/workspace/sre/secrets/env/clickhousex.env; set +a
//! cd /home/workspace/bytechainx/clickhousex
//! CARGO_TARGET_DIR=/home/workspace/bytechainx/.cargo/target \
//!   cargo test --test e2e_clickhouse -- --ignored --test-threads=1
//! ```

use std::collections::BTreeSet;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use clickhousex::{
    build_query_url, chunk_ranges, parse_tab_separated_rows, BatchInsertOptions, ClickHouseClient,
    ClickHouseConfig, ClickHouseConfigBuilder, ClickHouseError, ClickHousePool,
    ClickHousePoolStats, ClickHouseResult, RetryConfig, DEFAULT_DATABASE, DEFAULT_HTTP_PORT,
    DEFAULT_USER, ENV_ACQUIRE_TIMEOUT_MS, ENV_AUTH_IN_URL, ENV_CONNECT_TIMEOUT_MS, ENV_DATABASE,
    ENV_HOST, ENV_HTTP_PORT, ENV_MAX_IDLE_PER_HOST, ENV_MAX_IN_FLIGHT, ENV_PASSWORD,
    ENV_PLAIN_HTTP_HOSTS, ENV_PORT, ENV_PREFIX, ENV_TIMEOUT_MS, ENV_TLS, ENV_TLS_CA_FILE,
    ENV_TLS_CLIENT_CERT_FILE, ENV_TLS_CLIENT_KEY_FILE, ENV_USER,
};
use serde_json::json;

/// 公开面清单：`(条目类别, 入口 id)`，由 `cargo +nightly public-api --simplified` 派生并冻结。
///
/// 类别取值域：`fn` / `type` / `field` / `const` / `variant`。
/// 该清单是运行时登记的**唯一事实源**——`cover::hit` 拒绝清单外的 id，收尾断言拒绝
/// 「声明了却没执行」的条目。清单本身的时效性由外部核对器与公开面 diff 保证。
const E2E_MANIFEST: &[(&str, &str)] = &[
    ("type", "ClickHouseError"),
    ("variant", "ClickHouseError::Backend"),
    ("variant", "ClickHouseError::Closed"),
    ("variant", "ClickHouseError::Config"),
    ("variant", "ClickHouseError::Connection"),
    ("variant", "ClickHouseError::Invalid"),
    ("variant", "ClickHouseError::Io"),
    ("variant", "ClickHouseError::Serialization"),
    ("variant", "ClickHouseError::Timeout"),
    ("variant", "ClickHouseError::Unavailable"),
    ("variant", "ClickHouseError::Unsupported"),
    ("fn", "ClickHouseError::is_retryable"),
    ("type", "BatchInsertOptions"),
    ("field", "BatchInsertOptions::batch_size"),
    ("field", "BatchInsertOptions::max_bytes_per_chunk"),
    ("field", "BatchInsertOptions::max_rows_per_chunk"),
    ("fn", "BatchInsertOptions::batch_size"),
    ("fn", "BatchInsertOptions::max_bytes_per_chunk"),
    ("fn", "BatchInsertOptions::max_rows_per_chunk"),
    ("type", "ClickHouseClient"),
    ("fn", "ClickHouseClient::close"),
    ("fn", "ClickHouseClient::config"),
    ("fn", "ClickHouseClient::execute"),
    ("fn", "ClickHouseClient::health_check"),
    ("fn", "ClickHouseClient::insert_batch"),
    ("fn", "ClickHouseClient::insert_json_each_row"),
    ("fn", "ClickHouseClient::is_closed"),
    ("fn", "ClickHouseClient::ping"),
    ("fn", "ClickHouseClient::query"),
    ("fn", "ClickHouseClient::query_text"),
    ("fn", "ClickHouseClient::query_with_params"),
    ("fn", "ClickHouseClient::stats"),
    ("fn", "ClickHouseClient::new"),
    ("type", "ClickHouseConfig"),
    ("field", "ClickHouseConfig::acquire_timeout"),
    ("field", "ClickHouseConfig::auth_in_url"),
    ("field", "ClickHouseConfig::connect_timeout"),
    ("field", "ClickHouseConfig::database"),
    ("field", "ClickHouseConfig::host"),
    ("field", "ClickHouseConfig::http_port"),
    ("field", "ClickHouseConfig::max_idle_per_host"),
    ("field", "ClickHouseConfig::max_in_flight"),
    ("field", "ClickHouseConfig::password"),
    ("field", "ClickHouseConfig::retry"),
    ("field", "ClickHouseConfig::timeout"),
    ("field", "ClickHouseConfig::tls"),
    ("field", "ClickHouseConfig::tls_ca_file"),
    ("field", "ClickHouseConfig::tls_client_cert_file"),
    ("field", "ClickHouseConfig::tls_client_key_file"),
    ("field", "ClickHouseConfig::user"),
    ("fn", "ClickHouseConfig::base_url"),
    ("fn", "ClickHouseConfig::builder"),
    ("fn", "ClickHouseConfig::from_toml"),
    ("fn", "ClickHouseConfig::from_toml_file"),
    ("fn", "ClickHouseConfig::from_env"),
    ("fn", "ClickHouseConfig::validate"),
    ("type", "ClickHouseConfigBuilder"),
    ("fn", "ClickHouseConfigBuilder::acquire_timeout"),
    ("fn", "ClickHouseConfigBuilder::auth_in_url"),
    ("fn", "ClickHouseConfigBuilder::build"),
    ("fn", "ClickHouseConfigBuilder::connect_timeout"),
    ("fn", "ClickHouseConfigBuilder::database"),
    ("fn", "ClickHouseConfigBuilder::from_config"),
    ("fn", "ClickHouseConfigBuilder::host"),
    ("fn", "ClickHouseConfigBuilder::http_port"),
    ("fn", "ClickHouseConfigBuilder::max_idle_per_host"),
    ("fn", "ClickHouseConfigBuilder::max_in_flight"),
    ("fn", "ClickHouseConfigBuilder::new"),
    ("fn", "ClickHouseConfigBuilder::password"),
    ("fn", "ClickHouseConfigBuilder::retry"),
    ("fn", "ClickHouseConfigBuilder::timeout"),
    ("fn", "ClickHouseConfigBuilder::tls"),
    ("fn", "ClickHouseConfigBuilder::tls_ca_file"),
    ("fn", "ClickHouseConfigBuilder::tls_client_cert_file"),
    ("fn", "ClickHouseConfigBuilder::tls_client_key_file"),
    ("fn", "ClickHouseConfigBuilder::user"),
    ("type", "ClickHouseHealth"),
    ("field", "ClickHouseHealth::healthy"),
    ("field", "ClickHouseHealth::latency_ms"),
    ("field", "ClickHouseHealth::version"),
    ("type", "ClickHousePool"),
    ("fn", "ClickHousePool::close"),
    ("fn", "ClickHousePool::config"),
    ("fn", "ClickHousePool::execute"),
    ("fn", "ClickHousePool::health_check"),
    ("fn", "ClickHousePool::insert_batch"),
    ("fn", "ClickHousePool::insert_json_each_row"),
    ("fn", "ClickHousePool::is_closed"),
    ("fn", "ClickHousePool::ping"),
    ("fn", "ClickHousePool::query"),
    ("fn", "ClickHousePool::query_text"),
    ("fn", "ClickHousePool::query_with_params"),
    ("fn", "ClickHousePool::stats"),
    ("fn", "ClickHousePool::connect"),
    ("fn", "ClickHousePool::connect_from_env"),
    ("type", "ClickHousePoolStats"),
    ("field", "ClickHousePoolStats::closed"),
    ("field", "ClickHousePoolStats::error"),
    ("field", "ClickHousePoolStats::in_flight"),
    ("field", "ClickHousePoolStats::ok"),
    ("field", "ClickHousePoolStats::open"),
    ("field", "ClickHousePoolStats::total"),
    ("field", "ClickHousePoolStats::waiters"),
    ("type", "RetryConfig"),
    ("field", "RetryConfig::enabled"),
    ("field", "RetryConfig::initial_delay"),
    ("field", "RetryConfig::max_delay"),
    ("field", "RetryConfig::max_retries"),
    ("fn", "RetryConfig::delay_for"),
    ("fn", "RetryConfig::validate"),
    ("const", "DEFAULT_DATABASE"),
    ("const", "DEFAULT_HTTP_PORT"),
    ("const", "DEFAULT_USER"),
    ("const", "ENV_ACQUIRE_TIMEOUT_MS"),
    ("const", "ENV_AUTH_IN_URL"),
    ("const", "ENV_CONNECT_TIMEOUT_MS"),
    ("const", "ENV_DATABASE"),
    ("const", "ENV_HOST"),
    ("const", "ENV_HTTP_PORT"),
    ("const", "ENV_MAX_IDLE_PER_HOST"),
    ("const", "ENV_MAX_IN_FLIGHT"),
    ("const", "ENV_PASSWORD"),
    ("const", "ENV_PLAIN_HTTP_HOSTS"),
    ("const", "ENV_PORT"),
    ("const", "ENV_PREFIX"),
    ("const", "ENV_TIMEOUT_MS"),
    ("const", "ENV_TLS"),
    ("const", "ENV_TLS_CA_FILE"),
    ("const", "ENV_TLS_CLIENT_CERT_FILE"),
    ("const", "ENV_TLS_CLIENT_KEY_FILE"),
    ("const", "ENV_USER"),
    ("fn", "build_query_url"),
    ("fn", "chunk_ranges"),
    ("fn", "parse_tab_separated_rows"),
    ("type", "ClickHouseResult"),
];

/// 覆盖登记表：只登记**真实发生**的调用/读取，不登记「计划要调用」。
mod cover {
    use std::collections::BTreeSet;
    use std::sync::{Mutex, OnceLock};

    static EXECUTED: OnceLock<Mutex<BTreeSet<(&'static str, &'static str)>>> = OnceLock::new();

    fn log() -> &'static Mutex<BTreeSet<(&'static str, &'static str)>> {
        EXECUTED.get_or_init(|| Mutex::new(BTreeSet::new()))
    }

    /// 登记一次真实执行。清单外的 `(类别, id)` 立即 panic，防止调用点与清单漂移。
    pub fn hit(kind: &'static str, id: &'static str) {
        assert!(
            super::E2E_MANIFEST
                .iter()
                .any(|(declared_kind, declared_id)| *declared_kind == kind && *declared_id == id),
            "登记了清单外的公开条目：{kind} {id}"
        );
        log().lock().expect("覆盖登记表锁中毒").insert((kind, id));
    }

    pub fn executed() -> BTreeSet<(&'static str, &'static str)> {
        log().lock().expect("覆盖登记表锁中毒").clone()
    }
}

/// 覆盖登记的简写入口（保持调用点可读）。
fn hit(kind: &'static str, id: &'static str) {
    cover::hit(kind, id);
}

/// 清单自身良构：类别取值域合法、`(类别, id)` 不重复。
fn assert_manifest_wellformed() {
    let mut seen: BTreeSet<(&str, &str)> = BTreeSet::new();
    for (kind, id) in E2E_MANIFEST {
        assert!(
            matches!(*kind, "fn" | "type" | "field" | "const" | "variant"),
            "未知条目类别 {kind}（id={id}）"
        );
        assert!(seen.insert((kind, id)), "清单重复条目：{kind} {id}");
    }
    assert!(!E2E_MANIFEST.is_empty(), "清单不得为空");
}

/// 收尾断言：声明集合与执行集合必须**双向相等**。
fn assert_coverage_complete() {
    let declared: BTreeSet<(&str, &str)> = E2E_MANIFEST.iter().copied().collect();
    let executed = cover::executed();

    let missing: Vec<&(&str, &str)> = declared.difference(&executed).collect();
    let ghost: Vec<&(&str, &str)> = executed.difference(&declared).collect();

    assert!(
        missing.is_empty(),
        "以下 {} 条公开条目被声明却未执行：{missing:?}",
        missing.len()
    );
    assert!(
        ghost.is_empty(),
        "以下 {} 条执行未登记在清单：{ghost:?}",
        ghost.len()
    );
    eprintln!(
        "E2E 覆盖：{}/{} 条公开条目全部执行（clickhousex）",
        executed.len(),
        declared.len()
    );
}

/// 进程内唯一的资源名：`<前缀>_<pid>_<纳秒>`。
fn unique_name(prefix: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("系统时钟应晚于 UNIX_EPOCH")
        .as_nanos();
    format!("{prefix}_{}_{}", std::process::id(), nanos)
}

/// 临时目录（收尾删除并断言删除生效）。
fn unique_temp_dir(prefix: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(unique_name(prefix));
    std::fs::create_dir_all(&dir).expect("创建临时目录必须成功");
    dir
}

/// 阶段 1：21 个公开常量逐条取值断言。
fn phase_constants() {
    let env_consts: [(&'static str, &str); 18] = [
        ("ENV_PREFIX", ENV_PREFIX),
        ("ENV_HOST", ENV_HOST),
        ("ENV_HTTP_PORT", ENV_HTTP_PORT),
        ("ENV_PORT", ENV_PORT),
        ("ENV_TLS", ENV_TLS),
        ("ENV_TLS_CA_FILE", ENV_TLS_CA_FILE),
        ("ENV_TLS_CLIENT_CERT_FILE", ENV_TLS_CLIENT_CERT_FILE),
        ("ENV_TLS_CLIENT_KEY_FILE", ENV_TLS_CLIENT_KEY_FILE),
        ("ENV_USER", ENV_USER),
        ("ENV_PASSWORD", ENV_PASSWORD),
        ("ENV_DATABASE", ENV_DATABASE),
        ("ENV_TIMEOUT_MS", ENV_TIMEOUT_MS),
        ("ENV_CONNECT_TIMEOUT_MS", ENV_CONNECT_TIMEOUT_MS),
        ("ENV_MAX_IDLE_PER_HOST", ENV_MAX_IDLE_PER_HOST),
        ("ENV_MAX_IN_FLIGHT", ENV_MAX_IN_FLIGHT),
        ("ENV_ACQUIRE_TIMEOUT_MS", ENV_ACQUIRE_TIMEOUT_MS),
        ("ENV_AUTH_IN_URL", ENV_AUTH_IN_URL),
        ("ENV_PLAIN_HTTP_HOSTS", ENV_PLAIN_HTTP_HOSTS),
    ];
    let mut seen = BTreeSet::new();
    for (id, value) in env_consts {
        hit("const", id);
        assert!(
            value.starts_with(ENV_PREFIX),
            "{id} 必须带前缀 {ENV_PREFIX}，实际 {value}"
        );
        assert!(seen.insert(value), "{id} 与其它常量重复：{value}");
    }
    assert_eq!(ENV_PREFIX, "FOUNDATIONX_CLICKHOUSEX_");

    hit("const", "DEFAULT_USER");
    assert_eq!(DEFAULT_USER, "default");
    hit("const", "DEFAULT_DATABASE");
    assert_eq!(DEFAULT_DATABASE, "default");
    hit("const", "DEFAULT_HTTP_PORT");
    assert_eq!(DEFAULT_HTTP_PORT, 8123);
}

/// 阶段 2：值类型（含穷尽解构读字段、枚举变体逐个构造）。
fn phase_value_types() {
    // BatchInsertOptions：类型 + 3 字段 + 3 个构建方法。
    let options = BatchInsertOptions::default()
        .max_rows_per_chunk(7)
        .max_bytes_per_chunk(4096)
        .batch_size(100);
    hit("type", "BatchInsertOptions");
    hit("fn", "BatchInsertOptions::max_rows_per_chunk");
    hit("fn", "BatchInsertOptions::max_bytes_per_chunk");
    hit("fn", "BatchInsertOptions::batch_size");

    // 穷尽解构（不写 `..`）：新增公开字段会在此处编译失败，强制补齐覆盖。
    let BatchInsertOptions {
        max_rows_per_chunk,
        max_bytes_per_chunk,
        batch_size,
    } = options;
    hit("field", "BatchInsertOptions::max_rows_per_chunk");
    assert_eq!(max_rows_per_chunk, 7);
    hit("field", "BatchInsertOptions::max_bytes_per_chunk");
    assert_eq!(max_bytes_per_chunk, 4096);
    hit("field", "BatchInsertOptions::batch_size");
    assert_eq!(batch_size, 100);

    // RetryConfig：类型 + 4 字段 + 2 方法。
    let retry = RetryConfig {
        max_retries: 2,
        initial_delay: Duration::from_millis(50),
        max_delay: Duration::from_millis(400),
        enabled: true,
    };
    hit("type", "RetryConfig");
    retry.validate().expect("合法 RetryConfig 必须通过校验");
    hit("fn", "RetryConfig::validate");
    hit("fn", "RetryConfig::delay_for");
    let initial_ms = retry.initial_delay.as_millis() as u64;
    let first_delay = retry.delay_for(0);
    assert!(
        first_delay >= Duration::from_millis(initial_ms * 3 / 4)
            && first_delay <= Duration::from_millis(initial_ms * 5 / 4 + 1),
        "首次退避应落在 initial_delay ±25% 抖动内，实际 {first_delay:?}"
    );
    // 指数增长且受 max_delay 约束（含 ±25% 抖动的上界放宽）。
    assert!(
        retry.delay_for(2) <= Duration::from_millis(retry.max_delay.as_millis() as u64 * 5 / 4 + 1),
        "退避不得突破 max_delay 的抖动上界"
    );
    assert!(
        retry.delay_for(2) > retry.delay_for(0),
        "退避应随重试次数增长"
    );

    let RetryConfig {
        max_retries,
        initial_delay,
        max_delay,
        enabled,
    } = retry;
    hit("field", "RetryConfig::max_retries");
    assert_eq!(max_retries, 2);
    hit("field", "RetryConfig::initial_delay");
    assert_eq!(initial_delay, Duration::from_millis(50));
    hit("field", "RetryConfig::max_delay");
    assert_eq!(max_delay, Duration::from_millis(400));
    hit("field", "RetryConfig::enabled");
    assert!(enabled);

    let bad = RetryConfig {
        max_retries: 99,
        ..RetryConfig::default()
    };
    assert!(bad.validate().is_err(), "超过上界的重试预算必须被拒绝");

    // ClickHouseError：类型本身 + 10 个变体逐个构造 + is_retryable 分类。
    //
    // 注：`ClickHouseError` 是 `#[non_exhaustive]`，`cargo public-api` 把它印成
    // `#[non_exhaustive] pub enum clickhousex::ClickHouseError`（属性在行首）。
    // 核对器若按 `^pub` 行锚定就会整条漏掉该 type 条目，而清单与权威面「双向都缺」
    // ⇒ diff 恒绿、计数偏小。核对器已修（匹配前剥离行首 `#[…]`），此处补齐登记。
    hit("type", "ClickHouseError");
    let variants: [(&'static str, ClickHouseError, bool); 10] = [
        (
            "ClickHouseError::Backend",
            ClickHouseError::Backend("e2e".into()),
            false,
        ),
        (
            "ClickHouseError::Closed",
            ClickHouseError::Closed("e2e".into()),
            false,
        ),
        (
            "ClickHouseError::Config",
            ClickHouseError::Config("e2e".into()),
            false,
        ),
        (
            "ClickHouseError::Connection",
            ClickHouseError::Connection("e2e".into()),
            true,
        ),
        (
            "ClickHouseError::Invalid",
            ClickHouseError::Invalid("e2e".into()),
            false,
        ),
        (
            "ClickHouseError::Io",
            ClickHouseError::Io(std::io::Error::other("e2e")),
            true,
        ),
        (
            "ClickHouseError::Serialization",
            ClickHouseError::Serialization("e2e".into()),
            false,
        ),
        (
            "ClickHouseError::Timeout",
            ClickHouseError::Timeout("e2e".into()),
            true,
        ),
        (
            "ClickHouseError::Unavailable",
            ClickHouseError::Unavailable("e2e".into()),
            true,
        ),
        (
            "ClickHouseError::Unsupported",
            ClickHouseError::Unsupported("e2e".into()),
            false,
        ),
    ];
    for (id, error, retryable) in variants {
        hit("variant", id);
        hit("fn", "ClickHouseError::is_retryable");
        assert_eq!(
            error.is_retryable(),
            retryable,
            "{id} 的可重试分类不符合契约"
        );
        // 错误消息不得回显参数内容以外的敏感信息；此处只确认 Display 可用且非空。
        assert!(!error.to_string().is_empty());
    }

    // ClickHouseResult 别名（错误路径 + 成功路径各一次）。
    fn as_result(value: u8) -> ClickHouseResult<u8> {
        Ok(value)
    }
    let ok = as_result(7);
    hit("type", "ClickHouseResult");
    assert_eq!(ok.expect("Ok 分支"), 7);
    let err: ClickHouseResult<u8> = Err(ClickHouseError::Config("e2e".into()));
    assert!(err.is_err());

    // 三个自由纯函数。
    hit("fn", "chunk_ranges");
    assert_eq!(
        chunk_ranges(10, 3),
        vec![(0, 3), (3, 6), (6, 9), (9, 10)],
        "分块区间必须覆盖 [0,10) 且不重叠"
    );
    hit("fn", "parse_tab_separated_rows");
    assert_eq!(
        parse_tab_separated_rows("a\tb\n1\t2\n"),
        vec![
            vec!["a".to_owned(), "b".to_owned()],
            vec!["1".to_owned(), "2".to_owned()],
        ]
    );
}

/// 阶段 3：配置面（`from_env` 读真实注入的 `FOUNDATIONX_CLICKHOUSEX_*`）。
///
/// 返回可直连环境的配置；其余构造路径（TOML / 构建器）只做配置层断言，不用于连服务。
fn phase_config_plane() -> ClickHouseConfig {
    // —— from_env：真实环境变量 ——
    let env_config = ClickHouseConfig::from_env()
        .expect("必须能读取 FOUNDATIONX_CLICKHOUSEX_*：请先 source clickhousex.env");
    hit("fn", "ClickHouseConfig::from_env");
    hit("type", "ClickHouseConfig");
    assert_eq!(env_config.host, "127.0.0.1", "env 主机必须被读到");
    assert_eq!(env_config.http_port, 8123, "env 端口必须被读到");
    assert_eq!(env_config.user, "default");
    assert_eq!(env_config.database, "default");
    assert!(!env_config.tls, "dev 环境为明文 HTTP");
    assert!(!env_config.password.is_empty(), "env 密码必须被读到");

    hit("fn", "ClickHouseConfig::validate");
    env_config.validate().expect("env 配置必须合法");
    hit("fn", "ClickHouseConfig::base_url");
    assert_eq!(env_config.base_url(), "http://127.0.0.1:8123");

    // 16 个公开字段穷尽解构（新增字段会编译失败）。
    let ClickHouseConfig {
        host,
        http_port,
        user,
        password,
        database,
        timeout,
        connect_timeout,
        max_idle_per_host,
        max_in_flight,
        acquire_timeout,
        auth_in_url,
        tls,
        tls_ca_file,
        tls_client_cert_file,
        tls_client_key_file,
        retry,
    } = env_config.clone();
    hit("field", "ClickHouseConfig::host");
    assert_eq!(host, "127.0.0.1");
    hit("field", "ClickHouseConfig::http_port");
    assert_eq!(http_port, 8123);
    hit("field", "ClickHouseConfig::user");
    assert_eq!(user, "default");
    hit("field", "ClickHouseConfig::password");
    assert!(!password.is_empty());
    hit("field", "ClickHouseConfig::database");
    assert_eq!(database, "default");
    hit("field", "ClickHouseConfig::timeout");
    assert!(!timeout.is_zero());
    hit("field", "ClickHouseConfig::connect_timeout");
    assert!(connect_timeout.is_none_or(|value| !value.is_zero()));
    hit("field", "ClickHouseConfig::max_idle_per_host");
    assert!(max_idle_per_host >= 1);
    hit("field", "ClickHouseConfig::max_in_flight");
    assert!(max_in_flight >= 1);
    hit("field", "ClickHouseConfig::acquire_timeout");
    assert!(!acquire_timeout.is_zero());
    hit("field", "ClickHouseConfig::auth_in_url");
    assert!(!auth_in_url, "dev env 未开启 URL 传递凭据");
    hit("field", "ClickHouseConfig::tls");
    assert!(!tls);
    hit("field", "ClickHouseConfig::tls_ca_file");
    assert!(tls_ca_file.is_none());
    hit("field", "ClickHouseConfig::tls_client_cert_file");
    assert!(tls_client_cert_file.is_none());
    hit("field", "ClickHouseConfig::tls_client_key_file");
    assert!(tls_client_key_file.is_none());
    hit("field", "ClickHouseConfig::retry");
    assert!(retry.enabled);

    // —— from_toml / from_toml_file ——
    let toml_text = r#"
schema_version = 1
host = "127.0.0.1"
http_port = 8123
database = "default"
timeout_ms = 2500
"#;
    let from_toml = ClickHouseConfig::from_toml(toml_text).expect("合法 TOML 必须可解析");
    hit("fn", "ClickHouseConfig::from_toml");
    assert_eq!(from_toml.http_port, 8123);
    assert_eq!(from_toml.timeout, Duration::from_millis(2500));
    // 拒绝路径必须**带上 schema_version**，否则会因「缺 schema_version」而假通过。
    assert!(
        ClickHouseConfig::from_toml("schema_version = 1\npassword = \"hunter2\"\n").is_err(),
        "from_toml 必须拒绝非空明文密码"
    );
    assert!(
        ClickHouseConfig::from_toml("schema_version = 1\nsink_id = \"x\"\n").is_err(),
        "from_toml 必须拒绝未知字段（fail-closed）"
    );
    assert!(
        ClickHouseConfig::from_toml("schema_version = 2\n").is_err(),
        "from_toml 必须拒绝未知 schema_version"
    );

    let dir = unique_temp_dir("clickhousex_e2e_toml");
    let toml_path = dir.join("config.toml");
    std::fs::write(&toml_path, toml_text).expect("写 TOML 必须成功");
    let from_file = ClickHouseConfig::from_toml_file(&toml_path).expect("TOML 文件必须可解析");
    hit("fn", "ClickHouseConfig::from_toml_file");
    assert_eq!(from_file.database, "default");
    assert!(ClickHouseConfig::from_toml_file(dir.join("missing.toml")).is_err());

    // —— 构建器：全部 19 个公开方法 ——
    hit("type", "ClickHouseConfigBuilder");
    let ca = dir.join("ca.pem");
    let defaults = ClickHouseConfigBuilder::new()
        .build()
        .expect("默认值必须合法");
    hit("fn", "ClickHouseConfigBuilder::new");
    assert_eq!(defaults.host, "127.0.0.1");
    let seeded = ClickHouseConfigBuilder::from_config(env_config.clone())
        .build()
        .expect("从既有配置出发必须合法");
    hit("fn", "ClickHouseConfigBuilder::from_config");
    assert_eq!(seeded.http_port, env_config.http_port);
    hit("fn", "ClickHouseConfig::builder");
    let built = ClickHouseConfig::builder()
        .host("127.0.0.1")
        .http_port(8123)
        .user("default")
        .password("e2e-secret")
        .database("default")
        .timeout(Duration::from_secs(7))
        .connect_timeout(Duration::from_secs(3))
        .max_idle_per_host(4)
        .max_in_flight(6)
        .acquire_timeout(Duration::from_secs(2))
        .auth_in_url(false)
        .tls(true)
        .tls_ca_file(ca.clone())
        .tls_client_cert_file(ca.clone())
        .tls_client_key_file(ca.clone())
        .retry(RetryConfig::default())
        .build()
        .expect("构建器产出的配置必须合法（路径存在性不参与校验）");
    for method in [
        "ClickHouseConfigBuilder::new",
        "ClickHouseConfigBuilder::from_config",
        "ClickHouseConfigBuilder::host",
        "ClickHouseConfigBuilder::http_port",
        "ClickHouseConfigBuilder::user",
        "ClickHouseConfigBuilder::password",
        "ClickHouseConfigBuilder::database",
        "ClickHouseConfigBuilder::timeout",
        "ClickHouseConfigBuilder::connect_timeout",
        "ClickHouseConfigBuilder::max_idle_per_host",
        "ClickHouseConfigBuilder::max_in_flight",
        "ClickHouseConfigBuilder::acquire_timeout",
        "ClickHouseConfigBuilder::auth_in_url",
        "ClickHouseConfigBuilder::tls",
        "ClickHouseConfigBuilder::tls_ca_file",
        "ClickHouseConfigBuilder::tls_client_cert_file",
        "ClickHouseConfigBuilder::tls_client_key_file",
        "ClickHouseConfigBuilder::retry",
        "ClickHouseConfigBuilder::build",
    ] {
        hit("fn", method);
    }
    assert_eq!(built.http_port, 8123);
    assert_eq!(built.max_in_flight, 6);
    assert_eq!(built.timeout, Duration::from_secs(7));
    assert!(built.tls);
    assert_eq!(built.tls_ca_file.as_deref(), Some(ca.as_path()));
    // 构建器 fail-closed：非法输入必须在 build() 处被拒绝。
    assert!(ClickHouseConfigBuilder::new().host("").build().is_err());
    // 密码不得出现在 Debug 输出中。
    assert!(!format!("{built:?}").contains("e2e-secret"));

    // build_query_url（纯函数，不联网）。
    hit("fn", "build_query_url");
    let url = build_query_url(&env_config, &[("database", "default")]).expect("URL 构造必须成功");
    assert!(url.starts_with("http://127.0.0.1:8123"), "实际 {url}");
    assert!(url.contains("database=default"), "实际 {url}");
    assert!(
        build_query_url(&env_config, &[("bad name", "x")]).is_err(),
        "非法参数名必须被拒绝"
    );

    // 临时目录收尾：删除并断言删除生效。
    std::fs::remove_dir_all(&dir).expect("清理临时目录必须成功");
    assert!(!dir.exists(), "清理后临时目录不得残留");

    env_config
}

/// 阶段 4：连接池 / 单连接客户端在真实服务上的数据面往返。
async fn phase_service_plane(env_config: &ClickHouseConfig) {
    let database = env_config.database.clone();

    // —— ClickHousePool::connect（内部含 ping 冒烟）——
    let pool = ClickHousePool::connect(env_config.clone())
        .await
        .expect("连接 ClickHouse 必须成功（检查服务可达性与 FOUNDATIONX_CLICKHOUSEX_*）");
    hit("type", "ClickHousePool");
    hit("fn", "ClickHousePool::connect");
    assert_eq!(pool.stats().ok, 1, "connect 必须先成功 ping 一次");

    // connect_from_env：独立于显式配置的等价路径。
    let env_pool = ClickHousePool::connect_from_env()
        .await
        .expect("connect_from_env 必须成功");
    hit("fn", "ClickHousePool::connect_from_env");
    env_pool.ping().await.expect("env 池 ping 必须成功");
    env_pool.close().await.expect("env 池关闭必须成功");

    hit("fn", "ClickHousePool::config");
    assert_eq!(pool.config().http_port, env_config.http_port);

    hit("fn", "ClickHousePool::ping");
    pool.ping().await.expect("ping 必须成功");

    hit("fn", "ClickHousePool::health_check");
    let health = pool.health_check().await;
    hit("type", "ClickHouseHealth");
    assert!(health.healthy, "health_check 应健康：{health:?}");
    hit("field", "ClickHouseHealth::healthy");
    hit("field", "ClickHouseHealth::latency_ms");
    assert!(health.latency_ms < Duration::from_secs(10).as_millis() as u64);
    hit("field", "ClickHouseHealth::version");
    assert!(
        health
            .version
            .as_deref()
            .is_some_and(|value| !value.is_empty()),
        "health_check 必须返回服务端版本"
    );

    // —— 数据面：建表 → 写 → 查 → 删 ——
    let table = unique_name("clickhousex_e2e");
    pool.execute(&format!(
        "CREATE TABLE IF NOT EXISTS {table} (id UInt64, name String) ENGINE = Memory"
    ))
    .await
    .expect("建表必须成功");
    hit("fn", "ClickHousePool::execute");

    let rows = vec![
        json!({ "id": 1, "name": "alpha" }),
        json!({ "id": 2, "name": "beta" }),
    ];
    pool.insert_json_each_row(&table, &rows)
        .await
        .expect("insert_json_each_row（池）必须成功");
    hit("fn", "ClickHousePool::insert_json_each_row");

    pool.insert_batch(&table, &rows, BatchInsertOptions::default())
        .await
        .expect("insert_batch（池）必须成功");
    hit("fn", "ClickHousePool::insert_batch");

    hit("fn", "ClickHousePool::query");
    let selected = pool
        .query(&format!("SELECT name FROM {table} WHERE id = 1"))
        .await
        .expect("query 必须成功");
    assert_eq!(
        selected,
        vec![vec!["alpha".to_owned()], vec!["alpha".to_owned()]],
        "两条写入路径各写 1 行 id=1"
    );

    hit("fn", "ClickHousePool::query_text");
    let text = pool
        .query_text(&format!("SELECT count() FROM {table}"))
        .await
        .expect("query_text 必须成功");
    assert_eq!(text.trim(), "4", "两轮写入各 2 行");

    hit("fn", "ClickHousePool::query_with_params");
    let parameterized = pool
        .query_with_params(
            &format!("SELECT name FROM {table} WHERE id = {{target:UInt64}}"),
            &[("target", "2")],
        )
        .await
        .expect("query_with_params 必须成功");
    assert_eq!(
        parameterized,
        vec![vec!["beta".to_owned()], vec!["beta".to_owned()]]
    );

    // 真实远端错误路径：走服务端返回的 Backend 类错误，且不可重试。
    let backend_error = pool
        .query("SELECT * FROM clickhousex_e2e_definitely_missing")
        .await
        .expect_err("查询不存在的表必须失败");
    assert!(
        !backend_error.is_retryable(),
        "远端业务错误不得被判定为可重试：{backend_error:?}"
    );
    assert!(
        !backend_error.to_string().contains("SELECT"),
        "错误消息不得回显 SQL 片段：{backend_error}"
    );

    // —— ClickHouseClient（单连接语义）——
    let client = ClickHouseClient::new(env_config.clone()).expect("同步构造必须成功");
    hit("type", "ClickHouseClient");
    hit("fn", "ClickHouseClient::new");
    hit("fn", "ClickHouseClient::config");
    assert_eq!(client.config().database, database);
    hit("fn", "ClickHouseClient::ping");
    client.ping().await.expect("client ping 必须成功");
    hit("fn", "ClickHouseClient::health_check");
    let client_health = client.health_check().await;
    assert!(client_health.healthy, "client 健康检查应通过");

    hit("fn", "ClickHouseClient::execute");
    client
        .execute(&format!(
            "CREATE TABLE IF NOT EXISTS {table}_c (id UInt64) ENGINE = Memory"
        ))
        .await
        .expect("client execute 必须成功");
    hit("fn", "ClickHouseClient::insert_json_each_row");
    client
        .insert_json_each_row(&format!("{table}_c"), &[json!({ "id": 9 })])
        .await
        .expect("client insert_json_each_row 必须成功");
    hit("fn", "ClickHouseClient::insert_batch");
    client
        .insert_batch(
            &format!("{table}_c"),
            &[json!({ "id": 10 })],
            BatchInsertOptions::default(),
        )
        .await
        .expect("client insert_batch 必须成功");
    hit("fn", "ClickHouseClient::query");
    assert_eq!(
        client
            .query(&format!("SELECT id FROM {table}_c ORDER BY id"))
            .await
            .expect("client query 必须成功"),
        vec![vec!["9".to_owned()], vec!["10".to_owned()]]
    );
    hit("fn", "ClickHouseClient::query_text");
    assert_eq!(
        client
            .query_text(&format!("SELECT count() FROM {table}_c"))
            .await
            .expect("client query_text 必须成功")
            .trim(),
        "2"
    );
    hit("fn", "ClickHouseClient::query_with_params");
    assert_eq!(
        client
            .query_with_params(
                &format!("SELECT id FROM {table}_c WHERE id = {{target:UInt64}}"),
                &[("target", "10")],
            )
            .await
            .expect("client query_with_params 必须成功"),
        vec![vec!["10".to_owned()]]
    );

    // —— 统计快照：7 个字段逐条读出 ——
    hit("type", "ClickHousePoolStats");
    let stats = pool.stats();
    let ClickHousePoolStats {
        total,
        open,
        in_flight,
        waiters,
        ok,
        error,
        closed,
    } = stats;
    hit("field", "ClickHousePoolStats::total");
    assert!(total >= 1);
    hit("field", "ClickHousePoolStats::open");
    assert!(open >= 1);
    hit("field", "ClickHousePoolStats::in_flight");
    assert_eq!(in_flight, 0, "无在途请求");
    hit("field", "ClickHousePoolStats::waiters");
    assert_eq!(waiters, 0);
    hit("field", "ClickHousePoolStats::ok");
    assert!(ok >= 1);
    hit("field", "ClickHousePoolStats::error");
    let _ = error;
    hit("field", "ClickHousePoolStats::closed");
    assert!(!closed);
    hit("fn", "ClickHousePool::stats");
    hit("fn", "ClickHousePool::is_closed");
    assert!(!pool.is_closed());
    hit("fn", "ClickHouseClient::stats");
    assert!(client.stats().ok >= 1);
    hit("fn", "ClickHouseClient::is_closed");
    assert!(!client.is_closed());

    // —— 清理并断言清理生效 ——
    pool.execute(&format!("DROP TABLE IF EXISTS {table}"))
        .await
        .expect("删表必须成功");
    client
        .execute(&format!("DROP TABLE IF EXISTS {table}_c"))
        .await
        .expect("删表（client）必须成功");
    let remaining = pool
        .query(&format!(
            "SELECT count() FROM system.tables WHERE database = '{database}' AND name LIKE '{table}%'"
        ))
        .await
        .expect("清理确认查询必须成功");
    assert_eq!(
        remaining,
        vec![vec!["0".to_owned()]],
        "清理后不得残留 {table} / {table}_c"
    );

    // —— 关停：池 → 客户端，并断言关闭后拒绝新请求 ——
    hit("fn", "ClickHouseClient::close");
    client.close().await.expect("client close 必须成功");
    assert!(client.is_closed());
    assert!(
        client.ping().await.is_err(),
        "close 后的客户端必须拒绝新请求"
    );

    hit("fn", "ClickHousePool::close");
    pool.close().await.expect("pool close 必须成功");
    assert!(pool.is_closed());
    let after_close = pool.stats();
    assert_eq!(after_close.open, 0, "close 后不得再提供额度");
    hit("fn", "ClickHousePool::close");
}

/// 单一驱动用例：保证阶段顺序与覆盖断言在同一个进程内完成。
#[tokio::test]
#[ignore = "需要真实 ClickHouse（HTTP 8123）与 FOUNDATIONX_CLICKHOUSEX_* 环境变量"]
async fn e2e_clickhouse_all_public_api() {
    assert_manifest_wellformed();
    phase_constants();
    phase_value_types();
    let env_config = phase_config_plane();
    phase_service_plane(&env_config).await;
    assert_coverage_complete();
}
