//! ClickHouse 连接配置（`ClickHouseConfig` / `ClickHouseConfigBuilder`）。
//!
//! 配置来源与优先级：
//!
//! 1. [`ClickHouseConfig::default`] 内置默认值；
//! 2. TOML 文本（[`ClickHouseConfig::from_toml`]）或环境变量（[`ClickHouseConfig::from_env`]）；
//! 3. [`ClickHouseConfig::validate`] 在建立连接前 fail-fast。
//!
//! 密码属于敏感字段：不参与 `Debug` 输出，不能从 TOML 读取，只能通过
//! 构建器 [`ClickHouseConfigBuilder::password`] 或环境变量
//! `FOUNDATIONX_CLICKHOUSEX_PASSWORD` 注入。

use std::fmt;
use std::path::{Path, PathBuf};
use std::time::Duration;

use serde::Deserialize;

use crate::error::{ClickHouseError, ClickHouseResult};

/// 环境变量前缀。
pub const ENV_PREFIX: &str = "FOUNDATIONX_CLICKHOUSEX_";
/// 环境变量：主机名或 IP。
pub const ENV_HOST: &str = "FOUNDATIONX_CLICKHOUSEX_HOST";
/// 环境变量：HTTP 端口（主变量）。
pub const ENV_HTTP_PORT: &str = "FOUNDATIONX_CLICKHOUSEX_HTTP_PORT";
/// 环境变量：HTTP 端口（兼容别名，与主变量冲突时拒绝启动）。
pub const ENV_PORT: &str = "FOUNDATIONX_CLICKHOUSEX_PORT";
/// 环境变量：是否启用 HTTPS。
pub const ENV_TLS: &str = "FOUNDATIONX_CLICKHOUSEX_TLS";
/// 环境变量：PEM CA 文件路径。
pub const ENV_TLS_CA_FILE: &str = "FOUNDATIONX_CLICKHOUSEX_TLS_CA_FILE";
/// 环境变量：mTLS 客户端证书 PEM 路径。
pub const ENV_TLS_CLIENT_CERT_FILE: &str = "FOUNDATIONX_CLICKHOUSEX_TLS_CLIENT_CERT_FILE";
/// 环境变量：mTLS 客户端私钥 PEM 路径。
pub const ENV_TLS_CLIENT_KEY_FILE: &str = "FOUNDATIONX_CLICKHOUSEX_TLS_CLIENT_KEY_FILE";
/// 环境变量：用户名。
pub const ENV_USER: &str = "FOUNDATIONX_CLICKHOUSEX_USER";
/// 环境变量：密码（**唯一**允许注入密码的通道之一）。
pub const ENV_PASSWORD: &str = "FOUNDATIONX_CLICKHOUSEX_PASSWORD";
/// 环境变量：默认数据库。
pub const ENV_DATABASE: &str = "FOUNDATIONX_CLICKHOUSEX_DATABASE";
/// 环境变量：请求超时（毫秒）。
pub const ENV_TIMEOUT_MS: &str = "FOUNDATIONX_CLICKHOUSEX_TIMEOUT_MS";
/// 环境变量：连接（TCP/TLS 握手）超时（毫秒，缺省时不单独限制）。
pub const ENV_CONNECT_TIMEOUT_MS: &str = "FOUNDATIONX_CLICKHOUSEX_CONNECT_TIMEOUT_MS";
/// 环境变量：每主机最大空闲连接数。
pub const ENV_MAX_IDLE_PER_HOST: &str = "FOUNDATIONX_CLICKHOUSEX_MAX_IDLE_PER_HOST";
/// 环境变量：全局 in-flight 上限。
pub const ENV_MAX_IN_FLIGHT: &str = "FOUNDATIONX_CLICKHOUSEX_MAX_IN_FLIGHT";
/// 环境变量：获取 in-flight 许可超时（毫秒）。
pub const ENV_ACQUIRE_TIMEOUT_MS: &str = "FOUNDATIONX_CLICKHOUSEX_ACQUIRE_TIMEOUT_MS";
/// 环境变量：是否把 `user` / `password` 放进 URL 查询参数（默认使用 Basic 认证头）。
pub const ENV_AUTH_IN_URL: &str = "FOUNDATIONX_CLICKHOUSEX_AUTH_IN_URL";
/// 环境变量：明文 HTTP 允许名单（逗号分隔，仅供内网/CI 显式放行）。
pub const ENV_PLAIN_HTTP_HOSTS: &str = "FOUNDATIONX_CLICKHOUSEX_PLAIN_HTTP_HOSTS";

/// 默认用户名。
pub const DEFAULT_USER: &str = "default";
/// 默认数据库。
pub const DEFAULT_DATABASE: &str = "default";
/// 默认 HTTP 端口。
pub const DEFAULT_HTTP_PORT: u16 = 8123;

/// ClickHouse HTTP 客户端配置。
///
/// 所有字段均为 `pub`，可直接用结构体字面量 + `..Default::default()` 构造；
/// 唯一敏感字段 `password` 的 `Debug` 输出被脱敏，且不从 TOML 反序列化。
#[derive(Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ClickHouseConfig {
    /// 主机名或 IP。
    pub host: String,
    /// HTTP 端口（默认 8123）；TOML 兼容别名 `port`。
    #[serde(alias = "port")]
    pub http_port: u16,
    /// 是否使用 HTTPS（非 loopback 主机必须为 `true`）。
    pub tls: bool,
    /// 可选 PEM CA 文件；未设置时使用 reqwest/rustls 的公开可信根。
    pub tls_ca_file: Option<PathBuf>,
    /// 可选客户端证书 PEM 文件（mTLS 身份认证）。
    pub tls_client_cert_file: Option<PathBuf>,
    /// 可选客户端私钥 PEM 文件（mTLS 身份认证）。
    pub tls_client_key_file: Option<PathBuf>,
    /// 用户名。
    pub user: String,
    /// 密码。
    ///
    /// **敏感字段**：`Debug` 输出固定脱敏为 `***`，且不从 TOML 反序列化
    /// （只能通过环境变量或 [`ClickHouseConfigBuilder::password`] 注入）。
    #[serde(skip)]
    pub password: String,
    /// 默认数据库。
    pub database: String,
    /// 请求超时（TOML 字段名 `timeout_ms`）。
    #[serde(rename = "timeout_ms", deserialize_with = "de_millis")]
    pub timeout: Duration,
    /// 连接（TCP/TLS 握手）超时；`None` 表示只受请求超时约束。
    #[serde(rename = "connect_timeout_ms", deserialize_with = "de_optional_millis")]
    pub connect_timeout: Option<Duration>,
    /// reqwest 每主机最大空闲连接数。
    pub max_idle_per_host: usize,
    /// 全局 in-flight 上限（Semaphore 许可数，≥1）。
    pub max_in_flight: usize,
    /// 获取 in-flight 许可超时（TOML 字段名 `acquire_timeout_ms`）。
    #[serde(rename = "acquire_timeout_ms", deserialize_with = "de_millis")]
    pub acquire_timeout: Duration,
    /// 认证方式：`false` 使用 `Authorization: Basic` 头，`true` 使用 URL 查询参数。
    pub auth_in_url: bool,
}

impl Default for ClickHouseConfig {
    fn default() -> Self {
        Self {
            host: "127.0.0.1".into(),
            http_port: DEFAULT_HTTP_PORT,
            tls: false,
            tls_ca_file: None,
            tls_client_cert_file: None,
            tls_client_key_file: None,
            user: DEFAULT_USER.into(),
            password: String::new(),
            database: DEFAULT_DATABASE.into(),
            timeout: Duration::from_secs(10),
            connect_timeout: None,
            max_idle_per_host: 8,
            max_in_flight: 64,
            acquire_timeout: Duration::from_secs(5),
            auth_in_url: false,
        }
    }
}

impl fmt::Debug for ClickHouseConfig {
    /// 手写 `Debug`：密码固定渲染为 `***`，其余字段原样输出。
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ClickHouseConfig")
            .field("host", &self.host)
            .field("http_port", &self.http_port)
            .field("tls", &self.tls)
            .field("tls_ca_file", &self.tls_ca_file)
            .field("tls_client_cert_file", &self.tls_client_cert_file)
            .field("tls_client_key_file", &self.tls_client_key_file)
            .field("user", &self.user)
            .field("password", &"***")
            .field("database", &self.database)
            .field("timeout", &self.timeout)
            .field("connect_timeout", &self.connect_timeout)
            .field("max_idle_per_host", &self.max_idle_per_host)
            .field("max_in_flight", &self.max_in_flight)
            .field("acquire_timeout", &self.acquire_timeout)
            .field("auth_in_url", &self.auth_in_url)
            .finish()
    }
}

impl ClickHouseConfig {
    /// 从环境变量加载（前缀 `FOUNDATIONX_CLICKHOUSEX_`），未设置项使用默认值。
    ///
    /// 加载后立即 [`validate`](Self::validate)。
    pub fn from_env() -> ClickHouseResult<Self> {
        let mut config = Self::default();
        config.apply_env_overrides()?;
        config.validate()?;
        Ok(config)
    }

    /// 从 TOML 文本解析并校验（**不**读取环境变量，便于确定性测试）。
    ///
    /// 期望结构为 `schema_version = 1` + 扁平字段；`password` 只允许空占位，
    /// 非空值一律拒绝，避免密钥进入版本库。
    pub fn from_toml(text: &str) -> ClickHouseResult<Self> {
        let mut root: toml::Table = toml::from_str(text).map_err(|error| {
            ClickHouseError::Config(format!("TOML 解析失败: {}", error.message()))
        })?;

        let version = root
            .remove("schema_version")
            .ok_or_else(|| ClickHouseError::Config("TOML 缺少 schema_version 字段".to_owned()))?;
        let version = version
            .as_integer()
            .ok_or_else(|| ClickHouseError::Config("TOML schema_version 必须为整数".to_owned()))?;
        if version != 1 {
            return Err(ClickHouseError::Config(format!(
                "TOML schema_version 不支持: {version}"
            )));
        }

        if let Some(password) = root.remove("password") {
            let password = password.as_str().unwrap_or("非字符串");
            if !password.is_empty() {
                return Err(ClickHouseError::Config(
                    "TOML 禁止非空 password 字段，请改用环境变量注入".to_owned(),
                ));
            }
        }

        let config: Self = toml::Value::Table(root).try_into().map_err(|error| {
            ClickHouseError::Config(format!("TOML 反序列化失败: {}", error.message()))
        })?;
        config.validate()?;
        Ok(config)
    }

    /// 从 TOML 文件读取；语义同 [`ClickHouseConfig::from_toml`]。
    pub fn from_toml_file(path: impl AsRef<Path>) -> ClickHouseResult<Self> {
        let text = std::fs::read_to_string(path.as_ref()).map_err(|error| {
            ClickHouseError::Config(format!(
                "TOML 文件读取失败 `{}`: {}",
                path.as_ref().display(),
                error.kind()
            ))
        })?;
        Self::from_toml(&text)
    }

    /// 校验配置合法性（建立连接前 fail-fast）。
    pub fn validate(&self) -> ClickHouseResult<()> {
        if self.max_in_flight < 1 {
            return Err(ClickHouseError::Config("max_in_flight 必须 ≥ 1".to_owned()));
        }
        if self.timeout.is_zero() || self.acquire_timeout.is_zero() {
            return Err(ClickHouseError::Config(
                "timeout 与 acquire_timeout 必须大于零".to_owned(),
            ));
        }
        if self
            .connect_timeout
            .is_some_and(|timeout| timeout.is_zero())
        {
            return Err(ClickHouseError::Config(
                "connect_timeout 必须大于零".to_owned(),
            ));
        }
        if self.host.trim().is_empty() || self.http_port == 0 {
            return Err(ClickHouseError::Config("host/port 非法".to_owned()));
        }
        if self.tls_ca_file.is_some() && !self.tls {
            return Err(ClickHouseError::Config(
                "配置 tls_ca_file 时必须启用 tls".to_owned(),
            ));
        }
        match (&self.tls_client_cert_file, &self.tls_client_key_file) {
            (Some(_), None) | (None, Some(_)) => {
                return Err(ClickHouseError::Config(
                    "tls_client_cert_file 与 tls_client_key_file 必须同时配置".to_owned(),
                ));
            }
            _ => {}
        }
        if !self.tls && !host_is_loopback(&self.host) && !host_allows_plain_http(&self.host) {
            return Err(ClickHouseError::Config(
                "远程 ClickHouse 必须使用 HTTPS（如需明文 HTTP 请显式配置允许名单）".to_owned(),
            ));
        }
        Ok(())
    }

    /// 链式构建器入口。
    #[must_use]
    pub fn builder() -> ClickHouseConfigBuilder {
        ClickHouseConfigBuilder::new()
    }

    /// HTTP(S) 基址（不含查询参数与凭据）。
    #[must_use]
    pub fn base_url(&self) -> String {
        let scheme = if self.tls { "https" } else { "http" };
        format!("{scheme}://{}:{}", self.host, self.http_port)
    }

    /// 从环境变量覆盖当前配置（env 值优先于结构体已有值）。
    fn apply_env_overrides(&mut self) -> ClickHouseResult<()> {
        if let Some(value) = env_non_empty(ENV_HOST) {
            self.host = value;
        }
        let http_port = env_parsed::<u16>(ENV_HTTP_PORT)?;
        let port_alias = env_parsed::<u16>(ENV_PORT)?;
        self.http_port = resolve_http_port(http_port, port_alias, self.http_port)?;
        if let Some(value) = env_bool(ENV_TLS)? {
            self.tls = value;
        }
        if let Some(value) = env_trimmed(ENV_TLS_CA_FILE) {
            self.tls_ca_file = Some(PathBuf::from(value));
        }
        if let Some(value) = env_trimmed(ENV_TLS_CLIENT_CERT_FILE) {
            self.tls_client_cert_file = Some(PathBuf::from(value));
        }
        if let Some(value) = env_trimmed(ENV_TLS_CLIENT_KEY_FILE) {
            self.tls_client_key_file = Some(PathBuf::from(value));
        }
        if let Some(value) = env_non_empty(ENV_USER) {
            self.user = value;
        }
        if let Ok(value) = std::env::var(ENV_PASSWORD) {
            self.password = value;
        }
        if let Some(value) = env_non_empty(ENV_DATABASE) {
            self.database = value;
        }
        if let Some(value) = env_parsed::<u64>(ENV_TIMEOUT_MS)? {
            self.timeout = Duration::from_millis(value);
        }
        if let Some(value) = env_parsed::<u64>(ENV_CONNECT_TIMEOUT_MS)? {
            self.connect_timeout = Some(Duration::from_millis(value));
        }
        if let Some(value) = env_parsed::<usize>(ENV_MAX_IDLE_PER_HOST)? {
            self.max_idle_per_host = value;
        }
        if let Some(value) = env_parsed::<usize>(ENV_MAX_IN_FLIGHT)? {
            self.max_in_flight = value;
        }
        if let Some(value) = env_parsed::<u64>(ENV_ACQUIRE_TIMEOUT_MS)? {
            self.acquire_timeout = Duration::from_millis(value);
        }
        if let Some(value) = env_bool(ENV_AUTH_IN_URL)? {
            self.auth_in_url = value;
        }
        Ok(())
    }
}

/// [`ClickHouseConfig`] 的链式构建器。
///
/// 密码等敏感字段只能通过构建器或环境变量注入。
#[derive(Clone, Debug)]
pub struct ClickHouseConfigBuilder {
    inner: ClickHouseConfig,
}

impl Default for ClickHouseConfigBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl ClickHouseConfigBuilder {
    /// 从默认值开始。
    #[must_use]
    pub fn new() -> Self {
        Self {
            inner: ClickHouseConfig::default(),
        }
    }

    /// 从已有配置开始（便于在既有配置上覆盖少量字段）。
    #[must_use]
    pub fn from_config(config: ClickHouseConfig) -> Self {
        Self { inner: config }
    }

    /// 设置主机名或 IP。
    #[must_use]
    pub fn host(mut self, host: impl Into<String>) -> Self {
        self.inner.host = host.into();
        self
    }

    /// 设置 HTTP 端口。
    #[must_use]
    pub fn http_port(mut self, port: u16) -> Self {
        self.inner.http_port = port;
        self
    }

    /// 设置是否启用 HTTPS。
    #[must_use]
    pub fn tls(mut self, enabled: bool) -> Self {
        self.inner.tls = enabled;
        self
    }

    /// 设置 PEM CA 文件路径。
    #[must_use]
    pub fn tls_ca_file(mut self, path: impl Into<PathBuf>) -> Self {
        self.inner.tls_ca_file = Some(path.into());
        self
    }

    /// 设置 mTLS 客户端证书路径。
    #[must_use]
    pub fn tls_client_cert_file(mut self, path: impl Into<PathBuf>) -> Self {
        self.inner.tls_client_cert_file = Some(path.into());
        self
    }

    /// 设置 mTLS 客户端私钥路径。
    #[must_use]
    pub fn tls_client_key_file(mut self, path: impl Into<PathBuf>) -> Self {
        self.inner.tls_client_key_file = Some(path.into());
        self
    }

    /// 设置用户名。
    #[must_use]
    pub fn user(mut self, user: impl Into<String>) -> Self {
        self.inner.user = user.into();
        self
    }

    /// 设置密码；密码不会出现在 `Debug` 输出中。
    #[must_use]
    pub fn password(mut self, password: impl Into<String>) -> Self {
        self.inner.password = password.into();
        self
    }

    /// 设置默认数据库。
    #[must_use]
    pub fn database(mut self, database: impl Into<String>) -> Self {
        self.inner.database = database.into();
        self
    }

    /// 设置请求超时。
    #[must_use]
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.inner.timeout = timeout;
        self
    }

    /// 设置连接（TCP/TLS 握手）超时。
    #[must_use]
    pub fn connect_timeout(mut self, timeout: Duration) -> Self {
        self.inner.connect_timeout = Some(timeout);
        self
    }

    /// 设置每主机最大空闲连接数。
    #[must_use]
    pub fn max_idle_per_host(mut self, max_idle_per_host: usize) -> Self {
        self.inner.max_idle_per_host = max_idle_per_host;
        self
    }

    /// 设置全局 in-flight 上限。
    #[must_use]
    pub fn max_in_flight(mut self, max_in_flight: usize) -> Self {
        self.inner.max_in_flight = max_in_flight;
        self
    }

    /// 设置获取 in-flight 许可的超时。
    #[must_use]
    pub fn acquire_timeout(mut self, timeout: Duration) -> Self {
        self.inner.acquire_timeout = timeout;
        self
    }

    /// 设置是否通过 URL 查询参数传递凭据。
    #[must_use]
    pub fn auth_in_url(mut self, enabled: bool) -> Self {
        self.inner.auth_in_url = enabled;
        self
    }

    /// 校验并产出配置。
    pub fn build(self) -> ClickHouseResult<ClickHouseConfig> {
        self.inner.validate()?;
        Ok(self.inner)
    }
}

/// TOML 中 `timeout_ms` / `acquire_timeout_ms`（毫秒）的解析器。
fn de_millis<'de, D>(deserializer: D) -> Result<Duration, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let millis = <u64 as Deserialize>::deserialize(deserializer)?;
    Ok(Duration::from_millis(millis))
}

/// TOML 中可选毫秒字段（如 `connect_timeout_ms`）的解析器。
fn de_optional_millis<'de, D>(deserializer: D) -> Result<Option<Duration>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let millis = <Option<u64> as Deserialize>::deserialize(deserializer)?;
    Ok(millis.map(Duration::from_millis))
}

/// 读取非空环境变量（不做 trim）。
fn env_non_empty(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|value| !value.is_empty())
}

/// 读取 trim 后非空的环境变量。
fn env_trimmed(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
}

/// 读取并解析环境变量；解析失败只报告变量名，不回显取值。
fn env_parsed<T>(name: &str) -> ClickHouseResult<Option<T>>
where
    T: std::str::FromStr,
{
    match std::env::var(name) {
        Ok(value) => value
            .trim()
            .parse::<T>()
            .map(Some)
            .map_err(|_| ClickHouseError::Config(format!("环境变量 {name} 取值非法"))),
        Err(_) => Ok(None),
    }
}

/// 读取布尔型环境变量，兼容 `1/0`、`true/false`、`yes/no`、`on/off`。
fn env_bool(name: &str) -> ClickHouseResult<Option<bool>> {
    let Some(value) = env_trimmed(name) else {
        return Ok(None);
    };
    match value.to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Ok(Some(true)),
        "0" | "false" | "no" | "off" => Ok(Some(false)),
        _ => Err(ClickHouseError::Config(format!("环境变量 {name} 取值非法"))),
    }
}

/// 解析 `HTTP_PORT` / `PORT` 两个变量，值冲突时 fail-closed。
fn resolve_http_port(
    http_port: Option<u16>,
    port_alias: Option<u16>,
    default: u16,
) -> ClickHouseResult<u16> {
    match (http_port, port_alias) {
        (Some(primary), Some(alias)) if primary != alias => Err(ClickHouseError::Config(format!(
            "{ENV_HTTP_PORT} 与 {ENV_PORT} 取值冲突"
        ))),
        (Some(primary), _) => Ok(primary),
        (None, Some(alias)) => Ok(alias),
        (None, None) => Ok(default),
    }
}

/// 判断主机名是否为 loopback（`localhost` 或环回 IP）。
fn host_is_loopback(host: &str) -> bool {
    let host = host
        .strip_prefix('[')
        .and_then(|value| value.strip_suffix(']'))
        .unwrap_or(host);
    host.eq_ignore_ascii_case("localhost")
        || host
            .parse::<std::net::IpAddr>()
            .is_ok_and(|ip| ip.is_loopback())
}

/// 非 loopback 明文 HTTP 的显式放行名单。
fn host_allows_plain_http(host: &str) -> bool {
    host_allows_plain_http_with_list(host, std::env::var(ENV_PLAIN_HTTP_HOSTS).ok().as_deref())
}

/// [`host_allows_plain_http`] 的纯函数形式，供单元测试直接驱动。
fn host_allows_plain_http_with_list(host: &str, list: Option<&str>) -> bool {
    let host = host
        .strip_prefix('[')
        .and_then(|value| value.strip_suffix(']'))
        .unwrap_or(host);
    list.is_some_and(|entries| {
        entries
            .split(',')
            .map(str::trim)
            .filter(|entry| !entry.is_empty())
            .any(|entry| entry.eq_ignore_ascii_case(host))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn toml(text: &str) -> ClickHouseResult<ClickHouseConfig> {
        ClickHouseConfig::from_toml(text)
    }

    #[test]
    fn default_values_are_loopback_http() {
        let config = ClickHouseConfig::default();
        assert_eq!(config.host, "127.0.0.1");
        assert_eq!(config.http_port, DEFAULT_HTTP_PORT);
        assert_eq!(config.user, DEFAULT_USER);
        assert_eq!(config.database, DEFAULT_DATABASE);
        assert!(config.password.is_empty());
        assert_eq!(config.base_url(), "http://127.0.0.1:8123");
        config.validate().expect("默认配置必须有效");
    }

    #[test]
    fn toml_parses_flat_fields_and_port_alias() {
        let config = toml(
            r#"
schema_version = 1
host = "10.0.0.5"
port = 8443
tls = true
user = "writer"
database = "analytics"
timeout_ms = 15000
connect_timeout_ms = 800
max_in_flight = 32
acquire_timeout_ms = 2500
auth_in_url = true
"#,
        )
        .expect("TOML 解析必须成功");
        assert_eq!(config.host, "10.0.0.5");
        assert_eq!(config.http_port, 8443);
        assert!(config.tls);
        assert_eq!(config.user, "writer");
        assert_eq!(config.database, "analytics");
        assert_eq!(config.timeout, Duration::from_millis(15000));
        assert_eq!(config.connect_timeout, Some(Duration::from_millis(800)));
        assert_eq!(config.acquire_timeout, Duration::from_millis(2500));
        assert_eq!(config.max_in_flight, 32);
        assert!(config.auth_in_url);
        assert!(config.password.is_empty());
    }

    #[test]
    fn toml_rejects_unsupported_schema_version() {
        let error = toml("schema_version = 99\nhost = \"127.0.0.1\"\n").expect_err("必须拒绝");
        assert!(matches!(error, ClickHouseError::Config(_)));
        assert!(error.to_string().contains("schema_version"));
    }

    #[test]
    fn toml_requires_schema_version_and_rejects_unknown_fields() {
        assert!(toml("host = \"127.0.0.1\"\n").is_err());
        assert!(toml("schema_version = 1\nsink_id = \"analytics\"\n").is_err());
    }

    #[test]
    fn toml_rejects_non_empty_password_without_echoing_it() {
        let error = toml("schema_version = 1\nhost = \"127.0.0.1\"\npassword = \"hunter2\"\n")
            .expect_err("非空 password 必须拒绝");
        assert!(error.to_string().contains("password"));
        assert!(
            !error.to_string().contains("hunter2"),
            "错误不得回显 password"
        );
    }

    #[test]
    fn toml_allows_empty_password_placeholder() {
        let config = toml("schema_version = 1\npassword = \"\"\n").expect("空占位应通过");
        assert!(config.password.is_empty());
    }

    #[test]
    fn debug_redacts_password() {
        let config = ClickHouseConfig::builder()
            .password("secret-value")
            .build()
            .expect("配置有效");
        let rendered = format!("{config:?}");
        assert!(rendered.contains("***"));
        assert!(!rendered.contains("secret-value"));
    }

    #[test]
    fn validate_rejects_zero_capacity_and_zero_timeout() {
        let zero_in_flight = ClickHouseConfig {
            max_in_flight: 0,
            ..Default::default()
        };
        assert!(zero_in_flight
            .validate()
            .expect_err("0 并发必须拒绝")
            .to_string()
            .contains("max_in_flight"));

        let zero_timeout = ClickHouseConfig {
            timeout: Duration::ZERO,
            ..Default::default()
        };
        assert!(zero_timeout.validate().is_err());

        let zero_acquire = ClickHouseConfig {
            acquire_timeout: Duration::ZERO,
            ..Default::default()
        };
        assert!(zero_acquire.validate().is_err());

        let zero_connect = ClickHouseConfig {
            connect_timeout: Some(Duration::ZERO),
            ..Default::default()
        };
        assert!(zero_connect.validate().is_err());

        let empty_host = ClickHouseConfig {
            host: "  ".into(),
            ..Default::default()
        };
        assert!(empty_host.validate().is_err());
    }

    #[test]
    fn remote_plain_http_fails_closed_unless_allowlisted() {
        let remote = ClickHouseConfig {
            host: "clickhouse.example.com".into(),
            ..Default::default()
        };
        assert!(remote.validate().is_err());

        assert!(host_allows_plain_http_with_list(
            "clickhouse",
            Some("clickhouse,localhost")
        ));
        assert!(!host_allows_plain_http_with_list(
            "clickhouse.example.com",
            Some("clickhouse")
        ));
        assert!(!host_allows_plain_http_with_list("clickhouse", None));

        let https = ClickHouseConfig {
            host: "clickhouse.example.com".into(),
            http_port: 8443,
            tls: true,
            ..Default::default()
        };
        https.validate().expect("远程 HTTPS 必须通过");
        assert_eq!(https.base_url(), "https://clickhouse.example.com:8443");
    }

    #[test]
    fn ca_file_requires_tls_and_mtls_pair_is_enforced() {
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

        let pair = ClickHouseConfig {
            tls_client_cert_file: Some("/tmp/cert.pem".into()),
            tls_client_key_file: Some("/tmp/key.pem".into()),
            ..Default::default()
        };
        pair.validate().expect("cert + key 成对应通过");
    }

    #[test]
    fn resolve_http_port_handles_alias_and_conflict() {
        assert_eq!(resolve_http_port(None, None, 8123).expect("默认端口"), 8123);
        assert_eq!(
            resolve_http_port(Some(9440), None, 8123).expect("主变量优先"),
            9440
        );
        assert_eq!(
            resolve_http_port(None, Some(8443), 8123).expect("兼容别名"),
            8443
        );
        assert_eq!(
            resolve_http_port(Some(9000), Some(9000), 8123).expect("同值不冲突"),
            9000
        );
        let conflict = resolve_http_port(Some(8123), Some(8443), 8123).expect_err("冲突必须拒绝");
        assert!(conflict.to_string().contains("冲突"));
    }

    #[test]
    fn env_parsed_reports_variable_name_without_echoing_value() {
        std::env::set_var(ENV_TIMEOUT_MS, "secret-not-a-number");
        let error = env_parsed::<u64>(ENV_TIMEOUT_MS).expect_err("非法数值必须拒绝");
        std::env::remove_var(ENV_TIMEOUT_MS);
        assert!(error.to_string().contains(ENV_TIMEOUT_MS));
        assert!(!error.to_string().contains("secret-not-a-number"));
    }

    #[test]
    fn from_toml_file_missing_path_fails_closed() {
        let missing =
            std::env::temp_dir().join(format!("clickhousex-missing-{}.toml", std::process::id()));
        let error = ClickHouseConfig::from_toml_file(&missing).expect_err("缺失文件必须拒绝");
        assert!(error.to_string().contains("TOML 文件读取失败"));
        assert!(!error.to_string().contains("hunter"));
    }

    #[test]
    fn from_toml_does_not_read_env_overrides() {
        std::env::set_var(ENV_DATABASE, "from_env_database");
        let config =
            toml("schema_version = 1\ndatabase = \"from_toml_database\"\n").expect("TOML 解析");
        std::env::remove_var(ENV_DATABASE);
        assert_eq!(
            config.database, "from_toml_database",
            "from_toml 必须与环境变量隔离"
        );
    }

    #[test]
    fn builder_overrides_and_builds() {
        let config = ClickHouseConfig::builder()
            .host("127.0.0.1")
            .http_port(9000)
            .database("analytics")
            .user("writer")
            .password("p")
            .max_in_flight(4)
            .max_idle_per_host(2)
            .timeout(Duration::from_millis(500))
            .acquire_timeout(Duration::from_millis(100))
            .auth_in_url(true)
            .build()
            .expect("构建必须成功");
        assert_eq!(config.base_url(), "http://127.0.0.1:9000");
        assert_eq!(config.max_in_flight, 4);
        assert!(config.auth_in_url);

        let rebuilt = ClickHouseConfigBuilder::from_config(config)
            .build()
            .expect("重新构建");
        assert_eq!(rebuilt.database, "analytics");
    }

    #[test]
    fn builder_rejects_invalid_config() {
        let error = ClickHouseConfig::builder()
            .host("")
            .build()
            .expect_err("空 host 必须拒绝");
        assert!(!error.is_retryable(), "配置错误不可重试");
    }
}
