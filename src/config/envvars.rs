//! 环境变量加载层：`from_env` 与其读取 / 解析辅助。
//!
//! 从门面 `config.rs` 下沉（`MR-STRUCT-007` 腾余量）。子模块可访问父模块的
//! `ENV_*` 常量与 `ClickHouseConfig` 私有字段，故只有被门面测试直接驱动的
//! `env_parsed` / `resolve_http_port` 提为 `pub(super)`。

use std::path::PathBuf;
use std::time::Duration;

use crate::error::{ClickHouseError, ClickHouseResult};

use super::{
    ClickHouseConfig, ENV_ACQUIRE_TIMEOUT_MS, ENV_AUTH_IN_URL, ENV_CONNECT_TIMEOUT_MS,
    ENV_DATABASE, ENV_HOST, ENV_HTTP_PORT, ENV_MAX_IDLE_PER_HOST, ENV_MAX_IN_FLIGHT, ENV_PASSWORD,
    ENV_PORT, ENV_TIMEOUT_MS, ENV_TLS, ENV_TLS_CA_FILE, ENV_TLS_CLIENT_CERT_FILE,
    ENV_TLS_CLIENT_KEY_FILE, ENV_USER,
};

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
pub(super) fn env_parsed<T>(name: &str) -> ClickHouseResult<Option<T>>
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
pub(super) fn resolve_http_port(
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
