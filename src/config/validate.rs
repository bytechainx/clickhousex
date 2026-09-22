//! 配置校验与明文 HTTP 放行判定。
//!
//! 从门面 `config.rs` 下沉（`MR-STRUCT-007` 腾余量）。子模块可访问父模块的
//! `ENV_PLAIN_HTTP_HOSTS` 常量；`host_allows_plain_http_with_list` 因被门面测试
//! 直接驱动而提为 `pub(super)`，另两个 host 辅助只被本模块的 `validate` 使用、保持私有。

use crate::error::{ClickHouseError, ClickHouseResult};

use super::{ClickHouseConfig, ENV_PLAIN_HTTP_HOSTS};

impl ClickHouseConfig {
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
        // 重试配置 fail-fast：非法预算（超上界/零延迟/max<initial）在建立连接前拒绝
        self.retry.validate()?;
        Ok(())
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
pub(super) fn host_allows_plain_http_with_list(host: &str, list: Option<&str>) -> bool {
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
