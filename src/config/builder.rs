//! [`ClickHouseConfigBuilder`] 的链式构建器实现。
//!
//! 从门面 `config.rs` 下沉（`MR-STRUCT-007` 腾余量）。搬走的 15 个构建方法全是
//! `pub`，故**无需任何可见性调整**；结构体本身留在门面。

use std::path::PathBuf;
use std::time::Duration;

use crate::error::ClickHouseResult;

use super::{ClickHouseConfig, ClickHouseConfigBuilder};

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
