//! 查询 URL 构造与 SQL 标识符 / 查询参数校验。

use crate::config::ClickHouseConfig;
use crate::error::{ClickHouseError, ClickHouseResult};

/// SQL 标识符最大长度。
const MAX_IDENT_LEN: usize = 192;

/// 构造查询 URL（纯函数，不含网络 IO）。
///
/// 始终附带 `database` 查询参数；当 [`ClickHouseConfig::auth_in_url`] 为 `true`
/// 时附带 `user` / `password`；`params` 以 `param_<name>` 追加。
///
/// # Errors
///
/// 配置的 host/port 无法构成合法 URL，或参数名非法时返回
/// [`ClickHouseError::Config`] / [`ClickHouseError::Invalid`]。
pub fn build_query_url(
    config: &ClickHouseConfig,
    params: &[(&str, &str)],
) -> ClickHouseResult<String> {
    validate_params(params)?;
    Ok(build_url(config, params)?.to_string())
}

/// 内部 URL 构造。
pub(super) fn build_url(
    config: &ClickHouseConfig,
    params: &[(&str, &str)],
) -> ClickHouseResult<url::Url> {
    let mut url = url::Url::parse(&config.base_url()).map_err(|_| {
        ClickHouseError::Config(format!("host/port 无法构成合法 URL: {}", config.base_url()))
    })?;
    {
        let mut query = url.query_pairs_mut();
        query.append_pair("database", &config.database);
        if config.auth_in_url {
            query.append_pair("user", &config.user);
            query.append_pair("password", &config.password);
        }
        for (name, value) in params {
            query.append_pair(&format!("param_{name}"), value);
        }
    }
    Ok(url)
}

/// 是否需要发送 Basic 认证头。
pub(super) fn needs_basic_auth(config: &ClickHouseConfig) -> bool {
    config.user != crate::config::DEFAULT_USER || !config.password.is_empty()
}

/// 校验 SQL 标识符（表名等），避免拼接出非法或注入型语句。
pub(super) fn validate_ident(name: &str) -> ClickHouseResult<()> {
    if name.is_empty() || name.len() > MAX_IDENT_LEN {
        return Err(ClickHouseError::Invalid("标识符长度非法".to_owned()));
    }
    let mut chars = name.chars();
    let first = chars
        .next()
        .ok_or_else(|| ClickHouseError::Invalid("标识符为空".to_owned()))?;
    if !(first.is_ascii_alphabetic() || first == '_') {
        return Err(ClickHouseError::Invalid(format!(
            "标识符须以字母或下划线开头: {name}"
        )));
    }
    if !chars.all(|c| c.is_ascii_alphanumeric() || c == '_') {
        return Err(ClickHouseError::Invalid(format!(
            "标识符含非法字符: {name}"
        )));
    }
    Ok(())
}

/// 校验查询参数名。
pub(super) fn validate_params(params: &[(&str, &str)]) -> ClickHouseResult<()> {
    for (name, _) in params {
        if name.is_empty()
            || !name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
            || name.chars().next().is_some_and(|c| c.is_ascii_digit())
        {
            return Err(ClickHouseError::Invalid(format!("查询参数名非法: {name}")));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn local_config(max_in_flight: usize) -> ClickHouseConfig {
        ClickHouseConfig::builder()
            .host("127.0.0.1")
            .http_port(1)
            .timeout(Duration::from_millis(200))
            .acquire_timeout(Duration::from_millis(200))
            .max_in_flight(max_in_flight)
            .build()
            .expect("测试配置必须有效")
    }

    #[test]
    fn build_query_url_contains_database_and_params() {
        let config = local_config(4);
        let url = build_query_url(&config, &[("id", "42")]).expect("URL 构造");
        assert!(url.starts_with("http://127.0.0.1:1/?"), "url={url}");
        assert!(url.contains("database=default"));
        assert!(url.contains("param_id=42"));
        assert!(!url.contains("password"), "默认不把凭据放进 URL");

        let auth_in_url = ClickHouseConfig::builder()
            .host("127.0.0.1")
            .user("writer")
            .password("s3cret")
            .auth_in_url(true)
            .build()
            .expect("配置有效");
        let url = build_query_url(&auth_in_url, &[]).expect("URL 构造");
        assert!(url.contains("user=writer"));
        assert!(url.contains("password=s3cret"));

        assert!(build_query_url(&config, &[("bad-name", "1")]).is_err());
    }

    #[test]
    fn ident_and_param_validation() {
        assert!(validate_ident("infra_draft_smoke").is_ok());
        assert!(validate_ident("1bad").is_err());
        assert!(validate_ident("a;drop").is_err());
        assert!(validate_ident("").is_err());
        assert!(validate_ident(&"a".repeat(MAX_IDENT_LEN + 1)).is_err());
        assert!(validate_params(&[("id", "1")]).is_ok());
        assert!(validate_params(&[("1id", "1")]).is_err());
        assert!(validate_params(&[("", "1")]).is_err());
    }
}
