//! ClickHouse HTTP 传输与编码辅助。
//!
//! 本模块集中 `client` 的**实现细节**：URL 构造、参数与标识符校验、行编码与字节分片、
//! HTTP 客户端构造，以及传输 / HTTP 错误到 [`ClickHouseError`] 的映射。
//! 这些项均为 crate 内部使用，不构成公共 API。

use reqwest::StatusCode;
use serde_json::Value;

use crate::config::ClickHouseConfig;
use crate::error::{ClickHouseError, ClickHouseResult};

use super::{ERROR_RESPONSE_CAPTURE_LIMIT, MAX_IDENT_LEN};

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

/// 校验并序列化待写入的行（每行一个 `JSONEachRow` 文档）。
pub(super) fn encode_rows(rows: &[Value]) -> ClickHouseResult<Vec<String>> {
    let mut encoded = Vec::with_capacity(rows.len());
    for row in rows {
        if !row.is_object() {
            return Err(ClickHouseError::Serialization(
                "insert 行必须为 JSON object".to_owned(),
            ));
        }
        encoded.push(row.to_string());
    }
    Ok(encoded)
}

/// 把已序列化的行拼成请求体（每行以 `\n` 结尾）。
pub(super) fn join_lines(lines: &[String]) -> String {
    let mut body = String::new();
    for line in lines {
        body.push_str(line);
        body.push('\n');
    }
    body
}

/// 在 `[start, end)` 内按字节上限继续切分。
///
/// `max_bytes` 为 0 时返回原区间；单行超过上限时该行单独成块（保证推进）。
pub(super) fn split_by_bytes(
    encoded: &[String],
    start: usize,
    end: usize,
    max_bytes: usize,
) -> Vec<(usize, usize)> {
    if max_bytes == 0 || start >= end {
        return vec![(start, end)];
    }
    let mut chunks = Vec::new();
    let mut chunk_start = start;
    while chunk_start < end {
        let mut chunk_end = chunk_start;
        let mut bytes = 0usize;
        while chunk_end < end {
            let next = encoded[chunk_end].len() + 1;
            if chunk_end > chunk_start && bytes + next > max_bytes {
                break;
            }
            bytes += next;
            chunk_end += 1;
        }
        chunks.push((chunk_start, chunk_end));
        chunk_start = chunk_end;
    }
    chunks
}

/// 构造 `reqwest::Client`（连接池、超时与 TLS 材料）。
pub(super) fn build_http_client(config: &ClickHouseConfig) -> ClickHouseResult<reqwest::Client> {
    let mut builder = reqwest::Client::builder()
        .timeout(config.timeout)
        .pool_max_idle_per_host(config.max_idle_per_host);
    if let Some(connect_timeout) = config.connect_timeout {
        builder = builder.connect_timeout(connect_timeout);
    }

    if let Some(path) = &config.tls_ca_file {
        let pem = std::fs::read(path).map_err(|error| {
            ClickHouseError::Config(format!(
                "无法读取 TLS CA `{}`: {}",
                path.display(),
                error.kind()
            ))
        })?;
        let certificate = reqwest::Certificate::from_pem(&pem)
            .map_err(|_| ClickHouseError::Config("TLS CA 不是合法 PEM".to_owned()))?;
        builder = builder.add_root_certificate(certificate);
    }

    if let (Some(cert_path), Some(key_path)) =
        (&config.tls_client_cert_file, &config.tls_client_key_file)
    {
        let cert_pem = std::fs::read(cert_path).map_err(|error| {
            ClickHouseError::Config(format!(
                "无法读取客户端证书 `{}`: {}",
                cert_path.display(),
                error.kind()
            ))
        })?;
        let key_pem = std::fs::read(key_path).map_err(|error| {
            ClickHouseError::Config(format!(
                "无法读取客户端私钥 `{}`: {}",
                key_path.display(),
                error.kind()
            ))
        })?;
        // reqwest::Identity::from_pem 要求 cert + key 合并为单个 PEM buffer。
        let mut combined = cert_pem;
        combined.extend_from_slice(&key_pem);
        let identity = reqwest::Identity::from_pem(&combined)
            .map_err(|_| ClickHouseError::Config("客户端证书/私钥 PEM 无效".to_owned()))?;
        builder = builder.identity(identity);
    }

    builder
        .build()
        .map_err(|_| ClickHouseError::Config("HTTP 客户端构建失败（TLS 配置非法）".to_owned()))
}

/// 把传输层错误映射为连接/超时错误（不回显 URL、正文或凭据）。
pub(super) fn map_transport_error(error: &reqwest::Error) -> ClickHouseError {
    if error.is_timeout() {
        return ClickHouseError::Timeout("请求超时（远端未在时限内响应）".to_owned());
    }
    let stage = if error.is_connect() {
        "建立连接"
    } else if error.is_body() {
        "发送请求体"
    } else if error.is_decode() {
        "解析响应"
    } else if error.is_redirect() {
        "跟随重定向"
    } else if error.is_request() {
        "构造请求"
    } else {
        "传输"
    };
    ClickHouseError::Connection(format!("{stage}失败（远端不可达或被拒绝）"))
}

/// 把 HTTP 状态码 + ClickHouse 错误码映射为错误；响应正文永不进入消息。
pub(super) fn map_http_error(status: StatusCode, body: &[u8]) -> ClickHouseError {
    let server_code = clickhouse_server_code(body);
    let context = safe_http_error_context(status, server_code);
    if status == StatusCode::UNAUTHORIZED || status == StatusCode::FORBIDDEN {
        return ClickHouseError::Backend(format!("认证/授权失败；{context}"));
    }
    // 429 与 5xx 属于可安全重试的瞬时错误。
    if status == StatusCode::TOO_MANY_REQUESTS || status.is_server_error() {
        return ClickHouseError::Unavailable(context);
    }
    match server_code {
        // UNKNOWN_TABLE(60) / UNKNOWN_DATABASE(81) / TABLE_ALREADY_EXISTS(57)。
        Some(60 | 81 | 57) => ClickHouseError::Backend(context),
        // TIMEOUT_EXCEEDED(159)：服务端侧超时，可重试。
        Some(159) => ClickHouseError::Unavailable(context),
        // 其余 4xx 多为 SQL 语法或参数问题。
        _ => ClickHouseError::Backend(context),
    }
}

/// 读取错误响应前缀（有界，避免无界读取大响应）。
pub(super) async fn read_error_prefix(
    mut response: reqwest::Response,
) -> ClickHouseResult<Vec<u8>> {
    let mut prefix = Vec::with_capacity(ERROR_RESPONSE_CAPTURE_LIMIT);
    while prefix.len() < ERROR_RESPONSE_CAPTURE_LIMIT {
        let chunk = response
            .chunk()
            .await
            .map_err(|_| ClickHouseError::Connection("读取错误响应失败".to_owned()))?;
        let Some(chunk) = chunk else { break };
        let remaining = ERROR_RESPONSE_CAPTURE_LIMIT - prefix.len();
        prefix.extend_from_slice(&chunk[..chunk.len().min(remaining)]);
        if chunk.len() >= remaining {
            break;
        }
    }
    Ok(prefix)
}

/// 解析 ClickHouse 错误体开头的 `Code: <n>`（仅扫描前缀）。
pub(super) fn clickhouse_server_code(body: &[u8]) -> Option<u32> {
    let body = std::str::from_utf8(body).ok()?.trim_start();
    let rest = body.strip_prefix("Code:")?.trim_start();
    let digits = rest
        .chars()
        .take_while(char::is_ascii_digit)
        .collect::<String>();
    if digits.is_empty() {
        None
    } else {
        digits.parse().ok()
    }
}

/// 生成不含响应正文的错误上下文。
fn safe_http_error_context(status: StatusCode, server_code: Option<u32>) -> String {
    match server_code {
        Some(code) => format!("HTTP {status}（server_code={code}，响应正文已省略）"),
        None => format!("HTTP {status}（响应正文已省略）"),
    }
}
