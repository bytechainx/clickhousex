//! HTTP 客户端构造、错误响应读取与传输 / 状态码错误映射。

use reqwest::StatusCode;

use crate::config::ClickHouseConfig;
use crate::error::{ClickHouseError, ClickHouseResult};

/// 错误响应最多读取的字节数（超限部分直接丢弃，避免无界读取）。
const ERROR_RESPONSE_CAPTURE_LIMIT: usize = 4096;

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
fn clickhouse_server_code(body: &[u8]) -> Option<u32> {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn http_error_mapping_hides_response_body() {
        let secret = "SELECT private_column; payload=secret-value";
        let body = format!("Code: 60. DB::Exception: UNKNOWN_TABLE; {secret}");

        let missing = map_http_error(StatusCode::BAD_REQUEST, body.as_bytes());
        assert!(matches!(missing, ClickHouseError::Backend(_)));
        assert!(missing.to_string().contains("server_code=60"));
        assert!(
            !missing.to_string().contains(secret),
            "错误不得回显响应正文"
        );
        assert!(!missing.is_retryable());

        let conflict = map_http_error(StatusCode::BAD_REQUEST, b"Code: 57. DB::Exception: exists");
        assert!(!conflict.is_retryable());

        let unknown_db = map_http_error(StatusCode::BAD_REQUEST, b"Code: 81. DB::Exception");
        assert!(!unknown_db.is_retryable());
    }

    #[test]
    fn http_error_mapping_classifies_retryable_statuses() {
        let too_many = map_http_error(StatusCode::TOO_MANY_REQUESTS, b"");
        assert!(matches!(too_many, ClickHouseError::Unavailable(_)));
        assert!(too_many.is_retryable());

        let server_error = map_http_error(StatusCode::INTERNAL_SERVER_ERROR, b"boom");
        assert!(server_error.is_retryable());

        let unavailable = map_http_error(StatusCode::SERVICE_UNAVAILABLE, b"");
        assert!(unavailable.is_retryable());

        let server_timeout = map_http_error(StatusCode::BAD_REQUEST, b"Code: 159. DB::Exception");
        assert!(server_timeout.is_retryable());

        let unauthorized = map_http_error(StatusCode::UNAUTHORIZED, b"");
        assert!(!unauthorized.is_retryable());
        let forbidden = map_http_error(StatusCode::FORBIDDEN, b"denied");
        assert!(!forbidden.is_retryable());
    }

    #[test]
    fn server_code_parser_is_bounded_to_prefix() {
        assert_eq!(clickhouse_server_code(b"Code: 81. DB::Exception"), Some(81));
        assert_eq!(clickhouse_server_code(b"not a ClickHouse exception"), None);
        assert_eq!(clickhouse_server_code(&[0xff, 0xfe]), None);
        assert_eq!(clickhouse_server_code(b"Code: abc"), None);
    }

    #[test]
    fn mtls_reads_reject_missing_files_without_echoing_contents() {
        let config = ClickHouseConfig::builder()
            .tls_client_cert_file("/nonexistent/cert.pem")
            .tls_client_key_file("/nonexistent/key.pem")
            .build()
            .expect("配置有效");
        let error = build_http_client(&config).expect_err("缺失证书必须失败");
        assert!(error.to_string().contains("客户端证书"));
        assert!(!error.is_retryable());

        let ca_config = ClickHouseConfig::builder()
            .tls(true)
            .tls_ca_file("/nonexistent/ca.pem")
            .build()
            .expect("配置有效");
        let error = build_http_client(&ca_config).expect_err("缺失 CA 必须失败");
        assert!(error.to_string().contains("TLS CA"));
    }
}
