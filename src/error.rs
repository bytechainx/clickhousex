//! ClickHouse 错误类型。
//!
//! 远端错误只保留 HTTP 状态码与 ClickHouse 数字错误码；服务端响应正文、
//! SQL 片段、payload 与认证信息一律不进入错误消息，避免随日志外泄。

/// `clickhousex` 统一错误类型。
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ClickHouseError {
    /// 配置非法（构造或校验阶段即可判定）。
    #[error("配置无效: {0}")]
    Config(String),
    /// 连接建立或维护失败（远端不可达、被拒绝、连接被中断）。
    #[error("连接失败: {0}")]
    Connection(String),
    /// 远端返回业务或协议错误（不可重试：语法、未知表、认证失败等）。
    #[error("远端返回错误: {0}")]
    Backend(String),
    /// 远端暂时不可用（HTTP 429 / 5xx / 显式超时），重试有机会成功。
    #[error("远端暂时不可用: {0}")]
    Unavailable(String),
    /// 序列化或解析失败（例如 insert 行不是 JSON object）。
    #[error("序列化失败: {0}")]
    Serialization(String),
    /// 网络或底层 I/O 失败。
    #[error("I/O 失败: {0}")]
    Io(#[from] std::io::Error),
    /// 操作超时（请求超时或等待并发额度超时）。
    #[error("操作超时: {0}")]
    Timeout(String),
    /// 调用参数非法（例如 SQL 标识符不合法）。
    #[error("参数无效: {0}")]
    Invalid(String),
    /// 目标资源已关闭。
    #[error("资源已关闭: {0}")]
    Closed(String),
    /// 当前能力不支持。
    #[error("不支持的操作: {0}")]
    Unsupported(String),
}

impl ClickHouseError {
    /// 是否属于可以安全重试的瞬时错误。
    ///
    /// - 可重试：[`ClickHouseError::Connection`]、[`ClickHouseError::Unavailable`]、
    ///   [`ClickHouseError::Timeout`]、[`ClickHouseError::Io`]。
    /// - 不可重试：配置、参数、序列化、远端业务错误与已关闭资源。
    #[must_use]
    pub fn is_retryable(&self) -> bool {
        match self {
            Self::Connection(_) | Self::Unavailable(_) | Self::Timeout(_) | Self::Io(_) => true,
            Self::Config(_)
            | Self::Backend(_)
            | Self::Serialization(_)
            | Self::Invalid(_)
            | Self::Closed(_)
            | Self::Unsupported(_) => false,
        }
    }
}

/// crate 专用 `Result` 别名。
pub type ClickHouseResult<T> = Result<T, ClickHouseError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retryable_matrix_is_exhaustive() {
        let retryable = [
            ClickHouseError::Connection("x".into()),
            ClickHouseError::Unavailable("x".into()),
            ClickHouseError::Timeout("x".into()),
            ClickHouseError::Io(std::io::Error::other("x")),
        ];
        for error in retryable {
            assert!(error.is_retryable(), "{error:?} 应可重试");
        }

        let permanent = [
            ClickHouseError::Config("x".into()),
            ClickHouseError::Backend("x".into()),
            ClickHouseError::Serialization("x".into()),
            ClickHouseError::Invalid("x".into()),
            ClickHouseError::Closed("x".into()),
            ClickHouseError::Unsupported("x".into()),
        ];
        for error in permanent {
            assert!(!error.is_retryable(), "{error:?} 不应可重试");
        }
    }

    #[test]
    fn io_error_converts_via_from() {
        fn read() -> ClickHouseResult<()> {
            Err(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "eof").into())
        }
        assert!(matches!(read(), Err(ClickHouseError::Io(_))));
    }
}
