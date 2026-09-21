//! 客户端公开数据类型：批量写入选项、运行时统计快照与健康检查结果。

/// 批量插入选项。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BatchInsertOptions {
    /// 每个 HTTP 请求最大行数（0 会被抬升为 1）。
    pub max_rows_per_chunk: usize,
    /// 每个 HTTP 请求最大字节数（0 表示不限制；单行超限时仍单独成块）。
    pub max_bytes_per_chunk: usize,
    /// 单次 [`crate::ClickHousePool::insert_batch`] 允许的最大总行数（0 表示不限制）。
    pub batch_size: usize,
}

impl Default for BatchInsertOptions {
    fn default() -> Self {
        Self {
            max_rows_per_chunk: 1000,
            max_bytes_per_chunk: 0,
            batch_size: 0,
        }
    }
}

impl BatchInsertOptions {
    /// 设置每个 HTTP 请求的最大行数。
    #[must_use]
    pub fn max_rows_per_chunk(mut self, max_rows_per_chunk: usize) -> Self {
        self.max_rows_per_chunk = max_rows_per_chunk;
        self
    }

    /// 设置每个 HTTP 请求的最大字节数（0 表示不限制）。
    #[must_use]
    pub fn max_bytes_per_chunk(mut self, max_bytes_per_chunk: usize) -> Self {
        self.max_bytes_per_chunk = max_bytes_per_chunk;
        self
    }

    /// 设置单次批量插入允许的最大总行数（0 表示不限制）。
    #[must_use]
    pub fn batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }
}

/// 连接池 / 客户端运行时快照。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClickHousePoolStats {
    /// 并发额度上限（连接池为 `max_in_flight`，单连接客户端为 1）。
    pub total: usize,
    /// 当前可立即使用的额度。
    pub open: usize,
    /// 正在执行的请求数。
    pub in_flight: usize,
    /// 正在等待额度的请求数。
    pub waiters: usize,
    /// 累计成功完成的请求数。
    pub ok: u64,
    /// 累计失败的请求数。
    pub error: u64,
    /// 是否已关闭。
    pub closed: bool,
}

/// 健康检查结果。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClickHouseHealth {
    /// `SELECT 1` 是否成功。
    pub healthy: bool,
    /// 服务端版本（`SELECT version()`；失败时为 `None`）。
    pub version: Option<String>,
    /// `SELECT 1` 往返耗时（毫秒）。
    pub latency_ms: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn batch_options_default_and_builders() {
        let options = BatchInsertOptions::default();
        assert_eq!(options.max_rows_per_chunk, 1000);
        assert_eq!(options.max_bytes_per_chunk, 0);
        assert_eq!(options.batch_size, 0);

        let tuned = BatchInsertOptions::default()
            .max_rows_per_chunk(2)
            .max_bytes_per_chunk(64)
            .batch_size(10);
        assert_eq!(tuned.max_rows_per_chunk, 2);
        assert_eq!(tuned.max_bytes_per_chunk, 64);
        assert_eq!(tuned.batch_size, 10);
    }
}
