//! 结果行解析、分块与写入体编码（纯函数，无网络 IO）。

use serde_json::Value;

use crate::error::{ClickHouseError, ClickHouseResult};

/// 解析 ClickHouse 默认 `TabSeparated` 文本为行列。
///
/// 跳过空行；按 tab 分列。纯函数。
///
/// # Examples
///
/// ```
/// use clickhousex::parse_tab_separated_rows;
///
/// let rows = parse_tab_separated_rows("id\tname\n1\tfoo\n\n");
/// assert_eq!(
///     rows,
///     vec![
///         vec!["id".to_owned(), "name".to_owned()],
///         vec!["1".to_owned(), "foo".to_owned()],
///     ]
/// );
/// ```
#[must_use]
pub fn parse_tab_separated_rows(text: &str) -> Vec<Vec<String>> {
    let mut rows = Vec::new();
    for line in text.lines() {
        if line.is_empty() {
            continue;
        }
        rows.push(line.split('\t').map(str::to_owned).collect());
    }
    rows
}

/// 计算分块范围：`(start, end)` 半开区间。
///
/// `max_per_chunk` 为 0 时抬升为 1；`total` 为 0 时返回空。纯函数。
#[must_use]
pub fn chunk_ranges(total: usize, max_per_chunk: usize) -> Vec<(usize, usize)> {
    if total == 0 {
        return Vec::new();
    }
    let size = max_per_chunk.max(1);
    let mut ranges = Vec::new();
    let mut start = 0;
    while start < total {
        let end = (start + size).min(total);
        ranges.push((start, end));
        start = end;
    }
    ranges
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chunk_ranges_concrete_sizes() {
        assert!(chunk_ranges(0, 10).is_empty());
        assert_eq!(chunk_ranges(5, 10), vec![(0, 5)]);
        assert_eq!(chunk_ranges(5, 2), vec![(0, 2), (2, 4), (4, 5)]);
        assert_eq!(chunk_ranges(3, 1), vec![(0, 1), (1, 2), (2, 3)]);
        assert_eq!(chunk_ranges(2, 0), vec![(0, 1), (1, 2)]);
        assert_eq!(chunk_ranges(7, 3), vec![(0, 3), (3, 6), (6, 7)]);
    }

    #[test]
    fn parse_tab_separated_rows_skips_blank_lines() {
        let rows = parse_tab_separated_rows("a\tb\n\nc\td\te\n");
        assert_eq!(
            rows,
            vec![
                vec!["a".to_owned(), "b".to_owned()],
                vec!["c".to_owned(), "d".to_owned(), "e".to_owned()]
            ]
        );
        assert!(parse_tab_separated_rows("").is_empty());
        assert!(parse_tab_separated_rows("\n\n\n").is_empty());
    }

    #[test]
    fn split_by_bytes_keeps_progress_for_oversized_line() {
        let encoded = vec!["a".repeat(10), "b".repeat(4), "c".repeat(4)];
        assert_eq!(split_by_bytes(&encoded, 0, 3, 0), vec![(0, 3)]);
        // 单行 11 字节已超过上限，仍须单独成块。
        assert_eq!(split_by_bytes(&encoded, 0, 3, 12), vec![(0, 1), (1, 3)]);
        assert_eq!(split_by_bytes(&encoded, 1, 3, 100), vec![(1, 3)]);
        assert_eq!(split_by_bytes(&encoded, 2, 2, 5), vec![(2, 2)]);
    }

    #[test]
    fn encode_rows_rejects_non_object() {
        let error = encode_rows(&[serde_json::json!(["not", "an", "object"])])
            .expect_err("非 object 行必须拒绝");
        assert!(matches!(error, ClickHouseError::Serialization(_)));

        let encoded = encode_rows(&[serde_json::json!({ "a": 1 }), serde_json::json!({ "b": 2 })])
            .expect("object 行应可序列化");
        assert_eq!(encoded.len(), 2);
        assert_eq!(join_lines(&encoded), "{\"a\":1}\n{\"b\":2}\n");
    }
}
