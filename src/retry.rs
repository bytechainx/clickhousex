//! ClickHouse 重试策略：指数退避 + 抖动。
//!
//! 重试判定完全依赖 [`ClickHouseError::is_retryable`]：
//! - 可重试错误（网络抖动、超时、HTTP 429/5xx、远端暂不可用）→ 按配置退避后重试；
//! - 永久错误（配置、参数、序列化、远端业务错误、已关闭资源）→ 立即返回。
//!
//! 重试退避公式：
//! `delay = min(initial_delay * 2^(attempt-1), max_delay)`，叠加 ±25% 随机抖动，
//! 避免多实例同步重试造成惊群。
//!
//! # 非幂等写入约束
//!
//! 组织规则 R-RT-031 要求非幂等写操作默认不重试。本 crate 在 [`super::Inner::post_query`]
//! 层区分读写路径：`execute`、`insert_batch`、`insert_json_each_row` 等写操作不经重试；
//! 仅 `query`、`query_text`、`query_with_params`、`ping`、`health_check`
//! 等查询/连接类路径启用重试。调用方若确定写入具有幂等性，可通过
//! [`RetryConfig::enabled`] 全局启用并自行承担重复写入风险。

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tracing::debug;

use crate::error::{ClickHouseError, ClickHouseResult};

/// 重试次数硬上界，防止错误配置造成无界放大。
pub const MAX_RETRIES: u32 = 10;

/// 重试配置。
///
/// 所有字段均可通过 [`ClickHouseConfigBuilder`](crate::ClickHouseConfigBuilder) 设置；
/// 默认值保守（3 次 / 100ms 起 / 5s 上限 / 启用）。
///
/// # 示例
///
/// ```no_run
/// use std::time::Duration;
/// use clickhousex::{ClickHouseConfig, RetryConfig};
///
/// let config = ClickHouseConfig::builder()
///     .retry(RetryConfig {
///         max_retries: 5,
///         initial_delay: Duration::from_millis(50),
///         ..Default::default()
///     })
///     .build()
///     .unwrap();
/// ```
#[derive(Clone, Debug, PartialEq)]
pub struct RetryConfig {
    /// 最大重试次数（不含首次尝试）。例如 3 表示最多 1 次首发 + 3 次重试 = 4 次总尝试。
    /// 合法范围 `0..=MAX_RETRIES`（10 次硬上界保留一定缓冲）。
    pub max_retries: u32,
    /// 首次退避延迟。
    pub initial_delay: Duration,
    /// 单次退避上限（退避按指数增长但不会超过此值）。
    pub max_delay: Duration,
    /// 是否启用重试。关闭后所有操作直接执行单次请求，
    /// 可避免对非幂等写入的意外重试。
    pub enabled: bool,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_retries: 3,
            initial_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(5),
            enabled: true,
        }
    }
}

impl RetryConfig {
    /// 校验配置合法性；失败返回 [`ClickHouseError::Config`]。
    ///
    /// 校验规则：
    /// - `max_retries` 不得超过 [`MAX_RETRIES`]；
    /// - `max_delay` 不得小于 `initial_delay`；
    /// - `initial_delay` 与 `max_delay` 均不得为零（启用重试时）。
    pub fn validate(&self) -> ClickHouseResult<()> {
        if self.max_retries > MAX_RETRIES {
            return Err(ClickHouseError::Config(format!(
                "retry max_retries {} 超过硬上界 {MAX_RETRIES}",
                self.max_retries
            )));
        }
        if self.max_delay < self.initial_delay {
            return Err(ClickHouseError::Config(
                "retry max_delay 不得小于 initial_delay".to_owned(),
            ));
        }
        if self.enabled {
            if self.initial_delay.is_zero() {
                return Err(ClickHouseError::Config(
                    "retry initial_delay 不得为零（启用重试时）".to_owned(),
                ));
            }
            if self.max_delay.is_zero() {
                return Err(ClickHouseError::Config(
                    "retry max_delay 不得为零（启用重试时）".to_owned(),
                ));
            }
        }
        Ok(())
    }

    /// 计算第 `retry_index` 次（1 起）重试前的退避时长。
    ///
    /// 指数退避公式：`min(initial_delay * 2^(retry_index-1), max_delay)`，
    /// 再叠加 ±25% 随机抖动。
    #[must_use]
    pub fn delay_for(&self, retry_index: u32) -> Duration {
        let base_ms = self.initial_delay.as_millis() as u64;
        let max_ms = self.max_delay.as_millis() as u64;
        if base_ms == 0 || max_ms == 0 {
            return Duration::ZERO;
        }
        let shift = retry_index.saturating_sub(1).min(31);
        let capped = base_ms.saturating_mul(1u64 << shift).min(max_ms);
        // 叠加 ±25% 抖动，保证不会被抖成零
        let jittered = jitter(capped);
        Duration::from_millis(jittered.max(1))
    }
}

/// 对可重试操作执行退避重试。
///
/// - 可重试错误 → 按配置退避后重试，直到 `max_retries` 用尽；
/// - 永久错误 → 立即返回，不消耗重试预算；
/// - 配置非法 → 直接返回 [`ClickHouseError::Config`]。
///
/// `op_name` 仅用于日志上下文（中文）。
pub(super) async fn execute_with_retry<F, Fut, T>(
    config: &RetryConfig,
    op_name: &str,
    mut operation: F,
) -> ClickHouseResult<T>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = ClickHouseResult<T>>,
{
    config.validate()?;

    let mut attempt: u32 = 0;
    loop {
        match operation().await {
            Ok(value) => return Ok(value),
            Err(error) => {
                if !error.is_retryable() || attempt >= config.max_retries {
                    return Err(error);
                }
                attempt += 1;
                let delay = config.delay_for(attempt);
                debug!(
                    target: "clickhousex",
                    op = op_name,
                    attempt = attempt,
                    max_retries = config.max_retries,
                    delay_ms = delay.as_millis() as u64,
                    error = %error,
                    "clickhouse 操作失败，按退避策略重试",
                );
                if !delay.is_zero() {
                    tokio::time::sleep(delay).await;
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// 无依赖伪随机抖动（xorshift64），仅供退避使用，不需要密码学强度。
// ---------------------------------------------------------------------------

/// 抖动因子常量（±25%）。
const JITTER_RATIO: f64 = 0.25;

/// 金伽马（xorshift 乘数）。
const GOLDEN_GAMMA: u64 = 0x9E37_79B9_7F4A_7C15;
/// 兜底种子。
const FALLBACK_SEED: u64 = 0x2545_F491_4F6C_DD1D;

static STATE: AtomicU64 = AtomicU64::new(0);
static COUNTER: AtomicU64 = AtomicU64::new(0);

/// 在 `±JITTER_RATIO` 范围内抖动延迟毫秒值。
fn jitter(base_ms: u64) -> u64 {
    let factor = 1.0 - JITTER_RATIO + 2.0 * JITTER_RATIO * random_unit();
    (base_ms as f64 * factor).max(1.0) as u64
}

/// 返回 `[0, 1)` 范围内的无依赖伪随机数。
fn random_unit() -> f64 {
    let mut state = STATE.load(Ordering::Relaxed);
    if state == 0 {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|elapsed| elapsed.as_nanos() as u64)
            .unwrap_or(FALLBACK_SEED);
        state = nanos ^ COUNTER.fetch_add(GOLDEN_GAMMA, Ordering::Relaxed);
        if state == 0 {
            state = FALLBACK_SEED;
        }
    }
    state ^= state << 13;
    state ^= state >> 7;
    state ^= state << 17;
    STATE.store(state, Ordering::Relaxed);
    (state >> 11) as f64 / (1u64 << 53) as f64
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicU32;

    // -----------------------------------------------------------------------
    // RetryConfig
    // -----------------------------------------------------------------------

    #[test]
    fn default_config_is_valid() {
        RetryConfig::default()
            .validate()
            .expect("默认配置必须通过校验");
    }

    #[test]
    fn validate_rejects_excessive_max_retries() {
        let err = RetryConfig {
            max_retries: MAX_RETRIES + 1,
            ..Default::default()
        }
        .validate()
        .expect_err("超过硬上界必须拒绝");
        assert!(err.to_string().contains("硬上界"));
    }

    #[test]
    fn validate_rejects_max_delay_less_than_initial() {
        let err = RetryConfig {
            initial_delay: Duration::from_millis(200),
            max_delay: Duration::from_millis(100),
            ..Default::default()
        }
        .validate()
        .expect_err("max_delay < initial_delay 必须拒绝");
        assert!(err.to_string().contains("max_delay"));
    }

    #[test]
    fn validate_rejects_zero_delays_when_enabled() {
        let err = RetryConfig {
            initial_delay: Duration::ZERO,
            ..Default::default()
        }
        .validate()
        .expect_err("零 initial_delay 必须拒绝");
        assert!(err.to_string().contains("initial_delay"));

        let err = RetryConfig {
            max_delay: Duration::ZERO,
            ..Default::default()
        }
        .validate()
        .expect_err("零 max_delay 必须拒绝");
        assert!(err.to_string().contains("max_delay"));
    }

    #[test]
    fn validate_passes_when_disabled_with_zero_delays() {
        RetryConfig {
            max_retries: 0,
            initial_delay: Duration::ZERO,
            max_delay: Duration::ZERO,
            enabled: false,
        }
        .validate()
        .expect("禁用时零延迟应通过");
    }

    #[test]
    fn delay_for_exponential_growth_and_cap() {
        let config = RetryConfig {
            max_retries: 5,
            initial_delay: Duration::from_millis(100),
            max_delay: Duration::from_millis(400),
            enabled: true,
        };
        // 无抖动校验（两次调用可能不同值，仅断言范围）
        let d1 = config.delay_for(1).as_millis() as u64;
        let d2 = config.delay_for(2).as_millis() as u64;
        let d3 = config.delay_for(3).as_millis() as u64;
        let d4 = config.delay_for(4).as_millis() as u64;
        assert!((75..=125).contains(&d1), "d1={d1} 应在 100±25% 范围");
        assert!((150..=250).contains(&d2), "d2={d2} 应在 200±25% 范围");
        assert!((300..=500).contains(&d3), "d3={d3} 应在 400±25% 范围");
        assert!((300..=500).contains(&d4), "d4={d4} 应封顶在 400±25%");
    }

    #[test]
    fn delay_for_zero_base_returns_zero() {
        let config = RetryConfig {
            max_retries: 3,
            initial_delay: Duration::ZERO,
            max_delay: Duration::from_secs(5),
            enabled: false,
        };
        assert_eq!(config.delay_for(1), Duration::ZERO);
        assert_eq!(config.delay_for(99), Duration::ZERO);
    }

    #[test]
    fn jitter_stays_inside_configured_window() {
        let config = RetryConfig {
            max_retries: 3,
            initial_delay: Duration::from_millis(1_000),
            max_delay: Duration::from_millis(1_000),
            enabled: true,
        };
        let mut seen = std::collections::HashSet::new();
        for _ in 0..64 {
            let millis = config.delay_for(1).as_millis() as u64;
            assert!((750..=1_250).contains(&millis), "抖动越界: {millis}");
            seen.insert(millis);
        }
        assert!(seen.len() > 1, "抖动必须产生随机性: {seen:?}");
    }

    #[test]
    fn delay_for_shift_does_not_overflow() {
        let config = RetryConfig {
            max_retries: MAX_RETRIES,
            initial_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(30),
            enabled: true,
        };
        // 极大 retry_index 不会 panic
        let _ = config.delay_for(u32::MAX);
    }

    // -----------------------------------------------------------------------
    // execute_with_retry
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn retries_transient_error_then_succeeds() {
        let attempts = AtomicU32::new(0);
        let config = RetryConfig {
            max_retries: 5,
            initial_delay: Duration::from_millis(1),
            max_delay: Duration::from_millis(10),
            enabled: true,
        };
        let result = execute_with_retry(&config, "test", || {
            let n = attempts.fetch_add(1, Ordering::SeqCst) + 1;
            async move {
                if n < 3 {
                    Err(ClickHouseError::Connection(format!("blip-{n}")))
                } else {
                    Ok(42u32)
                }
            }
        })
        .await
        .expect("重试后应成功");
        assert_eq!(result, 42);
        assert_eq!(attempts.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn exhausts_retry_budget_and_returns_last_error() {
        let attempts = AtomicU32::new(0);
        let config = RetryConfig {
            max_retries: 2,
            initial_delay: Duration::from_millis(1),
            max_delay: Duration::from_millis(10),
            enabled: true,
        };
        let error = execute_with_retry(&config, "always-fail", || {
            attempts.fetch_add(1, Ordering::SeqCst);
            async { Err::<(), _>(ClickHouseError::Timeout("still down".into())) }
        })
        .await
        .expect_err("必须耗尽预算");
        assert!(error.is_retryable());
        assert_eq!(attempts.load(Ordering::SeqCst), 3); // 1 首次 + 2 重试
    }

    #[tokio::test]
    async fn non_retryable_error_returns_immediately() {
        let attempts = AtomicU32::new(0);
        let config = RetryConfig {
            max_retries: 5,
            initial_delay: Duration::from_millis(1),
            max_delay: Duration::from_millis(10),
            enabled: true,
        };
        let error = execute_with_retry(&config, "bad-config", || {
            attempts.fetch_add(1, Ordering::SeqCst);
            async { Err::<(), _>(ClickHouseError::Config("非法配置".into())) }
        })
        .await
        .expect_err("永久错误必须立即失败");
        assert!(matches!(error, ClickHouseError::Config(_)));
        assert_eq!(
            attempts.load(Ordering::SeqCst),
            1,
            "永久错误不得消耗重试预算"
        );
    }

    #[tokio::test]
    async fn retry_disabled_still_calls_once() {
        let attempts = AtomicU32::new(0);
        let config = RetryConfig {
            max_retries: 5,
            initial_delay: Duration::from_millis(1),
            max_delay: Duration::from_millis(10),
            enabled: false,
        };
        // 注意：即使 RetryConfig::enabled = false，execute_with_retry 内部的
        // 重试机制仍生效，enabled 的控制在调用方（post_query_retry 检查）。
        // 这里直接测试核心重试逻辑，不涉及 enabled 网关。
        let result = execute_with_retry(&config, "test", || {
            attempts.fetch_add(1, Ordering::SeqCst);
            async { Ok::<_, ClickHouseError>("ok") }
        })
        .await
        .expect("应成功");
        assert_eq!(result, "ok");
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn backoff_sequence_respects_intervals() {
        use std::time::Instant;

        let config = RetryConfig {
            max_retries: 2,
            initial_delay: Duration::from_millis(10),
            max_delay: Duration::from_millis(50),
            enabled: true,
        };
        let started = Instant::now();
        let _error = execute_with_retry(&config, "backoff-test", || async {
            Err::<(), _>(ClickHouseError::Connection("fail".into()))
        })
        .await;
        let elapsed = started.elapsed();
        // 至少经历两次退避（attempt 1: ~10ms jittered, attempt 2: ~20ms jittered）
        assert!(
            elapsed >= Duration::from_millis(15), // 最小: 7.5+15ms
            "退避总时长应 ≥ 15ms，实际 {elapsed:?}"
        );
    }

    #[tokio::test]
    async fn invalid_config_returns_error_immediately() {
        let config = RetryConfig {
            max_retries: MAX_RETRIES + 1,
            ..Default::default()
        };
        let error = execute_with_retry(&config, "bad", || async { Ok::<_, ClickHouseError>(()) })
            .await
            .expect_err("非法配置必须 fail-closed");
        assert!(matches!(error, ClickHouseError::Config(_)));
    }

    // -----------------------------------------------------------------------
    // random_unit / jitter
    // -----------------------------------------------------------------------

    #[test]
    fn random_unit_is_in_unit_interval() {
        for _ in 0..1_000 {
            let value = random_unit();
            assert!((0.0..1.0).contains(&value), "{value}");
        }
    }
}
