//! [`Inner`] 的请求路径实现：构造、背压、关闭等待与查询收发。
//!
//! 从门面 `client.rs` 下沉，仅为缩短该文件的生产段（`MR-STRUCT-007`）。
//! `Inner` 的结构与字段仍留在门面：子模块可直接访问父模块的私有项，因此这里
//! 只把门面与 `impl_connection_api!` 实际调用到的方法标为 `pub(super)`；
//! `ensure_open` / `acquire` / `post_query_inner` 只被本模块内部调用，保持私有。

use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;

use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::time::timeout;
use tracing::debug;

use super::transport::{
    build_http_client, build_url, map_http_error, map_transport_error, needs_basic_auth,
    read_error_prefix,
};
use super::{ClickHouseHealth, ClickHousePoolStats, Inner};
use crate::config::ClickHouseConfig;
use crate::error::{ClickHouseError, ClickHouseResult};
use crate::retry::execute_with_retry;

impl Inner {
    pub(super) fn new(config: ClickHouseConfig, permits: usize) -> ClickHouseResult<Self> {
        config.validate()?;
        if permits == 0 {
            return Err(ClickHouseError::Config("并发额度必须 ≥ 1".to_owned()));
        }
        let http = build_http_client(&config)?;
        Ok(Self {
            http,
            config,
            sem: Arc::new(Semaphore::new(permits)),
            total: permits,
            in_flight: AtomicUsize::new(0),
            waiters: AtomicUsize::new(0),
            ok: AtomicU64::new(0),
            error: AtomicU64::new(0),
            closed: AtomicBool::new(false),
        })
    }

    pub(super) fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }

    pub(super) fn stats(&self) -> ClickHousePoolStats {
        let closed = self.is_closed();
        ClickHousePoolStats {
            total: self.total,
            // 已关闭时不再对外提供任何额度。
            open: if closed {
                0
            } else {
                self.sem.available_permits()
            },
            in_flight: self.in_flight.load(Ordering::Relaxed),
            waiters: self.waiters.load(Ordering::Relaxed),
            ok: self.ok.load(Ordering::Relaxed),
            error: self.error.load(Ordering::Relaxed),
            closed,
        }
    }

    /// 关闭：置关闭位以拒绝新请求，并等待在途操作释放完全部额度后返回。
    ///
    /// 新请求由 `closed` 位在 [`Inner::ensure_open`] 处立即拒绝，因此**不**关闭信号量：
    /// 在途操作各自持有 1 个额度、完成时释放，「取回全部额度」即等价于「在途操作已
    /// 全部结束」。等待上界理论上由在途请求自身的超时（`config.timeout`）隐式给出，
    /// 此处再显式加 2×`config.timeout` 兜底——防止异常路径（如请求超时未生效）下
    /// close 无限期阻塞（对抗审查 P1-2）。
    ///
    /// 兜底超时仍未取回全部额度时返回 [`ClickHouseError::Timeout`]；`closed` 位保持
    /// 置位（新请求依旧被拒绝），调用方可稍后重试 close 或查 `stats().in_flight`。
    ///
    /// 幂等：无在途操作时立即返回，重复调用同样成功。
    pub(super) async fn close(&self) -> ClickHouseResult<()> {
        self.closed.store(true, Ordering::SeqCst);
        let permits = u32::try_from(self.total).unwrap_or(u32::MAX);
        let drain_deadline = self.config.timeout.saturating_mul(2);
        let all_permits = timeout(drain_deadline, self.sem.clone().acquire_many_owned(permits))
            .await
            .map_err(|_| {
                ClickHouseError::Timeout(format!(
                    "close 等待在途操作排空超时（{}ms）；closed 位已置位，可稍后重试或查 stats().in_flight",
                    drain_deadline.as_millis()
                ))
            })?
            .map_err(|_| ClickHouseError::Closed("背压信号量已关闭".to_owned()))?;
        drop(all_permits);
        Ok(())
    }

    fn ensure_open(&self) -> ClickHouseResult<()> {
        if self.is_closed() {
            return Err(ClickHouseError::Closed("连接已关闭".to_owned()));
        }
        Ok(())
    }

    /// 获取一次 in-flight 许可（背压入口）。
    async fn acquire(&self) -> ClickHouseResult<OwnedSemaphorePermit> {
        self.ensure_open()?;
        self.waiters.fetch_add(1, Ordering::SeqCst);
        let acquired = timeout(
            self.config.acquire_timeout,
            self.sem.clone().acquire_owned(),
        )
        .await;
        self.waiters.fetch_sub(1, Ordering::SeqCst);
        match acquired {
            Ok(Ok(permit)) => {
                if self.is_closed() {
                    drop(permit);
                    return Err(ClickHouseError::Closed("连接已关闭".to_owned()));
                }
                Ok(permit)
            }
            Ok(Err(_)) => Err(ClickHouseError::Closed("背压信号量已关闭".to_owned())),
            Err(_) => Err(ClickHouseError::Timeout(format!(
                "等待 in-flight 许可超时（total={}）",
                self.total
            ))),
        }
    }

    pub(super) async fn ping(&self) -> ClickHouseResult<()> {
        let body = self.post_query_read("SELECT 1", &[]).await?;
        if body.trim() != "1" {
            return Err(ClickHouseError::Backend(
                "ping 响应不符合协议（响应正文已省略）".to_owned(),
            ));
        }
        Ok(())
    }

    pub(super) async fn health_check(&self) -> ClickHouseHealth {
        let started = Instant::now();
        let healthy = self.ping().await.is_ok();
        let latency_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
        let version = if healthy {
            self.post_query_read("SELECT version()", &[])
                .await
                .ok()
                .map(|text| text.trim().to_owned())
                .filter(|version| !version.is_empty())
        } else {
            None
        };
        ClickHouseHealth {
            healthy,
            version,
            latency_ms,
        }
    }

    /// 发送一次请求并统计成功/失败。
    pub(super) async fn post_query(
        &self,
        sql: &str,
        body_suffix: Option<&str>,
        params: &[(&str, &str)],
    ) -> ClickHouseResult<String> {
        let _permit = self.acquire().await?;
        self.in_flight.fetch_add(1, Ordering::SeqCst);
        let outcome = self.post_query_inner(sql, body_suffix, params).await;
        self.in_flight.fetch_sub(1, Ordering::SeqCst);
        if outcome.is_ok() {
            self.ok.fetch_add(1, Ordering::Relaxed);
        } else {
            self.error.fetch_add(1, Ordering::Relaxed);
        }
        outcome
    }

    /// 只读查询路径：按 `config.retry` 的重试预算包装 `post_query`。
    ///
    /// 仅供 `query` / `query_text` / `query_with_params` 与 `ping` /
    /// `health_check` 等读操作使用；写操作（`execute` / `insert_batch` 等）
    /// **不**走本路径（R-RT-031：非幂等写默认不重试）。`enabled = false`
    /// 时短路为单次执行，不消耗任何重试预算。
    pub(super) async fn post_query_read(
        &self,
        sql: &str,
        params: &[(&str, &str)],
    ) -> ClickHouseResult<String> {
        if !self.config.retry.enabled {
            return self.post_query(sql, None, params).await;
        }
        execute_with_retry(&self.config.retry, "clickhouse.query", || {
            self.post_query(sql, None, params)
        })
        .await
    }

    async fn post_query_inner(
        &self,
        sql: &str,
        body_suffix: Option<&str>,
        params: &[(&str, &str)],
    ) -> ClickHouseResult<String> {
        let config = &self.config;
        let url = build_url(config, params)?;
        let body = match body_suffix {
            Some(extra) => format!("{sql}\n{extra}"),
            None => sql.to_owned(),
        };

        debug!(target: "clickhousex", database = %config.database, "clickhouse 请求");

        let mut request = self.http.post(url);
        if !config.auth_in_url && needs_basic_auth(config) {
            request = request.basic_auth(&config.user, Some(&config.password));
        }
        let response = request
            .header(reqwest::header::CONTENT_TYPE, "text/plain; charset=utf-8")
            .body(body)
            .send()
            .await
            .map_err(|error| map_transport_error(&error))?;

        let status = response.status();
        if !status.is_success() {
            let prefix = read_error_prefix(response).await?;
            return Err(map_http_error(status, &prefix));
        }
        response
            .text()
            .await
            .map_err(|_| ClickHouseError::Connection("读取响应失败（远端正文已省略）".to_owned()))
    }
}
