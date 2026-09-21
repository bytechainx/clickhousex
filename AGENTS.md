# clickhousex Agent 指南

> 本文件为 AI Agent 在本仓库工作时的入口指南。

## 项目定位

零内部耦合的 ClickHouse HTTP 客户端库（默认端口 8123），提供连接复用、并发背压、查询、批量写入、认证、超时与健康检查等基础设施原语；不包含任何领域模型、业务表结构或调度编排。

## 技术栈

- Rust edition 2021（rust-version 1.85）
- 关键依赖：`reqwest`（rustls）、`tokio`、`serde` / `serde_json`、`thiserror`、`toml`、`tracing`、`url`
- 不依赖内部框架/私有 crate，零内部耦合，仅使用 crates.io 公开依赖

## 代码结构

```
src/
├── lib.rs          # 入口 + 公共 API re-export（#![deny(missing_docs)] / #![forbid(unsafe_code)]）
├── error.rs        # ClickHouseError（#[non_exhaustive] + is_retryable）+ ClickHouseResult 别名
├── config.rs       # ClickHouseConfig + ClickHouseConfigBuilder + from_env/from_toml + validate
└── client.rs       # ClickHouseClient（单连接）/ ClickHousePool（并发受控）+ 纯函数工具
tests/
├── api_surface.rs      # 公开 API 面
├── config_env.rs       # 环境变量 / TOML 配置
├── http_roundtrip.rs   # 本地一次性 TCP 服务的 HTTP 往返
├── ping_unreachable.rs # 失败路径（127.0.0.1:1）
└── pure_functions.rs   # parse_tab_separated_rows / chunk_ranges / build_query_url
benches/
└── hot_path.rs     # 热路径基准（harness = false，支持 --quick）
docs/
├── API.md          # 公开 API 一览与能力边界
└── 标准.md         # 定位、字段治理与验收标准
```

## 开发约定

- 注释与文档使用简体中文；标识符保持英文
- 错误类型：thiserror 枚举 + `#[non_exhaustive]` + `pub type ClickHouseResult<T> = ...`；错误消息不回显服务端响应正文、SQL 片段或凭据
- 配置：`ClickHouseConfig` 结构体 + `builder()` + `from_env()` / `from_toml()` / `from_toml_file()` + `validate()` + fail-fast
- 密码只能经 `FOUNDATIONX_CLICKHOUSEX_PASSWORD` 环境变量或 builder 注入；`Debug` 脱敏为 `***`；`from_toml()` 拒绝非空 `password`
- 非 loopback 主机默认强制 TLS；明文 HTTP 需显式放行名单
- 禁止裸 `unwrap()`（库代码）/ 无注释 `expect()`
- 异步代码使用 tokio，禁止在 async 中做阻塞 I/O
- 集成测试全部离线运行：不触碰真实网络，失败路径统一用 `127.0.0.1:1`

## 门禁三件套（P0）

```bash
cargo fmt --all -- --check
cargo clippy --all-targets -- -D warnings
cargo test --all-targets
```

## 相关文档

- 组织 Rust 规范：`~/org-config/rulesets/rust/RULES.md`
- API 文档：`docs/API.md`
- 标准与验收：`docs/标准.md`
- 术语与领域语言：`CONTEXT.md`
- 贡献指南：`CONTRIBUTING.md`
- 变更记录：`CHANGELOG.md`
- 基准测试：`benches/hot_path.rs`
