# CONTRIBUTING.md — 贡献指南（clickhousex）

本文件面向贡献者，汇总本地门禁与提交约定。
AI Agent 的工作约定另见 [`AGENTS.md`](./AGENTS.md)；术语与领域语言见 [`CONTEXT.md`](./CONTEXT.md)。

## 开发流程

- 本仓库是**独立的单 crate 仓库**，不依赖 `xhyper.rs` 主工程及其内部 crate（`kernel` /
  `contracts` 等），只使用 crates.io 公开依赖。
- substantial 变更走 feature branch → PR → review → merge，**禁止直接 push `main`**。
- `main` 已启用分支保护：要求 PR + 必需检查 `fmt / clippy / test`，
  `required_approving_review_count = 0`（单人也能合并），禁止强推与删除。
- 合并方式固定为 **create a merge commit**。注意仓库设置是
  `merge_commit_title = MERGE_MESSAGE` + `merge_commit_message = PR_TITLE`，因此
  `gh pr merge` 必须显式传 `--subject` 与 `--body`，否则会产出通用
  `Merge pull request #N from …` 标题。
- 提交信息遵循 Conventional Commits（`feat:` / `fix:` / `docs:` / `ci:` / `chore:` /
  `refactor:`），描述用简体中文。

## 本地门禁（P0 三件套）

```bash
cargo fmt --all -- --check
cargo clippy --all-targets -- -D warnings
cargo test --all-targets
```

无需 `--all-features`：本 crate 没有可切换 feature。

元数据完整性门禁（**不发布 crates.io**，此命令只校验打包元数据）：

```bash
cargo package --no-verify --allow-dirty
```

`--allow-dirty` 用于工作区存在未提交改动时；`cargo package` 会打印 `Packaged N files`，
可用于确认新文档已进入打包白名单。

## 复用口径（不发布 crates.io）

- 本 crate **不发布到 crates.io**，仅以 GitHub 源码 / git 依赖形式复用。
- 文档与元数据中不得出现「可独立发布」「可直接 `cargo publish`」等表述，
  也不得放置 crates.io / docs.rs 徽章与外链。
- `Cargo.toml` 的 `documentation` 指向 `https://github.com/bytechainx/clickhousex#readme`。
- 消费方引入方式（README「安装」小节为准）：

  ```toml
  [dependencies]
  clickhousex = { git = "https://github.com/bytechainx/clickhousex" }
  ```

## 开发约定

- 注释、文档、错误消息使用**简体中文**；标识符保持英文。
- 错误类型：`thiserror` 枚举 + `#[non_exhaustive]` + `pub type ClickHouseResult<T>` 别名。
- 不在库代码里裸 `unwrap()`（`[lints.clippy]` 已 `deny` `unwrap_used` / `expect_used` /
  `panic`）；单测与集成测试目标经 `#![cfg_attr(test, allow(...))]` / 文件级
  `#![allow(...)]` 豁免。
- 所有 `pub` 项必须有中文 `///` 文档（`src/lib.rs` 已 `deny(missing_docs)`，
  并 `forbid(unsafe_code)`）。
- 集成测试**必须离线运行**，不触碰真实网络；HTTP 用例使用本地一次性 TCP 服务，失败路径统一用
  `127.0.0.1:1`（必然拒绝连接）。
- 密码只能经 `FOUNDATIONX_CLICKHOUSEX_PASSWORD` 环境变量或 `ClickHouseConfigBuilder::password`
  注入；`Debug` 脱敏为 `***`，`from_toml()` 拒绝非空 `password`。
- 非 loopback 主机默认强制 TLS；明文 HTTP 需显式配置 `FOUNDATIONX_CLICKHOUSEX_PLAIN_HTTP_HOSTS` 放行名单。
- 错误消息不回显服务端响应正文、SQL 片段或凭据；错误映射只保留 HTTP 状态码与 ClickHouse 数字错误码。
- MSRV 为 `1.85`，edition 2021（与 `Cargo.toml` 声明一致，不要使用更新的语言特性）。

## 提交前自检清单

- [ ] `cargo fmt --all -- --check` 通过
- [ ] `cargo clippy --all-targets -- -D warnings` 通过
- [ ] `cargo test --all-targets` 通过
- [ ] `cargo package --no-verify --allow-dirty` 通过
- [ ] 新增 `pub` 项都有中文 `///` 文档
- [ ] 文档中无「可独立发布」/ crates.io / docs.rs 表述
- [ ] 新增 TOML 字段已同步 `schema_version` 约束与 `docs/标准.md` 的字段治理说明
- [ ] 未引入内部 crate 依赖（零内部耦合）
