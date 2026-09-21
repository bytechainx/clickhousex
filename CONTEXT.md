# clickhousex 上下文

本文件定义 `clickhousex` 与其使用方共享的核心词汇。它只记录领域含义与能力边界，
不记录具体实现、API 签名、存储或部署决定。

## 角色与边界

**适配器**：把 ClickHouse 的 HTTP wire 协议收敛为一组稳定 Rust 原语的库；它不承载领域模型、
不假设业务表结构，也不负责调度编排。
_Avoid_: ClickHouse SDK（SDK 通常还含重试策略、遥测后端与凭据供应链，超出本仓库边界）

**连接池**（`ClickHousePool`）：以 `max_in_flight` 信号量约束全局并发的入口，多个调用方共享
同一份连接与背压额度；它不是连接数上限，也不保证同一请求固定落在同一连接上。
_Avoid_: 连接管理器（容易误解为还会做健康巡检与自动重建）

**单连接客户端**（`ClickHouseClient`）：请求串行化的入口，同一时刻只有一个请求在途；它不做
池化，也不提供并发额度。
_Avoid_: 轻量模式（「轻量」暗示能力子集，而这里只有并发语义不同）

**配置**（`ClickHouseConfig`）：构造期一次性校验、之后只读的声明式参数集合；校验失败即拒绝
构造，不提供「先构造、后补验」的路径。
_Avoid_: 运行时设置（本仓库没有可变更的运行时配置面）

## 数据面与语义

**执行**（`execute`）：只发送 DDL / DML 且不读取结果集的操作；它与「查询」的分界在于是否
关心返回行。
_Avoid_: 写入（`execute` 同样用于 DDL，语义比写入宽）

**查询结果**（`query` / `query_with_params`）：以 `TabSeparated` 文本逐行返回的
`Vec<Vec<String>>`；它不携带列类型，把文本转成业务类型是调用方的责任。
_Avoid_: 行集（容易误解为已完成类型反序列化的结构）

**批量写入**（`insert_batch`）：按 `BatchInsertOptions` 的 `max_rows_per_chunk` /
`max_bytes_per_chunk` 切成若干分块、每块一次独立 HTTP 请求的写入；分块之间**不承诺原子性**。
_Avoid_: 事务写入（没有任何跨块回滚语义）

**分块**（chunk）：批量写入的最小独立提交单位，边界由行数与字节数共同决定；它不是 ClickHouse
的概念，只在本仓库的写入路径上有意义。
_Avoid_: 批次（批次常暗示「一次提交全部成功或失败」）

**JSONEachRow**：批量写入使用的行编码，要求每行是一个 JSON object；它不校验列名是否与目标表
匹配，也不做 schema 推断。
_Avoid_: JSON 模式（本仓库只实现这一种行编码，没有可切换的模式）

## 错误与可靠性

**可重试错误**：调用方可以安全重复施加同一操作的那类失败（网络 / IO / 超时 / HTTP 429 /
HTTP 5xx / ClickHouse `159`）；它与「不可重试错误」互为补集，由 `is_retryable()` 判定。
_Avoid_: 网络错误（网络只是成因之一，不能表达「重试是否安全」这一判据）

**健康检查**：返回 `ClickHouseHealth` 的显式可达性探测，失败时**不**返回 `Err`，而是以
`healthy = false` 表达；它与构造解耦，构造成功不代表服务可达。
_Avoid_: 建连（`connect` 只做校验与构造，不发请求）

**许可等待**（acquire）：为进入 in-flight 额度而做的等待，受 `acquire_timeout` 约束，超时
返回可重试错误；它不是请求超时，也不代表服务端已经收到任何字节。
_Avoid_: 超时（不加限定会被误认为请求超时）

**关闭**（`close`）：拒绝新请求并等待在途操作结束的显式动作；它不做隐式重试，也不保证服务端
已经提交了最后一次写入。
_Avoid_: 断开（`close` 是语义化的排空，不是单纯释放句柄）

## 安全与配置治理

**凭据注入**：密码只能经 `FOUNDATIONX_CLICKHOUSEX_PASSWORD` 环境变量或构建器进入；它**不是**
TOML 字段，`from_toml()` 遇到非空 `password` 直接报错。
_Avoid_: 配置项（密码被刻意排除在 TOML 配置面之外）

**脱敏**：`Debug` 输出固定显示 `***`，错误消息不回显服务端响应正文与 SQL 片段；它只作用于
展示与日志路径，不改变任何返回值。
_Avoid_: 加密（脱敏不改变存储或传输中的真实值）

**明文放行名单**（`FOUNDATIONX_CLICKHOUSEX_PLAIN_HTTP_HOSTS`）：显式允许非 loopback 主机使用
明文 HTTP 的逗号分隔名单；它是唯一的例外通道，不是默认行为。
_Avoid_: TLS 白名单（本名单表达的是「放行明文」，与证书校验信任列表无关）

**兼容别名**（`FOUNDATIONX_CLICKHOUSEX_PORT`）：为旧配置保留的端口变量；与
`FOUNDATIONX_CLICKHOUSEX_HTTP_PORT` 同时出现且取值冲突时立即报错，不静默取其一。
_Avoid_: 回退值（它不是优先级更低的值，而是冲突即失败）
