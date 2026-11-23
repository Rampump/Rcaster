# RTKLIB Isolation Spike Report

- 日期：2026-11-22
- 分支：`gate-1-rtklib-isolation`
- 实现：[`rcaster-rtklib`](../../../crates/rcaster-rtklib/src/lib.rs)

## 结论

已证明受监督 decoder worker 的隔离机制可行：

- 使用版本化 JSON-lines 子进程协议。
- 每个请求校验 protocol version 和 request ID。
- 输入、输出和读取时间均有上限。
- worker 崩溃、EOF、I/O 错误、超时和超限响应都会终止 worker。
- 下一次请求自动启动新 worker。
- capability probe 返回明确的 engine 和格式集合。

该里程碑使用 fixture worker 验证隔离机制，并增加了可选的 RTKLIB C ABI worker。RTCM3 主路径仍应继续使用纯 Rust 解码。

## 真实 RTKLIB 适配器

- 使用 RTKLIB `v2.4.3-b34`，提交 `180043ee24b6d2b168f98b64be15f69d50046b1a`。
- C shim 只暴露初始化、释放、版本和 RTCM3 消息号提取接口。
- `rtklib-ffi` feature 通过 `RTKLIB_SOURCE_DIR` 在构建时链接源码，不把 RTKLIB 纳入仓库。
- worker 复用现有 JSON-lines 监督协议；每个请求按完整记录批次处理，半帧不会延续到下一个请求。
- capability probe 只在固定 1005 fixture 成功时声明 `rtcm3`，不推断 RTCM2、BINEX 或 CMR 支持。

## 测试

| 场景 | 结果 |
|---|---|
| capability probe | 返回 fixture engine 和声明格式 |
| 正常 decode | 返回固定 DTO |
| 输入超过 16 字节 | 写进程前拒绝，`InputTooLarge` |
| 子进程主动退出 | `Crashed`，下一请求自动重启 |
| 子进程卡死 | 100ms 超时并 kill，下一请求自动重启 |
| 子进程输出超过 1 KiB | `OutputTooLarge`，下一请求自动重启 |
| RTKLIB 2.4.3 capability | 返回 `RTKLIB 2.4.3` 和 `rtcm3` |
| RTKLIB 2.4.3 固定 1005 帧 | 返回消息号 `1005` |
| 半帧后新请求 | 半帧不污染下一批完整记录 |

## 生产边界

- Windows 下当前只 kill 直接子进程；生产 worker 不应自行派生孙进程，Linux 部署还应考虑进程组/job object 清理。
- 当前协议用于隔离验证，最终 RTKLIB worker 必须暴露固定版本的 C ABI shim 和同等限界 DTO。
- 必须用真实设备 fixture 做 capability probe；fixture 声明格式不代表 RTKLIB 支持。
- worker 崩溃隔离已证明，但 CPU、内存和句柄配额尚未由 OS sandbox 强制。
- 当前真实适配器只提取 RTCM3 消息号并通过一个 1005 样本完成 capability probe，尚未覆盖真实设备语料、RTCM2 或 fuzz。
