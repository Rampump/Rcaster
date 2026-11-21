# Etcd Admission Spike Report

- 日期：2026-11-22
- 分支：`gate-1-etcd-admission-spike`
- 实现：Rust release build，`tokio`、`etcd-client 0.21.1`
- 驱动：[`admission_spike.rs`](../../../crates/rcaster-service/examples/admission_spike.rs)

## 结论

当前锁方案（A）可以作为 1000 admissions/s 的**唯一用户**基线，但无法满足热点用户和大 NAT IP 场景：

- 唯一用户：1000/s 目标可完成，p99 约 50 ms。
- 单一热点用户：500/s 和 1000/s 目标实际只有约 102–106/s，并出现 `USER_LOCK_BUSY`。
- 大 NAT IP：500/s 和 1000/s 目标实际约 66–67/s，并出现 `IP_LOCK_BUSY`。
- 1 万 session 前缀仍可在约 100/s 热点准入下完成，p99 约 70 ms；count-only 读避免了返回 1 万个 value。
- leader 成员停止后，使用仍存活 endpoints 的 entry 可继续；使用包含失效 endpoint 的完整列表时，新客户端存在连接失败。leader 重启后准入恢复。

因此 Phase 0 选择采用方案 **B：per-user admission record + revision CAS** 作为下一设计门，并需要在 B 的证据中补充 per-IP admission record 或等价分片方案，解决大 NAT IP 串行瓶颈。当前锁方案继续保留，作为低竞争基线，不应据此宣称支持热点 1000/s。

本报告不选择 C；独立 admission service 只有在 B 仍无法满足目标或运维复杂度被证明可接受时再进入概要设计。

## 环境

- CPU：AMD Ryzen 5 5600，6 cores / 12 logical processors
- 内存：31.9 GiB
- 系统：Windows 11 IoT Enterprise LTSC
- 编译：`stable-x86_64-pc-windows-gnu`，release profile
- etcd：3.5.21，三成员 quorum，全部位于同一主机
- endpoint：`127.0.0.1:2379`、`127.0.0.1:22379`、`127.0.0.1:32379`
- workload：每轮 5 秒，64 concurrent in-flight；NAT 1000/s 使用 3 秒，1 万 session 前缀使用 3 秒
- 每轮使用独立 cluster 前缀，避免上一轮 session 影响

同主机三进程没有覆盖跨主机网络、独立磁盘 fsync、真实 WAL 设备差异，因此这些数据是设计证据，不是生产容量承诺。

## 实测结果

### 唯一用户

| offered/s | success | failure | achieved/s | p50 ms | p95 ms | p99 ms | lock retries |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 100 | 500 | 0 | 100.10 | 4.271 | 5.787 | 6.655 | 0 |
| 500 | 2500 | 0 | 499.61 | 6.250 | 7.994 | 8.967 | 0 |
| 1000 | 5000 | 0 | 993.80 | 10.576 | 39.482 | 50.180 | 0 |

### 单一热点用户

| offered/s | success | failure | achieved/s | p50 ms | p95 ms | p99 ms | lock retries |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 100 | 500 | 0 | 99.94 | 4.166 | 18.399 | 24.043 | 196 |
| 500 | 2163 | 337 | 106.47 | 471.418 | 1321.139 | 1862.053 | 271433 |
| 1000 | 4264 | 736 | 102.02 | 510.552 | 1335.761 | 1994.500 | 559050 |

热点用户的 session 数持续增长，而同一 user lock 将写入串行化；500/1000 offered/s 下等待超过 benchmark 的 250 ms retry 窗口，部分请求被拒绝。

### 1 万 session 前缀

预置同一用户的 10000 个 session key 后，以 100 offered/s 运行 3 秒：

| offered/s | success | failure | achieved/s | p50 ms | p95 ms | p99 ms | lock retries |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 100 | 300 | 0 | 99.46 | 13.608 | 43.319 | 70.071 | 607 |

本次读取改为等等价线性化 count-only range。该结果只证明 1 万前缀在当前硬件下的热点基线；不证明 10 万 key 或跨节点 RTT 下仍可接受。

### 共享 NAT IP

每个请求使用独立用户，但全部客户端来自同一 canonical IP，并启用正整数 IP quota。

| offered/s | duration s | success | failure | achieved/s | p50 ms | p95 ms | p99 ms | IP lock retries |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 100 | 5 | 500 | 0 | 99.87 | 8.203 | 29.560 | 64.121 | 310 |
| 500 | 5 | 2487 | 13 | 66.19 | 645.036 | 3002.146 | 4440.574 | 114113 |
| 1000 | 3 | 2981 | 19 | 67.03 | 634.421 | 2921.160 | 4536.315 | 139292 |

单 IP lock 将所有 NAT 客户端写入串行化，是比单用户锁更早出现的瓶颈。

## 故障恢复

初始 leader 为 n1（`127.0.0.1:2379`）。停止 n1 后，n2/n3 保持 quorum，raft term 从 2 升至 3。

| 场景 | endpoints | success | failure | p99 ms | 观察 |
|---|---:|---:|---:|---:|---|
| leader 停止，完整 endpoint 列表 | 3 个 | 251/300 | 49 | 5.977 | 新 worker 对 2379 的 TCP connect 被拒绝 |
| leader 停止，仅存活 endpoints | 2 个 | 300/300 | 0 | 5.835 | quorum 下准入正常 |
| n1 重启，完整列表恢复 | 3 个 | 300/300 | 0 | 5.989 | leader 现为 n2，准入恢复 |

这表明集群本身可维持 quorum，但当前 `etcd-client` 使用方式不能依赖“把失效节点留在 endpoint 列表里”自动覆盖所有新建客户端。后续必须加入 endpoint 健康筛选、重连或明确的 bootstrap 失败策略。

## 未覆盖项

- 未模拟 20/50/100/250 ms network RTT；本机 loopback 没有可用流量整形环境。
- 未覆盖跨主机 WAL fsync 和磁盘故障差异。
- 未覆盖 Watch lag、compaction 与配置变更并发。
- NEAR 尚未实现，因此未跑 Source/Client/NEAR 三方最终额度竞争。
- Lease 剩余时间小于一次 admission 已由逻辑测试覆盖，但未做 timed network race。

这些项目不能因当前结果通过而视为已验证。
