# Etcd Admission Spike Report

- 日期：2026-11-22
- 分支：A：`gate-1-etcd-admission-spike`；B：`gate-1-revision-admission`；C：`gate-1-coordinator-admission`
- 实现：Rust release build，`tokio`、`etcd-client 0.21.1`
- 驱动：[`admission_spike.rs`](../../../crates/rcaster-service/examples/admission_spike.rs)

## 结论

当前锁方案（A）可以作为 1000 admissions/s 的**唯一用户**基线，但无法满足热点用户和大 NAT IP 场景：

- 唯一用户：1000/s 目标可完成，p99 约 50 ms。
- 单一热点用户：500/s 和 1000/s 目标实际只有约 102–106/s，并出现 `USER_LOCK_BUSY`。
- 大 NAT IP：500/s 和 1000/s 目标实际约 66–67/s，并出现 `IP_LOCK_BUSY`。
- 1 万 session 前缀仍可在约 100/s 热点准入下完成，p99 约 70 ms；count-only 读避免了返回 1 万个 value。
- leader 成员停止后，使用仍存活 endpoints 的 entry 可继续；使用包含失效 endpoint 的完整列表时，新客户端存在连接失败。leader 重启后准入恢复。

因此最初选择方案 **B：per-user admission record + revision CAS** 进行原型验证。B 显著改善了独立用户路径，但同一 user/IP key 仍将热点准入串行限制在约 130/s。C 原型完成后，热点 Client/NAT 准入稳定达到 1000/s，因此最终选择 **C：单 owner 批量 admission coordinator** 作为生产 admission 架构。

B 已证明无法满足热点 500/1000/s，因此进入 C 的条件已经满足。C 的 Client/NAT 原型随后达到目标；Source 批量化、网络 transport 和分片仍是后续工作。

## 方案 B 复测

B 使用：

- `/runtime/admission/users/{user_id}` 与 `/runtime/admission/ips/{canonical_ip}` 持久 record。
- record 本身不保存计数，只通过 etcd `mod_revision` 提供串行化版本。
- 每次成功准入重写 record，并在同一 Txn 写 session；Source claim 仍与 owner session 同 Txn。
- CAS 冲突从新的 record revision 和线性 count 重试，带 1–8 ms 随机抖动。
- record 不绑定 lease，因此 lease 过期不会留下错误计数；session count 仍来自 count-only 线性读。

| workload | offered/s | success | failure | achieved/s | p50 ms | p95 ms | p99 ms |
|---|---:|---:|---:|---:|---:|---:|---:|
| 独立用户 | 1000 | 5000 | 0 | 999.64 | 3.043 | 4.421 | 7.827 |
| 热点用户 | 100 | 500 | 0 | 99.04 | 18.109 | 125.790 | 231.114 |
| 热点用户 | 500 | 2496 | 4 | 141.14 | 308.759 | 1350.990 | 2019.441 |
| 热点用户 | 1000 | 4990 | 10 | 129.71 | 337.136 | 1472.570 | 2380.760 |
| 共享 NAT IP | 500 | 2500 | 0 | 138.41 | 323.785 | 1396.002 | 2153.134 |
| 热点用户 + 1 万 session | 100 | 300 | 0 | 99.54 | 8.027 | 64.665 | 81.164 |

B 消除了多数 `USER_LOCK_BUSY` / `IP_LOCK_BUSY` 失败，但在热点下仍由单 record CAS 串行化；每个成功请求仍包含 record 读、session count 读和 Txn。若要求热点 1000/s，需要 C 级批量/分片或独立 admission 协调，而不能继续只调 retry。

方案 B 功能测试覆盖 Client/Source 最终额度竞争、100 Caster 竞争、NAT IP 隔离、配置 revision 拒绝和 Source claim 回滚，均通过。

### Source coordinator integration

Source 已接入 C：

- 同一 batch 的 Client 和 Source 通过同一个 user 配额分配序列。
- Source claim plan、generation 更新、owner session 和 VALIDATING route 记录追加到 coordinator batch Txn。
- owner/config revision 条件覆盖整批写入。
- 同一批重复 mountpoint 最多允许一个 Source，失败请求不消耗 user slot。
- Source 不读写 IP session，也不受 NAT IP quota 影响。
- gRPC 新增 `AdmitSource`，成功响应返回 `source_generation`。

默认 batch size 从 64 调整为 16，以限制 Source claim 每项多个 etcd op 带来的 Txn 操作数增长。调整后热点用户 1000/s 仍完成 5000/5000，实际 `996.17/s`，p99 `19.0ms`。

当前未单独做 Source 高并发吞吐基准；Source 主要验证了共享额度、原子性和重复 claim 语义。

## 方案 C 原型

C 原型使用一个 etcd lease 绑定的 admission owner：

- owner key 保证同一时刻只有一个 coordinator 提交准入。
- 请求在一个进程内按最多 64 条或 5 ms 窗口聚合。
- 每个 batch 对涉及的 user/IP 前缀并发执行线性 count-only 查询。
- coordinator 在内存中按 batch 顺序分配额度，并将所有接受请求的 session/index 写入同一个 etcd Txn。
- Txn 同时比较 config revision 和 owner key revision/value/lease。
- owner keepalive 失败或 owner Txn 比较失败时，新准入立即 fencing。

| workload | offered/s | success | failure | achieved/s | p50 ms | p95 ms | p99 ms |
|---|---:|---:|---:|---:|---:|---:|---:|
| 独立用户 | 1000 | 5000 | 0 | 994.85 | 17.982 | 18.978 | 19.741 |
| 热点用户 | 1000 | 5000 | 0 | 993.47 | 17.828 | 18.732 | 19.424 |
| 共享 NAT IP | 500 | 2500 | 0 | 498.05 | 18.504 | 20.126 | 24.224 |
| 共享 NAT IP | 1000 | 3000 | 0 | 993.64 | 18.777 | 20.003 | 20.767 |
| 热点用户 + 1 万 session | 100 | 300 | 0 | 99.62 | 18.732 | 19.686 | 20.161 |

正确性测试覆盖 100 Caster 最终额度竞争、Source 不占 IP 配额、旧配置拒绝、owner 排他和 lease 到期后的新 owner 接管。

### C gRPC transport

C 已通过 tonic gRPC 暴露 `AdmitClient`：

- protobuf 请求携带 session identity、Caster lease、连接上限和配置 generation/revision。
- 服务端复用同一个批量 coordinator；配额和配置错误作为结构化业务响应返回。
- loopback benchmark 使用真实 gRPC channel、真实 coordinator 和真实 etcd。

| workload | offered/s | success | failure | achieved/s | p50 ms | p95 ms | p99 ms |
|---|---:|---:|---:|---:|---:|---:|---:|
| 热点用户 gRPC | 1000 | 5000 | 0 | 994.25 | 18.293 | 19.308 | 20.332 |
| 共享 NAT IP gRPC | 1000 | 3000 | 0 | 991.85 | 18.849 | 20.336 | 30.718 |

gRPC 已增加内部 mTLS 和 service token：

- 服务端要求由内部 CA 签发的客户端证书。
- 每个请求必须携带 Bearer service token，并具有当前 cluster 的 `admission:write` scope。
- token 使用 constant-time 比较；认证发生在 coordinator 之前。
- 无效 token、错误 scope、错误 cluster 和缺失客户端证书都会被拒绝，且不会创建 session。

安全测试证书由 `rcgen` 在内存生成，不提交可用凭据。跨主机 RTT 尚未验证。

### mTLS certificate rotation

`ReloadableAdmissionMtlsServer` 使用固定 TCP listener 和动态 rustls acceptor：

- 每个新 TLS 握手读取当前 server certificate、private key 和 client CA。
- `rotate` 原子替换新连接的 TLS 配置，不需要重启 listener。
- 已建立连接继续使用旧会话配置；新连接立即使用新 CA。
- 测试从 CA-1 轮换到 CA-2 后，新 CA-1 客户端被拒绝，新 CA-2 客户端可直接准入。
- 被拒绝的连接不产生 session。

证书热轮换不主动断开旧连接；需要强制下线时仍由 Caster/session 管理逻辑处理。

### Token rotation and rate limit

`AdmissionAccessPolicy` 现在由共享 `Arc<RwLock<_>>` 保存：

- `replace_tokens` 可在服务运行中替换 token 集合。
- 旧 token 对后续 RPC 立即失效，新 token 无需重启即可使用。
- 每个 `ServiceToken` 可配置 `(requests_per_second, burst)` 令牌桶。
- 限流是 per-token，不是 per-connection；共享同 token 的多个客户端共同消耗 bucket。
- gRPC `ResourceExhausted` 映射为 `AdmissionError::RateLimited`，拒绝请求不写在线状态。

测试覆盖在线轮换、旧 token 拒绝、新 token 准入和 burst=1 时第二次请求被限流。

本原型尚未实现：

- C 的跨主机 RTT；证书热轮换、service token 轮换和按 token 限流已完成。
- Source 高并发性能基准；功能路径已完成 coordinator 批量化。
- 多 owner 分片和跨分片 user/IP 额度协调。
- 活动 owner 崩溃过程中的单个 in-flight batch 语义；当前由 owner lease 和 session lease 保守清理。

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
