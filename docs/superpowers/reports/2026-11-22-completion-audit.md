# RCaster Completion Audit

- 日期：2026-11-22
- 最终本地复核：2026-11-23
- 分支：`gate-1-rtklib-isolation`
- 计划：[`rust-rewrite-plan.md`](../../../rust-rewrite-plan.md)

## 结论

当前实现已具备可运行的 standalone/cluster Caster、NTRIP1/2、数据面 fan-out、
源站准入、集群 claim、relay、NEAR、管理 API、公网目录、WebSocket monitor、
审计、健康指标和滚动升级基础。

本地自动门禁已通过，但真实设备、真实负载、跨地域网络和故障演练仍未完成，
因此不能将当前分支标记为生产完成或给出 SLA。

## 本轮验证

| 检查 | 结果 |
|---|---|
| `cargo fmt --all -- --check` | 通过 |
| `cargo clippy --workspace --all-targets -- -D warnings` | 通过 |
| 按 crate 执行 `cargo test -p <crate> --tests --locked` | 通过；仅需要外部二进制的测试按默认标记 ignored |
| release 关键回归 | service 分区恢复、Hub、NTRIP、public、relay、upgrade，core 五格式 demux，以及 app 独立进程/备份/配置测试均通过 |
| `etcd_credentials_reload -- --ignored` | 通过 CA-1 到 CA-2 轮换和 etcd 重启 |
| `data_plane_soak --sources 2 --subscribers 50 --duration-s 5 --rate-kib 64` | 通过，约 2.87 MiB/s，连接排空后源站与订阅者为 0 |
| 数据面连接容量 | 10 源/10000 订阅者连续 5 分钟约 8.295 MiB/s；同规模 30 秒立即重跑约 7.9 MiB/s；1 源/10000 订阅者；10000 源/1 订阅者；均无连接残留 |
| 5 分钟数据面 soak | 10 源/1000 订阅者、1 KiB/s/源，约 0.894 MiB/s；RSS 约 54.3→57.8 MiB、2218 handles 恒定，结束残留为 0 |
| 3000 订阅者持续 fan-out | 10 源/3000 订阅者运行 60 秒，约 2.683 MiB/s；结束后源站和订阅者残留均为 0 |
| 8000 订阅者连续重跑 | 10 源/8000 订阅者连续两轮 30 秒均通过，约 6.894 和 6.655 MiB/s，结束残留为 0 |
| systemd 部署模板 | 直接服务与 updater 监督两种 hardened unit、动态用户、只读文件系统、`LimitNOFILE=131072`，并通过 `Conflicts=` 强制二选一；CI 校验两个 unit |
| NTRIP1 拒绝响应 | 用户配额/限流失败不再空响应，返回标准 `ERROR - ...` |
| HAProxy PROXY v2 | 真实 HAProxy 端到端测试已加入 CI；测试使用 `send-proxy-v2` 并以 `127.0.0.2` 源地址命中后端拒绝规则 |
| etcd leader failover | 三成员本机集群杀死 leader 后选出新 leader；原 ConfigWatcher 恢复健康并收敛到新 generation |
| etcd TCP 分区恢复 | 切断 Watcher 到 etcd 的全部 TCP 连接并发布新 generation；恢复代理端口后原 Watcher 自动重连并收敛 |
| 真实三成员 etcd peer 分区 | 隔离单个成员后，存活两成员维持 quorum 并完成写入；恢复后三成员重新同步 |
| 三成员全 quorum 丢失 | 三成员 peer 全部互相隔离 45 秒后四台 Caster 均 not-ready；恢复后两端点健康、四台同 PID 回到 ready |
| Caster etcd client 全断路 | 节点 15 秒内进入 not-ready；恢复后同一 PID 自动 grant 新 lease 并回到 ready |
| RTKLIB str2str | 官方 master `71db0ffa` 在 Windows 编译；真实源站上传和客户端订阅收到 156,802 字节，含完整 RTCM3 1005 |
| 官方 RTCM fixture | RTKLIB `testglo.rtcm2` 解析 1727 帧，`testglo.rtcm3` 429 帧，`GMSD7_20121014.rtcm3` 1143 帧；CRC/parity error 均为 0 |
| 公开 BINEX fixture | nav-solutions 数据提交 `ac313e64` 的 17,596,434 字节流解析为 90,010 条 BINEX 记录，unknown/CRC error 为 0 |
| RTKLIB FFI worker | 官方源码构建成功；worker 返回 `RTKLIB 2.4.2`、`rtcm3` capability 和 RTCM3 1005 消息号 |
| `cargo build --workspace --release --locked` | 通过 |
| 明文/TLS 双 listener 认证测试 | TLS 源站和客户端成功；明文认证请求被拒绝 |
| 五格式畸形连续流 fuzz seed | 无 panic，后续 RTCM3 可重新同步 |
| 管理端客户端 CRL 热加载 | 未吊销客户端成功；吊销后新握手被拒绝 |
| Relay gRPC 客户端 CRL 热加载 | 更新前请求成功；写入吊销记录后新请求失败 |
| NTRIP 集群共享认证失败窗口 | 两个 Runtime 共享锁定，成功认证可清除 |
| Prometheus 告警规则 | YAML 解析通过；6 条规则覆盖 readiness、依赖、限速降级、证书和源站异常 |
| 预生成 OCSP stapling | DER response 加载与空文件热更新回退测试通过 |
| 在线 OCSP responder | 配置解析/校验、NTRIP/public/admin/relay 接入路径和后台刷新测试通过 |
| etcd 备份与恢复保护 | 快照写入/拒绝覆盖测试通过；`etcdutl` 恢复到隔离集群后清除旧 runtime/online/locks，保留 config |
| 目标集群真实快照恢复 | 581,664 字节快照隔离恢复 58 ms；12 ms 清除 11 个 stale runtime key，保留 6 用户、6 挂载点、generation 2 |
| 周期 etcd 备份 | `--backup-dir/--retention`、私有文件原子发布和前缀限定保留测试通过；systemd service/timer 模板已加入 |
| updater Debian 12 兼容 | Debian 12 节点 release 构建通过；产物仅要求 GLIBC 2.34，四台已安装且服务保持 inactive |
| 跨节点 relay soak | 10 源/50 订阅者跨 4 台 Caster 运行 60 秒，源端 674,880 字节、订阅端 3,374,400 字节，无错误 |
| Reloadable gRPC TLS | ALPN `h2` 在证书/CRL/OCSP 热加载后保持；焦点测试通过 |
| standalone 备份 | 运行中 Caster 周期导出一致 redb 快照和 Config；损坏数据库可从 accounts.json 恢复到新库并保留密码哈希 |
| 1000 次配置更新 | ConfigWatcher 保持健康并收敛到 generation 1001 |
| Linux 进程资源指标 | RSS/FD 采集路径与指标测试已加入 |
| Rust 1.88 MSRV | `check --all-targets`、Clippy `-D warnings` 和完整 workspace 测试通过；CI 已增加固定 1.88 作业 |
| RustSec advisory 门禁 | GitHub Actions 已实际执行并通过，包含 RustSec、MSRV、Clippy 和完整测试 |

2026-11-23 的最终本地复核将 `time` 升级到 0.3.47，移除已停止维护的
`rustls-pemfile`，改用 `rustls-pki-types` 的 PEM 解码接口，并将 MSRV
提高到 1.88。随后通过 Rust 1.88 的 `cargo check --workspace --all-targets`、
`cargo clippy --workspace --all-targets -- -D warnings`，并分别完成
`rcaster-app`、`rcaster-core`、`rcaster-service` 和 `rcaster-rtklib` 的
包级测试；TLS 证书/CRL/OCSP 热加载、管理端 mTLS 与 Relay mTLS 行为测试
也均通过。RustSec 使用 1560 条 advisory 扫描 237 个依赖，结果为 0 漏洞。

当前 Windows GNU 环境存在 Cargo 聚合测试异常：`cargo test --workspace`
在运行若干 test binary 后会以状态 0 提前结束，未执行剩余 package；这不是
测试失败，但该命令的退出状态不能作为本地全量通过证据。最终本地结论来自
上述逐 crate 测试。RTKLIB worker 测试还必须先把普通 worker bin 复制到
test 构建目录之外，再通过 `RCASTER_RTKLIB_WORKER_BIN` 指定；否则
`cargo test --test` 的产物替换会使测试找不到 worker。

release 复核还发现并修复了 standalone 周期备份测试的发布竞态：测试轮询曾
把正在写入的隐藏临时目录误认为已完成备份，随后正式目录原子发布导致临时
路径消失。测试现在只接受最终 `standalone-*` 目录；隔离 release 运行连续
20 次通过，完整 app 独立进程测试也在 debug/release 下通过。

本轮补充了数据面容量探针及其 CI smoke，并修复 NTRIP listener shutdown：
停止 accept 后会主动取消并等待所有已接连接，source lease、订阅关系、连接
配额和 session guard 在 listener 返回前释放。进程收到终止信号时先将
readiness 置为 false 并停止健康采样，再开始关闭 listener。

## 已完成的主要能力

- NTRIP1/NTRIP2 源站与客户端认证、TLS、Basic auth、chunked/Content-Length
  上传和 GGA 双工。
- `client_auth_require_tls` 和 `source_auth_require_tls` 默认开启；没有 TLS
  listener 的认证配置拒绝启动，明文 listener 不接受需要 TLS 的认证请求。
- RTCM3 解析、CRC24Q、MSM/legacy observation、1005/1006 位置、1033 设备
  信息；RTCM2/BINEX/CMR 格式识别和透明转发。
- RTCM2 和 NMEA 未完成候选不再吞掉后续强同步帧；畸形随机流会跳到
  D3/02/E2/$ 边界后继续解析。
- 有界 source admission、observe/enforce、共享 cooldown、source bitrate
  限制、验证源和订阅者容量限制。
- etcd lease、source claim fencing、revision CAS、配置/账号/黑名单 Watch、
  共享 cooldown 和连接指令。
- Caster lease supervisor 对 keepalive、lease grant 和版本状态发布使用基于
  TTL 的请求超时。旧 lease 丢失后会重新 grant 新 lease、原子替换 admission
  使用的 lease ID、重写版本状态并自动恢复 ready，无需重启进程。
- Caster 间 mTLS relay、每 mountpoint relay 复用、空闲关闭和 owner health
  查询。
- NEAR RTCM3 候选选择、逐连接 GGA、观察窗口、位置跳变过滤和 source 切换。
- 管理会话、API token scope、CSRF、用户/挂载点/黑名单 CRUD、配置归档、
  operation、审计、版本状态和签名升级基础。
- 管理员和公网登录的成功、失败、锁定、限流及认证不可用事件进入审计日志。
- 公网 STR/地图、登录、短期 monitor ticket、Cookie WebSocket 和资源限额。
- 公共地图 API 支持严格校验的 `bbox=min_lon,min_lat,max_lon,max_lat`
  视口过滤，非法坐标返回 400。
- 公共地图页面在初始加载、平移、缩放和周期刷新时只请求当前视口 bbox。
- 低缩放级别按视口网格聚合站点，簇显示数量并可点击放大；高缩放恢复单站。
- 公共目录、地图和历史 JSON 返回内容 ETag，支持 `If-None-Match` 304，
  并使用 5 秒 public cache；至少 128 字节的响应优先使用 Brotli，并回退到
  gzip，ETag 按编码区分。
- TLS、etcd client、coordinator 和升级签名私钥的权限与热轮换检查。
- 管理 listener 可配置 `admin_client_crl`；CRL 随证书/CA 一起热加载，
  被吊销客户端在下次握手时拒绝。
- NTRIP client/source 认证失败使用 etcd TTL 状态跨 Caster 共享；成功认证
  清除共享窗口。
- 管理端和公网登录的 IP、用户名失败窗口也使用同一共享状态。
- `rcaster_auth_failure_fallback_total` 记录共享认证限速因 etcd
  不可用而退化为节点本地保护的次数。
- Prometheus 规则与 runbook 覆盖 readiness、控制面依赖、认证共享限速降级、
  listener 证书到期、源站拒绝风暴和 CRC 错误风暴。
- Linux 暴露进程 RSS 和打开 FD 指标；非 Linux 平台明确输出 `NaN`。
- 数据面 soak 示例每秒采样并输出进程 RSS/FD 的起始、峰值和结束值。
- 数据面探针支持显式容量上限和连接角色诊断，把客户端源地址分散到
  `127/8`，使用零 linger 立即释放本地端口，并把 accept 速率提高到请求的
  连接容量，避免限流和 `TIME_WAIT` 污染容量结论。Windows 本地完成 1 万
  同时订阅者连续 5 分钟和 1 万 mountpoint smoke，负载结束均无连接残留。
  探针在全部连接建立后才启动数据循环，避免连接建立阶段污染吞吐计时。
- NTRIP1 的通用 401/403 失败返回 `Bad Password`，配额、冷却和不可用失败
  返回 `Mount Point Taken or Invalid`，避免客户端只看到 EOF。
- 新增真实 HAProxy PROXY Protocol v2 集成测试：HAProxy 从 `127.0.0.2`
  建立客户端连接并发送 v2 头，后端只有采用头内源 IP 时才会返回 403。
  测试已在本地和 GitHub Actions Ubuntu runner 上通过。
- 新增三成员 etcd leader 故障测试：杀死 leader 后，两个存活成员维持
  quorum，原 ConfigWatcher 重连并应用新 generation；本地真实二进制执行通过。
- 新增本地 TCP 分区测试：Watcher 只通过受控 TCP 代理访问 etcd，代理断开
  后 readiness 组件转 unhealthy；分区期间发布新 generation，恢复代理端口
  后原 Watcher 自动重连并收敛。
- 使用官方 RTKLIB `str2str` 源码提交 `71db0ffa0d9735697c6adfd06fdf766d0e5ce807`
  完成真实进程兼容验证：一个 `str2str` 作为 NTRIP 源站上传连续 RTCM3，
  另一个作为 NTRIP 客户端订阅；客户端收到 156,802 字节并包含完整 1005
  帧。Windows 临时构建仅需官方源码未覆盖的 `_WIN32` signal 条件编译。
  同一链路已固化为 `rtklib_str2str` 集成测试并在本机通过；CI 将构建官方
  RTKLIB 后重复执行。
- 官方 RTCM fixture 已按 1、127、4096 字节分块逐帧比较，结果完全一致。
  另有 16 个确定性随机 chunk 序列，每段 1–257 字节；测试覆盖 RTCM2 跨
  chunk tail、最早候选和嵌套候选优先级，CRC/parity error 均为 0；合成
  真实 RTCM2 帧也通过逐字节拆分。
- 公开 BINEX 真实流 `mfle20200105.bnx` 已通过：90,010 帧、unknown=0、
  CRC error=0；完整流、127/4096 字节固定分块和 4 个随机 chunk seed
  帧序列完全一致。测试已加入 CI，并固定 nav-solutions/data 数据提交。
- `rtklib-ffi` shim 修复缺失 `<stdint.h>` 的构建错误；官方源码 worker 已
  返回 RTKLIB 版本、rtcm3 capability 和 1005 消息号。CI 构建普通 worker
  bin 后通过 `RCASTER_RTKLIB_WORKER_BIN` 执行隔离测试。workspace
  `--all-features` 测试、Clippy 和 release 构建均已通过。
- NTRIP/public TLS 及管理/relay mTLS 支持预生成 DER OCSP response
  stapling；response、证书和私钥一起原子热加载，空或无效文件保留上一份
  有效配置。
- NTRIP/public TLS 支持 `tls_ocsp_responder`，管理/relay mTLS 支持
  `coordinator_tls.server_ocsp_responder`。两者按配置间隔从 HTTP(S) OCSP
  端点获取 response；在线获取优先，预生成 DER 文件作为无缓存失败时的回退。
- Relay gRPC 可配置 `relay_client_crl`，吊销客户端在建立 gRPC 数据流前被
  TLS 拒绝。
- 升级配置默认要求双人审批：创建者不能自行批准，批准时重新校验 preview
  digest，成功后才获取全局 rollout lock 并发出升级命令。
- 待审批 operation 持久化在 etcd，可由另一个 UpgradeManager 实例读取、
  重新 preflight 并批准。
- 管理 API 使用稳定 `user_id` 区分审批人，并将 operation 状态、创建者和
  批准者写入审计详情。
- API Token 列表返回最近使用时间；时间独立存储并最多每小时回写，不覆盖
  token 自身过期 lease。
- 新增 `etcd-recovery-prepare`：恢复旧快照后先验证 Config、账号引用和
  Argon2 密码哈希，再原子清除旧 runtime、online 和 lock 状态。真实
  `etcdutl` 快照恢复演练已通过，清理后配置 generation 和版本归档保持不变。
- `etcd-backup --backup-dir` 现在生成带时间戳的私有快照，并只清理固定
  `rcaster-*.db` 前缀文件；默认保留 672 份。systemd oneshot service 与
  15-minute persistent timer 模板已提供，未安装 timer 的节点不会自动备份。
- `rcaster-updater` 改为在 Debian 12 上构建兼容产物，SHA-256
  `125efc6615bf32e29de48dc8de52512d3e299f6290a418ad9f2c49794a0b787d`，
  最高 GLIBC 要求 2.34。四台节点已安装该文件，但仍未启用 updater service。
- relay 使用的可热加载 TLS 配置现在显式保留 gRPC ALPN `h2`；修复了
  OpenSSL 握手成功但 Tonic 无法建立 HTTP/2、跨节点订阅返回 503 的问题。
- etcd Watch 增加 30 秒空闲上限；账号 Watch 从最后 revision 重连，其余
  Watch 超时后重建快照，避免 quorum 恢复后出现“健康但不收敛”。
- 新增 standalone 周期备份任务和 `backup-standalone` 离线导出命令。服务
  复用已持有的 redb 句柄输出账号和挂载点，同时复制 Caster Config；文件
  先写入隐藏临时目录，全部落盘并同步后再原子发布；失败时不留下半成品
  目录。
- standalone 周期备份新增有界保留，默认 168 份；只清理固定前缀的
  `standalone-*` 目录，配置为 0 或超出安全范围时拒绝启动。

## 仍阻塞生产验收

- 已使用真实 RTK 基准站模块完成 NTRIP2 源站/订阅链路，并通过 RTKLIB Linux
  `str2str`、Windows `strsvr` 2.5.0、BKG BNC 2.13.7、Lefebure NTRIP
  Client 2017.07.27 和 u-center 25.03；矩阵见
  `docs/runbooks/ntrip-compatibility-matrix.md`。其他接收机型号仍未验证。
- 已在 Debian 12 上使用真实 HAProxy 进程验证 PROXY Protocol v2 源 IP；
  反向代理 Host/path 和真实 tile 服务环境仍未验证。
- 已完成真实三成员 etcd 单成员分区、三成员全 quorum 丢失，以及 Caster
  到全部 etcd client endpoint 的 blackhole 自愈测试。三台决策节点与边缘
  节点跨城部署，本机到四台服务器也跨域上千公里；已完成真实跨城 TLS relay
  和公网建连基线。仍未完成跨域全分区、DNS/流量切换、长时间故障注入和正式
  RTO/RPO 演练。
- 目标 Linux 2C2G 节点已完成 10 源/1000、3000、8000 订阅者以及 1 万订阅者
  和 1 万 mountpoint smoke；四节点本地 soak 与 10 源/50 订阅者跨节点 relay
  预跑因开发仍在继续而于 2026-11-24 01:44 主动停止，只保留预跑记录。
  72 小时长稳态正式验收需在最终候选冻结后重新执行。
- BINEX 已有真实完整流校验 fixture；CMR/CMR+ 深层语义和真实冻结 fixture
  仍缺失，RTKLIB worker 也尚未完成设备语料和 fuzz 覆盖。
- CMR/CMR+ 已补合成包络逐字节拆分、CMR+NMEA 混合主格式和坏 checksum
  后重同步测试；仍没有公开可追溯的真实 CMR 抓包。
- 在线 OCSP responder 已接入 `StandaloneConfig`、NTRIP/public listener、
  admin/relay listener 和 CI。当前四节点使用没有 responder 地址的自签测试
  证书，因此该功能尚未在运行节点启用；生产证书和 OCSP 端点就绪后需完成
  一次真实 stapling 验证。
- 已完成目标测试集群真实 etcd 快照的隔离恢复和 stale runtime 清理；仍需
  完成 etcd 单成员恢复、全 quorum 丢失、redb/TLS 私钥恢复及正式 RPO/RTO
  记录。
- GitHub Actions 首轮暴露了 service container 和 HAProxy 安装缺失两个环境
  问题；改为直接启动 etcd 二进制并补齐 HAProxy 后，run `45242799276` 的
  `msrv` 与 `test` 两个 job 全部通过，包括 RustSec、完整测试、PROXY v2、
  etcd 故障/恢复、官方 RTKLIB、RTCM/BINEX 和 release 构建。

本报告中的本地结果只证明对应代码路径在当前 Windows 环境可运行。生产结论
必须补充目标 Linux 节点、真实网络、真实设备和长稳态证据。
