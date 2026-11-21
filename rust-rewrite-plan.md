# RCaster：Rust NTRIP Caster 集群设计

> 项目性质：基于 Millipede 功能经验，用 Rust 全新设计并实现
> 参考实现：Millipede 0.8.2，Git `7a9ce17`，BSD-3-Clause
> 设计日期：2026-11-22
> 重要边界：原项目只作为 NTRIP、RTCM、NEAR 和故障处理的参考，不作为代码复用、配置兼容或接口兼容目标。



***

## 1. 项目目标

构建一个可在单机或跨地域集群中运行的 NTRIP Caster。系统需要统一管理用户、挂载点、上传权限、黑名单、在线连接、STR 和最近基站，并允许客户端连接任意 Caster 后访问集群内任意在线挂载点。

### 1.1 核心目标



* 支持 NTRIP 1.0 和 NTRIP 2.0 源站上传与客户端订阅。

* 支持 IPv4、IPv6、TCP 和 TLS。

* 支持 standalone 单机部署，不依赖 etcd。

* 支持 etcd 集群部署，多个 Caster 实例共享配置和在线状态。

* 用户、挂载点、挂载点所属用户保存在数据库文件中，不使用关系表模型。

* NTRIP1 源站使用挂载点独立上传密码。

* NTRIP2 源站使用挂载点所属用户的用户名和用户密码。

* 客户端认证可配置开关；开启时只验证客户端用户，不验证挂载点所属关系。

* 同一挂载点在集群内只能有一个真实源站上传者。

* 支持 RTCM3、RTCM2、BINEX、CMR/CMR+ 和 NMEA 的透明转发与增量识别；第一版只对 RTCM3 和 NMEA 做语义解析。

* 源站上传后在准入窗口识别实际数据格式、RTCM3 观测值 / SSR、MSM 版本、位置信息和完整性，再按 `observe` 或 `enforce` 策略决定仅标记还是拒绝。

* 源站连接时先写临时 STR 记录，准入完成后修正为 ACTIVE 的格式、状态、设备和在线信息；之后不周期刷新 etcd。

* `enforce` 模式下不合格源站会被断开，并通过集群共享退避避免快速重连风暴；`observe` 模式只标记并继续转发。

* 用户连接数可在整个集群范围内限制。

* 用户连接上限、认证开关、端口和黑名单策略保存在 Caster Config；具体黑名单 CIDR 规则作为独立配置记录同步到所有节点。

* 所有 Caster 使用统一的 NTRIP、TLS、relay、管理和 metrics 端口。

* 提供管理 API 修改 Caster Config、用户、挂载点和黑名单。

* 管理 API 使用管理员账号、短期浏览器 session 和可撤销 API Token。

* 提供 Web 管理界面，展示在线用户、源站、relay、STR、地图和多协议数据流可视化。

* RTCM3 framing、CRC 和语义解析优先使用纯 Rust `rtcm-rs`；RTCM2/BINEX/CMR 只识别格式，NMEA 只解析所需语句，RTKLIB 作为经验证的可选兼容后端。

* 客户端连接任意 Caster 后，系统自动发现源站所在 Caster，并通过自动 relay 获取数据。

* 每个 Caster 对每个远端 mountpoint 最多维护一条 relay，本地客户端共享该 relay。

* 提供集群公共 STR，可配置全局表、区域表和自动模式。

* 根据集群在线挂载点在本地计算最近基站，不依赖 etcd 热查询。

* NEAR 由配置开关启用，所有 Caster 都提供该合成挂载点；对外名称由 `near.mountpoint` 配置，不固定为字面量 `NEAR`。

* 源站、Caster 或网络故障后，在线状态能够自动清理，并避免长期双源。

* 提供新的管理 API、监控指标和运维接口。

### 1.2 非目标



* 不兼容旧 Millipede 的 CLI、`caster.yaml`、`source.auth`、`host.auth`、`/adm` 或 P2P syncer。

* 不逐个模块迁移 C 代码。

* 不在 CMake、C 结构体或 libevent 生命周期上建立 Rust 抽象。

* 不把 etcd 用作差分数据路径或高频消息队列。

* 不实现客户端与挂载点 owner 的访问绑定；客户端认证与挂载点 owner 是两套独立概念。

* 第一版提供必要的管理页面和监控页面，但不实现复杂大屏、挂载点级 ACL、计费或完整多租户。

### 1.3 基本术语



| 名称              | 含义                                                |
| --------------- | ------------------------------------------------- |
| Caster          | 一个运行中的 Rust NTRIP Caster 进程                       |
| Source          | 向 Caster 上传差分数据的 NTRIP 源站                         |
| Client          | 从 Caster 订阅差分数据的 NTRIP 客户端                        |
| Mountpoint      | NTRIP 挂载点名称                                       |
| Owner           | 挂载点所属用户，用于源站认证和连接计数                               |
| Active Source   | 当前有真实上传连接的挂载点                                     |
| Relay           | Caster 之间共享远端源数据的上游连接                             |
| Account Store   | 保存用户、挂载点和所属关系的单文件嵌入式 KV 数据库                       |
| Caster Config   | 保存连接上限、黑名单策略、认证开关、端口、relay、STR 等参数的版本化配置          |
| Online State    | 在线用户 session、在线源站和 relay 路由状态                     |
| Source Software | 上传程序信息，来自 User-Agent、Source-Agent 和 Ntrip-Version |
| Source Device   | 基准站接收机信息，主要来自 RTCM3 1033，其他格式可能为空                 |
| Data Profile    | 从源站流中增量识别出的格式、帧类型、MSM 版本、位置和校验状态                  |
| Admin Session   | 浏览器登录后的短期 Cookie session                          |
| API Token       | 自动化管理客户端使用的可撤销 Bearer token                       |
| Stream Monitor  | 管理页面按需订阅源或 relay 获取的只读数据流摘要                       |



***

## 2. 总体架构

### 2.1 部署模式

#### Standalone



```
Client / Source
       |
   NTRIP Caster
      / \
Account DB  Caster Config File
```



* Account DB 保存用户、挂载点和所属关系。

* Caster 配置文件保存连接上限、黑名单、认证开关、端口和功能参数。

* 不连接 etcd。

* 在线状态只保存在进程内。

* 适用于单站、测试和小规模部署。

#### Cluster



```
                 3 x etcd voting members
                 /          |          \
          core Caster   core Caster   core Caster
             /    \          |          /    \
     edge Caster  edge Caster ... edge Caster ...
          |                         |
       Sources                   Clients
```



* 3 个或 5 个 etcd member 组成 quorum。

* etcd member 是独立 Raft 投票节点；Caster 只是 etcd gRPC 客户端，不是 etcd member，不参与投票，也不持久保存集群共享状态。

* 生产环境 etcd member 使用专用机器和磁盘，不与 Caster 混部；Caster 数量与 etcd member 数量无关，可独立扩缩。混部只用于开发和小规模测试。

* core/edge 只是部署和容量标签，系统不维护 node 表。

* 每个 Caster 使用本地 Account DB 和 Caster Config 文件保存 etcd 配置快照。

* 用户 session、在线源站和 relay 路由保存在 etcd。

* Caster Config 存放用户连接上限、黑名单策略、端口等集群策略；具体 CIDR 规则独立 Watch；新 Caster 加入后自动同步。

* 任意 Caster 都可以接受客户端和源站。

* Caster 本地 node config 只保存 bootstrap 信息：etcd endpoints、mTLS 证书、`caster_id`、bind 地址和 relay advertised address；这些不写入共享 etcd。

### 2.2 不维护 Node 节点表

系统不需要 `nodes/` 目录、节点注册表或节点状态表。

Caster 只需要：



* 一个实例标识 `caster_id`。

* 一个每次启动 /lease 重建都会变化的 `runtime_epoch`。

* 一个用于自动清理在线 key 的 etcd lease。

* 一个对集群内部开放的 relay endpoint。

* 一个本地配置快照。

源站在线记录直接包含 owner Caster 的 relay endpoint。其他 Caster 发现远端源后，直接向该 endpoint 建立自动 relay。

### 2.3 控制面与数据面

#### 控制面



* 用户、挂载点、黑名单和全局设置。

* 用户连接配额。

* 源站唯一性占用。

* 在线用户和在线源站状态。

* STR 和最近基站索引。

* 管理 API 和配置同步。

#### 数据面



* NTRIP1/2 请求解析。

* 源站上传。

* 客户端订阅。

* 多协议组帧、校验、过滤和转换。

* 本地 fan-out。

* Caster 间自动 relay。

控制面只处理连接级事件，不处理每个差分数据包。

### 2.4 Relay 网络前提

任意可能成为源站 owner 的 Caster，其内部 relay endpoint 必须能够被集群内其他 Caster 访问。

推荐：



* 使用 WireGuard、Tailscale 或专有内网建立 Caster 之间的 L3 覆盖网络。

* relay listener 只绑定内部网络。

* 使用 mTLS 和服务身份认证。

* Caster 启动自检时验证 relay endpoint 可达。

如果某地 Caster 位于 NAT 后且无法被其他 Caster 访问，它不能成为集群内可 relay 的源站 owner。

本方案默认是 “合作节点故障模型”：依赖 lease、self-fence 和 FenceGuard。若部署合同要求连恶意节点或无限暂停节点也必须物理单源，必须在网络架构中增加外部 source ingress fencing；应用层本身无法提供该保证。

生产部署等级：

| 等级 | 保证范围 | 必要措施 |
|---|---|---|
| 普通集群 | 合作节点、进程可被调度、单调时钟可靠 | Lease + self-fencing + FenceGuard |
| 强隔离集群 | 旧 owner 无法继续提供 Source ingress | 外部 L4/source ingress controller，可撤销旧 Caster 数据面 |
| 严格单源 | 恶意节点/无限暂停也不得双源 | 网络设备或独立 ingress gateway 作为唯一上传入口 |

对外 SLA 必须注明：已进入内核 socket buffer 的字节不可撤回；应用层只能保证 lease deadline 后不再提交新差分帧。需要物理级单源时必须选择“强隔离/严格单源”部署。

### 2.5 外部地址、DNS 和区域接入

推荐把客户端入口和数据源入口分开：



```
客户端统一入口：
  caster.example.com
     -> 所有健康 core Caster
     -> 返回相同 scope 的 STR

数据源区域入口：
  src-east.example.com  -> east edge/core Caster
  src-west.example.com  -> west edge/core Caster

管理 API：
  admin.example.com     -> core Caster 或独立管理 VIP
```

原则：



* 普通 DNS 多 A/AAAA 记录只能提供候选地址，不能保证客户端选择 “最近” 或 “负载最低” 的节点。

* 如果要求就近选择，必须使用 GeoDNS、ASN / 子网策略、加权 DNS 或区域 L4 VIP；仅把所有节点放进一个域名不够。

* 客户端可以用统一域名连接任意健康 core，因为集群通过 Relay 访问其他区域的源站。

* 数据源应使用区域域名或区域 VIP，优先连接近距离节点，避免跨区上传和跨区 Relay。

* 同一源站重连到另一区域会重新 claim 和重新写临时 STR；旧 source 必须先 fencing/lease 清理。

* DNS 只影响新连接；已建立的 NTRIP TCP 不会因 DNS 记录变化自动切换。

* DNS / 健康检查必须探测 NTRIP listener 和 `/health/ready`，不能只检查主机 ping。

* 健康节点从 DNS 中摘除时设置较短 TTL，并保留足够重叠时间；客户端仍需要自己的重连机制。

* 使用区域 L4 负载均衡时必须启用 PROXY Protocol，才能保留客户端 / 源站真实 IP。

* TLS 证书应覆盖 `caster.example.com`、`admin.example.com` 和各区域 `src-*.example.com`，或使用受控通配符证书。

推荐拓扑：



```
East clients -> caster.example.com -> East core(s)
West clients -> caster.example.com -> West core(s)

East source -> src-east.example.com -> East core/edge
West source -> src-west.example.com -> West core/edge
```

公共 STR 的 scope 必须与入口一致：统一客户端域名最好只返回 global 表；区域入口可以返回 regional 表。否则同一域名落到不同节点时，客户端会看到不同的 STR。



***

## 3. 领域模型

用户、挂载点和所属关系使用单文件嵌入式 KV 数据库保存，不建立 SQL 关系表。推荐使用 `redb`，通过 key 前缀组织账号和挂载点记录。连接上限、黑名单和 Caster 运行参数使用单独的版本化配置文件。

### 3.1 User



```
{
  "schema_version": 1,
  "mod_revision": 1842,
  "user_id": "u_01j...",
  "username": "rtk_user",
  "password_hash": "$argon2id$...",
  "auth_version": 1,
  "enabled": true,
  "ntrip_enabled": true,
  "web_enabled": true,
  "role": "user",
  "created_at": "2026-11-19T10:00:00Z",
  "updated_at": "2026-11-19T10:00:00Z"
}
```

规则：



* `user_id` 永久不变。

* `mod_revision` 来自 etcd 元数据或 standalone 本地单调 revision，用于判断本地缓存是否过期。

* `username` 唯一且用于 NTRIP Basic 用户名。

* `role` 至少包含 `user` 和 `admin`。

* `ntrip_enabled` 独立控制该账号能否登录 NTRIP；管理员账号默认不自动获得 NTRIP 数据流权限。

* `web_enabled` 控制普通用户能否登录 Web monitor；管理员管理页面仍由 `role=admin` 和 admin scopes 决定。

* 密码、角色、`enabled` 或 `web_enabled` 变化时递增 `auth_version`；`ntrip_enabled` 只影响 NTRIP 服务权限。

* 用户并发连接上限不保存在 User 记录中，而由 Caster Config 的 `connection_limits` 决定。

* 用户被禁用后不能创建新的源站或客户端 session。

* 用户名限制为 1–128 字节 UTF-8，不允许 `:`、控制字符、CR、LF 和 NUL；认证按大小写敏感处理。

### 3.2 Mountpoint



```
{
  "schema_version": 1,
  "mod_revision": 931,
  "mountpoint": "C63",
  "owner_user_id": "u_01j...",
  "enabled": true,
  "ntrip1_upload_password_hash": "$argon2id$...",
  "region": "east-china",
  "on_demand": false,
  "on_demand_upstream": null,
  "advertise": true,
  "lat": 31.234,
  "lon": 121.456,
  "coverage_radius_m": 30000,
  "str_fields": {
    "identifier": "Shanghai Farm",
    "format": "RTCM3",
    "formats": "1004,1005,1006,1077,1087,1097,1107,1127,1230",
    "carrier": "2",
    "nav_system": "GPS+GLO+GAL+BDS+QZS",
    "network": "CENTIPEDE-RTK",
    "country": "CHN",
    "solution": "0"
  },
  "source_expectation": {
    "formats": ["rtcm3"],
    "msm_versions": [4]
  },
  "created_at": "2026-11-19T10:00:00Z",
  "updated_at": "2026-11-19T10:00:00Z"
}
```

规则：



* `owner_user_id` 必填。

* `mod_revision` 与 User 使用相同的 revision 语义。

* 挂载点名称限制为 1–100 字节 ASCII，允许字母、数字、`-`、`_`、`.`；禁止 `/`、`?`、`%`、控制字符和保留名 `/`、`SOURCETABLE`、`near.mountpoint`。

* 挂载点名称按大小写敏感处理，`C63` 与 `c63` 是两个不同挂载点。

* `ntrip1_upload_password_hash` 可以为空；为空时禁止该挂载点的 NTRIP1 上传。

* NTRIP1 上传只验证该密码。

* NTRIP2 上传不验证该密码，而是验证 owner 用户名和 owner 用户密码。

* 客户端订阅不验证 owner，也不验证该上传密码。

* `on_demand=true` 表示可按需建立源站或外部拉流。

* `on_demand_upstream` 仅在 `on_demand=true` 时使用，只能由管理员配置，不能接受请求 URL 或客户端提供的上游地址。

* 外部地址必须通过 allowlist、TLS 和 loop-detection 检查；禁止访问 link-local、云 metadata、[localhost](https://localhost) 和未授权私网。

* 上游用户名 / 密码使用 secret reference，不把明文放入 etcd 业务 JSON；日志不记录 URL userinfo 或 Authorization。

* `advertise=true` 表示允许进入公开 STR；是否在线另行判断。

* `enabled=false` 立即拒绝新源站 / 客户端；已有源站和订阅默认通过 owner command 关闭，管理 API 必须返回影响数量。

* 挂载点改名不是原地修改 key；必须创建新 Mountpoint、迁移 owner/STR，再显式下线旧挂载点。

* 删除存在 ACTIVE source 的 Mountpoint 默认返回 409；管理员使用 force 选项时才发送断开命令并等待 claim 清理。

* `source_expectation` 可选，用于发现 “挂载点配置与实际上传模块不一致”；为空时只使用 Caster Config 的全局规则。

* `source_expectation.formats` 与 `msm_versions` 是校验期望，不是展示字段；不得从 `str_fields` 反推校验规则。

* `source_expectation.msm_versions` 只对 RTCM3 生效；RTCM2、BINEX、CMR 和 NMEA 不使用该字段。

* `coverage_radius_m` 用于公网覆盖图和 NEAR 展示，是运营半径提示，不代表任何条件下的 RTK 精度保证。

* 公开地图默认只显示 `advertise=true` 且 ACTIVE 的站点；显示离线站点必须由管理配置显式允许。

* 删除仍拥有 Mountpoint 的 User 必须显式转移 owner 或级联删除；修改 owner 时若存在 ACTIVE source，默认拒绝并要求先断开源站。

### 3.3 Caster Config

连接上限、黑名单策略、认证开关、端口和功能参数保存在一个版本化配置文件中。具体黑名单 CIDR 记录使用独立 config keys，不属于用户数据库记录。



```
schema_version: 1
generation: 18

cluster:
  config_max_staleness_s: 30
  admission_requires_current_config: true

ports:
  ntrip: 2101
  ntrip_tls: 2102
  relay: 12102
  admin: 8080
  metrics: 9100

client_auth:
  enabled: true
  require_tls: true
  track_anonymous_streams: false
  disconnect_existing_on_change: false

source_auth:
  require_tls: true

admin_api:
  require_tls: true
  session_ttl_s: 3600
  login_rate_limit_per_minute: 10
  max_failed_logins: 5
  failed_login_lockout_s: 300
  websocket_enabled: true
  stream_monitor_interval_ms: 1000
  max_stream_monitors: 4
  stream_monitor_idle_timeout_s: 60
  max_ws_per_admin: 4
  ws_queue_events: 256
  ws_max_message_bytes: 65536
  ws_heartbeat_s: 30

public_web:
  enabled: true
  monitor_requires_auth: true
  coverage_default_radius_m: 30000
  monitor_ticket_ttl_s: 60
  monitor_snapshot_interval_ms: 1000
  monitor_idle_grace_s: 0
  max_ws_connections: 2000
  max_ws_per_ip: 4
  max_ws_per_user: 2
  ws_queue_events: 64
  ws_heartbeat_s: 30
  max_monitor_subscribers_per_ip: 2
  max_monitor_subscriptions_per_user: 2
  max_monitor_sessions_per_node: 16
  max_subscribers_per_monitor: 200
  monitor_start_rate_per_minute: 30
  busy_retry_after_s: 5

limits:
  max_connections: 3000
  max_sources: 500
  max_subscribers: 2500
  max_subscribers_per_source: 2000
  max_relays: 100
  max_internal_relays: 500
  max_validating_sources: 64
  subscriber_backlog_bytes: 16384
  socket_send_buffer_bytes: 114688
  validation_window_bytes_per_source: 65536
  validation_window_global_bytes: 67108864
  parser_queue_frames_per_source: 32
  parser_queue_global_bytes: 33554432
  max_source_bitrate_bps: 2000000
  max_total_ingress_bps: 500000000
  max_total_egress_bps: 1000000000
  source_bitrate_action: disconnect
  fd_budget: 32768

connection_limits:
  default_max_sessions: 3
  per_username:
    rtk_user: 5
    survey_team: 3

blacklist_policy:
  disconnect_existing: false

relay:
  require_mtls: true
  idle_grace_ms: 500
  connect_timeout_ms: 5000
  read_timeout_s: 30
  on_demand_source_timeout_s: 60
  reconnect_delay_s: 10
  max_concurrent_starting: 16
  max_input_bitrate_bps: 2000000
  max_output_bitrate_bps: 2000000
  max_reconnect_attempts: 5

ntrip:
  accept_rate_per_second: 200
  tls_handshake_timeout_ms: 5000
  request_line_max_bytes: 4096
  headers_max_bytes: 16384
  header_count_max: 64
  header_timeout_ms: 10000
  content_length_max_bytes: 67108864
  auth_max_concurrency: 4
  gga_line_max_bytes: 256
  source_read_timeout_s: 60
  client_idle_timeout_s: 60
  write_timeout_s: 30

upgrade:
  enabled: true
  max_unavailable_core: 1
  max_unavailable_edge: 1
  drain_timeout_s: 60
  post_upgrade_stability_s: 120
  health_timeout_s: 30
  auto_rollback: true
  require_signed_manifest: true

sourcetable:
  scope: auto
  ntrip2_content_type: gnss/sourcetable
  global_max_lines: 5000
  global_max_bytes: 1048576
  defaults:
    network_identifier: CENTIPEDE-RTK
    operator: RTK Network
    country: CHN
    generator: RCaster
    authentication: B
    fee: N
  cas:
    host: caster.example.net
    port: 2101
    identifier: MAIN
    operator: RTK Network
    country: CHN
    website: https://caster.example.net
  net:
    identifier: CENTIPEDE-RTK
    operator: RTK Network
    country: CHN
    website: https://caster.example.net
    reference: NONE

near:
  enabled: true
  mountpoint: AUTO_NEAR
  advertise: true
  allowed_formats: [rtcm3]
  max_distance_m: 1000000
  hysteresis_m: 50
  min_recompute_interval_ms: 1000
  max_recompute_interval_ms: 5000
  min_recompute_distance_m: 4.0
  switch_observe_ms: 3000
  switch_position_threshold_m: 4.0
  max_spatial_queries_per_second: 10000
  max_query_concurrency: 8
  gga_max_input_per_second: 10
  gga_max_jump_speed_mps: 1000
  max_hdop: 5.0
  hdop_m_per_unit: 20.0
  base_position_uncertainty_m: 10.0
  no_source_timeout_s: 30
  position_ttl_s: 0
  position_ttl_action: exclude_candidate

source_validation:
  mode: observe
  scope: admission_only
  allowed_formats: [rtcm3, rtcm2, binex, cmr, nmea]
  require_correction_data: true
  correction_formats: [rtcm3, rtcm2, binex, cmr]
  initial_data_timeout_s: 15
  admission_profile_window_ms: 3000
  max_frames_inspected: 200
  require_base_position: true
  position_required_for_formats: [rtcm3]
  position_types: [1005, 1006]
  position_timeout_s: 30
  rtcm3:
    require_usable_correction: true
    usable_classes: [observation, ssr]
    min_valid_frames: 3
    min_correction_frames: 2
    min_valid_frame_ratio: 0.8
    max_crc_error_ratio: 0.1
    observation_types: [1001, 1002, 1003, 1004, 1009, 1010, 1011, 1012]
    observation_msm_range: [1071, 1137]
    ssr_types: [1057, 1058, 1059, 1060, 1061, 1062, 1063, 1064, 1065, 1066, 1067, 1068]
  msm:
    restrict_versions: false
    allowed_versions: [4]
    require_any: false
    required_within_s: 30
  str_refresh:
    enabled: true
  max_crc_errors: 3
  unknown_data:
    max_ratio: 0.95
    min_observed_bytes: 4096
  violation_window_s: 60
  backoff:
    initial_ms: 5000
    max_ms: 300000
    multiplier: 2

source_health:
  enabled: true
  short_window_s: 10
  long_window_s: 60
  observation_timeout_s: 60
  valid_frame_ratio_min: 0.8
  crc_error_ratio_max: 0.1
  alert_clear_s: 30
  station_id_change_alert: true
  base_position_jump_m: 100
  action: alert

decoder:
  rtcm3_backend: rtcm-rs
  semantic_formats: [rtcm3, nmea]
  recognition_only_formats: [rtcm2, binex, cmr]

rtklib:
  enabled: false
  process_isolation: true
  worker_count: 2
```

规则：



* 上面的 `limits` 默认值对应 2 vCPU / 2 GiB；4 vCPU / 4 GiB 档值见 “容量和性能”。

* `default_max_sessions` 是所有用户的默认最大数据流连接数。

* 用户有效连接数包含该用户的客户端 session 和其作为 owner 的源站连接；拥有多个基准站的用户必须显式提高 `per_username` 上限。

* `per_username` 为指定用户覆盖默认值，例如设为 3 或 5。

* `per_username` 引用的用户不存在时，配置校验失败。

* 配置中没有覆盖的用户使用 `default_max_sessions`。

* 用户名按大小写敏感匹配；修改用户名时必须同步更新连接上限配置。

* 在线连接数不写进该配置文件；实时连接数由 etcd session 汇总。

* `limits` 是每台 Caster 的绝对上限；用户连接上限和黑名单是集群业务上限，两者同时生效。

* `max_connections` 统计 NTRIP 客户端、源站、传入 internal relay 和 internal monitor，不统计管理 HTTP；对外 relay 连接另由 `max_relays` 限制。

* `max_relays` 限制本 Caster 建立的外向上游 relay；`max_internal_relays` 限制本 Caster 作为 owner 接受的集群内部 relay 订阅。

* `subscriber_backlog_bytes` 默认 16 KiB，沿用 Millipede 的 `backlog_evbuffer=16384`；`socket_send_buffer_bytes` 默认 114688，对应 `backlog_socket=112 KiB`。

* `source_read_timeout_s=60` 沿用 Millipede 默认值；relay 和 on-demand 上游使用同类读超时。

* `client_idle_timeout_s` 只约束请求体、GGA 上行或需要客户端输入的阶段；普通只下行客户端即使不上传 GGA 也不能仅因读空闲而断开。

* `max_source_bitrate_bps` 和 `source_bitrate_action` 是资源保护规则；默认超过 2 Mbps 断开源站并进入短 cooldown。

* `max_total_ingress_bps` 和 `max_total_egress_bps` 是该节点所有流量的总保护阈值；超过时优先拒绝新连接，再按慢客户端策略降载。

* `auth_max_concurrency` 限制同时执行的 Argon2 校验数量，认证队列满时直接拒绝，不能让连接无限排队。

* 集群模式下配置文件由 etcd 统一下发，各 Caster 自动同步并在本地缓存。

* 新 Caster 加入集群时不手工复制该文件，而是从 etcd 获取最新 generation。

* 连接上限降低时默认只影响后续新连接，不自动踢除已有 session。

* `limits` 降低时同样只影响新连接 / 新 relay；需要立即降载时使用管理 API 主动断开。

* `ntrip` 限制请求行、Header、GGA 行和读写超时；任何行超过上限都关闭连接并计数。

* `source_validation.scope=admission_only`：只在源站从 `VALIDATING` 进入 `ACTIVE` 前检查格式、可用纠偏数据、MSM 版本和位置，不做周期性内容复查。

* `source_validation.mode=observe`：准入检查只生成 `data_profile` 和告警，不踢人，允许带风险上线。

* `source_validation.mode=enforce`：准入检查不通过则拒绝源站、关闭连接并写共享 cooldown。

* `rtcm3.require_usable_correction=true` 要求启动窗口内出现观测值或 SSR；只有 1005/1006/1033 的 RTCM3 源不算合格。

* RTCM2、BINEX、CMR 第一版只做格式级识别，识别成功即视为 “格式级纠偏源”，不解析具体观测值或 SSR。

* 无论模式如何，认证失败、单帧长度超限、内存 / 队列上限、读取超时、etcd lease 丢失和完全不产生数据都是硬故障，必须保护 Caster。

* `allowed_formats` 是允许出现的数据类型集合，`require_correction_data`、挂载点 `source_expectation` 和 MSM 规则分别独立判断是否合格。

### 3.4 BlacklistRule



```
{
  "schema_version": 1,
  "mod_revision": 452,
  "id": "b_01j...",
  "cidr": "192.168.0.0/16",
  "quota": 10,
  "enabled": true,
  "description": "Office NAT clients",
  "updated_at": "2026-11-19T10:00:00Z"
}
```

规则：



* 黑名单硬封禁（`quota=0`）适用于 NTRIP 客户端数据流和公网 monitor/ticket 入口，不适用于 NTRIP1/NTRIP2 源站上传、内部 Relay 或管理 API。

* 正数 CIDR quota 只计 NTRIP 客户端数据流；公网 WebSocket 使用独立的每 IP / 全局 WebSocket 限额。

* `quota=0` 是认证前的硬封禁；客户端数据流必须先完成 IP 规则判断，再执行 Argon2、用户认证和 etcd admission。

* `quota>0` 表示认证后参与跨 Caster 并发计数；`quota=-1` 表示既不放行封禁也不参与 IP 并发额度检查。

* 使用最长前缀匹配得到适用于当前 IP 的 quota。

* `id` 永久唯一；同一规范化 CIDR 不允许存在多个启用规则；更新使用 `mod_revision`/ETag 做 CAS。

* `quota=-1` 表示不限。

* `quota=0` 表示禁止。

* 正整数表示**每个客户端 IP** 的全局并发数据流上限，不是整个 CIDR 的合计上限。

* 只有直接对端命中可信代理前缀时才采信 XFF。

* XFF 从右向左解析，跳过可信代理，取第一个非可信地址作为客户端 IP。

* NTRIP 源站上传忽略 HTTP XFF；如需保留真实源 IP，只使用可信 L4 的 PROXY Protocol。

* IPv4、IPv4-mapped IPv6 和普通 IPv6 都要规范化为统一 key。

* 必须显式配置 IPv6 默认规则（例如 `::/0`）和 ULA 策略，不能用仅 IPv4 的示例推导默认行为。

* IPv6 临时地址会使 “同一设备” 的 quota 身份变化；客户端需要稳定配额时由管理员配置受控前缀或使用认证账号。

* 黑名单变更默认影响新连接；若启用 `blacklist_policy.disconnect_existing`，owner Caster 通过本地匹配关闭已连接客户端。

* NTRIP2 被 IP 封禁返回 403；NTRIP1 返回 403 或直接关闭连接，但不能进入密码校验或创建 session。

### 3.5 Client Session

客户端完成认证并订阅数据流后创建 session：



```
{
  "schema_version": 1,
  "session_id": "s_01j...",
  "session_type": "client",
  "user_id": "u_01j...",
  "username": "rtk_user",
  "auth_version": 1,
  "mountpoint": "C63",
  "near_current_mountpoint": null,
  "client_ip": "203.0.113.5",
  "remote_port": 52431,
  "caster_id": "caster-east-01",
  "runtime_epoch": "boot_01j...",
  "started_at": "2026-11-19T10:00:00Z",
  "ntrip_version": "Ntrip/2.0",
  "request_method": "GET",
  "auth_scheme": "Basic",
  "client_software": "NTRIP RTKLIB/2.4.3"
}
```

`GET /` 获取 STR、管理 API 和认证失败连接不进入该 session 计数。

集群管理 API 可以按 user、mountpoint 和 caster 聚合 session，并实时计算当前连接数；该数量不另设持久 count key。

客户端 session 在连接建立、连接结束和 NEAR 完成切换时写 / 删 etcd，不发送周期心跳或重复写记录；存活由所在 Caster 的 lease 维持。

NEAR 客户端 session 的 `mountpoint` 为配置值 `near.mountpoint`，`near_current_mountpoint` 记录当前选中的真实基站；一次切换完成后事件式更新该字段，不按 GGA 频率更新。

客户端没有 `User-Agent` 时 `client_software` 留空。Header 内容只作为不可信展示信息，不参与授权。

`caster_id` 是这条在线记录的 write owner：只有该 Caster 可以更新 / 删除自己的 session；其他 Caster 只读 Watch，需要强制断开时写 owner command。`runtime_epoch` 在每次 Caster 启动或 lease 重建后变化，用于识别旧进程恢复出来的幽灵状态。

### 3.6 Active Source



```
{
  "schema_version": 1,
  "mountpoint": "C63",
  "owner_user_id": "u_01j...",
  "auth_mode": "ntrip2_owner",
  "caster_id": "caster-west-02",
  "runtime_epoch": "boot_01j...",
  "source_generation": 1002,
  "connection_id": "c_01j...",
  "relay_endpoint": "10.20.0.12:12102",
  "state": "ACTIVE",
  "started_at": "2026-11-19T10:00:00Z",
  "str": {
    "identifier": "Shanghai Farm",
    "format": "RTCM 3",
    "format_details": "1005(10),1074(1),1084(1),1094(1),1124(1)",
    "carrier": "2",
    "nav_system": "GPS+GLO+GAL+BDS+QZS",
    "network": "CENTIPEDE-RTK",
    "country": "CHN",
    "lat": 31.234,
    "lon": 121.456,
    "solution": "0",
    "generator": "NTRIP RTKLIB/2.4.3",
    "bitrate": 1200
  },
  "admission": {
    "result": "PASS",
    "primary_format": "rtcm3",
    "msm_versions": [4],
    "position_seen": true,
    "usable_correction": true,
    "integrity_state": "VERIFIED",
    "warning_codes": []
  }
}
```

状态：



```
CLAIMING -> VALIDATING -> ACTIVE -> DRAINING -> ABSENT
                |
                +-> REJECTED -> ABSENT
```



* `CLAIMING`：已获得集群唯一源站名额，但数据源尚未就绪。

* `VALIDATING`：已收到源站请求，正在用有界样本窗口识别格式、检查纠偏数据、位置和 MSM。此时数据不进入公共 STR、fan-out 或 relay。

* `ACTIVE`：已按当前策略准入，开始接收并转发源数据；准入 profile 冻结，后续不周期重检。

* `REJECTED`：源数据未通过校验，正在写入 backoff 并清理。

* `DRAINING`：正在关闭，不再接受新订阅。

数据准入和内容质量是两个维度：



* `state=ACTIVE` 表示当前策略允许该源站被订阅。

* `validation_result=PASS` 表示全部启用的内容规则通过。

* `validation_result=WARN` 表示存在非阻断告警或 `observe` 模式下发现了不符合期望的数据。

* `validation_result=FAIL` 只在 `observe` 模式下允许与 `ACTIVE` 同时出现，用于明确标记错误模块；`enforce` 模式必须转为 `REJECTED`。

* `profile_state=UNKNOWN` 表示已收到数据但尚不能可靠识别协议；`profile_state=INCOMPLETE` 表示主要格式已识别，但位置、MSM 或字段统计尚未完整。

`runtime/sources/{mountpoint}` 是每个挂载点的 canonical online record：



* `VALIDATING`：连接后立即写入的临时 STR，使用配置中的默认站点 / 格式 / 坐标，公开 STR 不包含。

* `ACTIVE`：解析后修正轻量 STR 字段、位置、格式、MSM 摘要和准入结果，公开 STR 才包含。

* `DRAINING`：停止新订阅，已有连接按关闭策略处理。

* `REJECTED`：短暂记录原因用于管理审计；随后删除并写 cooldown。

`runtime/sources` 只保存轻量路由和 STR 必需字段：

* source identity、generation、connection、owner epoch、relay endpoint、state。
* STR 的 identifier、format、format-details、carrier、nav-system、lat/lon、solution、generator、bitrate。
* 准入摘要：primary format、MSM versions、position seen、usable correction、integrity 和 warning codes。

完整 `data_profile`、消息类型计数、CRC/unknown 计数、Source IP、Source-Agent、1033 设备、天线序列号和实时 health 只保留在 owner Caster 本地。管理 API 通过 `caster_id` 回源查询，不写第二个 etcd profile key。这样 source route Watch 只在上线、状态变化、最终 STR 修正和下线时触发。

不能向 etcd 写整张多挂载点 STR 文本；整表由每个 Caster 根据轻量 canonical records 本地生成。

### 3.7 Relay

Relay 只存在于 Caster 本地内存，不进入公共 STR，也不写 `runtime/sources`。



```
{
  "mountpoint": "C63",
  "upstream_caster_id": "caster-west-02",
  "upstream_source_generation": 1002,
  "upstream_connection_id": "c_01j...",
  "upstream_endpoint": "10.20.0.12:12102",
  "state": "ACTIVE",
  "local_subscribers": 12,
  "last_packet_at": "2026-11-19T10:05:01Z"
}
```

### 3.8 核心不变量



1. 一个 mountpoint 只能绑定一个 owner user。

2. 一个 mountpoint 在集群内最多有一个 `ACTIVE` 源站。

3. NTRIP1 源站认证只使用挂载点上传密码。

4. NTRIP2 源站认证只使用 owner 用户名和用户密码。

5. 客户端认证不检查挂载点 owner。

6. 每个用户的数据流 session 总数不超过 Caster Config 中该用户的有效 `max_sessions`。

7. 每个 Caster 对每个远端 mountpoint 最多一条 relay。

8. relay 不能被当作新的源站占用。

9. 每个差分数据包不访问 etcd。

10. 所有集群在线 key 都绑定 Caster lease。

11. NEAR 是系统合成挂载点，其名称来自 `near.mountpoint`，不属于任何用户或普通 Mountpoint。

12. 任何源站都不能向配置的 `near.mountpoint` 上传数据。



***

## 4. 认证与访问控制

### 4.1 认证矩阵



| 场景                              | 必须验证                                             | 不验证                    | 计入用户             |
| ------------------------------- | ------------------------------------------------ | ---------------------- | ---------------- |
| NTRIP1 `SOURCE password /MOUNT` | mountpoint 的 NTRIP1 上传密码                         | 用户名、用户密码               | mountpoint owner |
| NTRIP2 `POST /MOUNT` + Basic    | Basic username 必须是 owner；password 必须是 owner 用户密码 | NTRIP1 上传密码            | mountpoint owner |
| 客户端认证开启                         | Basic 用户名和用户密码                                   | mountpoint owner、上传密码  | 当前客户端用户          |
| 客户端认证关闭                         | 无                                                | 用户身份和 mountpoint owner | 不创建用户 session    |

### 4.2 源站认证

NTRIP1：



```
SOURCE password [/]MOUNT
```



* 根据 mountpoint 找到配置。

* 验证 `ntrip1_upload_password_hash`。

* 通过后 effective user 为 `owner_user_id`。

* owner 必须 `enabled=true` 且 `ntrip_enabled=true`。

如果该挂载点没有 NTRIP1 上传密码，则拒绝 NTRIP1 上传。

NTRIP2：



```
POST /MOUNT
Authorization: Basic base64(owner_username:owner_password)
```



* 必须存在 Basic 用户名和密码。

* 用户名必须等于 mountpoint owner 的用户名。

* 密码必须通过 owner 用户 `password_hash` 校验。

* owner 必须 `enabled=true` 且 `ntrip_enabled=true`。

* 不检查 `ntrip1_upload_password_hash`。

### 4.2.1 源站软件信息

Basic `Authorization` 本身只有用户名和密码，不包含基准站软件版本。软件信息从允许记录的请求 Header 提取：



* `User-Agent`

* `Source-Agent`

* `Ntrip-Version`

* 实际请求方法 `SOURCE` 或 `POST`

两个来源同时存在时，原始值都记录，展示时优先 `Source-Agent`。

两个字段都不存在时保存空值，不伪造客户端名称。

只保存截断后的安全字符串，不保存 `Authorization` 原文或密码。信息写入 `runtime/sources/{mountpoint}`，供管理 API 展示源站使用的软件和协议。

这些 Header 是不可信展示信息，不能用于授权判断。

进入日志或 STR 前必须去除控制字符、CR/LF，并替换分号；建议最大保留 128 字节。

### 4.3 客户端认证开关

配置保存在 Caster Config 的 `client_auth` 节点：



```
client_auth:
  enabled: true
  require_tls: true
  track_anonymous_streams: false
  disconnect_existing_on_change: false

source_auth:
  require_tls: true
```

`client_auth.require_tls` 和 `source_auth.require_tls` 是唯一 TLS 强制配置；不再使用 `require_tls_for_client_auth` 等其他同义字段。允许明文认证需要管理员显式关闭对应选项。



* `enabled=true`：`GET /MOUNT` 必须提供有效的客户端用户名和密码。

* `enabled=false`：`GET /MOUNT` 可以匿名访问。

* 无论客户端认证开关是否开启，`GET /MOUNT` 都先检查客户端 IP 黑名单；认证开关不能绕过 IP 封禁。

* IP 封禁检查在 Argon2 和用户认证之前执行；`quota=0` 立即拒绝，`quota>0` 的在线计数在认证完成后执行。

* 客户端认证只验证用户账号，并要求 `enabled=true`、`ntrip_enabled=true`。

* 客户端不使用 `ntrip1_upload_password`。

* 客户端不检查 mountpoint owner。

* `GET /` 获取公共 STR 不要求客户端认证，也不创建用户 session。

* 管理 API 不计入 NTRIP 数据流 session。

* 开关变更默认只影响新连接；管理员可以启用 `disconnect_existing_on_change` 立即撤销已有客户端。

### 4.4 用户禁用和密码修改



* 新连接在准入前比较本地缓存配置的 `mod_revision`，防止使用过期密码或已禁用用户。

* 用户被删除或禁用时，立即关闭该用户的源站连接。

* 用户被删除或禁用时，默认立即断开该用户的客户端数据流；可以配置为仅禁止新连接。

* 密码修改不自动踢除已有客户端或源站，除非管理员主动断开或配置要求重新认证。

* `ntrip_enabled=false` 立即关闭该用户的 NTRIP 客户端和源站连接，但不影响仍在有效期内的管理 API Token（Token 由 auth\_version / 自身状态单独控制）。

### 4.5 管理 API 认证

管理账号复用 User 存储，但必须具有 `role=admin`。不使用单独的明文管理员密码文件。

#### 浏览器会话



1. 管理页面通过 TLS 向 `POST /admin/v1/auth/login` 提交用户名和密码。

2. 服务端使用 Argon2id 校验用户密码。

3. 校验成功后生成 256-bit 随机会话 token。

4. 返回 `HttpOnly; Secure; SameSite=Strict` Cookie。

5. 同时返回一次性 CSRF token，写操作必须携带对应 Header。

6. 服务端只保存 session token 的哈希，不保存 token 明文。

登录请求使用 JSON 或表单 body，不使用 URL query。登录成功后浏览器只持有 session Cookie 和 CSRF token。

集群共享会话：



```
/rcaster/{cluster_id}/runtime/admin_sessions/{token_hash}
```

值包含：



* user\_id

* auth\_version

* 创建 / 过期时间

* 登录 IP

* User-Agent

* CSRF token hash

* scopes

Admin Session 使用绝对 `expires_at`，不绑定创建它的 Caster lease；任意 Caster 的 janitor 在过期后删除。退出登录立即删除；请求时若 session 的 auth\_version 与当前 User 不一致，立即拒绝并删除。

#### API Token

自动化调用使用 `Authorization: Bearer <token>`。



* Token 由管理员创建，明文只展示一次。

* 服务端保存 HMAC/SHA-256 哈希、token ID、user\_id、scopes、过期时间和启用状态。

* Token 可单独撤销。

* Token 记录创建时的 auth\_version；用户密码、角色或状态变化后旧 token 自动失效。

* 不在 URL、日志或 WebSocket query 中传递 token。

存储记录：



```
{
  "schema_version": 1,
  "token_id": "atk_01j...",
  "user_id": "u_01j...",
  "auth_version": 1,
  "token_hash": "...",
  "scopes": ["sources:read", "stream:monitor"],
  "enabled": true,
  "expires_at": "2027-01-01T00:00:00Z"
}
```

建议 scopes：



```
users:read users:write
mounts:read mounts:write
config:read config:write
sessions:read sessions:drop
sources:read sources:drop
monitor:read
stream:monitor
operations:read operations:write
upgrade:read upgrade:write
cluster:admin
```

NTRIP Basic 用户认证和管理 API 认证相互独立。NTRIP 密码不能直接作为管理员 API Bearer token。

NTRIP2 源站和客户端复用 User 密码是本方案的明确选择。该模型的残余风险是 Rover 凭据泄露后可尝试上传该用户拥有的基准站；第一版通过 TLS、源站 owner 校验和 rate limit 降低风险。`credential_scope` / 独立 Source Credential / 客户端专用密码列为正式 V2 安全版本范围，不作为永久占位字段。

### 4.6 WebSocket 认证

浏览器 WebSocket 使用登录 Cookie 建立连接，并额外校验：



* `Origin` 必须匹配配置允许的管理站点。

* Cookie session 必须有效。

* 建立连接后先发送认证确认消息。

* 权限变更、session 撤销或用户禁用时关闭连接。

* 每个管理员最多 `max_ws_per_admin` 条连接；每条连接使用 `ws_queue_events` 有界发送队列。

* 单条 WebSocket 消息不得超过 `ws_max_message_bytes`，服务端每 `ws_heartbeat_s` 发心跳并清理失活连接。

* 慢 WebSocket 只丢自己的监视事件，不能反压解析器、relay 或其他管理员。

不使用 `?token=` 查询参数，避免 token 出现在代理和访问日志中。

### 4.7 公网 STR、地图和登录后解析可视化

匿名公网只开放 STR 和覆盖地图：



* `GET /api/v1/public/str`：本地公共 STR 快照的 JSON 投影。

* `GET /api/v1/public/map/stations`：从同一公共 STR 快照提取 mountpoint、lat/lon 和状态，不读取私有离线站点表。

* 覆盖圈半径可来自 STR `misc` 的 `coverage_m`；缺失时使用 `public_web.coverage_default_radius_m`。

* 匿名接口不返回用户信息、Source IP、设备序列号、Token、内部 Relay endpoint 或不可发布字段。

Web 登录：



* `POST /api/v1/auth/login` 使用共享 User 数据库和 Argon2id，要求 `enabled=true`、`web_enabled=true`。

* 普通 User 默认获得 `monitor:read` scope；管理员按角色和 scopes 获取更多能力。

* 登录 session 写入 `runtime/web_sessions/{token_hash}`，绑定 user\_id、auth\_version、过期时间和 CSRF hash。

* Cookie 为 `HttpOnly; Secure; SameSite=Strict; Path=/api/v1`，不保存密码或 token 明文。

* Web session 不绑定某个 Caster；任意承接 Web 的管理节点都可以验证。

* 用户禁用、`web_enabled=false`、密码 / 角色变化或 session 撤销时关闭对应 WebSocket。

* 每个 Web 节点维护 `user_id -> local websocket subscriptions` 反向索引；`config/users` Watch 发现 auth\_version 或 web\_enabled 变化时立即关闭本节点连接。

* Web monitor session 不创建 NTRIP user session，也不占用 NTRIP 用户连接数。

点击 STR 条目进行解析前必须登录 User：



1. 前端调用独立 Web 登录入口，用户名 / 密码从共享 User 数据库验证。

2. 要求 User `enabled=true`、`web_enabled=true`，创建短期 Web session。

3. 登录后申请 monitor ticket，ticket 绑定 user\_id、session\_id、canonical IP、mountpoint、source generation 和过期时间。

4. ticket 只能针对当前公共 STR 中 `ACTIVE` 且可发布的 mountpoint；不允许通过猜测名称访问非公开记录。

5. 浏览器通过 HttpOnly Cookie 建立同源 WebSocket，不接受 query token。

6. 用户被禁用、`web_enabled=false`、auth\_version 变化、session 过期或权限撤销时，立即关闭 WebSocket。

解析位置：



* 解析在**当前承接该 Web 连接的管理节点**执行，不在 source owner 创建远程解析线程。

* 本机有 source 时直接订阅本地 Hub；本机没有 source 时先建立或复用 Relay，再在本地 Relay 输出上解析。

* 每台 Web 管理节点对每个 Mountpoint 最多一个本地 `MonitorSession`；本节点所有授权 WebSocket 订阅者共享它。

* `MonitorSession` 使用 `DashMap entry + Starting/Active/Closing` 原子状态机，后续 WebSocket 只增加 refcount 和独立 bounded queue。

* MonitorSession 生成不可变 snapshot 后通过每个 WebSocket 的独立 `mpsc` 发送；不使用共享 broadcast ring，避免慢浏览器互相影响。

* 不同 Web 管理节点可以各自有一个本地 MonitorSession；若要求全集群唯一解析，必须把所有 monitor 固定路由到单一 monitor gateway，本默认架构不这样做。

* 订阅数从 0→1 时启动解析；从 1→0 时立即取消 MonitorSession，并关闭仅供该 monitor 使用的 Relay consumer。

* `monitor_idle_grace_s` 默认 0；如现场页面频繁重开，可配置短暂 grace，但不能把解析器永久保留给无人订阅的挂载点。

* source generation/connection\_id 变化或 Relay 上游断开时关闭旧 MonitorSession，前端收到事件后重新订阅。

* RTCM3 提供卫星 / MSM/C/N0 可视化；RTCM2/BINEX/CMR 只显示格式识别状态；NMEA-only 显示为无效纠偏源。

WebSocket 事件：



```
monitor_ready
monitor_snapshot
monitor_busy
source_changed
monitor_closed
heartbeat
```

`monitor_snapshot` 只发送聚合字段：mountpoint、UTC 时间、format、MSM 版本、卫星统计、信号系统 / 载波、C/N0、位置可用性和 bitrate。不得发送原始 RTCM/CMR/BINEX 数据。

资源限制：



* `public_web.max_ws_connections`：节点 WebSocket 总上限。

* `public_web.max_ws_per_ip` 和 `max_ws_per_user`：每 IP/User WebSocket 上限。

* `public_web.max_monitor_subscribers_per_ip` 和 `max_monitor_subscriptions_per_user`：每 IP/User monitor 订阅上限。

* `public_web.max_monitor_sessions_per_node`：每 Web 管理节点最大本地解析会话数。

* `public_web.max_subscribers_per_monitor`：单挂载点最大 viewer。

* `public_web.monitor_start_rate_per_minute`：限制解析器启动频率。

* 超过限制返回 429/503 `MONITOR_BUSY` 和 `Retry-After`；前端显示 “解析繁忙，请稍后再试”，不能为每个浏览器单独启动解析。

* 慢 WebSocket 只丢弃自己的快照，不影响解析器、Relay 或其他 viewer。

匿名 STR / 地图来自本地不可变 STR 快照；登录后的解析订阅才建立 Relay/MonitorSession，不在每次页面请求时扫描 etcd。



***

## 5. 配置与存储

### 5.1 本地数据库

用户、挂载点和 owner 关系使用单文件嵌入式 KV 数据库，不使用 SQL 业务表。推荐 `redb`：



```
meta/schema_version

user/id/{user_id}
user/name/{urlsafe_username} -> user_id
api_token/id/{token_id}

mount/name/{urlsafe_mountpoint}
blacklist/id/{rule_id}
```

所有记录采用带 `schema_version` 的 JSON 或二进制序列化。

### 5.2 Caster Config 文件

Caster Config 保存所有节点共享的运行参数：

默认本地文件为 `caster-config.yaml`；集群模式由 etcd 下发并保留本地快照。



* 默认和按用户连接上限。

* 黑名单的全局策略；具体 CIDR/quota 规则保存在独立 `config/blacklist/`。

* 客户端认证开关。

* 统一端口。

* relay、STR、NEAR 和超时参数。

文件带单调递增 `generation`。所有 Caster 必须使用同一个 generation。配置校验包括：



* 端口范围和冲突。

* `cluster.config_max_staleness_s` 必须大于 0；启用 `admission_requires_current_config` 时，安全准入必须校验当前 etcd config revision。

* CIDR 和最长前缀合法性。

* `per_username` 对应用户是否存在。

* 连接上限是否为正数。

* STR scope 和阈值。

* `near.mountpoint` 不得与普通 mountpoint 重名。

* `max_hdop`、`hdop_m_per_unit` 和重算时间必须合法。

* `source_validation.mode` 只能是 `observe` 或 `enforce`。

* `initial_data_timeout_s`、`admission_profile_window_ms` 和 `max_frames_inspected` 必须大于 0。

* `allowed_formats` 和 `correction_formats` 只能使用 `rtcm3`、`rtcm2`、`binex`、`cmr`、`nmea`；纠偏格式不能包含 `nmea`。

* `source_validation.msm.allowed_versions` 可包含 1–7；`msm.restrict_versions=true` 时不得为空。默认常用值是 4 或 7。

* Mountpoint 的 `source_expectation.formats` 必须使用同一格式枚举；`msm_versions` 可包含 1–7。

* `unknown_data.max_ratio` 必须在 0 到 1 之间，`unknown_data.min_observed_bytes` 必须大于 0。

* `source_validation.rtcm3.min_valid_frames` 和 `min_correction_frames` 必须大于 0；`min_valid_frame_ratio`、`max_crc_error_ratio` 必须在 0 到 1 之间。

* `source_health.observation_timeout_s` 必须大于 0；有效帧比例和 CRC 比例必须在 0 到 1 之间；第一版 `action` 只能是 `alert`。

* `source_health.short_window_s`、`long_window_s`、`alert_clear_s` 和 `base_position_jump_m` 必须大于 0；short window 不得大于 long window。

* `near.allowed_formats` 第一版只能包含 `rtcm3`；`switch_observe_ms` 必须大于 0，切换位置阈值和 hysteresis 不得为负。

* `near.position_ttl_s` 必须为非负数；0 表示只在当前 source 在线期间有效，`position_ttl_action` 第一版只能为 `exclude_candidate`。

* `near.gga_max_input_per_second` 和 `gga_max_jump_speed_mps` 必须大于 0；`max_query_concurrency` 必须大于 0。

* `limits.max_connections`、`max_sources`、`max_subscribers`、`max_relays`、`max_internal_relays`、各队列长度、单源 bitrate 和总带宽必须为正数。

* `limits.source_bitrate_action` 只能是 `observe` 或 `disconnect`。

* `max_sources <= max_connections`，`max_subscribers <= max_connections`，`max_subscribers_per_source <= max_subscribers`。

* `max_internal_relays` 与 `max_relays` 独立，但两者都必须受节点 FD 和内存预算约束。

* `ntrip.gga_line_max_bytes` 不得超过 `ntrip.headers_max_bytes`，所有超时值必须大于 0。

* `ntrip.content_length_max_bytes` 必须大于 0；源站持续流不受该值限制，但显式 Content-Length 和普通 HTTP body 受该值限制。

* `ntrip.accept_rate_per_second` 和 `tls_handshake_timeout_ms` 必须大于 0；4C4G 档可按压测结果提高 accept rate。

* 集群模式存在正整数匿名 IP quota 时，不得同时设置 `client_auth.enabled=false` 和 `track_anonymous_streams=false`。

* `admin_api.max_ws_per_admin`、`ws_queue_events`、`ws_max_message_bytes` 和 `ws_heartbeat_s` 必须大于 0。

* `public_web.max_ws_connections`、`max_ws_per_ip`、`max_ws_per_user`、`ws_queue_events`、`ws_heartbeat_s`、`max_monitor_subscribers_per_ip`、`max_monitor_subscriptions_per_user`、`max_monitor_sessions_per_node`、`max_subscribers_per_monitor` 和 `monitor_start_rate_per_minute` 必须大于 0。

* `public_web.monitor_snapshot_interval_ms` 必须是合理值；当前 UI 默认不低于 500ms，不能允许无限制高频推送。

* `public_web.monitor_idle_grace_s` 必须大于或等于 0；0 表示最后一个订阅者离开后立即取消 MonitorSession。

端口由集群配置统一下发。Caster 本地只配置绑定 IP、证书路径和 bootstrap etcd 地址。修改公共端口需要所有 Caster 重新绑定 listener，管理 API 应返回需要重启或滚动重载的状态。

集群模式下的配置流：



```
管理员修改配置文件
  -> rcaster-admin config validate
  -> rcaster-admin config apply
  -> etcd Txn 保存新 generation
  -> 所有 Caster watch
  -> 本地配置缓存更新
  -> ArcSwap<ConfigSnapshot> 原子替换
```

也可以由管理 API 修改同一份配置。直接修改某台 Caster 的本地缓存文件不会传播到集群。

配置生效分类：



| 类别           | 示例                                         | 行为                             |
| ------------ | ------------------------------------------ | ------------------------------ |
| 热更新          | 连接上限、黑名单、认证开关、STR 默认值、NEAR 参数              | 校验后 ArcSwap，新连接立即采用            |
| drain/reload | 已有客户端认证策略、Connections 降载、source validation | 可保持旧连接，按配置决定是否断开               |
| 重启绑定         | NTRIP/TLS/relay/admin/metrics 端口、bind 地址   | 返回 `restart_required`，滚动重启     |
| 仅 local node | 证书、etcd bootstrap、relay advertise address  | 写节点本地配置，不进入 etcd Caster Config |

配置校验失败时保留旧 `ConfigSnapshot`，新 generation 不生效并产生告警。安全相关的客户端认证、源站认证、黑名单和连接上限不得降级为 “尽力而为”。

### 5.3 Standalone



* Account DB 是用户和挂载点主存储。

* Caster Config 文件是连接上限、黑名单策略和运行参数主存储；standalone 的黑名单规则存入本地 Account DB 独立前缀。

* 管理 API 写入 Account DB 或 Caster Config。

* 写入成功后重建内存快照。

* 不依赖 etcd。

### 5.4 Cluster



* etcd 是用户、挂载点和 Caster Config 的主存储。

* 本地 Account DB 保存账号和挂载点快照。

* 本地 Caster Config 文件保存当前 generation 的配置快照。

* 所有业务修改由管理 API 或 `rcaster-admin config apply` 写入 etcd。

* 各 Caster 首次加入时读取 users/mounts/config 一致快照，之后只接收前缀 Watch 增量并更新本地快照和内存配置。

* 不允许后台任务周期把全部用户、挂载点或在线状态重新推送给每个 Caster。

* 禁止仅修改本地文件后假设其他 Caster 会自动同步。

加入新 Caster：



1. 使用 bootstrap 配置连接 etcd。

2. 校验 cluster ID 和 schema version。

3. 获取 users、mounts、blacklist、api\_tokens、Caster Config generation 和当前 revision。

4. 写入本地 Account DB 与配置缓存。

5. 从 revision + 1 开始 watch。

6. 配置加载完成后启动 NTRIP listener。

### 5.5 配置缓存一致性



```
读取 etcd accounts/Caster Config 快照和 revision
  -> 本地 Account DB 事务替换
  -> 写入本地 Caster Config generation
  -> 构建 ConfigSnapshot
  -> 从 revision + 1 开始 watch
  -> 每个 watch batch 落盘并推进 revision
  -> ArcSwap 原子替换内存快照
```

如果 watch 返回 `compacted` 或 revision 断裂：



1. 停止使用过期业务配置。

2. 重新读取 etcd 一致快照。

3. 整体替换本地 Account DB 和配置缓存。

4. 重建索引后恢复服务。

配置生效确认：



```
runtime/config-acks/{generation}/{caster_id}
```



* Caster 校验并应用 ConfigSnapshot 后写入 ack，ack 绑定该 Caster lease。

* ack 包含 generation、etcd mod\_revision、软件版本、applied\_at、need\_restart 和 listener\_status。

* `runtime/config-acks` 只描述当前 active generation 的应用状态，不是持久 node 表；Caster 停机后由 lease 清理。

* 管理 API 返回 `COMMITTED`、`APPLIED_OBSERVED`、`RESTART_REQUIRED` 或 `STALE_OBSERVED`，并显示已确认 Caster 数；没有外部部署清单时不能声称 “全部节点已应用”，只能报告实际观察到的 active lease ack。

* 安全准入不完全依赖 ack 传播：数据面 Txn 另外比较当前 `config/caster` mod\_revision，旧 Caster 不能继续接受新 session。

### 5.6 配置写入语义



* 每条记录带 revision。

* 用户、挂载点和黑名单规则分别使用 compare-and-swap 更新单条记录。

* Caster Config 使用 generation CAS 整份更新。

* 关联修改，例如修改连接上限并更新黑名单，通过一次 Caster Config Txn 提交。

* 管理员必须显式确认批量删除。

* 所有配置修改写入审计日志。

### 5.7 旧数据

不兼容旧 `source.auth`、`host.auth` 和 `sourcetable.dat` 作为运行格式。

可以提供一次性导入工具：



* 从旧文件读取用户、挂载点和密码。

* 要求管理员补充 mountpoint owner。

* 将旧明文密码升级为 Argon2id。

* 导入前生成校验报告，不直接覆盖现有配置。

正式迁移工具的最小流程：

1. 离线只读解析旧账号和 sourcetable，不改旧服务。
2. 管理员提供 username 到 user_id、mountpoint 到 owner user 的映射；无法自动推断的 owner 作为阻断项。
3. 旧明文或可识别 Hash 的密码逐个 rehash 为 Argon2id；无法验证的 Hash 不导入，生成一次性密码重置清单。
4. 先导入 staging Account DB，执行数量、重复 username/mountpoint、owner 引用、CIDR 和 schema 校验。
5. 校验通过后一次性切换；失败丢弃 staging，不触碰当前集群。
6. 分批导入集群时每批有 manifest、计数和 checksum；全部成功后才切换 generation。
7. 迁移前备份目标 Account DB/etcd，迁移后核对用户数、挂载点数、owner 数、黑名单数和密码重置数。

迁移工具只兼容数据，不兼容旧运行时、旧 API 或旧进程生命周期。

### 5.8 节点本地配置

以下内容不进入共享 `caster-config.yaml`，每台 Caster 使用独立的 node config：



```
cluster_id: main
caster_id: caster-west-02
node_role: core
region: west-china

public_endpoints:
  client_host: caster.example.com
  source_host: src-west.example.com
  admin_host: admin.example.com

bind:
  ntrip: ["0.0.0.0:2101", "[::]:2101"]
  ntrip_tls: ["0.0.0.0:2102", "[::]:2102"]
  relay: ["10.20.0.12:12102"]
  admin: ["0.0.0.0:8080"]
  metrics: ["127.0.0.1:9100"]

tls:
  cert_path: /etc/caster/tls/server.crt
  key_path: /etc/caster/tls/server.key
  ca_path: /etc/caster/tls/cluster-ca.crt

etcd:
  endpoints: ["https://10.20.0.11:2379", "https://10.20.0.12:2379", "https://10.20.0.13:2379"]
  client_cert: /etc/caster/etcd/client.crt
  client_key: /etc/caster/etcd/client.key
  ca_path: /etc/caster/etcd/ca.crt

relay:
  advertise_endpoint: 10.20.0.12:12102

proxy:
  trusted_l4_cidrs: ["10.20.0.0/24"]
  trusted_http_cidrs: ["10.20.0.0/24"]
  use_proxy_protocol: true

local:
  account_db_path: /var/lib/caster/accounts.redb
  config_cache_path: /var/lib/caster/caster-config.cache
  audit_log_path: /var/log/caster/audit.jsonl

updater:
  enabled: true
  manifest_public_key: /etc/caster/update-public.ed25519
  active_slot: /opt/caster/current
  staging_dir: /var/lib/caster/updates
  systemd_unit: caster.service
```

规则：



* 证书、私钥、etcd client key 和 internal service secret 使用文件权限保护，不写日志。

* `caster_id` 在一个 cluster 中必须全局唯一且重启后保持稳定；`cluster_id` 防止误连其他集群。

* `node_role=core` 的 Caster 启用完整管理 API；edge 默认关闭管理 listener。角色是节点本地部署属性，不建立 etcd node 表。

* `region` 决定该 Caster 适合的 Source 入口和 regional STR；数据源区域域名必须只路由到相同 region 的可用节点。

* `public_endpoints` 只用于证书、CAS 和管理展示，不是 listener bind 地址；实际本地地址仍由 `bind` 决定。

* L4 负载均衡启用 PROXY Protocol 时，必须配置可信 CIDR；非可信来源发送 PROXY Header 直接拒绝。

* PROXY Protocol v1/v2 必须在 TLS 握手前解析；解析失败不能继续当普通 HTTP/TLS 处理。

* TLS 证书支持原子热加载；配置变更失败时继续使用旧证书。

* 证书必须包含所有公开 Caster / 管理域名 SAN，并支持 SNI 选择；不依赖客户端默认证书名。

* 证书在到期前 30/14/7 天产生 WARNING/CRITICAL 告警；CA 轮换支持旧 + 新 CA bundle 并行宽限期。

* TLS 部署支持双证书切流：先让信任新 CA / 证书的节点共存，再切换新证书，最后移除旧 bundle。

* `relay.advertise_endpoint` 必须从其他 Caster 可达，否则本节点不能作为 source owner。

* `/metrics` 默认只绑定 loopback 或内网；暴露公网时必须单独认证。

* 客户端和源站建议通过 GeoDNS/L4 负载均衡进入任意 Caster；源站连接应在一次会话内保持同一后端。

### 5.9 存储边界和主从关系

集群模式明确采用：

```text
etcd = 定义层与集群运行时权威存储
本地 Account DB / Caster Config = 只读快照和故障降级数据
```

因此：

- User、Mountpoint、Blacklist、API Token 和 Caster Config 的集群权威版本只写 etcd；本地写成功后不会自动同步到 etcd。
- Standalone 模式下本地 Account DB 和 Caster Config 才是主存储，不需要 etcd。
- 第一版不做“本地离线写入后再合并”的双主模型；集群管理员在 etcd 不可用时写接口返回 503。
- 认证和挂载点读取使用本地快照做热路径，但写准入前仍校验 etcd revision，不能把本地缓存当成权威数据。

实现边界不使用一个泛化的 `Repository` trait。业务代码只需要两个窄接口：

```text
DefinitionStore
  - get user/mount/blacklist/config snapshot
  - admin CAS create/update/delete

ClusterRuntime
  - session/source claim Txn
  - lock + lease + fencing
  - watch revision and runtime state
```

`DefinitionStore` 在 standalone 使用 redb/file 实现，在 cluster 使用 etcd-backed snapshot 实现；`ClusterRuntime` 只由 `cluster`/session/source 模块调用。数据面的 hub、relay、parser、fan-out 不得依赖 etcd client。

故障原则：

- etcd 不可达但 lease 尚未过期时，已建立的数据流可以继续本地转发；新准入、owner 注册和定义变更受阻。
- lease deadline 到达后按 FenceGuard 关闭旧 source/relay 和需要精确在线状态的 session。
- 恢复后重新读取一致快照，从 `revision + 1` 追 Watch；不能把本地旧状态重新推回 etcd。



***

## 6. etcd 集群状态

### 6.1 Key 结构



```
/rcaster/{cluster_id}/config/users/{user_id}
/rcaster/{cluster_id}/config/api_tokens/{token_id}
/rcaster/{cluster_id}/config/mounts/{urlsafe_mountpoint}
/rcaster/{cluster_id}/config/blacklist/{rule_id}
/rcaster/{cluster_id}/config/caster
/rcaster/{cluster_id}/runtime/config-acks/{generation}/{caster_id}
/rcaster/{cluster_id}/runtime/version-status/{caster_id}
/rcaster/{cluster_id}/runtime/upgrade-states/{operation_id}/{caster_id}
/rcaster/{cluster_id}/runtime/upgrade-commands/{operation_id}/{caster_id}

/rcaster/{cluster_id}/runtime/source-claims/{urlsafe_mountpoint}
/rcaster/{cluster_id}/runtime/source-generations/{urlsafe_mountpoint}
/rcaster/{cluster_id}/runtime/sources/{urlsafe_mountpoint}
/rcaster/{cluster_id}/runtime/source-cooldowns/{source_fingerprint}
/rcaster/{cluster_id}/runtime/source-commands/{urlsafe_mountpoint}/{command_id}
/rcaster/{cluster_id}/runtime/operations/{operation_id}

/rcaster/{cluster_id}/online/users/{user_id}/sessions/{session_id}
/rcaster/{cluster_id}/online/ips/{canonical_ip}/client-sessions/{session_id}
/rcaster/{cluster_id}/online/commands/{caster_id}/{command_id}
/rcaster/{cluster_id}/runtime/admin_sessions/{token_hash}
/rcaster/{cluster_id}/runtime/web_sessions/{token_hash}

/rcaster/{cluster_id}/locks/users/{user_id}
/rcaster/{cluster_id}/locks/ips/{canonical_ip}
/rcaster/{cluster_id}/locks/upgrade/global
```

没有：



```
/runtime/nodes/*
/nodes/*
```

`source-generations` 是集群安全不变量的一部分：正常下线、删除 Mountpoint 或 Caster 崩溃都不能删除该计数器；恢复时必须确认它不低于已观察到的最大 generation，否则阻断所有新 Source claim，直到管理员确认旧 owner 全部退出并设置恢复下限。

### 6.2 Caster Lease

每个 Caster 进程创建一个 etcd lease：



* 自己创建的 NTRIP session key、source claim 和 source key 绑定 Caster lease。

* Admin Session 不绑定 Caster lease；它使用绝对 `expires_at`，由集群 janitor 清理，避免一个 Caster 崩溃导致所有管理员退出。

* 正常断开时主动删除 key。

* 进程崩溃时由 lease 自动删除。

* lease 本身不要求绑定 node key。

* keepalive 由独立 supervisor task 执行。

建议：



* lease TTL：10–15 秒。

* keepalive 间隔：TTL 的 1/3。

* safety margin：至少 2 秒。

### 6.3 自 Fencing

如果 lease 续约失败或 etcd 不可达：



* 在 lease 到期前停止接收新源数据。

* 在 lease 到期前关闭本机 source 连接并清理 source claim/runtime source。

* 停止创建新 relay。

* 停止创建新用户 session。

* `safe` 模式下关闭依赖集群计费的数据流；lease deadline 到达后关闭所有本机 NTRIP client/source session，避免本地继续存在 etcd 已无记录的幽灵连接。

* 租约恢复后重新同步配置并恢复服务。

关闭源站必须早于租约到期，不能等到 source key 已经被 etcd 删除后再关闭，否则其他 Caster 可能先获得同名源。

### 6.4 用户连接配额

配额不通过简单的 count +1 实现。流程：



```
规范化客户端 IP（仅客户端数据流）
  -> 最长前缀查询 IP rule
  -> quota == 0：立即拒绝，不执行 Argon2、不获取 user lock
  -> 认证用户（客户端认证开启时）
  -> 获取 user admission lock
  -> 保存 lock holder key/value/lease 和 create_revision
  -> 线性读取该用户全部 session
  -> 从当前 Caster Config 计算 effective max_sessions
  -> 仅 quota > 0 的客户端数据流获取 IP admission lock
  -> 保存 IP lock holder identity
  -> 线性读取该 IP 全部 session
  -> 检查正数 quota 的并发数量
  -> etcd Txn:
       IF user lock holder identity 仍匹配
       AND IP lock holder identity 仍匹配（如适用）
       AND config/caster mod_revision == ConfigSnapshot revision
       THEN 写入 user index；仅客户端写入 ip index
  -> 成功后返回 NTRIP 成功响应
```



* 锁顺序固定为 user -> IP。

* `quota=0` 是 pre-auth deny，不允许为了显示更明确的认证错误而先执行用户密码校验。

* `quota=-1` 只表示不限制并发，仍需检查是否存在更高优先级的 `quota=0` 前缀；最长前缀规则决定最终行为。

* 锁身份必须由 lock holder key 的 value、lease ID 和 create\_revision 共同确认；只保存 “已获取锁” 的内存布尔值不够。

* lock holder key 的 value 使用 256-bit 随机 owner token；该 token 不写日志，锁释放或被接管后不可复用。

* 如果 Txn 发现 user/IP lock 已过期或被其他 Caster 接管，必须释放本地状态、重新获取锁并重新读取 session，不能复用旧计数。

* lock lease 需要独立 supervisor keepalive；keepalive 失败时当前准入立即失败并 self-fence。

* `cluster.admission_requires_current_config=true` 时，任何新数据流准入都必须比较 etcd 当前 Caster Config mod\_revision；本地配置落后时 Txn 失败，返回 503 / 重试并立即重新同步配置。

* 已认证客户端还要比较 User key 的 `mod_revision` 与本地认证所用快照 revision，避免使用旧密码、旧角色或已禁用账号写 session。

* session 写入失败时不能返回成功。

* 已认证客户端数据流连接才创建用户 session。

* NTRIP1/2 源站连接使用 mountpoint owner 作为 effective user。

* 客户端、NTRIP1 源站和 NTRIP2 源站必须使用同一套 user admission lock；source claim 和 user session 在同一 etcd Txn 中提交。

* 源站计入 owner 的用户连接上限，但不创建 IP session，不检查 IP 黑名单、CIDR quota 或每 IP 并发上限。

* IP 黑名单和 IP quota 只作用于客户端数据流。

* relay 内部连接不消耗用户 session 配额。

* 当前连接数不写入配置，也不写固定 count key；它等于该用户有效 session key 的数量。

有效上限按以下顺序计算：



```
CasterConfig.connection_limits.per_username[username]
  ?? CasterConfig.connection_limits.default_max_sessions
```

客户端认证关闭时：



* `track_anonymous_streams=true`：只写 IP session index，不写 user index，用于集群 IP 配额和容量统计。

* `track_anonymous_streams=false`：不创建在线 session；IP 黑名单中的 `quota=0` 仍可本地拒绝，但正整数 quota 只能按单机尽力统计，无法执行精确的跨 Caster 匿名 IP 配额。

* 如果生产配置要求全局匿名 IP quota，配置校验必须要求 `track_anonymous_streams=true`。

* 匿名且不写 user/IP session 时仍然执行一次 linearizable Caster Config revision 检查；snapshot 过期则先重载，不能绕过黑名单 / 认证开关。

### 6.5 正常清理

连接结束时使用一个幂等的 `ConnectionCleanupGuard`，保证网络 close、取消任务和 supervisor 同时触发时只执行一次有效清理。

客户端 / NEAR 正常退出：



1. 从本地 hub 摘除订阅者，取消 writer 和 GGA reader。

2. 同一 etcd Txn 比较 session 的 `caster_id/runtime_epoch`，再删除 `online/users/{user_id}/sessions/{session_id}` 和 `online/ips/{ip}/client-sessions/{session_id}`。

3. 匿名且 `track_anonymous_streams=false` 时没有 session key，只做本地清理。

4. Txn 成功后才认为集群在线状态已更新；本地 socket 可以已经关闭。

源站正常退出：



1. source 任务先进入 DRAINING，停止继续接收和再次 fan-out。

2. 关闭源站 socket、本地订阅者和依赖该 source 的 relay。

3. etcd Txn 比较 `claim_revision/source_generation/connection_id/runtime_epoch`。

4. 条件匹配后原子删除 source claim、runtime source 和 owner user session；源站不创建 IP session。

5. 条件不匹配时不能删除新 source，只能记录 stale connection 并退出。

管理 Session 退出：



* 浏览器 logout 立即删除 `runtime/admin_sessions/{token_hash}` 和对应 CSRF 状态。

* Admin Session 不依赖 Caster lease；普通 NTRIP 退出也不会删除 Admin Session。

etcd 删除失败：



* 主动清理失败时不能返回 “已完全退出”，本地关闭仍继续执行，session/source key 保留到 Caster lease TTL 到期自动删除。

* 清理失败写 WARNING 和 audit/event，并递增 `etcd_cleanup_failed_total`。

* 由于 key 是保守保留，新连接在最长一个 lease TTL 内可能仍被计入旧 session；这是安全侧延迟，不做 force delete。

* Caster 崩溃、被 SIGKILL 或网络断开时，不依赖 finally 代码；统一由 lease 到期清理。

`near_current_mountpoint` 只在 NEAR 完成一次实际切换时事件式更新 session key；不是每收到一个 GGA 都写 etcd，普通在线期间也不做周期 rewrite。

### 6.6 Source Claim

源站占用使用独立、不可变的 claim key。claim 负责 “集群内唯一上传权”，`runtime/sources` 保存当前离线记录及临时 / 最终 STR，`state=ACTIVE` 后才可被公共 STR 和 relay 使用。两者都绑定 Caster lease。`runtime/source-generations/{mount}` 是持久的单调计数器，每次成功 source claim 递增；它不绑定 Caster lease，因此重连和 lease 过期后不会回退。

claim value：



```
{
  "schema_version": 1,
  "source_generation": 1002,
  "connection_id": "c_01j...",
  "mountpoint": "C63",
  "caster_id": "caster-west-02",
  "runtime_epoch": "boot_01j...",
  "relay_endpoint": "10.20.0.12:12102",
  "effective_user_id": "u_01j...",
  "created_at": "2026-11-19T10:00:00Z"
}
```

占用和上线流程：



```
读取本机 MountSnapshot
  -> 执行 NTRIP1 或 NTRIP2 认证
  -> 提取 User-Agent、Source-Agent、Ntrip-Version
  -> 检查 runtime/source-cooldowns
  -> 获取 owner user admission lock
  -> 保存 lock holder key/value/lease 和 create_revision
  -> 线性读取 owner 的全部 session，检查用户连接上限
  -> 获取本机 VALIDATING slot；达到 max_validating_sources 返回 503
  -> 生成 connection_id
  -> 读取 source-generations/{mount} 和 claims/{mount}
  -> Txn:
       IF runtime/source-claims/{mount} 不存在
       AND user lock holder identity 仍匹配
       AND config/caster mod_revision == local ConfigSnapshot revision
       AND config/users/{owner_user_id} mod_revision == owner snapshot revision
       AND config/mounts/{mount} mod_revision == mount snapshot revision
       AND source-generations/{mount} value == old_generation
       THEN PUT source-generations/{mount} = old_generation + 1
            PUT runtime/source-claims/{mount} with new generation
            PUT runtime/sources/{mount} with state=VALIDATING and provisional STR record
            PUT owner user session
  -> 如果 generation CAS 失败，重新读取并重试整个 claim Txn
  -> 释放 user admission lock
  -> 成功后返回 NTRIP 200
  -> 进入 VALIDATING
  -> 多协议 demux 累积有界样本
  -> observe: 收集 profile，最多 admission_profile_window_ms 或 max_frames_inspected
  -> observe: 窗口结束后准入并冻结 profile
  -> enforce: 全部启用规则通过后准入并冻结 profile
  -> Txn:
       IF runtime/source-claims/{mount}.mod_revision == claim_revision
       AND runtime/sources/{mount}.connection_id == self.connection_id
       THEN UPDATE runtime/sources/{mount} to state=ACTIVE with corrected STR and admission summary
  -> ACTIVE
```

source 不创建 IP session，因此 IP 黑名单和 IP quota 不作用于上传源。

任一步失败都不能留下 claim、source record 或 user session。`VALIDATING` source record 存在于 etcd 供 owner/admin 观察，但不会进入公共 STR、Relay route 或客户端订阅。

VALIDATING slot 在进入 ACTIVE 或拒绝 / 断开时释放。

generation key 不存在时使用 `version == 0` 的比较创建值 1；已存在时比较当前 value 做 `old + 1` CAS。claim 和 user session 必须与该 generation 更新在同一 Txn 中完成。

如果 User/Mountpoint revision 在认证后发生变化，Txn 失败后只允许重新加载并重新认证一次；仍不通过则拒绝，不能沿用旧密码验证结果。

Fencing 规则：



* 所有 claim 更新、source 发布和清理必须同时比较 claim 的 `mod_revision`（本地保存为 `claim_revision`）、值中的 `source_generation`、`connection_id` 和 `runtime_epoch`。

* 正常关闭使用 Txn 比较自己的 `claim_revision/source_generation/connection_id/runtime_epoch`，成功后一次性删除 claim、runtime source 和 owner user session。

* `connection_id` 不匹配时，旧任务只能关闭自己的 socket，不能删除或覆盖新 source。

* Caster 失去 lease 或续约不确定时，必须在 lease 到期前关闭本机所有 source；不能等待 key 自动过期。

* 新源只有在 claim key 不存在时才能成功；旧 `runtime/sources` 残留不能让新 claim 失败，但新 claim 成功前应由旧 owner 或 lease 清理。

* 每个 source dispatch 前通过本地 `FenceGuard` 检查 lease deadline 和 monotonic generation；如果进程从暂停恢复时已超过 deadline，必须先丢弃所有待发送帧并 self-fence，不能继续 fan-out。

* 源站本地 fan-out、内部 relay egress、internal monitor tap 都必须经过同一个 FenceGuard；不能存在绕过该检查的测试或快捷发送路径。

保证边界：



* 对正常合作节点，在本地调度器能够及时执行、所有发送路径都不绕过 FenceGuard 且本地单调时钟可靠的前提下，`lease + self-fence + claim revision + per-dispatch FenceGuard` 可以保证旧 owner 在 lease 失效后不再向客户端提交新的差分帧。

* 对恶意进程、内核 / 调度器无限期暂停、无法执行的节点，纯应用层 fencing 不能提供对 Byzantine 节点的物理隔离。

* 已经进入内核 socket send buffer 的字节无法撤回；FenceGuard 只能保证不再继续投递新帧。

* 如果需要 “任何情况下都绝不允许双源” 的强保证，必须引入外部网络 fencing、专用 source ingress gateway 或能撤销节点数据面权限的外部控制面；否则应把保证范围明确写成 “合作节点故障模型”。

跨 Caster 管理操作：



* 管理 API 删除一个挂载点 source 时，在 `runtime/source-commands/{mount}/{command_id}` 写入带 `target_connection_id` 和 `source_generation` 的 DISCONNECT 命令。

* owner Caster watch 自己 mountpoint 的命令；两个字段都匹配当前 source 时主动断开并走正常 fencing 清理。

* 管理 API 返回 202；owner 不可达或命令超时则由 lease 过期完成最终清理。

* 不允许管理 API 直接删除 claim 后让新源立即接管，因为旧 socket 可能还在发送数据。

* 删除客户端 session 使用 `online/commands/{caster_id}/{command_id}`，命令携带 `session_id` 和 `runtime_epoch`；每个 Caster 只 watch 自己的命令前缀，不维护 node 表。

源站 etcd 状态变更表：



| 事件                      | source claim | `runtime/sources`         | owner user session | 说明                                                   |
| ----------------------- | ------------ | ------------------------- | ------------------ | ---------------------------------------------------- |
| 认证 / 配额通过，开始 VALIDATING | 创建           | 创建临时 STR，state=VALIDATING | 创建                 | claim、generation、source record 和 user session 同一 Txn |
| 准入通过，进入 ACTIVE          | 保持           | 条件更新为 ACTIVE              | 保持                 | 修正轻量 STR、准入摘要和 relay route；完整 profile 留本地 |
| `observe` 准入有告警         | 保持           | 条件更新为 ACTIVE，带 WARN/FAIL  | 保持                 | profile 不周期重写                                        |
| `enforce` 准入失败          | 条件删除         | 条件删除                      | 删除                 | 写 source cooldown，关闭 socket                          |
| 源站正常退出                  | 条件删除         | 条件删除                      | 删除                 | 必须匹配 claim\_revision/generation/connection\_id       |
| Caster 崩溃或 etcd 清理失败    | lease 到期删除   | lease 到期删除                | lease 到期删除         | 最长保留一个 lease TTL                                     |
| 源站重连                    | 新 claim      | 新记录替换                     | 新 session          | 必须获得更大的 source\_generation                           |
| Stream Health 变化        | 不变           | 不变                        | 不变                 | 只更新 owner 本地 metrics/alert                           |

只有进入 ACTIVE 的源站才出现在公共 STR 和 relay 路由中；VALIDATING、REJECTED 不会作为可用源公开。

### 6.7 On-Demand Source

外部代理或按需源也必须先占用 source key：



```
CLAIMING
  -> 连接外部上游
  -> ACTIVE
  -> 无客户端且 idle 超时
  -> DRAINING
  -> ABSENT
```

这样多个 Caster 不会同时为同一个 on-demand mountpoint 建立上游连接。

### 6.8 Caster 和网络故障



* Caster 崩溃：Caster lease 到期，session/source key 自动删除。

* Caster 与 etcd 分区：自动 self-fencing，旧源站在其他 Caster 抢到源之前已经停止。

* 单个 etcd member 故障：quorum 保留时正常服务。

* etcd quorum 丢失：停止新认证、新源站和新 relay；safe 模式在 lease 失效时关闭相关数据流。

* relay 上游断开：关闭本地下游，不保留伪在线连接。

故障状态矩阵：



| 状态                       | 已有客户端流                             | 新准入    | Source/Relay            | 恢复条件               |
| ------------------------ | ---------------------------------- | ------ | ----------------------- | ------------------ |
| etcd 短暂不可达，lease 安全余量未耗尽 | 可继续本地转发                            | 暂停     | 保持                      | keepalive 恢复       |
| keepalive 失败但 lease 尚未到期 | 可短暂继续                              | 暂停     | 在 deadline 前 self-fence | 重新获得 lease         |
| lease deadline 已过        | 关闭所有本机 client/source/relay session | 拒绝     | 全部停止发布                  | 重新申请 lease 和 claim |
| etcd quorum 丢失           | 本地已建立流按 lease 余量继续                 | 拒绝     | 停止新 relay，不再承认新 claim   | quorum 恢复并同步快照     |
| watch compacted / 断裂     | 立即停止新准入                            | 拒绝     | 使用最新快照前不创建新 route       | 一致快照重载完成           |
| etcd 恢复                  | 不自动恢复旧 runtime key                 | 分批恢复准入 | 源站重新 claim              | 配置、lease、快照均确认     |

时间与暂停规则：



* lease 截止时间使用单调时钟计算；墙上时钟只用于展示和审计。

* 每次成功 keepalive 后更新 `lease_deadline_mono = now_mono + ttl - safety_margin`。

* 系统时间跳变不影响本地 deadline；NTP 大幅调整只记录告警。

* Tokio runtime 长时间暂停后恢复时，数据面先执行 FenceGuard，再处理任何 source/relay packet。

* Caster 启动或恢复准入时使用 jitter 和 accept rate limit，防止大量源站 / 客户端同时重连形成风暴。

* 配置快照超过 `config_max_staleness` 或 etcd revision 无法确认时，禁止新的集群级准入，已有本地流按 lease deadline 处理。

### 6.9 源站退避

源站因认证失败以外的数据校验问题被断开时，写入集群共享 cooldown：



```
/rcaster/{cluster_id}/runtime/source-cooldowns/{source_fingerprint}
```

`source_fingerprint` 建议包含：



* mountpoint

* owner user ID

* source IP

* auth mode

User-Agent/Source-Agent 只用于诊断，不放进 fingerprint，避免客户端更换 UA 绕过退避。

cooldown value：



```
{
  "schema_version": 1,
  "not_before": "2026-11-19T10:10:00Z",
  "attempt": 2,
  "reason": "NO_CORRECTION_DATA",
  "last_source_agent": "Station/1.2.3",
  "violations": 5
}
```

退避策略：



* 第一次：`initial_ms`。

* 后续按 `multiplier` 指数增长，直到 `max_ms`。

* cooldown 过期前再次上传，返回 429/503 和 `Retry-After`；NTRIP1 返回关闭连接。

* 新的一次源数据通过全部校验后删除该 cooldown。

* cooldown 使用 etcd lease 或显式过期清理，不使用 node 表。

### 6.10 连接准入的读写一致性

所有连接都先使用本地只读快照做快速判断，但真正创建 session、source claim 或更新在线状态前，必须确认写入依据仍然有效。



| 路径               | 读取值                              | 写入 / 提交前校验                                                 |
| ---------------- | -------------------------------- | ---------------------------------------------------------- |
| IP 封禁            | 本地 Caster Config 的 CIDR 规则       | snapshot 是否新鲜；`quota=0` 立即拒绝                               |
| 客户端密码            | 本地 User 快照                       | 用户 key `mod_revision == snapshot_user_revision`            |
| NTRIP1 Source 密码 | 本地 Mountpoint + owner User 快照    | mount key 和 owner user key revision 未变化                    |
| NTRIP2 Source 密码 | 本地 owner User + Mountpoint 快照    | mount key 和 owner user key revision 未变化                    |
| 用户连接数            | etcd 当前用户 session 前缀             | user/IP lock holder identity + config revision             |
| Source Claim     | source claim、generation、cooldown | claim absent + generation CAS + config/user/mount revision |
| 远端 Route         | 本地 `runtime/sources` watch cache | relay 握手再次校验 source generation/connection\_id              |
| NEAR 候选          | 本地空间索引快照                         | 连接真实源前再次校验 source active/generation                        |

具体规则：



* `User`、`Mountpoint` 和 `Caster Config` 的 local snapshot 必须保存 etcd `mod_revision`。

* 客户端 /source admission Txn 同时比较 User/Mountpoint 的预期 revision；变化时 Txn 失败，重新拉取单条记录并重试认证。

* 认证失败要区分 “凭据错误” 和 “配置已变化”；后者不能返回错误密码，应返回 503 或重新加载后重试一次。

* 匿名且不创建 user/IP session 的客户端没有写入 Txn，因此必须在准入前执行一次 linearizable config revision 检查，或拒绝到本地 snapshot 回到新鲜窗口内。

* 普通客户端写入两个 index：user session 和 IP client-session；两者在同一 Txn。

* Source 写入 claim、owner user session 和 generation 更新；三者在同一 Txn。

* 客户端和源站退出时使用同一个 admission owner identity 和 connection ID 清理，不能删除后来复用同一名称的状态。

* 本地 watch cache 只能作为路由候选；真正使用远端源前必须由 owner 再验证 generation。

不允许的实现方式：



* 只检查本地内存 map 后直接返回 NTRIP 200，再由后台异步补写 session。

* 认证通过后不校验 User snapshot revision 就写 session。

* 用 `runtime/sources` 缓存直接认定 Owner 当前仍可用，绕过 relay generation 校验。

* 匿名无 session 连接完全跳过 config revision 检查。

### 6.11 Etcd Watch 模式

原则：



* Caster 首次启动时先对需要的前缀做一次一致性 `Range` 快照，并记录响应 header revision。

* 随后从 `revision + 1` 建立该前缀的一个持续 Watch，不按 user、mountpoint 或 session 分别创建 watcher。

* 正常运行时没有周期全量推送、定时轮询和整表广播。

* Watch 事件按 revision 顺序应用；旧 revision、旧 generation 和旧 connection\_id 不能覆盖新状态。

* Watch 返回 `compacted` 时停止使用该前缀缓存，重新 Range 快照后从新 revision 继续 Watch。

数据分类：



| Key 前缀                    | 初始同步                | 运行模式        | 主要消费者       | 用途                          |
| ------------------------- | ------------------- | ----------- | ----------- | --------------------------- |
| `config/users/`           | Range 快照            | 持续 Watch    | 所有 Caster   | 用户新增、禁用、密码、权限、认证缓存          |
| `config/mounts/`          | Range 快照            | 持续 Watch    | 所有 Caster   | 挂载点新增、删除、owner、STR 默认字段     |
| `config/caster`           | 单 Key Get           | 单 Key Watch | 所有 Caster   | 全局 generation、黑名单策略、连接上限、端口 |
| `config/blacklist/`       | Range 快照            | 持续 Watch    | 所有 Caster   | CIDR 黑名单和每 IP quota 增删改     |
| `config/api_tokens/`      | Range 快照            | 持续 Watch    | 所有 Caster   | Token 创建、撤销、过期状态            |
| `runtime/sources/`        | Range 快照            | 持续 Watch    | 所有 Caster   | 源站上下线、临时 / 最终 STR、格式和状态修正   |
| `online/users/`           | Range 快照            | 持续 Watch    | 管理视图 / 统计   | 用户上线、下线、NEAR 当前源变化          |
| `runtime/config-acks/`    | 当前 generation Range | 前缀 Watch    | 管理 API      | 配置应用观察，不参与数据面               |
| `runtime/version-status/` | 当前版本 Range          | 前缀 Watch    | 管理 API / 运维 | 节点版本、build、schema、健康状态      |
| `runtime/operations/`     | 按查询读取               | 可选 Watch    | 管理 API      | 长任务进度                       |

不需要每个 Caster 全量订阅的数据：



* `runtime/source-claims/`：唯一性由 atomic Txn 保证，不靠 Watch；owner 只关注自己的 claim。

* `runtime/source-cooldowns/`：源站认证时按 fingerprint 做一次线性读取，不做全局 Watch。

* `locks/users/`、`locks/ips/`：只使用 etcd Lock/Lease 和 Txn 验证，不维护全量本地镜像。

* `online/ips/`：IP quota 的权威值在准入时通过 IP lock + 当前 session 范围读取，不用 Watch 结果冒险。

重要边界：



* Watch 适合维护本地视图，不能替代准入锁和线性读取。

* 用户 / 挂载点 Watch 更新本地 Account DB / 内存快照，但不能使用本地过期数据直接写 session。

* 用户在线状态 Watch 用于管理页面、统计和 NEAR 展示；精确连接上限仍以 lock + 当前 session 范围查询为准。

* `runtime/sources/` Watch 更新本地 canonical records 和 STR；远端真正订阅仍要校验 source generation。

* Watch lag 超过阈值时，安全配置相关准入暂停；只影响 STR / 管理视图的 Watch 可以继续追平。

### 6.12 Etcd 数据所有权和节点重连

必须区分 “集群配置” 和 “节点运行时记录”：



| 数据                                   | write owner                  | 其他 Caster 权限                 | 失效方式                 |
| ------------------------------------ | ---------------------------- | ---------------------------- | -------------------- |
| `config/users/{user_id}`             | 集群配置，不属于 Caster              | 只读缓存；管理员通过 API/CAS 修改        | 显式删除                 |
| `config/mounts/{mount}`              | 集群配置，不属于 Caster              | 只读缓存；管理员通过 API/CAS 修改        | 显式删除                 |
| `config/blacklist/{rule_id}`         | 集群配置，不属于 Caster              | 只读缓存；管理员通过 API/CAS 修改        | 显式删除                 |
| `config/caster`                      | 集群配置，不属于 Caster              | 只读 Watch                     | generation 替换        |
| `online/users/.../{session_id}`      | 创建该 session 的 Caster         | 只读 Watch，禁止直接修改              | owner 删除或 lease 到期   |
| `online/ips/.../{session_id}`        | 创建该 session 的 Caster         | 只读 / 范围读取，禁止直接修改             | owner 删除或 lease 到期   |
| `runtime/source-claims/{mount}`      | 创建 claim 的 Caster            | 只读；接管必须等 claim 不存在           | owner 条件删除或 lease 到期 |
| `runtime/sources/{mount}`            | 创建该 source 的 Caster          | 只读 Watch；通过 owner command 断开 | owner 条件删除或 lease 到期 |
| `runtime/source-generations/{mount}` | 无单节点 owner，使用 generation Txn | 只读                           | 永久保留，恢复时校验不回退        |
| `runtime/admin_sessions/{hash}`      | 创建它的管理入口                     | 可验证，不修改                      | `expires_at`/logout  |
| `runtime/web_sessions/{hash}`        | 创建它的 Web 登录入口                | 可验证，不修改                      | `expires_at`/logout  |

记录字段：



* User config：`user_id`、`username`、`password_hash`、`auth_version`、`role`、`enabled`、`ntrip_enabled`、`created_at`、`updated_at`、`mod_revision`。

* Mountpoint config：`mountpoint`、`owner_user_id`、`enabled`、`ntrip1_upload_password_hash`、`region`、`on_demand`、`advertise`、经纬度、`str_fields`、`source_expectation`、`mod_revision`。

* Client session：`session_id`、`user_id`、`username`、`auth_version`、`mountpoint`、`client_ip`、`remote_port`、`caster_id`、`runtime_epoch`、`session_type`、`ntrip_version`、`auth_scheme`、`client_software`、`started_at`。

* IP session：同一 `session_id`、canonical client IP、`caster_id`、`runtime_epoch`、lease。

* Source claim：mountpoint、`caster_id`、`runtime_epoch`、`connection_id`、`source_generation`、relay endpoint、effective user、created\_at。

* Runtime source/STR route：source identity、state、STR 必需字段、准入摘要、relay endpoint、started_at。完整设备、Source IP、消息计数和 Data Profile 只在 owner 本地。

控制约束：



* Caster 的 etcd 凭据只允许读取 config 前缀、操作 runtime/online 前缀，不能写 config。

* 管理 API 使用独立的 etcd 管理身份写 config；节点不能借 Caster 身份修改 User/Mountpoint。

* Runtime record 的 owner 由 value 中的 `caster_id + runtime_epoch + connection_id` 校验；目录路径本身不能作为唯一授权依据。

* 修改 / 删除 owner 不是自己的 runtime key 必须失败并产生安全审计。

节点与 etcd 分区后重连：

状态机：



```
READY
  -> ETCD_DEGRADED       // keepalive 失败
  -> FENCED              // lease deadline 到达
  -> REJOINING           // etcd 恢复，重新同步
  -> READY
```

分区时间判定：



* **deadline 前恢复**：已有连接可以继续，但暂停新准入；重新同步配置 revision 后回到 READY，不强制换 epoch。

* **deadline 后恢复**：必须进入 FENCED，关闭所有旧 client/source/relay，旧本地连接永远不能恢复。

deadline 后重连流程：



1. 先执行 FenceGuard，不再 dispatch 任何差分数据，关闭所有旧 NTRIP sockets。

2. 放弃旧 `runtime_epoch`；旧 epoch 的 session/source registry 整体作废，不能与新 registry 合并。

3. 获取新 lease 和新的 `runtime_epoch`。

4. 重新 Range 读取 users、mounts、config、runtime sources 和相关 runtime 状态。

5. 用该 revision 的整体快照替换本地 Account DB、STR/NEAR 索引和 runtime cache，不把旧分区视图合并进新视图。

6. 从 revision + 1 恢复 Watch；遇到 `compacted` 再次 Range 后重启 Watch。

7. 验证 config generation、source generation 下限、用户 / 挂载点 revision 后恢复新准入。

8. 旧 epoch 的本地连接不能重新发布为在线状态；客户端和源站必须重新认证。

9. 只要仍有旧 epoch task 或 socket 无法被取消，节点保持 FENCED/not-ready，不能接受新连接。

幽灵连接边界：



* Caster 崩溃或 lease 丢失后，etcd key 最终由 lease 清理，不会永久保留。

* Client 半开连接由 socket write error、write timeout、TCP keepalive 或管理强制断开检测；若两侧都无数据且 TCP keepalive 尚未发现，不能声称立刻感知。

* Source 用 source read timeout 和 lease fencing 检测；来源断开后必须主动删除 claim/source。

* 重新连接到其他 Caster 的相同用户名是新的 `session_id`，不能直接复用另一个 Epoch 的状态。

* 用户在 A 与 etcd 失联期间仍能与 A 收发数据，只允许发生在 lease deadline 之前；deadline 到达后 A 必须停止该数据面，客户端会表现为断流并重新连接。

源站和 STR 使用相同原则，并且增加 source generation 约束：



| 时间点                  | A 的 source claim/runtime STR   | 数据面                              | 重新接入后的行为                       |
| -------------------- | ------------------------------ | -------------------------------- | ------------------------------ |
| 失联但未过 lease deadline | claim 和 runtime source 仍有效     | 可短暂继续，但停止新准入                     | 恢复同一 lease/epoch 后重同步 Watch    |
| lease deadline 已过    | etcd 删除 claim 和 runtime source | A 必须关闭源站 socket，停止 fan-out/relay | 生成新 runtime\_epoch，全量同步，旧源不可恢复 |
| B 已获得新 claim         | B 的 generation 更大              | A 的旧 generation 全部失效             | A 重连后只能作为普通观察者，不能修改 B 的 STR    |
| A 重新接收源站             | 必须重新 claim                     | 重新写 VALIDATING 临时 STR            | 重新解析并条件更新为 ACTIVE              |

具体规则：



* `runtime/source-claims`、`runtime/sources` 和临时 / 最终 STR 都绑定 A 的 lease。

* A 触发 FENCED 时，先在本地把该 epoch 的全部 source/STR 标记为不可发布，停止本地客户端和 relay 输出。

* A 不能在恢复 etcd 后把旧内存 source 重新 Put 成 ACTIVE；必须重新占用 claim 并取得更大的 `source_generation`。

* 如果 B 已经成为 C63 的新 owner，A 的旧 source record 不能覆盖 B 的 record，旧 Relay 连接必须关闭。

* A 重新全量同步 `runtime/sources` 后，本地 STR 只使用 etcd 当前 ACTIVE records 和本地 NEAR，不采纳旧 epoch 的本地挂载点记录。

* 源站上传端必须重新连接并重新认证；NTRIP 没有可用于跨 lease 恢复的 session-resume 协议。

* 只要 A 的旧 source task 或 STR 发布任务仍未取消，A 保持 FENCED/not-ready，不能接受新 source。



***

## 7. NTRIP 协议层

本节以 NTRIP 1.0/2.0 的请求和响应约定为基线，并采用 Millipede 已验证的兼容行为：NTRIP1 `ICY/SOURCETABLE`、NTRIP1 固定错误文本、NTRIP2 HTTP 状态、数据流不复用连接、sourcetable 可 keep-alive。

### 7.1 入站请求

NTRIP listener 使用底层 TCP/TLS 和自定义状态机，不使用 Axum/Hyper 读取数据流。请求解析限制：



* 请求行最大 4096 字节。

* 全部 Header 最大 16384 字节，Header 最多 64 个。

* 单行 Header 最大 4096 字节，不支持 obs-fold。

* Header 读取超时 10 秒；超限返回 431 或直接关闭，不能无限等待。

* 行结束同时接受 `CRLF` 和 `LF`，响应固定使用 `CRLF`。

* Header 名不区分大小写，值去除首尾空白；禁止 NUL 和其他控制字符。

* 同时出现 `Content-Length` 和 `Transfer-Encoding: chunked` 返回 400。

* 重复 `Content-Length`、重复 `Transfer-Encoding` 或重复 Authorization 一律返回 400。

* `Transfer-Encoding` 只接受单一 `chunked`；任何其他或组合编码返回 501/400，不能尝试猜测 body 边界。

* 重复 Host 返回 400；Host 不参与认证。

* `Expect` 只接受 `100-continue`，认证和头部校验通过后才发送 100；其他 Expect 返回 417。

* Trailer 不允许改变 Content-Length、Transfer-Encoding、Authorization；trailer 总字节超限或非法 chunk 终止立即关闭连接。

* 每个连接第一行必须是 `GET`、`SOURCE` 或 `POST`；其他方法返回 501。

客户端请求示例：



```
GET /C63 HTTP/1.0
User-Agent: NTRIP RTKLIB/2.4.3
Authorization: Basic ...
Ntrip-GGA: $GPGGA,...
Connection: close
```



```
GET /C63 HTTP/1.1
Host: caster.example.net:2101
Ntrip-Version: Ntrip/2.0
User-Agent: NTRIP RTKLIB/2.4.3
Authorization: Basic ...
Ntrip-GGA: $GPGGA,...
Connection: close
```

源站请求示例：



```
NTRIP1: SOURCE <mount_upload_password> /C63
NTRIP2: POST /C63 HTTP/1.1 + Basic owner_username:owner_password
```

客户端和源站请求都必须记录请求方法、NTRIP 版本、User-Agent、Source-Agent 和远端 IP；Authorization 只记录 scheme 和用户名，不记录密码。

`SOURCE` 请求行中的密码在审计和 debug 日志中固定显示为 `***`。

#### 版本判定



1. 请求行是 `SOURCE`：NTRIP1 源站。

2. 请求行是 `POST` 且存在 Basic Authorization：NTRIP2 源站；POST 无 Basic 返回 401。

3. `GET` 带大小写不敏感的 `Ntrip-Version: Ntrip/2.0`：NTRIP2 客户端。

   `Ntrip-Version` 存在但不是 `Ntrip/2.0` 时，若 UA 表明是 NTRIP 则按 NTRIP1，否则返回 400。

4. 在专用 NTRIP listener 上，`GET /`、`GET /SOURCETABLE` 或 `GET /MOUNT` 没有 NTRIP2 Header 时兼容为 NTRIP1，不要求 User-Agent。

5. `GET /MOUNT` 只有两个请求行 token、没有 HTTP 版本时，按 HTTP/0.9 风格 NTRIP1 兼容处理。

6. 其他组合返回 400；管理 HTTP 只在管理 listener 处理，不参与 NTRIP 猜测。

NTRIP 设备经常省略 HTTP/1.1 必需的 `Host`。NTRIP2 缺少 Host 时记录兼容告警但继续处理；如果 Host 存在，只用于诊断，不用来生成认证身份。

`Ntrip-GGA` 只处理位置，不参与版本判定。

#### 请求目标



* `GET /`：返回 sourcetable。

* `GET /SOURCETABLE`：兼容别名，返回同一 sourcetable。

* `GET /MOUNT`：订阅实时数据；mountpoint 为空、包含 `?`、`%`、`/` 或控制字符时返回 400。

* `SOURCE <password> [/]MOUNT`：NTRIP1 源站上传，前导 `/` 可有可无。

* `POST /MOUNT`：NTRIP2 源站上传。

* Query string 不参与 mountpoint 认证；除调试字段外应忽略并记录。

* `SOURCE` 请求行必须恰好包含 `SOURCE`、password、mountpoint 三个以空格 / 制表符分隔的 token；额外或缺失 token 返回 400。

#### Basic 认证



* Scheme 只接受大小写不敏感的 `Basic`。

* Base64 解码后最多 512 字节，必须包含第一个 `:`；`:` 前是用户名，之后是密码。

* 同一请求出现多个 Authorization Header 返回 400。

* 用户名和密码按原始字节解码为 UTF-8，非法 UTF-8 返回 401。

* 认证失败响应不得区分 “用户不存在” 和 “密码错误”。

### 7.2 入站状态



```
READ_REQUEST_LINE
READ_HEADERS
READ_FIXED_BODY
READ_CHUNKED_BODY
SOURCE_STREAMING
CLIENT_STREAMING_DOWN
CLIENT_GGA_UPLINK
CLIENT_INPUT_DISCARD
DRAINING
CLOSING
CLOSED
```

请求解析与数据流状态分离。chunked 解码、`ProtocolDemux` framing 和 GGA 上行各自是独立状态机。客户端数据流响应发送完成后仍可读取有限的 NMEA 上行。

### 7.3 GET /



* `GET /` 和 `GET /SOURCETABLE` 返回集群公共 STR。

* 不创建用户数据 session。

* 不要求客户端认证。

* 返回内容来自本地缓存快照。

* 根据 Caster 配置选择 global、regional 或 auto。

* NTRIP1 响应状态行为 `SOURCETABLE 200 OK`，`Content-Type: text/plain`。

* NTRIP2 响应状态行为 `HTTP/1.1 200 OK`，默认 `Content-Type: gnss/sourcetable`；可配置回退为 `text/plain`。

* 正文以 `CAS`、`NET`、若干 `STR` 开始，必须以 `ENDSOURCETABLE\r\n` 结束。

* 正文使用 `Content-Length`，按 UTF-8 字节数计算，不使用 chunked。

* 非数据流响应可以协商 keep-alive；连接关闭前允许发送下一个完整请求。

### 7.4 GET /MOUNT



1. 解析 mountpoint。

2. 规范化客户端 IP，执行客户端 IP 黑名单判断；`quota=0` 在认证前直接拒绝。

3. 如果请求名等于 Caster Config 中的 `near.mountpoint`，启动 NEAR 流程。

4. 如果本机有 active source，直接订阅。

5. 如果集群其他 Caster 有 active source，自动建立 / 复用 relay。

6. 如果挂载点支持 on-demand，先占用 source claim，再建立上游。

7. 如果客户端认证开启，认证用户；认证通过后执行 user admission 和正数 IP quota 并发事务。

8. 如果挂载点不存在返回 404；挂载点存在但源站处于 VALIDATING、正在切换 owner 或暂时无可用源时返回 503；禁用返回 403。

客户端 session 写入前，本地源必须仍是 ACTIVE 且 generation 未变化；远端源必须完成 Relay 握手校验。不能用过期 STR 缓存直接返回成功。

IP 黑名单和 IP quota 适用于普通 `GET /MOUNT` 和 NEAR `GET /{near.mountpoint}`；不适用于 `GET /`、`GET /SOURCETABLE`、源站上传、内部 Relay 和管理 API。

NTRIP1 兼容规则：



* 请求不存在或不可订阅的 mountpoint 时，Millipede 等实现会返回 sourcetable 而不是单独错误页；新系统沿用该行为，返回 sourcetable 后关闭连接。

* 空 mountpoint、`/` 和 `/SOURCETABLE` 始终走 sourcetable。

NTRIP2 规则：



* 不存在或不可用返回 404，不返回 sourcetable。

* 客户端认证关闭时，GET 中的 Basic 只记录用户名，不创建用户 session，也不校验密码。

### 7.5 SOURCE / POST



* NTRIP1：


  * 解析 `SOURCE password /mount`。

  * 校验 mountpoint 的上传密码。

  * 以 owner 用户计费。

* NTRIP2：


  * 使用 Basic username/password。

  * 用户名必须是 mountpoint owner。

  * 密码校验 owner 用户密码。

  * 不校验 NTRIP1 上传密码。

  * 不能强制要求 `Content-Length`；差分数据流从请求头之后持续接收。

源站正文接收：



* `SOURCE` 和没有 body framing 的 `POST` 从 Header 结束后持续读取直到 EOF。

* `POST` 带 `Content-Length` 时按该长度读取，但长度大小受 `ntrip.content_length_max_bytes` 限制。

* `POST` 带 `Transfer-Encoding: chunked` 时先解 chunk，再送入 `ProtocolDemux`；支持 chunk 扩展，忽略 trailer，trailer 总长度受限。

* 不能把大文件式 `Content-Length` 缓冲到内存；读取流式处理，超过配置上限返回 413。

* 源站成功响应必须在 source claim 成功后发送；响应写失败时立即回滚 claim/session。

源站认证成功后执行 source claim；source claim 成功后才返回 NTRIP 成功响应。

如果请求 mountpoint 等于当前 `near.mountpoint`，无论 NTRIP1 还是 NTRIP2 都返回 404，禁止源站上传。

### 7.6 响应和错误

响应矩阵：



| 场景                 | NTRIP1                                     | NTRIP2                                        |
| ------------------ | ------------------------------------------ | --------------------------------------------- |
| 客户端订阅成功            | `ICY 200 OK\r\n\r\n`                       | HTTP 200 + 标准 Header                          |
| 源站上传成功             | `ICY 200 OK\r\n\r\n`                       | HTTP 200 + 标准 Header                          |
| sourcetable        | `SOURCETABLE 200 OK`                       | `HTTP/1.1 200 OK`                             |
| 源站密码错误             | `ERROR - Bad Password\r\n`                 | 401 + `WWW-Authenticate: Basic realm="NTRIP"` |
| 源站名已占用 / 无效        | `ERROR - Mount Point Taken or Invalid\r\n` | 409 或 404                                     |
| 客户端 mountpoint 不存在 | 返回 sourcetable 后关闭                         | 404                                           |
| 服务暂不可用             | 关闭连接                                       | 503 + `Retry-After`                           |

NTRIP2 数据流成功响应：



```
HTTP/1.1 200 OK
Server: NTRIP RCaster/<version>
Date: <IMF-fixdate>
Ntrip-Version: Ntrip/2.0
Content-Type: gnss/data
Cache-Control: no-store, no-cache, max-age=0
Pragma: no-cache
Connection: close
```

数据流响应不设置 `Content-Length`，不使用 chunked 编码，并从 Header 结束后直接输出原始差分字节。流式连接只处理一个请求，结束后关闭，不支持在该 TCP 连接上复用 HTTP 请求。

`Connection: close` 只表示不会再处理第二个 HTTP 请求；NEAR 客户端仍可在同一 TCP 连接的上行方向继续发送 GGA，Caster 不提前关闭读半边。

keep-alive 只允许用于 sourcetable、管理页面、静态文件和普通非流响应；只有请求明确发送 `Connection: keep-alive` 时才协商。NTRIP1 始终关闭。

错误响应规则：



* NTRIP2 状态使用 400 请求非法、401 未认证、403 被禁用、404 不存在、405 方法不允许、409 源站占用、413 内容过大、415 不支持类型、417 Expect 不支持、429 限流、431 Header 过大、500 内部错误、501 未实现、503 暂不可用。

* NTRIP2 401 必须带 `WWW-Authenticate: Basic realm="NTRIP"`。

* 认证失败只返回笼统原因，不能泄露用户是否存在。

* 413、415、429、431、503 都使用 `Content-Length: 0` 或结构化错误正文。

* NTRIP1 源站错误使用固定文本，不发 `WWW-Authenticate`。

* NTRIP1 客户端认证失败使用 `HTTP/1.1 401 Unauthorized` 和 `WWW-Authenticate: Basic realm="NTRIP"`，用于兼容 NTRIP1 Rover。

* 错误响应发送后关闭连接，除非该请求明确属于可复用的非流 HTTP。

### 7.7 数据流和 Content-Type



* 客户端差分数据：`Content-Type: gnss/data`。

* sourcetable：NTRIP1 使用 `text/plain`；NTRIP2 默认 `gnss/sourcetable`，可通过配置回退为 `text/plain`。

* 源站上传接受 `gnss/data`、`application/octet-stream`、空 Content-Type 和已列入兼容配置的厂商类型。

* 流响应不设置 `Content-Length`。

* 非流响应必须设置准确的 `Content-Length`。

* 下行数据只转发源字节，Caster 不自行插入 NMEA、Keepalive 空行或分隔符。

### 7.8 NEAR GGA 上行

无论 NTRIP1 `ICY 200 OK` 还是 NTRIP2 `HTTP/1.1 200 OK`，NEAR 客户端连接建立后都可以在同一连接上继续发送 NMEA 行：



```
GET /AUTO_NEAR HTTP/1.1
...

$GNGGA,...*hh\r\n
$GNGGA,...*hh\r\n
```

实现规则：



* NEAR 连接同时运行下行 writer 和上行 GGA reader，两个方向互不阻塞。

* `Ntrip-GGA` Header 作为首个位置样本；后续每行 NMEA 独立解析。

* Header 值可包含裸 `$G...GGA`，也兼容带 `Ntrip-GGA:` 前缀的旧客户端；解析前只去除该兼容前缀和空白。

* 单行最大 256 字节，超过后丢弃到下一个换行并计数，不能增长缓冲。

* 只接受 `$GPGGA`、`$GNGGA` 和 `$GLGGA`，其他 NMEA 语句直接丢弃。

* 有 checksum 时严格校验；无 checksum 的 GGA 标记 `checksum=absent`，默认仍接受。

* fix quality 为 0、数值越界、HDOP 超限或经纬度非法的样本不参与选站。

* 超过 `gga_max_input_per_second` 的输入直接丢弃并计数；与上一样本推算速度超过 `gga_max_jump_speed_mps` 时标记 `GGA_JUMP_SUSPECT`，不触发切换。

* 每条连接只保存一个 “最新 GGA” 槽位；新 GGA 覆盖旧 GGA，不建立逐行积压队列。

* 同一连接最多按 `near.min_recompute_interval_ms` 执行一次空间查询，不能按任意高频 GGA 触发 KD-tree 查询。

* 节点使用查询并发限制和 `max_spatial_queries_per_second`；达到上限时延迟到下一个计算 tick，不共享或复用其他客户端的选站结果。

* 非 NEAR 连接在数据流建立后收到的非结构数据直接丢弃，不进入差分 fan-out，也不计入用户下行数据。



***

## 8. 数据面

### 8.1 数据表示

源站流首先被视为连续字节流，不能假设整条连接只有一种格式。RTCM3 流中可能夹带 NMEA，CMR/BINEX 设备也可能发送混合诊断语句。



```
pub struct Packet {
    pub bytes: bytes::Bytes,
    pub metadata: PacketMetadata,
}

pub enum PacketMetadata {
    Rtcm3 { message_type: Option<u16>, crc_ok: bool },
    Rtcm2 { message_number: Option<u16>, parity_ok: bool },
    Binex { record_id: Option<u16>, checksum_ok: Option<bool> },
    Cmr { message_type: Option<u16> },
    Nmea { sentence_kind: NmeaKind, checksum_ok: Option<bool> },
    Unknown,
}

pub enum NmeaKind { Gga, Rmc, Gsa, Gsv, Other }
```

`Packet` 克隆只复制 `Bytes` 引用，不复制正文。每个源站只运行一个增量识别器，在线信息、STR 和管理页面共享同一份统计结果。

跨任务使用 `type SharedPacket = Arc<Packet>`。`Bytes` 自带引用计数并支持低成本 `slice()`，外层 `Arc` 承载只读元数据；订阅者、relay 和监视任务之间只传 `SharedPacket`，不得复制 payload。

### 8.2 多协议识别与 framing

每条源站连接维护独立的 `ProtocolDemux`，流程为：



1. 使用 `BytesMut` 累积不完整记录，默认探测窗口不超过 64 KiB。

2. 根据挂载点 `source_expectation`、Content-Type 和已确认格式选择首选 scanner，同时保留对明显其他格式的识别能力。

3. 按协议签名和 framing 规则等待完整记录；不能把一次 socket read 当一条记录。

4. 完整 RTCM3 帧执行 CRC24Q，RTCM2 执行 parity，BINEX/NMEA 执行可用 checksum。

5. 校验成功后将 `BytesMut` 冻结为 `Bytes`，提取消息号并生成一次 `PacketMetadata`。

6. 记录格式、消息类型、计数、频率和最近出现时间。

7. 将完整记录作为 `SharedPacket` 广播；未知字节只做有界观察和采样，不能无限缓存。

RTCM3 fast path：



* 最大 payload 为 1023 字节，完整帧最大 1029 字节；超过上限立即判定长度非法。

* 只在已确认的帧边界之间搜索 `0xD3`，负载内部出现 `0xD3` 不会被当成新帧头。

* 帧不齐时保留半帧并返回 `NeedMore`，不能按固定 read 长度切分。

* CRC 失败时只丢弃当前候选帧，并从下一个字节继续重同步，同时增加 CRC 和 resync 指标。

* CRC 通过后只提取一次 12-bit message type；后续过滤、计数和旁路解析复用该结果。

非 RTCM 字节不能一律当作垃圾，也不能像旧程序那样无条件透传：



* 在 `VALIDATING` 准入窗口内，未知字节计入 `unknown_data` 比例并参与 observe/enforce 判定。

* `observe` 且源站已经准入时，后续无法识别的字节可以作为 `Unknown` Packet 转发并记录本地计数，不再触发断线。

* `enforce` 且挂载点期望 RTCM3 时，ACTIVE 后的未知字节继续计数但不广播；该源保持在线，直到读取超时或管理员断开。

* `Passthrough` 必须是显式事件类型，不能被静默吞掉，也不能被误记为 CRC 正确的 RTCM3。

格式识别规则：



* **RTCM3**：按 `0xD3` 引导、长度字段和 CRC24Q 验证。CRC 正确的帧再由 `rtcm-rs` 解析 12-bit message type、1005/1006、1033 和 MSM；1071–1137 的末位数字就是 MSM 版本 1–7。

* **RTCM2**：按 RTCM2 帧头、字边界、长度和 6-bit parity 规则增量组帧；只读取稳定的消息号，不能套用 RTCM3 CRC24Q。

* **BINEX**：按 BINEX 同步 / 记录头和长度规则组帧，并验证该记录可用的基本 checksum；enhanced CRC 尚未由候选 crate 支持，未实现前必须标记 `integrity=partial`，不能声称完整校验。

* **CMR/CMR+**：按已知消息头和长度表识别，记录消息 ID 和 CMR 子类型。CMR 没有可靠的通用 Rust crate，第一版使用小型本地识别器并用真实设备 fixture 锁定行为。

* **NMEA**：识别 `$...*HH` 语句；有 checksum 时严格验证，缺失时标记 `checksum=absent`。只作为辅助数据时不能因此把整个源站判成纠偏源。

协议签名只是候选，不能仅凭几个字节下结论。RTCM3 CRC 正确或 NMEA checksum 正确时可凭一个完整记录确认；RTCM2、CMR 和 BINEX 默认至少连续验证 2 个完整记录后才把格式置信度提升为 `CONFIRMED`。证据不足时标记 `UNKNOWN` 或 `INCOMPLETE`，并且不声称已经完成语义解析。

同一条流允许同时出现一个纠偏格式和 NMEA。`detected_formats` 保留全部已确认格式，`primary_format` 取当前窗口内有效的纠偏格式；如果同时出现多个纠偏格式，则标记 `MIXED_CORRECTION_FORMATS`。

坏 CRC、坏 parity 或坏 checksum 的记录不广播。源站仍可继续发送其他有效记录；只有读取超时、内存 / 队列上限或 `enforce` 策略拒绝才关闭连接。

`VALIDATING` 阶段必须把待判断字节限制在有界缓冲内。`observe` 模式准入后，无法识别但持续到达的字节可以作为 `Unknown` Packet 透明转发并计入未知比例；`enforce` 模式不会转发未知字节，超时后拒绝。

`unknown_data.max_ratio` 的算法为 `unknown_bytes / (unknown_bytes + valid_record_bytes)`，只在累计观察字节达到 `min_observed_bytes` 后评估；CRC 错误属于单独完整性指标，不重复计入未知字节比例。

### 8.3 源站数据校验

源站认证成功并获得 source claim 后进入 `VALIDATING`，此时数据不得进入公共 STR、客户端 fan-out 或 relay。NTRIP1 `SOURCE` 和 NTRIP2 `POST` 认证通过后进入同一套多协议校验管线。

校验只在准入阶段执行一次。源站进入 `ACTIVE` 之后不周期复查格式、可用纠偏数据、MSM 版本或 base position，也不因这些内容策略再次断线。

`source_validation.mode` 只控制准入内容策略：



* `observe`：识别并记录全部规则结果，不因格式错误、NMEA-only、期望格式不符、MSM 版本不符或缺少位置而踢人。满足准入条件后以 `ACTIVE + validation_result=WARN|FAIL` 继续转发。

* `enforce`：所有启用的内容规则必须通过；否则进入 `REJECTED`、关闭连接、清理在线 key 并写共享 cooldown。

硬故障不受 `observe` 保护，包括认证失败、单帧长度超限、内存 / 队列上限、读取超时、etcd lease 丢失、自 fencing 和完全没有任何数据。无法识别的字节本身不是硬故障，必须先隔离并继续尝试重新同步。

`VALIDATING` 的准入规则：



1. 在 `initial_data_timeout_s` 内收到可识别且完整的记录。

2. 识别出的格式在 `allowed_formats` 中。

3. `require_correction_data=true` 时，至少出现一个 `correction_formats` 中的格式。

4. Mountpoint 有 `source_expectation` 时，实际主格式必须在 `formats` 中；RTCM3 还要检查 `msm_versions`，其他格式忽略 MSM 期望。

5. 仅当主格式为 RTCM3 且 `position_required_for_formats` 包含 `rtcm3` 时，才要求在 `position_timeout_s` 内出现 1005 或 1006。

6. 对 RTCM3，检查 `data_profile` 是否包含观测值或 SSR；只有 1005/1006、1033 等辅助消息不算可用纠偏数据。

7. `msm.restrict_versions=true` 且准入窗口检测到 MSM 时，MSM 版本必须在 `allowed_versions` 中。

8. `msm.require_any=true` 时，RTCM3 源必须在 `required_within_s` 内出现至少一个允许版本的 MSM。

9. 观察字节数达到 `unknown_data.min_observed_bytes` 后，未知字节比例不能超过 `unknown_data.max_ratio`；否则视为错误模块或未知协议。

10. 准入最多检查 `max_frames_inspected` 条记录，避免异常源站无限占用验证资源。

时间窗口定义：



* `initial_data_timeout_s` 从收到第一个源站字节开始计时，要求出现首个完整且校验通过的记录。

* `admission_profile_window_ms` 从首个有效记录开始计时，用于收集格式、观测 / SSR、MSM 和位置摘要。

* `position_timeout_s` 和 `msm.required_within_s` 从首个有效记录开始计时。

* `max_frames_inspected` 是记录数硬上限；达到上限仍不满足规则时按 `mode` 标记或拒绝。

VALIDATING 时间优先级：

| 时间点 | enforce | observe |
|---|---|---|
| 连接后 15 秒内没有首个完整有效记录 | REJECTED | 有字节则 ACTIVE/UNKNOWN + WARN；完全无字节则关闭 |
| 首个有效记录后 3 秒 profile 窗口结束 | 已有必需规则通过则 ACTIVE；位置/MSM 等必需项仍缺失则继续 VALIDATING | ACTIVE，冻结当前 profile，缺失项写 WARN |
| 第 5 秒补到 1005/1006 | 仍可继续等待，补到后满足位置规则才 ACTIVE | 已在 3 秒进入 ACTIVE；晚到详情只更新本地 health，不修改冻结 profile |
| `position_timeout_s=30` 仍无 1005/1006 | REJECTED | 保持 ACTIVE + `BASE_POSITION_MISSING` 告警 |
| `msm.required_within_s=30` 仍无必需 MSM | REJECTED | 保持 ACTIVE + `MSM_REQUIRED_MISSING` 告警 |
| `max_frames_inspected` 先到 | 未通过必需规则则 REJECTED | 冻结当前 profile，按 WARN/FAIL ACTIVE |

规则：

- `enforce` 只在所有启用且必需的规则通过后 ACTIVE；profile 窗口是采样下限，不是位置/MSM 的提前失败点。
- `observe` 在 profile 窗口结束后冻结准入 profile；之后的 1005/1006、MSM 或消息差异不能重写准入结果，只进入 owner 本地 health/告警。
- 位置/MSM 超时从首个有效记录开始计算；不是从连接建立或从 profile 窗口结束重复计时。

“可用纠偏数据” 判定：



* RTCM3 观测值包括传统观测类型和 MSM 1071–1137。

* RTCM3 SSR 包括 1057–1068；观测值或 SSR 至少出现一种才满足 `require_usable_correction`。

* RTCM2、BINEX、CMR 第一版只做格式级识别；格式被确认后标记为 `format_only`，不继续解析观测值。

* NMEA-only 的 `has_correction_data=false`，不能通过 RTCM3 usable correction 检查。

* `rtcm-rs` 只用于 RTCM3 准入字段和管理页面需要的语义；RTCM2/BINEX/CMR 不做语义解析。

RTCM3 阈值算法：



* `valid_frames` 指 CRC24Q 正确、长度合法且完成组帧的 RTCM3 帧数。

* `correction_frames` 指观测值或 SSR 消息帧数，必须达到 `min_correction_frames`。

* `valid_frame_ratio = valid_frames / (valid_frames + crc_bad_frames + malformed_frames)`。

* CRC 比例分母只包含成功识别为 RTCM3 候选帧的数量，不把普通未知字节混入 CRC 比例；未知字节由 `unknown_data.max_ratio` 单独计算。

* 至少观察到 `min_valid_frames` 个候选帧后才评估比例；绝对错误数和比例任一超限都产生 `CRC_THRESHOLD_EXCEEDED`。

* 所有阈值的违规只影响 VALIDATING 准入；ACTIVE 后继续计数用于健康告警。

模式差异：



* `enforce` 必须等待所有启用规则通过或超时后，才能进入 `ACTIVE`。

* `observe` 在 `admission_profile_window_ms` 或 `max_frames_inspected` 到达后进入 `ACTIVE + profile_state=INCOMPLETE`；profile 由准入窗口冻结，不周期刷新。

* `observe` 收到字节但完全无法识别时，在 `initial_data_timeout_s` 后进入 `ACTIVE + primary_format=UNKNOWN + validation_result=WARN`，以便管理员看到错误模块；一条字节都没有的连接仍按无数据硬故障关闭。

策略组合必须明确：



| `mode`    | `msm.restrict_versions` | 实际行为                                                              |
| --------- | ----------------------- | ----------------------------------------------------------------- |
| `observe` | `false`                 | 只记录 MSM 版本和消息类型，不加违规告警                                            |
| `observe` | `true`                  | 记录超出 `allowed_versions` 的 MSM，加 `MSM_VERSION_NOT_ALLOWED` 告警，但仍在线 |
| `enforce` | `false`                 | 不限制 MSM 版本，只记录实际版本                                                |
| `enforce` | `true`                  | 超出版本进入 `REJECTED`，写 cooldown                                      |

关闭数据格式约束时，使用 `mode=observe` 并保留 `allowed_formats` 作为观测基线。系统仍会记录 `FORMAT_NOT_ALLOWED` 或 `EXPECTED_FORMAT_MISMATCH` 告警，但不会因这些规则断开源站。

常见违规原因：



* `NO_CORRECTION_DATA`：只有 NMEA、文本或其他非纠偏数据。

* `FORMAT_NOT_ALLOWED`：识别到未允许格式。

* `EXPECTED_FORMAT_MISMATCH`：实际格式与 Mountpoint 的 `source_expectation` 不符。

* `MIXED_CORRECTION_FORMATS`：同一源站窗口内出现多个纠偏格式。

* `MSM_VERSION_NOT_ALLOWED`：RTCM3 中出现未允许的 MSM1–MSM7 版本。

* `MSM_REQUIRED_MISSING`：启用了 `msm.require_any` 但超时未出现 MSM。

* `BASE_POSITION_MISSING`：RTCM3 源启用了位置要求但未出现 1005/1006。

* `NO_OBSERVATION_OR_SSR`：RTCM3 只有位置 / 设备等辅助消息，没有观测值或 SSR。

* `UNRECOGNIZED_DATA`：未知记录比例超限，疑似错模块。

* `CRC_THRESHOLD_EXCEEDED`：窗口内坏帧超过阈值。

进入 `ACTIVE` 后，准入结果冻结并写入管理状态。源站中途切换格式、出现未允许的 MSM 或其他内容变化不会触发再次验证；只有 framing、长度、资源上限、读取超时或管理操作可以关闭源站。

配置从 `observe` 热切换到 `enforce` 时，只影响之后的新源站；现有 ACTIVE 源保持在线，管理员如需重新评估必须主动断开该源站。

`observe` 模式不得写 cooldown，也不能阻止同一源站重连；其目的只是观测和定位错误模块。

源站被策略拒绝时：



1. 停止广播并关闭源站 socket。

2. 删除 source claim 和 runtime source，记录结构化拒绝原因。

3. 删除 owner user session；源站不创建 IP session。

4. 关闭本地订阅者和依赖该源的 relay。

5. 写入短暂 backoff 记录。

### 8.4 在线信息与 Source Profile

源站连接 claim 成功时先写入 `runtime/sources/{mountpoint}` 的临时 STR 记录，`state=VALIDATING`。解析和准入完成后，同一 owner 通过 fencing Txn 将记录更新为 `ACTIVE`，只写轻量 STR 路由字段和准入摘要。

`runtime/sources` 记录：

- owner、caster_id、runtime_epoch、source_generation、connection_id、relay_endpoint、state。
- STR 必需字段：identifier、format、format-details、carrier、nav-system、lat/lon、solution、generator、bitrate。
- 准入摘要：primary_format、MSM versions、position_seen、usable_correction、integrity_state、warning_codes。

owner 本地 `SourceProfile` 记录：

- 完整 source_software。
- RTCM3/RTCM2/BINEX/CMR/NMEA 的详细消息和计数。
- 未知记录/字节、CRC/parity/checksum 计数和实时 health 窗口。
- source_ip。
- RTCM3 1033 receiver、firmware、antenna、序列号。
- 1005/1006 station ID、ECEF、height 和位置质量详情。
- first/last data、last violation 和更详细的 validation warnings。

管理 API 查看详细设备或实时 health 时，根据 `caster_id` 回源 owner 查询。不能把完整 SourceProfile 写进 `runtime/sources`，也不能为每个源建立高频 profile Watch key。

#### Source Availability 与 Stream Health

三个状态必须分开：



* `availability`：TCP 源站连接是否仍在线。

* `admission_validation`：上线时是否通过格式、可用纠偏、MSM 和位置检查。

* `stream_health`：在线期间最近有效帧、观测值 / SSR 时间、CRC 比例、bitrate 和未知数据比例。

`source_health` 只做本地观测和告警，不改变 ACTIVE 状态、不自动断开源站、不周期写 etcd。管理页面必须同时展示 “在线但数据已陈旧” 的状态，不能只显示绿色在线。

RTCM3 的快速健康统计只读取 message type，不需要完整语义解析：



* 最近任何有效帧时间。

* 最近 RTCM3 观测值或 SSR 时间。

* 最近 1005/1006 时间。

* CRC 错误比例、未知字节比例和输入 bitrate。

* 超过 `observation_timeout_s` 未出现可用纠偏内容时触发 `STREAM_HEALTH_WARNING`。

ACTIVE 后的执行边界：



| 工作                                           | 持续执行             |
| -------------------------------------------- | ---------------- |
| TCP 在线、字节数、bitrate                           | 是                |
| RTCM3 framing、CRC、message type、观测 / SSR 最近时间 | 是                |
| 1005/1006 提取和 station ID 变化告警                | 是，轻量借读           |
| 完整 MSM 卫星 / C/N0 语义                          | 仅 Web monitor 抽样 |
| 修改准入阶段冻结的 `data_profile`                     | 否                |
| 根据健康结果自动踢源                                   | 否                |

健康窗口：



* `short_window_s=10` 用于当前 bitrate、CRC 比例和短时突发。

* `long_window_s=60` 用于陈旧数据、观测值 / SSR 缺失和趋势。

* CRC 比例分母为同一窗口内完整 RTCM3 候选帧数；未知字节比例单独计算。

* 连续超过 `observation_timeout_s` 无观测 / SSR 时告警；恢复后需连续 `alert_clear_s` 正常才清除。

* 同一 Mountpoint 的 1005/1006 station ID 发生变化、ECEF 跳变超过配置阈值时触发 `STATION_ID_CHANGED` 或 `BASE_POSITION_JUMP`，第一版只告警。

第一版不自动根据 `stream_health` 踢源站；管理员可以在看到告警后主动断开或切换 `source_validation.mode` 后让新连接执行更严格策略。

### 8.5 运行时修正 STR 字段

参考 `E:/GitHub/NTRIPcaster` 中解析数据并修正 STR 的思路，通过 `source_validation.str_refresh.enabled` 控制运行时刷新。

格式映射：



| 实际主格式     | STR `format`            | `format-details` | 备注                             |
| --------- | ----------------------- | ---------------- | ------------------------------ |
| RTCM3     | `RTCM 3`，配置可覆盖具体 3.x 展示 | 消息号和频率           | 1005/1006 更新位置，MSM 更新导航系统 / 载波 |
| RTCM2     | `RTCM 2`                | RTCM2 消息号和频率     | 不要求 1005/1006                  |
| BINEX     | `BINEX`                 | record ID 和频率    | 仅输出已确认 subtype                 |
| CMR/CMR+  | `CMR` 或 `CMR+`          | 已知消息 ID          | 不套用 RTCM 字段                    |
| NMEA-only | `NMEA`                  | sentence kinds   | `misc` 标记 `nmea_only`          |
| Unknown   | `UNKNOWN`               | 匿名计数             | 只用于在线诊断，不能让配置伪装成已确认            |

源站准入时计算一次并在本次在线期间保持的字段：



* 主格式对应的 `format` 和 `format-details`。

* 已验证位置消息更新 lat/lon；其他格式无位置消息时保留 Mountpoint 配置位置。

* RTCM3 MSM 更新 nav-system 和 carrier。

* 准入样本窗口估算 bitrate。

* `Source-Agent`/`User-Agent` 可以更新 generator，但不得记录密码。

限制：



* `format-details` 只保留最常见的消息类型和频率，设置固定最大长度，禁止无限增长破坏 STR。

* `misc` 只放短状态标记，例如 `validation=warn,profile=rtcm3,msm=4`，不能用分号破坏字段边界。

* NMEA 混在 RTCM3/CMR 流中时，只记录辅助数据存在；不得把标准 `nmea` 字段误解为 “源站已发送 NMEA”。

* 站点名称、网络名、operator 等管理字段只来自持久配置，不从 Header 或数据流覆盖。

关掉 `str_refresh` 时，STR 使用 Mountpoint 配置字段；实际识别结果仍写入管理 API 和在线信息，管理员可据此修改配置。

STR 不根据每个数据帧实时更新，也不会在源站在线期间周期性改写 `runtime/sources` 或重新生成集群 STR。只有源站重新连接、Mountpoint 配置变化或管理员主动刷新时才产生新的在线 STR 视图。

### 8.6 解码后端

数据处理分为三层：



1. **安全 framing 层**：纯 Rust 完成 RTCM3 CRC24Q、RTCM2 parity、BINEX 可用 checksum / 长度、NMEA checksum、长度和未知字节隔离。

2. **协议识别层**：记录格式、消息号、MSM 版本、位置和计数；即使语义解码器不认识某条消息，也要保留其存在。

3. **语义解码层**：RTCM3 用 `rtcm-rs`，NMEA 用 GGA parser；RTCM2/BINEX/CMR 第一版只完成格式识别，不进入语义解码层。

后端能力如下：



| 格式       | framing / 识别         | 语义解析策略                                   |
| -------- | -------------------- | ---------------------------------------- |
| RTCM3    | 纯 Rust               | `rtcm-rs`，固定精确版本；支持 1005/1006、1033、MSM   |
| RTCM2    | 纯 Rust               | 只识别格式、framing 和必要消息号，不解析观测值              |
| BINEX    | 纯 Rust               | 只识别 record 边界和 subtype；不引入 `binex` 做语义解析 |
| CMR/CMR+ | 自研小型识别器              | 只识别格式和消息头，不解析观测值                         |
| NMEA     | `nmea` crate 或自研严格校验 | 只解析 GGA 和必要的 checksum                    |

解码接口应抽象为：



```
ProtocolDemux       // 所有流必用，纯 Rust framing
DataProfileBuilder  // 所有流量共用的在线统计和策略判断
Rtcm3Decoder        // rtcm-rs 默认实现
OptionalDecoder     // 第一版不进入主流程，仅供未来兼容验证
```

RTKLIB 只能作为 “经验证格式” 的可选 fallback：



* 固定版本，只编译实际需要的 decoder。

* 通过稳定 C ABI shim 和受监督独立 worker 运行，崩溃不能影响 NTRIP 数据面。

* 每个 mountpoint 使用独立解码上下文，输入 / 输出长度、线程和队列设限。

* 以 capability probe 和真实设备 fixture 证明能识别目标格式；不能因为链接了 RTKLIB 就宣称支持 CMR/BINEX/RTCM2。

* Rust framing 层不依赖 RTKLIB；RTKLIB 失败时仍能标记格式、CRC/parity 和 “无法语义解析”。

推荐的最终取舍：



| 层                                      | 第一版实现                  | 是否允许被 RTKLIB 替代 |
| -------------------------------------- | ---------------------- | --------------- |
| NTRIP 字节流、分块、超时                        | 纯 Rust                 | 否               |
| RTCM3/RTCM2/BINEX/CMR/NMEA 识别和 framing | 纯 Rust `ProtocolDemux` | 否               |
| 源站准入、Data Profile、STR、在线状态             | 纯 Rust                 | 否               |
| RTCM3 的 1005/1006、1033、MSM 语义          | `rtcm-rs`              | 可以，但默认不换        |
| NMEA/GGA 语义                            | `nmea`                 | 可以              |
| BINEX record 语义                        | 第一版不做                  | 后续独立评估          |
| RTCM2/CMR 的深度语义                        | 第一版不做                  | 后续独立评估          |

第一版不要追求 “所有格式的所有字段都完整解码”。转发、可靠识别、在线标记、STR 刷新和错误模块判断是主目标；深度字段只做管理页面真正使用的字段。这样比引入一个必须全程经过 RTKLIB 的统一解析层工作量更小，也更容易保证集群数据面稳定。

如果团队更偏好只维护一个 C/C++ 语义后端，可以把 RTKLIB 作为唯一的 `OptionalDecoder`，但仍然保留 Rust 侧 framing 和 Data Profile。不要使用 `unsafe` FFI 直接在每个 Tokio 连接任务里调用 RTKLIB；推荐一个受监督 worker pool，输入完整记录，输出有版本号的固定 DTO。RTCM3 仍建议保留 `rtcm-rs` 作为默认实现，因为纯 Rust 路径在部署、测试和崩溃隔离上更简单。

每个记录只交给本地 decoder 一次。Web 实时监视订阅本地解析摘要，不重新从网络复制原始流。

### 8.7 解析执行路径

解析不能简单分成 “全部在热路径” 或 “全部异步旁路”，应拆成三类：



| 路径     | 执行位置                  | 是否允许丢样 | 任务                                         |
| ------ | --------------------- | ------ | ------------------------------------------ |
| 准入解析   | 源站连接任务内，增量、无阻塞        | 否      | framing、校验、格式 / 纠偏判断、MSM 版本限制、期望格式不符       |
| 常规定量统计 | 源站连接任务内，原子计数          | 否      | 帧数、字节数、类型计数、频率、最近时间、CRC/parity/checksum 错误 |
| 富语义分析  | 有界队列 + 独立 parser task | 是      | 卫星列表、C/N0、载波、完整 MSM 字段、Web 图表              |

准入解析必须是轻量同步逻辑，因为它决定源站能否进入 STR 和 fan-out。它只读取必要字段，不做卫星数组、字符串或 serde 分配。

富语义分析只在承接 Web monitor 的管理节点上按需启动。每个节点上的每个 Mountpoint 最多一个 `MonitorSession`，使用一个 `mpsc::Sender<SharedPacket>`，由 `try_send` 投递：



* 队列满或 parser 速度不足时，丢弃该分析样本并增加 `parser_sample_dropped`，不能阻塞源站读取和客户端 fan-out。

* 公共 Web 图表默认按 `public_web.monitor_snapshot_interval_ms` 输出，不要求每个 MSM 帧都完整解码。

* 1005/1006 位置可先由轻量 parser 提取，避免 NEAR 因深度解析队列拥堵而失去位置更新。

* 长期状态只保存 owned 的 `DataProfile`、坐标、类型集合和统计值；借用帧的解析视图不得跨 `await` 保存。

* RTKLIB worker 只接收需要兼容解析的完整记录批次，不能成为全部流量的必经节点。

* 第一个已登录 Web subscriber 触发 Relay 和富解析，多个 subscriber 只增加 refcount；最后一个离开后默认立即取消 MonitorSession，可按配置给予短暂 grace。

RTCM3 的 MSM7→MSM4/3 转换不能按 “位段原位拷贝” 理解。MSM 版本变化涉及卫星 / 信号掩码、粗值和精细值字段布局，必须由经过 fixture 验证的 decoder/encoder 重新编码。第一版建议只实现类型过滤、允许或拒绝；转换功能保持关闭，后续作为独立功能验证后再启用。

### 8.8 Fan-out

每个本地订阅者拥有：



* 一个有界通道。

* 一个字节 backlog 计数。

* 一个独立 writer task。

fan-out：



```
source SharedPacket
  -> snapshot subscriber list
  -> check per-subscriber byte backlog
  -> try_send Arc<Packet> without copying payload
  -> backlog over limit: mark slow
  -> remove slow subscribers
```

要求：



* 通道存 `SharedPacket`，不存 payload 副本；`Bytes::slice()` 只用于解析头或过滤，不改变原帧生命周期。

* backlog 以实际数据字节为主，不能只按消息条数限流。

* 投递时不能 `await`，不能持锁写 socket，也不能因单个慢客户端阻塞其他订阅者。

* writer 成功写出后扣减 backlog；连接错误、写超时或超过硬水位时摘除该订阅者。

* 第一版允许 framing 阶段每帧组装一次；后续可评估 `BytesMut` 缓冲池，但不得提前回收仍被 `SharedPacket` 或解析视图引用的内存。

### 8.9 慢客户端



* backlog 超过 `limits.subscriber_backlog_bytes` 时断开客户端。

* channel 满时只影响该客户端。

* backlog 一旦超过硬水位直接摘除，不做丢帧后继续发送；避免 Rover 收到不连续差分流。

* 内核 socket 长时间无法写入由 `write_timeout_s` 触发断开。

* 同时受 `max_subscribers_per_source` 和 `max_subscribers` 限制。

* 踢出客户端后同步减少本地订阅计数。

* relay 本地订阅者归零后进入 idle close。



***

## 9. 自动 Relay

### 9.1 路由

客户端连接 Caster A，请求 C63：



```
A 本地 Hub 有 C63 -> 直接订阅
A 本地 Hub 没有 C63
  -> 读本地 watch cache 的 runtime/sources/C63
  -> 不存在：404/503 或 on-demand
  -> owner endpoint == A：本地转发
  -> owner endpoint != A：确保本地 C63 relay
```

### 9.2 Relay Registry



```
key: mountpoint
value: Arc<Relay>
```

每个 Caster 对每个 mountpoint 只有一条上游 relay。

状态：



```
ABSENT
  -> STARTING
  -> ACTIVE
  -> DRAINING
  -> ABSENT

STARTING/ACTIVE -> FAILED -> ABSENT
```

### 9.3 首个客户端



1. 创建或取得本地 relay entry。

2. 使用 mTLS 和内部服务身份连接 owner 的 relay endpoint。

3. 发送内部 NTRIP GET，并携带 `cluster_id`、mountpoint、source\_generation、source\_connection\_id、requesting\_caster\_id、`relay_hops=1` 和 request\_id。

4. 上游返回成功后，本地客户端才收到成功响应。

5. 多个并发客户端等待同一个 relay STARTING。

6. STARTING 失败时所有等待客户端返回错误。

内部上游规则：



* 接受 `ICY 200 OK` 或 `HTTP/1.1 200 OK`，其他状态按失败处理。

* owner 必须校验 cluster、服务身份、mountpoint、source\_generation 和 source\_connection\_id；任一不匹配返回 409/410，不能把旧 relay 接到新 source。

* mTLS 只证明节点身份；服务 token 还必须包含 `relay:read` scope，并按 cluster/mountpoint allowlist 授权，不能默认 “持有集群证书即可读取所有源”。

* 只允许一跳 relay；内部请求中的 `relay_hops` 必须为 1，owner 不接受另一个 relay 再转发。

* owner 必须确认 mountpoint 当前确实由本节点 claim，且 `max_internal_relays` 未超限；请求者不能借 relay listener 调用管理 API 或其他内部服务。

* 支持上游 chunked；响应 Header 大小沿用 `ntrip.headers_max_bytes`。

* 不跟随 HTTP redirect，不使用用户 Basic 凭据；只使用 mTLS identity 和 internal service token。

* 连接和响应 Header 受 `relay.connect_timeout_ms` 限制，正文读取受 `relay.read_timeout_s` 限制。

* 上游断开、半帧超时或 owner source 消失时立即关闭本地 relay 和下游。

### 9.4 后续客户端



* 直接订阅本地 relay。

* 不重复建立上游连接。

* 每个客户端仍独立执行客户端认证和用户配额。

Relay 数据路径：



```
owner 源站
  -> owner ProtocolDemux / SharedPacket
  -> 内部 relay NTRIP 字节流
  -> edge ProtocolDemux / SharedPacket
  -> edge 本地 fan-out
```



* relay 必须重新 framing，因为 TCP 不保留报文边界，但不能重新编码数据。

* edge relay 在开始和收到 source watch 变更时校验 `source_generation/connection_id`；不一致立即停止读取、关闭 relay、清空下游队列并断开下游。

* relay 的公共在线描述以 owner 的轻量 `runtime/sources` route 为主；详细 profile 和实时统计仍只在 owner 本地。

* relay 不要求对每个转发帧做完整语义解析；只有承接已登录 Web monitor 的管理节点在本地 Relay 输出上创建 MonitorSession，Relay 本身只转发原始差分字节。

* 某本地客户端慢或断开时只摘除该客户端，不能断开共享 relay 或其他客户端。

* owner 把内部 relay 连接当作普通 bounded subscriber；edge relay 读得太慢时由 owner 断开该 internal relay，不能拖累其他客户端。

* 每个 Caster 的 `limits.max_relays` 是硬上限；达到上限时返回 503，不能无限创建上游 relay。

* `relay.max_concurrent_starting` 限制同时打点 owner 的 STARTING 数量，避免故障后形成重连风暴。

* 每个外部 relay 的输入和输出 bitrate 分别受 `max_input_bitrate_bps` 和 `max_output_bitrate_bps` 限制，超限进入失败 / 退避，不无限占用 fan-out 队列。

* `relay_hops` 固定为 1；owner 不接受 relay 级联，任何环路请求直接拒绝并告警。

* 重连使用指数退避并受 `max_reconnect_attempts` 限制；没有本地 consumer 时不再后台重连。

* relay 失败后不为无人使用的挂载点后台无限重连；由新客户端重新触发 route。

### 9.5 空闲关闭



* 本地用户 subscriber 和 internal monitor 数量都降为 0。

* 经过 0–1 秒 grace。

* 关闭上游连接。

* 删除本地 relay entry。

### 9.6 上游断开



* 立即关闭本地下游客户端。

* relay 进入 FAILED。

* 不保留 2–5 秒等待窗口。

* 新客户端重新触发 route 和 relay。

### 9.7 Owner 或 Source 状态变化

watch 发现 `runtime/sources/{mount}`：



* 新增 VALIDATING：更新本地管理视图，但不加入公共 STR、NEAR 索引或 relay route。

* VALIDATING -> ACTIVE：使记录可发布，重建本地 STR 和 NEAR 索引。

* ACTIVE -> DRAINING/REJECTED：停止新订阅；已有 relay 按关闭策略处理。

* 消失：关闭 relay 和下游。

* endpoint 改变：关闭旧 relay 和下游，后续按新 endpoint 重连。

* `connection_id` 改变：视为新源，关闭旧下游。

relay 不创建 `runtime/sources`，不会形成 relay 环。



***

## 10. STR

### 10.1 数据来源



```
ConfigSnapshot
  + runtime/sources watch
  -> MountViewCache（保留 VALIDATING/ACTIVE/DRAINING，按 generation 防旧事件回退）
  -> filter publishable states
  -> SourcetableSnapshot
```

公开 STR 包含：



* 当前 `state=ACTIVE` 的 source。

* 启用且允许 advertise 的系统合成 mountpoint，名称取 `near.mountpoint`。

* 配置中允许 advertise 的 on-demand mountpoint。

不会把仅存在于内存的 relay、`VALIDATING` source 或失败的临时 STR 当成可订阅源。

源站连接后的 STR 流程：



1. source claim 成功，在 `runtime/sources/{mount}` 写入 `VALIDATING` 临时记录，使用配置默认名称、格式、坐标和占位状态。

2. 各 Caster 的 source watch 收到记录，更新本地 `MountViewCache`，但公共 STR 过滤器排除 VALIDATING。

3. Owner 解析准入窗口，修正格式、坐标、消息类型、设备、状态和 bitrate。

4. Owner 使用 claim revision/generation/connection\_id 条件 Txn 将记录更新为 `ACTIVE`。

5. 各 Caster watch 到 ACTIVE 后，在本地重建一次 STR 快照；之后请求直接发送已缓存的 `Bytes`。

etcd 中只保存每个 mountpoint 的 canonical record，不写整张多挂载点 STR。全表由各 Caster 独立组装，避免大对象覆盖和并发写冲突。

### 10.2 Scope



```
sourcetable:
  scope: auto
  global_max_lines: 5000
  global_max_bytes: 1048576
```



* `global`：所有可发布 mountpoint。

* `regional`：只包含当前 Caster 配置的 region。

* `auto`：低于阈值 global，超过阈值 regional。

主 Caster 可以固定 global，edge 固定 regional。

### 10.3 大表处理



* 使用本地 `BTreeMap` 维护 Mountpoint canonical record。

* watch 事件按 revision 合并，旧 revision / 旧 generation 不能覆盖新记录；高频事件做 50–250 ms debounce。

* 只为 `ACTIVE`、advertise 和 scope 允许的记录生成 STR 行。

* `CAS`、`NET` 放在普通记录前；NEAR 启用时，`near.mountpoint` 是该 Caster 的第一条 `STR` 数据行，其余 STR 按 mountpoint 稳定排序。

* 组装完成后生成不可变 `Bytes` 和对应 `Content-Length`。

* `ArcSwap<SourcetableSnapshot>` 原子切换。

* 每个连接只复制引用并写缓存字节。

* 不静默截断 STR。

* 全局浏览使用分页管理 / 公共目录 API。

### 10.4 客户端认证

`GET /` 的 STR 请求不要求客户端认证，也不创建 session。

客户端认证只作用于 `GET /MOUNT` 数据流订阅。

### 10.5 STR 字段和配置优先级

STR 使用 19 个分号字段：



```
STR;mountpoint;identifier;format;format-details;carrier;nav-system;network;country;lat;lon;nmea;solution;generator;compression;authentication;fee;bitrate;misc
```

字段来源优先级：



1. 已确认的运行时数据：主格式、消息 / 记录类型、位置、GNSS、载波、bitrate、source software。

2. Mountpoint 的 `str_fields` 配置：站点 identifier（展示名称）、format、network 等。

3. Caster Config 的 `sourcetable.defaults`：网络名、operator、国家、generator、认证和费用。

运行时可以更新的字段：



* `format` 按轻量 source route 的 `admission.primary_format` 映射为 `RTCM 3`、`RTCM 2`、`BINEX`、`CMR/CMR+`、`NMEA` 或 `UNKNOWN`。

* `format-details` 写入有界长度内的消息号、record ID、sentence kind 和频率。

* 1005/1006 更新 lat/lon。

* MSM 更新 format-details、nav-system 和 carrier。

* 统计窗口更新 bitrate。

* `Source-Agent`/`User-Agent` 可以更新在线 STR 的 generator。

站点名称、网络名、operator、CAS/NET 等管理字段只来自配置，不由源站数据自动覆盖。

当 `source_validation.str_refresh.enabled=false` 时，`format` 和 `format-details` 保持 Mountpoint 配置值，但管理 API 仍展示实际 `data_profile`。这样可避免错误模块被误写成正确格式，也允许管理员选择 “先观察、后改配置”。

NMEA-only 源在 `enforce` 模式下不会进入公共 STR；在 `observe` 模式下以 `format=NMEA` 和 `misc` 中的短标记展示，便于快速识别。RTCM3 流里夹带 NMEA 时，主格式仍为 RTCM3，并通过 `misc` 标记辅助 NMEA 存在，不能把标准 `nmea` 字段当作 “检测到了 NMEA” 使用。

配置值禁止包含分号、CR 和 LF；不引入非标准转义格式，避免破坏 19 字段解析。

CAS 和 NET 记录由 Caster Config 生成，并随配置 generation 自动同步到所有 Caster。



***

## 11. NEAR

### 11.1 系统合成挂载点

Caster Config 的 `near.enabled=true` 时，每个 Caster 在本地创建同名系统挂载点，名称完全取自 `near.mountpoint`。示例值是 `AUTO_NEAR`，没有固定保留字。

它：



* 不属于任何用户。

* 不属于任何普通 Mountpoint。

* 不写 `runtime/sources/{near.mountpoint}`。

* 不保存集群级 NEAR 在线源，也不把 NEAR 选择结果复用给其他客户端。

* 不能接收 NTRIP1 或 NTRIP2 源站上传。

* 每个连接只创建一个用户 session，但每个连接独立保存 GGA、当前基站、候选和切换状态。

* 在任意 Caster 上都可以通过 `GET /{near.mountpoint}` 使用。

* `near.advertise=true` 时进入公共 STR。

公共 STR 中只保留一条该配置名称的合成行，不因为每个 Caster 都提供 NEAR 功能而重复。

排序规则：`CAS` 和 `NET` 保持标准记录在前，随后第一条 `STR` 必须是 NEAR；其他真实 Mountpoint 按名称排序。NEAR 关闭时第一条 STR 为普通挂载点。

该合成 STR 行：



* `format=RTCM 3`，`format-details` 使用配置的允许 RTCM3 消息摘要。

* `nmea=1`，因为 NEAR 必须依赖客户端 GGA。

* `solution=0`，因为输出仍是单个真实基准站的差分流，不生成网络改正数。

* `misc` 可标记 `virtual=near`，不能使用该字段传递客户端状态。

如果 `near.enabled=false`：



* `GET /{near.mountpoint}` 返回 404。

* 公共 STR 不包含该合成挂载点。

* 已有 NEAR 客户端 session 关闭。

修改 `near.mountpoint` 时，旧名称立即停止接受新连接，已有 NEAR session 关闭；客户端使用新名称重新连接。

### 11.2 本地空间索引

每个 Caster 从本地快照构建：



```
MountViewCache
  -> immutable SpatialIndexSnapshot
  -> ArcSwap
```

索引包含集群当前可用于 NEAR 的真实挂载点：



* `primary_format=rtcm3` 的 ACTIVE source。

* 已通过准入且 `has_usable_rtcm3_correction=true`。

* 可用的 RTCM3 on-demand source；第一版不让 on-demand 源参与 NEAR，避免并发拉流放大。

* 排除配置的 `near.mountpoint` 合成挂载点。

* 排除 `validation_result=FAIL`、位置缺失或数据源已失效的挂载点。

实现：



* 坐标转换为 3D 单位球向量。

* 使用 KD-tree/R-tree 查询半径。

* 查询候选后再用 f32 Haversine 精确排序。

* 空间索引默认使用集群全局候选，不受 Caster 的 STR scope 限制。

* mountpoint、位置或在线状态变化时重建不可变快照。

* GGA 查询只访问本地索引，不访问 etcd。

### 11.3 客户端接入



1. 客户端在任意 Caster 请求 `GET /{near.mountpoint}`。

2. 先执行客户端 IP 黑名单；`quota=0` 在认证前拒绝。

3. 根据 `client_auth` 决定是否验证客户端用户，认证后执行正数 IP quota 的并发计数。

4. 如果请求带 `Ntrip-GGA` Header，立即解析位置。

5. 返回 NTRIP 成功响应后，建立独立的上行 GGA reader，等待客户端后续 NMEA。

6. 每个 NEAR 连接只创建一个客户端 session，但拥有独立的 `NearSessionState`。

7. 只有收到有效 GGA 后才选择真实 RTCM3 源；没有 GGA 时不发送差分数据。

8. 上行 GGA 不进入差分 fan-out；非 NEAR 连接收到的 GGA 或其他客户端输入直接丢弃。

NEAR 不执行普通挂载点的以下逻辑：



* 不占用 source claim，不创建 `runtime/sources`。

* 不验证 Mountpoint owner 或上传密码。

* 不执行源站准入、RTCM 观测值或 SSR 检查。

* 不对应固定 STR 消息集合；公共 STR 只有一条合成 NEAR 记录。

* 客户端认证和用户连接配额仍正常执行。

只要 NEAR 功能启用且客户端认证通过，即使当前没有候选源也先返回 200 并等待 GGA；在 `no_source_timeout_s` 内无可用源后关闭。

### 11.4 最近基站选择

收到 GGA 后：



```
接收 GGA
  -> 校验 NMEA checksum、fix quality、HDOP
  -> 空间索引查询附近候选
  -> Haversine 精确排序
  -> 过滤超过 max_distance_m 的候选
  -> 过滤非 RTCM3、FAIL 或位置缺失的候选
  -> 得到最近的真实 RTCM3 mountpoint
  -> 本机源直接订阅
  -> 远端源建立/复用 relay
```

如果当前没有数据源，直接选择最近可用源，不应用切换迟滞。

NEAR 只转发 RTCM3 Packet。底层同一 RTCM3 源被多个 NEAR 客户端选择时，可以复用本地 source/relay fan-out，减少上游连接；不能复用的是每个客户端的 GGA、最近站判断、切换状态和输出边界。

### 11.5 定位精度和切换

GGA 中的 fix quality 和 HDOP 用于避免在定位误差较大时频繁切换。

建议计算：



```
position_accuracy_m = hdop * hdop_m_per_unit
switch_guard_m = max(
    hysteresis_m,
    base_position_uncertainty_m + position_accuracy_m
)
```

只有当以下条件成立时才切换：



```
new_candidate_distance + switch_guard_m < current_base_distance
```

其他规则：



* `hdop > max_hdop` 时不切换。

* fix quality 无效时不切换。

* NEAR 客户端自行发送 GGA 的频率可能很高；空间索引查询最多每 `min_recompute_interval_ms=1000` 执行一次。

* GGA reader 只保留最新位置；计算 scheduler 按连接 tick，不对每条 GGA 建队列。

* 节点级最多同时执行 `max_query_concurrency=8` 次空间查询，并限制 `max_spatial_queries_per_second=10000`；超限只延迟计算，不跨客户端复用结果。

* 超过 `max_recompute_interval_ms=5000` 或距上次计算移动超过 `min_recompute_distance_m=4` 时允许重新查询。

* 当前数据源失效时忽略 HDOP 和 hysteresis，立即选择新的最近可用源。

* 切换前进入 `SWITCH_OBSERVING` 状态，持续 `switch_observe_ms=3000`；期间继续收集 GGA，但目标基站不因每个 GGA 改变。

* 观察窗口结束时，若起止位置移动不超过 `switch_position_threshold_m=4`，且目标仍是最近候选，则切换；移动超过阈值则放弃本次切换并重新评估。

* 如果当前连接没有已选源，直接选择最近源，不执行观察窗口。

HDOP 只是定位质量的近似指标，因此 `hdop_m_per_unit` 和 `base_position_uncertainty_m` 必须可配置。

切换状态：



```
WAITING_GGA
  -> SELECTED(A)
  -> SWITCH_OBSERVING(B)
  -> SWITCHING(B)
  -> SELECTED(B)
```

`SWITCH_OBSERVING` 是每客户端独立状态，不能保存到共享 NEAR 对象，也不能被同一节点上的其他客户端复用。

### 11.6 数据源切换

NEAR 客户端连接保持不变，只切换内部订阅：



```
当前源 A
  -> 新位置判断 RTCM3 源 B 更近
  -> 进入 SWITCH_OBSERVING，观察 3 秒
  -> GGA 稳定且 B 仍满足 hysteresis
  -> 预建立源 B（本机订阅或 relay），但暂不向客户端输出
  -> B 就绪后设置切换边界
  -> 丢弃尚未发送的 A 数据
  -> 如果 B 有 1005/1006 缓存，先发送 B 的位置
  -> 从下一条 B 数据开始连续输出
  -> 取消源 A 订阅
```



* 源 A 和源 B 都在本机：切换本地 LiveSource。

* A 本机、B 远端：取消 A，建立或复用 B relay。

* A 远端、B 本机：取消 A relay 订阅，切换本机源。

* A、B 都远端：取消 A relay 订阅，建立或复用 B relay。

* 更新客户端 session 中的 `near_current_mountpoint`，但不新建 session。

* 实际 `SWITCHING` 阶段只保存最新 GGA，不重新选站；切换完成后用最新 GGA 重新进入正常评估。

* NEAR 输出过滤器只放行 RTCM3 Packet；NMEA、CMR、BINEX、RTCM2 和 `Unknown` 不向 NEAR 客户端发送。

切换采用 make-before-break，避免 B 未建立时先断开 A；但在切换边界之后不得继续混发 A 的旧数据。

如果新源连接失败：



* 当前源仍可用时保持当前源，等待下次重算。

* 当前源已失效时选择下一个最近候选。

* 没有候选可用时等待 GGA 重试，并在 `no_source_timeout_s` 后关闭连接。

### 11.7 源站消失

当前 NEAR 数据源从本地索引消失时：



1. 立即解除当前源或 relay 订阅。

2. 使用最近一次有效 GGA 查询下一个最近源。

3. 存在可用源则切换。

4. 没有可用源则等待新 GGA，直到超时关闭。

### 11.8 位置来源



* 默认使用 STR 经纬度。

* RTCM3 源在准入窗口收到 1005/1006 时，用该位置覆盖本次在线记录的展示位置。

* ACTIVE 后不持续解析或更新 NEAR 位置；源站重新连接并重新准入后生成新的位置记录。

* 位置信息只在 source 上线时写入一次 etcd，不做周期刷新。

* 位置记录包含 `source`、`updated_at` 和 `confidence`；RTCM3 已校验位置优先于配置坐标，配置坐标优先于缺失值。

* `near.position_ttl_s=0` 表示位置在本次 source 在线期间不因时间过期；大于 0 时，超过 TTL 的源从 NEAR 候选中排除并告警，但不因此断开源站。

* 移动基准站或不定期变更坐标的部署必须设置合理的 TTL，或在迁移后主动重连 / 刷新 source；Caster 不通过持续解析 1005/1006 自动更新位置。



***

## 12. 管理 API 和运维

### 12.1 管理 API

使用新接口，不兼容旧 `/adm`：



```
POST   /admin/v1/auth/login
POST   /admin/v1/auth/logout
GET    /admin/v1/auth/session
GET    /admin/v1/auth/csrf

GET    /admin/v1/api-tokens
POST   /admin/v1/api-tokens
DELETE /admin/v1/api-tokens/{id}

GET    /admin/v1/users
POST   /admin/v1/users
GET    /admin/v1/users/{id}
PATCH  /admin/v1/users/{id}
DELETE /admin/v1/users/{id}

GET    /admin/v1/mountpoints
POST   /admin/v1/mountpoints
GET    /admin/v1/mountpoints/{name}
PATCH  /admin/v1/mountpoints/{name}
DELETE /admin/v1/mountpoints/{name}

GET    /admin/v1/blacklist
POST   /admin/v1/blacklist
GET    /admin/v1/blacklist/{id}
PATCH  /admin/v1/blacklist/{id}
DELETE /admin/v1/blacklist/{id}

GET    /admin/v1/config
PUT    /admin/v1/config
POST   /admin/v1/config/apply
GET    /admin/v1/config/revisions
GET    /admin/v1/config/status
POST   /admin/v1/operations/preview
POST   /admin/v1/operations
GET    /admin/v1/operations/{operation_id}
POST   /admin/v1/operations/{operation_id}/cancel
GET    /admin/v1/versions
POST   /admin/v1/upgrades/preview
POST   /admin/v1/upgrades
GET    /admin/v1/upgrades/{operation_id}
POST   /admin/v1/upgrades/{operation_id}/cancel
POST   /admin/v1/upgrades/{operation_id}/rollback

GET    /admin/v1/sessions
DELETE /admin/v1/sessions/{id}
GET    /admin/v1/users/{id}/sessions

GET    /admin/v1/sources
GET    /admin/v1/sources/{mountpoint}
GET    /admin/v1/sources/{mountpoint}/health
DELETE /admin/v1/sources/{mountpoint}
GET    /admin/v1/source-cooldowns
DELETE /admin/v1/source-cooldowns/{fingerprint}
GET    /admin/v1/audit

GET    /admin/v1/relays
GET    /admin/v1/near
GET    /admin/v1/near/clients
GET    /admin/v1/cluster/status
POST   /admin/v1/config/reload

GET    /admin/v1/streams/{mountpoint}/summary
POST   /admin/v1/streams/{mountpoint}/monitor
DELETE /admin/v1/streams/{mountpoint}/monitor
GET    /admin/v1/ws
```

认证使用管理用户账号或独立 service token，要求 TLS。



* 管理人员的浏览器 session 和自动化 API Token 是两个入口。

* Caster 之间的 internal service token 只用于 relay/admin command，不接受用户 API Token，也不能用于管理 API 登录。

任意 core Caster 的 API 行为必须一致：



* 所有 core 使用同一个 `cluster_id`、etcd endpoints、TLS 和认证配置，没有 “主管理节点”。

* 请求落在哪个 core 就在该 core 完成认证和业务校验，然后直接写 etcd；不要求把写请求代理到某个 leader。

* 管理员登录时从 etcd 线性读取当前 User、角色、enabled 和 auth\_version，不能只依赖过期的内存快照。

* 浏览器 session 和 API Token 状态保存在共享 etcd，因此请求可以落到任意 core。

* 写 User、Mountpoint、Blacklist 时直接对独立 config key 做 CAS；成功响应包含新 `revision`。

* 写入成功后，本节点先用返回值和 revision 更新 Account DB / 内存快照，保证 read-your-write；其他 core 通过 Watch 收敛。

* etcd 不可写或 lease/config revision 不满足时返回 503，不能只在本地内存修改后声称成功。

管理 API 修改用户连接上限或黑名单时，应更新 Caster Config generation，而不是更新 User 记录中的连接数字段。

`GET /admin/v1/sessions` 返回连接时间、mountpoint、IP、NTRIP 版本和客户端 / 源站软件；`GET /admin/v1/sources` 返回轻量 route、准入摘要和当前有效 STR 字段。单源详情/health 根据 `caster_id` 回源 owner 获取完整 SourceProfile。

完整 `data_profile` 和实时健康值只存在 owner Caster，不写 etcd；列表接口不接受过期本地副本冒充实时数据。

`GET /admin/v1/sources/{mountpoint}/health` 由任意管理入口转发到 owner Caster 读取本地 health snapshot，返回最近有效帧、可用纠偏内容时间、bitrate、CRC 比例和 `STREAM_HEALTH_WARNING`，不产生 etcd 写。

源站列表至少支持以下筛选：



* `primary_format=rtcm3|rtcm2|binex|cmr|nmea|unknown`

* `validation_result=pass|warn|fail`

* `expected_format_mismatch=true`

* `msm_version=1..7`

* `caster_id` 和 `region`

API 语义：



* 所有更新接口使用 `ETag`/`If-Match` 或显式 revision，冲突返回 409。

* 列表接口支持分页、筛选和稳定排序。

* 错误响应统一为 `{code,message,details,request_id}`。

* 密码和 token 永远不回读；新 token 只在创建响应中显示一次。

* 写操作写入审计日志，包含操作者、来源 IP、改动前后摘要和 request ID。

* `DELETE /admin/v1/sources/{mountpoint}` 不直接删除 claim，而是写 owner command 并要求匹配 `connection_id`；返回 202。

* `DELETE /admin/v1/sessions/{id}` 写目标 `caster_id` 的 command key，携带 session\_id/runtime\_epoch；owner Caster 收到后关闭 socket 并删除 session；返回 202。

* 管理操作命令带短 lease，超时未执行时通过 source/session lease 自行清理。

* `GET /admin/v1/auth/csrf` 为已登录浏览器重新签发 / 读取当前 session 的 CSRF token；刷新页面后不依赖内存中的一次性 token。

CRUD 语义：

User：



* Create：`username` 唯一；要求初始密码；允许设置 role、enabled、ntrip\_enabled。

* Update：PATCH 单字段使用 User revision/ETag；密码变化递增 auth\_version。

* Delete：仍拥有 Mountpoint 时返回 409，除非使用已 preview 的迁移 / 级联 operation。

* Password reset：单独接口或 PATCH `password`，只接受新密码，不回读旧密码。

Mountpoint：



* Create：mountpoint 名称唯一；owner User 必须存在且 enabled。

* Update：owner、enabled、region、STR、source\_expectation 等分别 CAS；修改 owner 前检查 ACTIVE source。

* Delete：有 ACTIVE source 时默认 409；force 走 source command 和 operation。

* 修改名称不原地覆盖 key：创建新 Mountpoint、迁移 owner/STR，再下线旧 Mountpoint。

Blacklist：



* Create：校验 CIDR、quota、ID 唯一；同一规范化 CIDR 不允许重复 enabled 规则。

* Update：PATCH cidr/quota/enabled；使用 rule revision/ETag。

* Delete：删除后不再影响新连接；已有连接仅在 `blacklist_policy.disconnect_existing=true` 时由 owner 关闭。

* 所有 CRUD 成功后由 `config/blacklist/` Watch 同步到所有 Caster；不要求写整份 Caster Config。

### 12.2 Web 可视化管理

Web 管理端采用 “REST 快照 + WebSocket 增量”：



* 页面首次加载通过 REST 获取用户、挂载点、在线 session、源站、relay 和配置快照。

* WebSocket 推送连接变化、源站上线 / 下线、多协议数据摘要和日志事件。

* 浏览器使用 HttpOnly session Cookie，不把密码或 API token 放入 localStorage。

建议页面：



1. 登录页。

2. 总览：在线用户、客户端 session、active source、relay、etcd 状态。

3. 用户及其连接：连接时间、IP、mountpoint、NTRIP 版本、客户端软件。

4. 挂载点：owner、STR、active source、订阅数。

5. 基准站：Source-Agent、客户端软件、实际格式、期望格式、MSM 版本、设备 / 位置、bitrate 和完整性错误。

6. Relay：本地 mountpoint、upstream Caster、本地订阅数、空闲 / 失败状态。

7. NEAR：当前客户端、最近源、切换历史和定位精度。

8. 数据流实时监视：格式分布、消息频率、未知字节比例、MSM / 导航系统 / 载波；RTCM3 再显示卫星数、C/N0 和位置。

9. 配置：连接上限、黑名单、端口、STR 默认值、NEAR 和校验策略。

10. 审计日志：谁在何时修改了什么配置。

公网可视化页面：



* 顶部显示当前地域可用的公共 STR 表，支持搜索、分页和按格式筛选。

* 地图直接由公共 STR 快照绘制站点位置；`coverage_m` 来自 STR misc，缺失时使用默认半径。

* 地图只显示公开 STR 中的站点，低缩放级别做 marker clustering。

* 点击 STR 行或地图站点后，如果未登录先打开登录框；登录成功后再申请 monitor ticket 并打开同源 WebSocket。

* 当前 Web 管理节点负责建立 / 复用 Relay 和本地解析，不把解析压力转移到 source owner。

* 无可用解析槽时显示 “解析线程繁忙，请稍后重试”，展示 retry\_after 倒计时，不重复创建本地解析器。

* 页面关闭、切换挂载点或 WebSocket 断开时释放 viewer；最后一个 viewer 离开后默认立即关闭 MonitorSession，可按配置给予短暂 grace。

REST 提供可缓存的历史摘要；WebSocket 只发送变化和聚合数据，不发送原始差分数据。

WebSocket 消息带单调 `event_id` 和 `schema_version`。浏览器断线重连后先重新拉取 REST 快照；如果事件游标不连续，丢弃增量并使用新快照。慢浏览器只能丢弃自己的监视事件，不能阻塞服务端广播。

### 12.3 公共目录 API

为 Web/App 提供分页查询：



```
GET /api/v1/mountpoints?region=...&cursor=...
GET /api/v1/public/str
GET /api/v1/public/map/stations
POST /api/v1/auth/login
POST /api/v1/auth/logout
GET /api/v1/auth/session
POST /api/v1/monitors/{mountpoint}/ticket
GET /api/v1/monitors/{mountpoint}/ws
```

用于解决完整 STR 过大时的人类浏览和搜索，不替代 NTRIP STR。

公共接口匿名可读，但受下列约束：



* 同源 Origin 校验；监控 ticket 绑定 canonical IP、mountpoint、source generation 和短过期时间。

* 仅 STR / 地图匿名；monitor ticket、WebSocket 和卫星解析必须先完成 User 登录，要求 `enabled=true`、`web_enabled=true`。

* 公共 IP 黑名单、每 IP/User WebSocket 和解析会话限额同样生效。

* 地图数据只包含公开字段；RTCM 设备序列号、Source IP 和用户 owner 不返回。

* 监控 WebSocket 只发送 1 Hz 聚合快照，不发送原始 RTCM、RTCM2、BINEX、CMR 字节。

* 地图 API 支持 bbox/zoom 查询并返回 compact GeoJSON；大表优先使用 vector tiles 或按视口分页，不能一次把上万站点全部下载。

* 公共 STR / 地图响应带 ETag、gzip/br 和短 Cache-Control；ETag 由本地 snapshot revision 生成。

### 12.4 健康检查



```
GET /health/live
GET /health/ready
GET /metrics
```

ready 条件：



* 本地配置已加载。

* 集群模式 etcd lease 正常。

* 配置快照未过期。

* listener 已准备完成。

### 12.5 指标

至少包括：



* 当前源站数。

* 当前客户端 session 数。

* 每用户 session 数。

* 每挂载点订阅数。

* 每挂载点 relay 数。

* 差分记录数、坏 CRC/parity/checksum 数。

* 源站校验失败数、按原因分类的拒绝数和当前 cooldown 数。

* 每种数据格式、消息类型和 MSM 版本的接收计数。

* 按 `primary_format/profile_state/validation_result` 分类的源站数。

* `FORMAT_NOT_ALLOWED`、`EXPECTED_FORMAT_MISMATCH`、`NO_CORRECTION_DATA` 和未知字节比例告警数。

* 每个源站的有效观测数据年龄、数据陈旧告警数、station ID 变化数和位置跳变数。

* NEAR 的 GGA 丢弃数、位置跳变数和切换取消数。

* backlog 踢出数。

* etcd watch lag。

* etcd admission 延迟。

* lease 剩余时间。

* STR 行数和字节数。

* NEAR 查询延迟和重算次数。

* parser 队列深度、抽样丢弃数、每格式 framing 错误和 resync 次数。

* public monitor session 数、viewer 数、WebSocket 连接数、`MONITOR_BUSY` 数、启动限流数和空闲关闭数。

* relay STARTING 数、连接失败数、内部慢消费者断开数。

* etcd 条件 Txn 冲突数和 stale connection 拒绝数。

指标标签不能无限使用 mountpoint、username、session id 或 source IP；这些维度只保留在日志和按需管理 API。Prometheus 指标使用固定低基数 label，必要的 mountpoint 明细按配置开启并设置上限。

监控分层：



* 节点健康：listener、FD、CPU、RSS、redb、证书有效期。

* 集群健康：etcd quorum、lease 剩余、watch lag、config generation、relay 可达性、时钟偏差。

* 业务健康：source/client 数、配额使用率、relay 失败率、NEAR 无候选比例。

* 数据健康：有效帧率、观测 / SSR 更新时间、bitrate、CRC 比例、坐标缺失、MSM 变化。

告警分为 INFO、WARNING、CRITICAL；相同 `mountpoint + reason` 在抑制窗口内去重，恢复时发送 clear 事件。告警只读取本地 / 集中指标，不把每次告警写回 etcd。

最小告警目录：

| 告警 | 级别 | 触发 / 恢复 |
|---|---|---|
| etcd quorum 丢失 | CRITICAL | 连续 quorum 失败 / quorum 恢复且审核通过 |
| Caster lease 续约失败 | CRITICAL | keepalive 失败 / 新 lease 建立 |
| FenceGuard 在 deadline 后触发 | CRITICAL | 任一 `dispatch_after_deadline` / 人工确认 |
| stale source generation 被拒绝 | WARNING | 次数超阈值 / 窗口内归零 |
| Relay 重连风暴 | WARNING | STARTING 或重连率超阈值 / 稳定窗口 |
| Source 观测/SSR 陈旧 | WARNING | 超过 `observation_timeout_s` / 连续正常 `alert_clear_s` |
| Admission 拒绝率异常 | WARNING/CRITICAL | 按速率阈值 / 恢复基线 |
| Watch lag / compacted | WARNING/CRITICAL | 超阈值或 revision 断裂 / 快照恢复 |
| 内存、FD、backlog 接近上限 | WARNING | 超过 80%/95% / 回落到低水位 |
| TLS 证书即将过期 | WARNING/CRITICAL | 30/14/7 天 / 轮换完成 |
| NEAR 无可用候选源 | WARNING | 超 `no_source_timeout_s` 比例 / 候选恢复 |

每条生产告警必须有 owner、runbook、抑制窗口和 clear 条件。

### 12.6 审计与前端安全



* 配置、用户、挂载点、Token、强制断开等高价值写操作生成不可变审计事件。

* 审计事件包含时间、request\_id、操作者、来源 IP、action、target、revision、before/after 摘要和结果。

* 操作失败、部分成功、强制断开超时、配置 apply 被拒绝也必须审计，不能只记录成功。

* 审计日志写本地 append-only JSONL 或转发到外部 syslog/SIEM；etcd 只保存业务状态，不承担长期审计存储。

* 默认保留 90 天，可配置；日志滚动、压缩和磁盘水位告警必须实现。

* 管理前端设置 CSP、HSTS、`X-Content-Type-Options: nosniff`、`Referrer-Policy` 和严格 CORS。

* Cookie 固定 `Path=/admin`、`Secure`、`HttpOnly`、`SameSite=Strict`；WebSocket 只允许配置的 Origin。

* 登录成功/失败、锁定、密码恢复、Token 创建/撤销和高风险操作均写审计事件。

* 管理员密码恢复使用离线 recovery key 或受控 CLI，不在 Web 上提供无审计重置入口。

* WebSocket 重连必须重新验证 Cookie、Origin、auth_version 和当前 scope。

### 12.7 生命周期、升级和恢复



* SIGTERM 后先 readiness=false、停止 accept，再进入 drain；NTRIP 流连接给出关闭前的短暂 flush 窗口。

* 源站点由旧进程关闭后由源站自动重连到任意可用 Caster；旧进程不能在新 claim 建立后继续写 source key。

* 关闭超时后强制取消任务，释放 lease，并确认 source/session key 清理。

* 配置 generation 校验失败时继续运行旧 generation 并告警；不能把半有效配置应用到 listener。

* 端口、证书和本地 bind 变化通过滚动重启完成；管理 API 返回是否需要重启。

* TLS 证书、CA bundle 和 etcd client 证书支持 reload；reload 失败保留旧证书。

* standalone 定期备份 redb 和 Caster Config；集群定期备份 etcd snapshot，并记录恢复演练。

* schema 升级支持滚动发布：新进程先只读校验 N/N-1 schema，再允许写入；旧进程不能读取新 schema 时健康检查必须失败。

* 恢复 etcd 时只恢复 config，不恢复旧 online/source/claim runtime key；恢复后所有 Caster 重新建立在线状态。

* 所有节点必须运行 NTP/chrony；lease、审计和切换观察窗口依赖相对单调时钟，展示时间使用 UTC。

### 12.8 备份和灾难恢复



| 数据                            | 方式                      | 频率            | RPO     |
| ----------------------------- | ----------------------- | ------------- | ------- |
| etcd 用户 / 挂载点 / Caster Config | etcd snapshot           | 每 15 分钟和重大变更前 | ≤15 分钟  |
| source-generations 计数器        | 与 etcd snapshot 一起备份并校验 | 每 15 分钟和重大变更前 | ≤15 分钟  |
| standalone redb               | 在线一致性备份                 | 每小时           | ≤1 小时   |
| Caster Config generation      | 版本归档                    | 每次 apply      | 0       |
| Node config                   | 加密版本库                   | 每次变更          | 0       |
| TLS/etcd/mTLS 私钥              | 加密备份                    | 证书轮换后         | 0       |
| 审计日志                          | 外部追加存储 / SIEM           | 实时            | 受外部存储约束 |

目标：



* 配置 RPO 默认 15 分钟，RTO 默认 60 分钟；密码 Hash 与加密私钥必须可恢复。

* 恢复流程：先恢复 etcd quorum 和 TLS/etcd 身份，再恢复 config，启动 Caster，重新获取 lease，最后允许源站和客户端准入。

* 恢复旧 snapshot 后不得恢复旧 online/source/claim/admin-session runtime key，也不得把旧 generation 直接当作新配置发布。

* `source-generations` 若随 runtime 一起丢失，只能在确认所有旧 Caster 已退出 / 已 fencing 后再重新初始化；恢复期间必须阻断新 source claim。

* recovery 完成后先运行只读一致性检查：用户→owner→Mountpoint 引用、config schema、证书有效期、密码 Hash 参数。

* 恢复后源站重新连接，用户重新建立 NTRIP session；任何缓存中的旧在线状态都不能作为当前事实。

* 每月至少演练一次：etcd 单成员恢复、全 quorum 丢失恢复、redb 损坏恢复、TLS/etcd 私钥恢复、generation 不回退和全 Caster lease 重建。

* 演练必须记录实际 RTO、数据缺口、失败步骤和修复项；RPO/RTO 未实测前不能作为 SLA 承诺。

### 12.9 批量操作和长任务

高风险批量操作先 preview，再创建可恢复 operation：



```
{
  "operation_id": "op_01j...",
  "action": "DISABLE_USER",
  "state": "PREVIEW",
  "targets": 200,
  "affected_mountpoints": 73,
  "affected_sources": 41,
  "affected_sessions": 1832,
  "created_by": "admin",
  "request_id": "req_01j..."
}
```

状态机：



```
PREVIEW -> QUEUED -> RUNNING -> SUCCEEDED
                         |-> PARTIAL
                         |-> FAILED
                         |-> CANCELLED
```



* 删除 / 禁用 User、批量改 owner、批量关闭 Mountpoint、批量 drop session 必须先返回影响范围。

* 任务按 target 分片，记录每个 target 的成功、失败和 reason；失败项支持幂等重试。

* 长任务不能在单个管理 HTTP 请求内占住连接；API 返回 202 和 operation\_id。

* operation 状态与审计分离，operation 可以有过期清理，审计事件必须保留。

* 对涉及在线 source 的任务先发送 owner command，不能直接批量删除 etcd claim。

* operation 记录保存在 `runtime/operations/{id}`，绑定 coordinator lease；coordinator 崩溃后可经 CAS 接管，target command 使用 operation\_id 保证幂等。

* 创建 operation 需要目标资源的 write scope + `operations:write`；取消需要是创建者或 `cluster:admin`；查看遵循目标资源 read scope。

* Preview 返回的 `preview_revision`/target revision 集必须在执行时再次校验；目标 User、Mountpoint、Source generation 或配置 revision 变化后返回 409，并要求重新 preview。

* `cluster:admin` 可以接管 / 取消其他管理员 operation，但必须写审计事件；普通管理员不能取消其他管理员的 operation。

### 12.10 Source 历史事件（P2 预留）

第一版保留事件接口，不把长期历史存放在 etcd：



```
{
  "event_id": "evt_01j...",
  "mountpoint": "C63",
  "event": "SOURCE_REJECTED",
  "caster_id": "caster-west-02",
  "source_generation": 1002,
  "connection_id": "c_01j...",
  "reason": "NO_OBSERVATION_OR_SSR",
  "timestamp": "2026-11-20T00:00:00Z"
}
```

事件写本地 append-only event log 或外部日志系统，用于上线、拒绝、断开、owner 切换、位置变化和异常恢复；不影响实时数据路径。

### 12.11 自动版本升级和滚动发布

升级必须由管理 API 创建 rollout operation，但 API 进程不能直接覆盖正在运行的自身二进制。每个节点使用独立 updater/supervisor 执行本地切换。

版本共享：



```
runtime/version-status/{caster_id}
```

由 Caster lease 维持，包含：



* software version、Git commit、build ID。

* OS/arch、config schema、etcd schema、API schema、NTRIP feature set。

* `runtime_epoch`、started\_at、health、maintenance 状态。

这是当前运行节点的短暂状态，不是持久 node 表。

升级 API：



```
GET  /admin/v1/versions
POST /admin/v1/upgrades/preview
POST /admin/v1/upgrades
GET  /admin/v1/upgrades/{operation_id}
POST /admin/v1/upgrades/{operation_id}/cancel
POST /admin/v1/upgrades/{operation_id}/rollback
```

Preflight：



1. 获取 `locks/upgrade/global`，同一集群只能有一个 rollout。

2. 校验目标 artifact manifest 的 Ed25519 签名、SHA-256、版本、OS/arch 和 schema 兼容性。

3. 校验 etcd quorum、当前活跃 Caster、容量、源站数量和磁盘空间。

4. 默认要求 `max_unavailable_core=1`、`max_unavailable_edge=1`。

5. 目标版本和当前版本必须支持 N/N-1 etcd/API schema；不兼容时禁止自动滚动。

6. 先创建 operation 和 preview；目标版本、节点列表或配置 revision 变化后必须重新 preview。

Rollout 顺序：



1. 默认先 edge 波次、后 core 波次；每波只允许一个节点。

2. 写 `runtime/upgrade-commands/{operation_id}/{caster_id}`，只由本地 updater 消费。

3. 节点进入 maintenance/drain，停止新连接，关闭 source/relay 并释放 lease。

4. updater 下载到 staging，校验签名和 Hash，切换 A/B slot，重启 service。

5. 新进程使用新 `runtime_epoch`，完成 config/source 全量校准并 Watch。

6. health/readiness/listener/relay/etcd/config schema 检查通过，写 `runtime/upgrade-states/{operation}/{caster}`。

7. 等待 `post_upgrade_stability_s`，再次检查 quorum、错误率、容量和版本。

8. 通过后升级下一节点。

失败规则：



* 任一节点升级或稳定性窗口失败，operation 进入 PAUSED/FAILED，不能继续下一节点。

* `auto_rollback=true` 时本地 updater 切回上一 A/B slot 并重启；回滚也走相同健康检查。

* 第一次失败后必须人工确认或新 preview 才能继续。

* coordinator core 自己升级时，operation 协调权通过 lease/CAS 交给另一 core，升级完成后可重新接管。

* etcd 本身不与 Caster 二进制同时自动升级；etcd 升级单独执行并逐 member 检查 quorum。

安全要求：



* 管理 API 需要 `upgrade:write`，高风险升级建议二次确认或双人审批。

* artifact 只从签名 manifest 指定的版本仓库下载，不接受任意 URL 或未签名二进制。

* updater 使用最小权限服务账号，不能修改 etcd 配置。

* 保留至少一个上一版本 slot，直到稳定性窗口结束。

* 所有版本、节点、失败原因、回滚和操作者写审计。

### 12.12 外部 DNS/LB 健康接入



* DNS/LB 不直接读取 etcd；通过每个节点的 `/health/ready`、NTRIP TCP 探活和 relay 可达性判断健康。

* `maintenance/draining` 开始时立即返回 readiness=false，让 DNS/LB 尽快停止分配新连接。

* `caster.example.com` 只收录健康 core，并返回一致的 global STR scope。

* `src-{region}.example.com` 只收录该 region 的健康 source 入口，可使用 GeoDNS、加权或区域 VIP。

* 节点故障、etcd lease 失效、配置快照过期或 relay endpoint 不可达时，ready=false。

* DNS TTL 建议 30–60 秒；L4 健康检查间隔短于 TTL，但已有 TCP 连接仍需要应用重连。

* DNS 记录和权重由外部运维系统管理，Caster 不自行修改公网 DNS。



***

## 13. 故障处理



| 故障                                      | 行为                                                              |
| --------------------------------------- | --------------------------------------------------------------- |
| 源站主动断开                                  | owner 删除 source key，关闭本地订阅者和 relay                              |
| 源站数据中断但 TCP 半开                          | `ntrip.source_read_timeout_s` 到期后关闭                             |
| 源站 Caster 崩溃                            | lease 到期，source/session key 清理                                  |
| relay 上游断开                              | 本地下游立即关闭                                                        |
| relay endpoint 不可达                      | 返回 503，不创建假在线状态                                                 |
| 源站 owner 改变                             | relay 关闭，旧客户端断开，新连接按新 route                                     |
| Caster 与 etcd 分区                        | lease 失效前停止源站和 relay，safe 模式抑制新连接                               |
| etcd member 单点故障                        | quorum 保留时继续                                                    |
| etcd quorum 丢失                          | 停止新认证、新源站和 relay，进入保护模式                                         |
| 用户达到 effective max\_sessions            | 拒绝新 session，不影响已有 session                                       |
| 用户被禁用                                   | 关闭源站和客户端数据流，拒绝新连接                                               |
| 客户端认证开关切换                               | 新连接按新规则；已有数据流按策略保留或关闭                                           |
| 源站只发 NMEA，没有纠偏数据                        | `enforce` 拒绝并写 cooldown；`observe` 标记 `NO_CORRECTION_DATA` 并继续在线 |
| 实际格式与 Mountpoint 期望不符                   | `enforce` 拒绝；`observe` 标记 `EXPECTED_FORMAT_MISMATCH`            |
| RTCM3 中出现未允许的 MSM 版本                    | 只有 `msm.restrict_versions=true` 时参与判定；`enforce` 踢出，`observe` 标记 |
| CRC/parity/checksum 错误超过窗口阈值            | 准入窗口内按策略标记或拒绝；ACTIVE 后坏记录只丢弃并计数，不因内容阈值再次断线                      |
| TCP 连续 `source_read_timeout_s` 没有收到任何数据 | 关闭源站，清理 claim/runtime source，写 cooldown                         |
| 源站持续发送未知协议                              | `observe` 标记 `UNKNOWN/UNRECOGNIZED_DATA`；`enforce` 超时后拒绝        |



***

## 14. 安全和密码



* 用户密码使用 Argon2id。

* NTRIP1 上传密码使用 Argon2id。

* 客户端认证开启时，要求 TLS。

* 源站认证包含密码，默认要求 TLS。

* etcd 使用 mTLS。

* Caster 间 relay 使用内部 mTLS 和 service token。

* 管理 API 必须使用 TLS。

* 日志不得记录密码、Authorization 原文和完整 XFF 链。

* 认证失败按 IP 限速。

* Argon2 校验设置并发上限，避免认证 DoS。

* Argon2 参数按 2C2G/4C4G 实测标定；认证队列满时快速拒绝，不能让 Tokio worker 排队等待。

* 登录成功后检测旧 hash 参数，必要时异步 rehash 并递增记录 revision。

* Basic 认证中的密码只在当前连接解析阶段短暂保留，不进入 session、etcd、日志或指标。

* 黑名单和连接配额对外部 IP 生效。

* 源站上传不参与 IP 黑名单和 IP quota，但仍受节点 `max_sources`、`max_connections` 和单源 bitrate 限制。

* XFF 只在可信代理来源下采信。

* etcd 定期做加密 snapshot 备份；恢复时只恢复配置，不恢复旧 online/runtime 状态。

* 配置 schema 升级必须支持滚动发布，并在发布前执行只读兼容检查。

* internal mTLS 证书、service token 和 etcd client 凭据都支持轮换；旧凭据在明确宽限期后失效。

### 14.1 数据保护和隐私



* Source IP、Client IP、User-Agent 和 Admin 登录 IP 默认保留 30 天；审计日志默认 90 天，均可配置但不能无限保留。

* 用户删除后，账号和凭据从主存储删除；审计中的 operator/user\_id 采用不可逆或受限引用，避免保留密码和完整认证材料。

* 1033 receiver、firmware、antenna serial 只对 `sources:read` 高权限管理员可见；普通用户和公共 STR 不显示序列号。

* Argon2 参数版本、内存成本、迭代次数和并行度写入 User credential metadata，登录成功时按策略 rehash。

* API Token 保存 creator、created\_at、expires\_at；last\_used\_at 先记本地指标，最多每小时回写一次，避免每个管理请求写 etcd。Token 明文只显示一次，Token hash 的 HMAC 密钥支持轮换。

* 数据导出接口要求独立 scope，并写入审计；批量导出不能通过普通列表接口隐式完成。



***

## 15. 容量和性能

### 15.1 设计预算

单节点资源不是按 “软件理论上无限” 配置，而是按传统 NTRIP backlog 和保守内存预算设置硬上限。10 万或 1 万规模通过多 Caster 分担，不要求单机承载全部连接。



| 资源                           | 2 vCPU / 2 GiB 档 | 4 vCPU / 4 GiB 档 |
| ---------------------------- | ---------------- | ---------------- |
| `max_connections`            | 3000             | 8000             |
| `max_sources`                | 500              | 1500             |
| `max_subscribers`            | 2500             | 7000             |
| `max_relays`                 | 100              | 300              |
| internal relay inbound       | 500              | 1500             |
| `max_subscribers_per_source` | 2000             | 5000             |
| `max_validating_sources`     | 64               | 128              |
| subscriber backlog           | 16 KiB / 连接      | 16 KiB / 连接      |
| socket send buffer           | 112 KiB / 连接     | 112 KiB / 连接     |
| parser queue                 | 32 MiB / 节点      | 64 MiB / 节点      |
| validation buffer            | 64 MiB / 节点      | 128 MiB / 节点     |
| `fd_budget`                  | 32768            | 65535            |

这些数字是**初始配置目标和开发验收门槛，不是已验证生产容量承诺**；必须在目标硬件、内核参数、TLS 配置和实际 fan-out 模式下压测后形成部署规格。socket buffer 的潜在内存还必须同时计入内核 TCP 内存，而不只是进程 RSS。

配置值是初始上限，必须通过压测确认：



```
queued_data ≈ active_subscribers × subscriber_backlog_bytes
active_memory ≈ connections × per_connection_overhead
              + queued_data
              + validation_buffer_budget
              + parser_queue_budget
              + STR/NEAR snapshots
```

2 GiB 档的排队数据预算不超过约 40–80 MiB，4 GiB 档不超过约 120–200 MiB；剩余空间留给 Tokio、TLS、redb、内核 socket、帧对象和突发流量。



* 超过 `max_connections` 时在 accept 后立即拒绝，不能先分配大缓冲再判断。

* 超过 `max_sources` 时拒绝新源，但已在线源不受影响。

* parser 队列超过预算时丢弃分析样本，不阻塞数据面。

* `RLIMIT_NOFILE` 至少为 `fd_budget`；systemd 服务设置 `LimitNOFILE`。

* `max_source_bitrate_bps=2000000` 是单源保护值，超过后按配置选择标记或断开；普通 RTCM3/MSM4 流量不应接近该值。

压测必须同时变化连接数和数据速率：



| 维度         | 档位                                    |
| ---------- | ------------------------------------- |
| 连接         | 100、1000、3000、8000                    |
| 单源 bitrate | 2、10、100、1000 KiB/s                   |
| Fan-out    | 1、100、500、2000、5000 subscribers       |
| Relay      | 10、100、300 external + 500 internal    |
| 慢客户端       | 正常、延迟 1s、延迟 30s、持续阻塞                  |
| NEAR       | 1、10、100 Hz GGA 输入，100/10k candidates |
| STR        | 5k、10k、20k lines                      |
| TLS        | 握手、断连、重连风暴                            |

每个场景记录 CPU、RSS、FD、网络收发、p50/p95/p99 admission/relay/fan-out 延迟、丢帧率、慢客户端断开率和资源恢复时间。不能只报告 “支持多少连接”。

### 15.2 Etcd 写入



* 客户端 /source 连接只在建立和断开时写 session/source 元数据。

* 差分数据包、帧计数、bitrate、解析队列和 backlog 不写 etcd。

* source route/STR 摘要只在准入完成、进入 ACTIVE 时更新一次；完整 SourceProfile 和实时统计留在 owner 本地，不写 etcd。

* relay 使用内部 NTRIP，不创建用户 session。

* 每次用户准入需要锁、读取和事务写入。

启动实施前必须做 etcd spike：



* 100、500、1000 admissions/s，且稳态不再产生周期 source 写入。

* 单用户并发抢最后一个名额。

* 大 NAT IP 并发。

* 1 万 session 键。

* leader 切换。

* WAL fsync 和 admission p99。

补充故障/竞争场景：

* 单用户 100 个并发 Caster 争抢最后一个 session。
* Source Claim、普通 Client 和 NEAR 同时争抢同一用户额度。
* etcd RTT 20/50/100/250 ms。
* Lease 剩余时间小于一次 admission 时提交 Txn。
* Watch lag + Compaction + 持续配置变更并发。
* 1k/5k/10k session 前缀下的锁等待、线性读和 Txn 冲突率。

如果无法满足，fallback 顺序固定为：

1. 保留当前锁方案的测试基线，记录每类准入的 etcd 往返数、锁等待、Txn 冲突和 p99。
2. 首先设计“per-user admission record + revision CAS”原型：每个用户一个轻量 key 保存有界 session 引用集合，CAS 成功后写 session；session 集合版本用 revision 替代两把锁。该 key 只是同一用户的串行化入口，不是独立 count key。
3. 再做按用户分片或独立 admission service 的概要设计。

不要等实现完成后才发现准入吞吐不足。Phase 0 报告必须明确采用 A（当前锁方案）、B（per-user admission record）还是 C（独立 admission service），并给出故障恢复和 lease 清理证明。

### 15.3 内存



* 共享差分数据正文使用 `Bytes`。

* 每客户端 backlog 上限可配置。

* 不得为每个订阅者复制整个数据包。

* 每连接缓冲按需增长。

* 压测时记录 RSS、task 数、socket 数和每连接队列长度。

* 2 GiB 档建议 RSS 硬预算 1.5 GiB，4 GiB 档建议 3 GiB；超过预算只拒绝新连接，不主动丢弃已有正常客户端。

* `bytes`/`mimalloc` 的对象池和 allocator 只在压测证明有收益后启用。

### 15.4 STR 和 NEAR



* STR 生成与连接处理分离。

* watch 更新合并后构建新快照。

* NEAR 索引使用不可变快照。

* 避免每个 GGA 重建全局索引。

* 对 5k、10k、20k mountpoint 做基准测试。

* NEAR 每连接最多每秒执行一次空间查询；大量客户端同时上报 GGA 时使用独立计算队列或分片，不阻塞源站 fan-out。



***

## 16. Rust 实现结构

### 16.1 Workspace



```
crates/
  rcaster-core/
    protocol/
    auth/
    config/
    domain/
    rtcm/
    sourcetable/
    geo/

  rcaster-service/
    app/
    cluster/
    storage/
    session/
    hub/
    relay/
    upstream/
    admin/
    observability/

  rcaster-rtklib/         # optional
    sys/
    adapter/

apps/
  rcaster/
  rcaster-admin-web/
```

### 16.2 核心模块



* `protocol`：NTRIP 请求解析、chunked、HTTP 响应。

* `auth`：用户、owner、NTRIP1/2 上传认证。

* `domain`：User、Mountpoint、Session、Source、Relay 状态。

* `data`：ProtocolDemux、RTCM3/RTCM2/BINEX/CMR/NMEA framing、profile、过滤、转换。

* `sourcetable`：STR 生成和解析。

* `geo`：GGA 和空间索引。

* `cluster`：etcd watch、lease、锁、Txn、fencing。

* `storage`：本地 KV 数据库。

* `session`：TCP/TLS 会话和认证。

* `hub`：本地源和 fan-out。

* `relay`：自动跨 Caster 拉流。

* `admin`：管理 API。

* `rcaster-rtklib::sys`：可选受控 C ABI。

* `rcaster-rtklib::adapter`：可选安全 Rust wrapper、worker pool 和解析结果 DTO。

* `rcaster-admin-web`：不含密码和 API token 的浏览器前端。

### 16.3 并发模型



* 每个连接一个 Tokio task。

* 会话状态归 task 私有。

* 跨 task 共享配置和 hub。

* 每订阅者独立 writer task。

* 锁内不 `await`。

* 不在持锁期间写 socket。

* 关闭由 cancellation token 和 supervisor 统一协调。

### 16.4 推荐依赖



* `tokio`

* `bytes`

* `httparse`

* `tokio-rustls`、`rustls`

* `serde`、`serde_json`、`serde_yaml_ng`

* `rtcm-rs 0.11.0`，RTCM3 语义解码，使用 `default-features=false,features=["std","all_msgs"]`

* BINEX 第一版不引入语义解析库，只实现有界 framing、record 边界和 subtype 识别；`binex 0.5.2` 仅作为离线 fixture 对照工具

* `nmea 0.8.0`，Apache-2.0、MSRV 1.87；解析 GGA 和校验 NMEA checksum

* `redb`

* `etcd-client`

* `arc-swap`

* `parking_lot`

* `dashmap`

* `argon2`

* `hmac`、`sha2`、`rand`、`subtle`

* `kiddo`，首选 KD-tree；`rstar` 作为替代

* `backon`，退避与重试

* `tracing`

* `tokio-util`，CancellationToken 和编解码辅助

* `clap`

* `axum`/`axum::extract::ws`、`axum-extra[cookie]`、`tower-http`，只用于管理 API 和 WebSocket

* `tower_governor`，管理登录和 API 限速

* `utoipa`，可选 OpenAPI 生成

* RTKLIB adapter 使用 `cc` 或 `bindgen` 构建，只暴露稳定 C ABI

### 16.5 功能与现成库映射

以下为截至 2026-11 查询 crates.io 后得到的优先选择：



| 功能                | 优先方案                                         | 可替代 / 结论                                                            |
| ----------------- | -------------------------------------------- | ------------------------------------------------------------------- |
| 异步网络运行时           | `tokio`                                      | 直接采用，不自行实现 event loop                                               |
| TCP/TLS           | `tokio-rustls`                               | 设备兼容失败再增加 `tokio-openssl` feature                                   |
| 管理 HTTP/WebSocket | `axum` + `axum-extra[cookie]` + `tower-http` | 只用于管理 API，不用于 NTRIP 入站                                              |
| NTRIP 入站请求        | `httparse` + 自研状态机                           | 没有成熟 “通用 NTRIP caster 框架” 可直接替代                                     |
| 标准 HTTP 出站        | `reqwest`                                    | 只用于标准 GET/POST；NTRIP 流仍用自研客户端                                       |
| etcd v3           | `etcd-client`                                | 已验证支持 Txn、Watch、Lease、Lock 和 TLS                                    |
| 本地 KV 数据库         | `redb`                                       | 单文件、事务型、无 SQL 表模型                                                   |
| YAML              | `serde_yaml_ng`                              | `serde_yaml` 已不再维护，不选                                               |
| 密码                | `argon2`                                     | RSA/API token 不要用 Argon2，Token 高熵值用 HMAC/SHA-256                    |
| API 限流            | `tower_governor`                             | 管理登录和接口限流；NTRIP IP 配额仍用业务实现                                         |
| RTCM v3 解码        | `rtcm-rs`                                    | **首选**；支持 RTCM v3.4 全消息集，包含 1005/1006、1033、1071-1137 MSM，MIT/Apache |
| RTCM2 识别          | 自研小型 framer/parser                           | 未找到可信的通用 Rust crate；实现消息号、长度和 parity 最小集，真实样本覆盖                     |
| BINEX 识别          | 自研 framing/identifier                        | 第一版只确认格式和 record 边界；`binex 0.5.2` 只用于离线对照，不进入生产依赖                   |
| CMR/CMR+ 识别       | 自研 identifier                                | 未找到可信的通用 Rust crate；必须以实际设备抓包建立格式识别 fixture，不做字段解析                  |
| NMEA/GGA          | `nmea`                                       | 解析语句和 checksum；NEAR 的 f32 距离保持自研                                    |
| 多协议深度兼容           | 可选 RTKLIB                                    | 只对经 capability probe 证明支持的 RTCM2/BINEX 等格式启用；不能默认宣称支持 CMR           |
| ECEF / 经纬度转换      | `geoconv`                                    | 1005/1006 坐标转换；与 RTKLIB fixture 对比                                  |
| 最近基站索引            | `kiddo`                                      | 首选固定快照 KD-tree；`rstar` 可替代                                          |
| 不可变配置切换           | `arc-swap`                                   | 配置和 STR/NEAR 快照                                                     |
| 并发哈希表             | `dashmap`                                    | mount/session/monitor 索引；不在热锁内 await                                |
| 取消和任务             | `tokio-util`                                 | `CancellationToken`、structured shutdown                             |
| 重试 / 退避           | `backon`                                     | 出站连接和 Graylog 等；源站 cooldown 仍写 etcd                                 |
| JSON              | `serde` + `serde_json`                       | API、etcd、redb 记录                                                    |
| 日志 / 指标           | `tracing` + `metrics`/Prometheus exporter    | GELF/Graylog 仍需自研 sink                                              |
| 管理 API 规范         | 可选 `utoipa`                                  | 从 Axum handler 生成 OpenAPI                                           |
| 前端图表              | TypeScript + uPlot/Chart.js + Leaflet        | 比 Rust WASM 前端更省工作量                                                 |

多协议快速判定策略：



* 所有连续字节流由 Rust `ProtocolDemux` 做 framing；RTCM3 独立提取 12-bit type，RTCM2 提取消息号，BINEX 提取 record ID，CMR 提取消息 ID。

* `rtcm-rs` 解析完整 RTCM3 frame，输出 1005/1006、1033 和 MSM 等结构化字段。

* BINEX/RTCM2/CMR 的第一目标是可靠识别格式和消息号，并透明转发；深度字段按现场需要逐步补齐。

* RTKLIB 不存在时，格式、完整性、消息号、MSM 版本和基础在线信息仍然可用。

* 只有 capability probe 和 fixture 证明 RTKLIB 能处理某格式时，才把 RTKLIB 设为该格式的 fallback。

* 解码器之间只对同一 fixture 做差分测试，不在生产流量上重复解析；RTKLIB 崩溃不能影响 framing 或 fan-out。

Rust workspace 的 MSRV 暂定为 1.87，由当前 `nmea 0.8.0` 决定；升级依赖前单独复核 MSRV。

不建议使用的现成方案：



* 现成 NTRIP client crate 不能直接作为 caster，因为服务端需要多个源站、订阅者、配额、STR 和 relay。

* Rust 生态存在 `ntrip-core`、`ntrip-client` 等客户端库，可用于对照或独立诊断工具，但不应把它们当作集群 caster 内核。

* Hyper/Axum 作为入站 NTRIP 数据流框架会产生错误的 body、chunk 和 keep-alive 假设。

* Python/pyrtcm 不适合高频生产热路径，只适合作为参考或离线 fixture 工具。



***

## 17. 实施阶段

### Phase 0：需求冻结和协议实验（2–3 周）



* 确认 NTRIP1/2 实际设备报文。

* 按 NTRIP1/2 和 Millipede 行为冻结请求、响应、错误和 GGA 双向通信 fixture。

* 确认认证矩阵。

* 确认 TLS 客户端兼容性。

* 收集 RTCM3、RTCM2、BINEX、CMR/CMR+、NMEA-only 和混合流的真实抓包，建立脱敏 fixture 集。

* 验证 BINEX/RTCM2/CMR 的公开规范、设备差异、校验规则和可用库许可证。

* 确定每种格式 “可识别、可语义解析、仅透明转发” 的能力边界。

* 完成 etcd admission 规模 spike，覆盖 100/500/1000 admissions/s、并发锁竞争、p99 和故障恢复。

* 完成 RTKLIB 隔离 spike：可选 worker 崩溃、重启、输入上限和 capability probe。

* 固定 admission fallback 选择：当前锁方案 / per-user admission record / 独立 admission service，三者必须选一个。

### Phase 1：领域核心（3–5 周）



* User、Mountpoint、Blacklist、Session、Source 模型。

* NTRIP 请求解析和 chunked。

* NTRIP1/2 请求矩阵、严格 Header 限制、`/SOURCETABLE` 和固定响应 fixture。

* ProtocolDemux、RTCM3/RTCM2/BINEX/CMR/NMEA framing 和位置缓存。

* `rtcm-rs` 全消息兼容性评估和 fixture 测试。

* BINEX/RTCM2/CMR 最小识别器和错误样本 /fuzz 测试。

* 可选 RTKLIB capability probe、C ABI spike 和 fuzz 测试。

* STR parser/generator。

* GGA 和距离函数。

### Phase 2：Standalone Caster（4–6 周）



* TCP/IPv6/TLS listener。

* NTRIP1/2 源站上传。

* 客户端订阅。

* 本地 KV 数据库。

* client auth 开关。

* 本地 fan-out 和慢客户端剔除。

* User-Agent/Source-Agent、准入 profile、1033 和一次性在线信息写入。

* 源站格式 / RTCM3 观测值或 SSR/MSM/ 完整性准入校验、`observe/enforce` 和本地退避。

* `DataProfileBuilder`、可选 decoder 抽象和纯 Rust 解析结果 DTO。

* 资源档位、连接 / 源站 /relay 上限、字节 backlog 和 2C2G/4C4G 压测入口。

### Phase 3：etcd 集群控制面（4–6 周）



* etcd watch 和本地 KV 快照。

* Caster lease 和自 fencing。

* 全局用户配额。

* IP 配额。

* source claim。

* source-claims fencing、admin command 和旧 connection\_id 清理。

* 在线 session/source。

* 集群共享 source cooldown。

* 配置 CRUD。

* Caster Config generation、自动下发和统一端口。

* 新 Caster 自动同步连接上限与 IP 黑名单。

### Phase 4：自动 Relay、STR 和 NEAR（4–6 周）



* source relay endpoint。

* 自动 relay registry。

* relay 故障和空闲关闭。

* global/regional/auto STR。

* 本地空间索引。

* NEAR 自动切换。

* NEAR 每连接 GGA 状态、3 秒观察窗口、4 米移动阈值和只允许 RTCM3。

### Phase 5：管理、安全和运维（4–6 周）



* 新管理 API。

* 管理登录、HttpOnly session、CSRF 和 API Token scope。

* 节点本地配置、证书 reload、PROXY Protocol、审计和滚动升级。

* WebSocket 与远程 mountpoint Stream Monitor。

* Web 总览、用户 / 挂载点 / 源站 /relay/NEAR/ 多协议数据流页面。

* 审计日志。

* TLS、mTLS、限速。

* metrics、health、ready。

* 配置导入工具。

### Phase 6：容量和故障验收（4–8 周）



* 3 etcd + 多 Caster 集群。

* 1 万连接和 1 万挂载点压测。

* etcd 分区和恢复。

* Caster 崩溃。

* relay 上游断开。

* 错误模块上传、格式切换、配置从 observe 热切到 enforce、cooldown 防重连风暴。

* 源站准入后不再周期复查；NEAR 高并发 GGA 和大规模 RTCM3 candidate 查询。

* STR 和 NEAR 索引规模测试。

25–40 人周只估算核心编码和基础验收，不包含真实设备兼容、商业客户端测试、管理前端打磨、跨地域部署、灾备演练和长期稳定性测试。完整生产化更合理的量级是 60–100 人周，必须在 Phase 0 后重新估算。

实施按正式 Gate 推进，不通过当前 Gate 不进入下一阶段：

| Gate | 必须可运行 / 可验证 |
|---|---|
| Gate 1：一致性原型 | Source Fencing、用户准入并发、配置 revision、etcd admission spike |
| Gate 2：单机数据面 | NTRIP1/2、RTCM3、本地 fan-out、慢客户端隔离、真实设备上传/订阅、基础 STR |
| Gate 3：集群数据面 | etcd lease/watch、claim、relay、跨 Caster 订阅、故障恢复 |
| Gate 4：完整功能 | 认证/黑名单/配额、NEAR、管理 API、Web monitor、升级和审计 |
| Gate 5：生产验收 | 压测、故障注入、灾备、真实设备兼容、72h soak 和告警演练 |

不能先写完所有协议模型、再写所有存储、最后才接通第一条 NTRIP 数据流。



***

## 18. 验收标准

### 18.1 功能



* NTRIP1 源站只通过 mountpoint 上传密码。

* NTRIP2 源站只通过 owner 用户名和用户密码。

* 客户端认证开启时只校验用户账号。

* 客户端不检查 mountpoint owner。

* 在合作节点故障模型下，一个 mountpoint 在任一时刻最多一个获得 claim 的 ACTIVE source；lease 到期后旧 owner 不再投递新的差分帧。

* 客户端能从任意 Caster 自动访问远端源。

* 每个 Caster 每个 mountpoint 最多一条 relay。

* relay 无本地客户端时自动关闭。

* relay 上游断开时下游立即关闭。

* NEAR 开关开启后公共 STR 出现一条 `near.mountpoint` 配置名称，关闭后消失。

* 修改 `near.mountpoint` 后，所有 Caster 使用新名称。

* NTRIP1/NTRIP2 均不能向 `near.mountpoint` 上传。

* NEAR 客户端可从 GGA 选择本机或远端最近源，并复用已有 relay。

* NEAR 基站切换保持客户端连接，不增加用户连接数。

* HDOP 超限、fix quality 无效、迟滞距离和最小重算间隔可阻止错误切换。

* 在线源站信息可由管理 API 查看 User-Agent、Source-Agent、NTRIP 版本和请求方法。

* NTRIP1 `SOURCE` 和 NTRIP2 `POST` 都记录来源软件信息，并进入相同多协议识别和 profile 流程。

* RTCM2、CMR/CMR+、BINEX、NMEA 和混合流都能被识别并透明转发，或至少在在线信息中标记 `UNKNOWN`。

* 管理 API 可以看到实际格式、消息 / 记录类型、MSM 版本、位置可用性、bitrate、校验错误和 `validation_result`。

* 收到 RTCM3 1033 后可以看到 receiver、firmware 和 antenna；非 RTCM3 格式明确显示设备字段不可用。

* `observe` 模式下 NMEA-only 源保持在线并标记 `NO_CORRECTION_DATA`；`enforce` 模式下被拒绝。

* `observe` 模式下实际格式不符合 `source_expectation` 时只标记；`enforce` 模式下拒绝。

* `msm.restrict_versions=false` 时 MSM4/7 都放行并记录；设为 `true` 后由 `mode` 决定标记或踢出。

* `enforce` 模式下仅允许 MSM4 时，出现 MSM7 会被拒绝并写入共享 cooldown。

* RTCM3 只包含 1005/1006/1033、没有观测值或 SSR 时，`enforce` 拒绝并标记 `NO_OBSERVATION_OR_SSR`。

* RTCM2、BINEX、CMR 只完成格式识别，不进行观测值字段解析。

* 配置从 `observe` 热切到 `enforce` 后，现有 ACTIVE 源不重新评估；新源按 `enforce` 准入。

* 源站在准入通过后不再按 MSM / 格式 / 纠偏内容周期复查。

* NEAR 只选择 RTCM3 候选，不选择 RTCM2、BINEX、CMR 或 NMEA-only 源。

* NEAR 客户端的 GGA 上行与下行流并行工作，非 GGA 输入被丢弃。

* NEAR 切换前观察 3 秒，位置移动不超过 4 米才允许切换。

* 每个 NEAR 连接独立保存当前源和 GGA 状态；底层同一源可以复用 fan-out/relay。

* 管理员浏览器登录使用 HttpOnly session，所有写请求校验 CSRF。

* API Token 按 scope 授权并可单独撤销。

* 管理端选择远端挂载点时，自动通过本地 relay 获取数据并显示多协议摘要。

* WebSocket 断开后 monitor 释放，relay 按 idle 规则关闭。

* 启用可选 RTKLIB worker 时，其崩溃不会中断 NTRIP fan-out，worker 可自动重启并恢复解析。

### 18.2 一致性



* 多个 Caster 同时抢同一用户最后一个 session，只能成功一个。

* 同一用户最后一个连接额度被 Client 和 Source 同时争抢时，也只能成功一个。

* 多个 Caster 同时上传同一 mountpoint，只能成功一个。

* 新 Caster 加入后自动获得相同连接上限、黑名单和端口配置。

* 将用户有效连接上限从 3 修改为 5 后，所有 Caster 都按新值处理新连接。

* 当前连接数来自 etcd session 汇总，不来自配置文件或固定 count 字段。

* 多台 Caster 使用相同的 NTRIP、TLS、relay、管理和 metrics 端口配置。

* generation 重复、回退或 schema 不兼容的配置 apply 必须失败，不能覆盖当前 generation。

* Caster 崩溃后 lease 到期清理 session/source。

* etcd 分区时旧源站先 fencing，再允许其他 Caster 接管。

* on-demand source 不会跨 Caster 重复拉取。

* 旧 source 任务恢复后不能覆盖或删除新 `connection_id` 的 claim/source。

* 旧 source\_generation 发起的 relay、publish 或 admin command 必须被拒绝。

* `stale_generation_rejected_total` 可观测，`dispatch_after_deadline_total` 必须为 0。

* 旧 generation 的 Relay 在两个 Watch/心跳周期内关闭，且不继续向下游发送新帧。

* Relay 收到过期 mountpoint route 时，owner 必须拒绝，edge 必须清理本地 relay 和下游，不能继续使用旧缓存。

* 源站和用户 session 只在建立 / 断开时写 etcd，稳态没有周期 source/session 写入。

* 客户端正常退出时 user session 和 IP client-session 在同一 Txn 删除；etcd 清理失败则由 lease 在 TTL 内回收。

* Source 正常退出只删除匹配 generation/connection\_id 的 claim/source/session，不影响新源。

* NEAR 仅在完成一次实际基站切换时事件式更新 `near_current_mountpoint`，不按 GGA 频率写 etcd。

* Source stream health 变化只产生本地指标 / 告警，不改变 ACTIVE、不写 etcd。

* IP 黑名单和 IP quota 不影响源站上传；源站仍计入 owner 用户连接上限。

* `quota=0` 客户端在 Argon2 前被拒绝，不创建 session；`quota>0` 在认证后执行跨 Caster 并发计数。

* `GET /`、`GET /SOURCETABLE`、Relay 和管理 API 不进入客户端 IP data-stream quota。

* 本地 ConfigSnapshot 落后时，新 Client/Source 准入 Txn 失败并触发配置重载。

* 本地 User/Mountpoint 快照落后时，认证后 admission Txn 失败，不能凭旧密码创建 session/source claim。

* 客户端 session 与 IP index 同 Txn 写入；Source claim、generation 和 owner session 同 Txn 写入。

* `enforce` 模式下不合格源站在一个 Caster 被拒绝后，短时间内连接其他 Caster 也受到相同 cooldown 限制。

* 校验通过的源站会提升 source 状态并清除自己的 cooldown。

* `observe` 模式不产生策略 cooldown，错误模块重连不会因标记本身被拒绝。

* 管理 session 在一个 Caster 登录后，可由集群内其他 Caster 验证。

* 创建管理 session 的 Caster 崩溃后，管理员 session 在绝对过期时间前仍然有效。

* API Token 撤销后，所有 Caster 在配置同步窗口内拒绝旧 token。

* 在任一 core 创建 / 修改 / 删除 User、Mountpoint 或 Blacklist 成功后，其他 core 通过 Watch 收敛到相同 revision。

* 两个 core 同时 PATCH 同一 User/Mountpoint/Blacklist 时只能一个成功，另一个返回 409。

* 黑名单 CRUD 不要求重写整份 Caster Config generation。

### 18.3 规模



* 2C2G 档单节点至少达到 3000 NTRIP connections、500 source、100 relay。

* 4C4G 档单节点至少达到 8000 NTRIP connections、1500 source、300 relay。

* 集群通过多 Caster 达到 1 万以上在线 session。

* 1 万 mountpoint 配置。

* 数千订阅者单挂载点 fan-out。

* 5000/10000/20000 行 STR。

* 10k 级候选的 NEAR 查询。

### 18.4 稳定性



* 72 小时 soak 无持续内存增长。

* 1000 次配置更新无泄漏。

* Caster 故障恢复后无孤儿 session/source。

* 认证失败风暴不会拖垮 Argon2。

* 从 etcd/redb/TLS 备份恢复到新环境后，只恢复配置，不恢复旧在线状态。

* 滚动升级和回滚过程中不产生双 source，不丢失配置 generation。

### 18.5 安全



* 认证开启时强制 TLS。

* etcd 和 relay 使用 mTLS。

* 管理接口没有未授权写入口。

* XFF 不可被未信任来源伪造。

* 日志不泄露密码。

* WebSocket 不接受 query token，不允许未授权 Origin。

* RTKLIB 输入有长度上限，并通过 fuzz 测试。

### 18.6 多协议回归

以下用例必须使用冻结的真实抓包或相同字节 fixture：



| 用例                             | 预期                                                                      |
| ------------------------------ | ----------------------------------------------------------------------- |
| RTCM3 + 1005/1006 + MSM4       | `primary_format=rtcm3`，位置和 MSM4 被识别，STR 反映实际消息和 bitrate                 |
| RTCM3 只有 1005/1006/1033        | enforce 标记 `NO_OBSERVATION_OR_SSR` 并拒绝；observe 保留但带告警                   |
| RTCM3 坏 CRC                    | 准入窗口内坏帧不广播并计数；observe 标记，enforce 达到阈值后拒绝；ACTIVE 后只丢坏帧                   |
| RTCM2 正常帧                      | 识别 RTCM2 消息号，不要求 1005/1006，不套用 CRC24Q                                   |
| BINEX 正常 record                | 识别 BINEX record ID 和 checksum 结果；未知 subtype 只标记                         |
| CMR/CMR+ 正常流                   | 识别 CMR 家族和消息 ID；不得输出 RTCM3 专属字段                                         |
| NMEA-only                      | `has_correction_data=false`、`primary_format=nmea`；observe 在线，enforce 拒绝 |
| RTCM3/CMR/BINEX + NMEA 混合      | 主格式为纠偏格式，NMEA 只作为辅助格式记录，不能误判为双纠偏源                                       |
| RTCM2 错传到期望 RTCM3 的挂载点         | observe 标记 `EXPECTED_FORMAT_MISMATCH`；enforce 拒绝并写 cooldown             |
| MSM7 + restrict=false          | 记录 MSM7，不产生版本违规                                                         |
| MSM7 + observe + restrict=true | 标记 `MSM_VERSION_NOT_ALLOWED`，源站保持在线                                     |
| MSM7 + enforce + restrict=true | 源站拒绝，其他 Caster 受同一 cooldown 限制                                          |
| 未知字节比例超过阈值                     | observe 标记 `UNRECOGNIZED_DATA`；enforce 拒绝                               |
| observe 热切 enforce             | 现有 ACTIVE 源保持；之后新连接的源按 enforce 检查                                       |
| 自动 relay                       | 上述所有格式均按原始字节透明转发，不在 relay 路径重新编码                                        |
| Source fencing 竞争              | 旧 connection\_id 的更新 / 删除全部失败，新 claim 不被覆盖                              |
| etcd 写入监听                      | 连接建立 / 断开时写删；在线稳态和周期时间到达后没有 source/session rewrite                      |
| NEAR 非 RTCM3 源                 | RTCM2/CMR/BINEX/NMEA-only 永远不进入 NEAR 候选                                 |
| NEAR GGA 双工                    | 持续发送 GGA 时下行不断，坏 GGA 和非 GGA 行被丢弃且不触发断线                                  |
| NEAR 切换观察                      | 3 秒观察内移动超过 4 米不切换；稳定且满足 hysteresis 才切换                                  |

每个格式都要有截断帧、超长长度、校验失败、随机字节和畸形连续流的 fuzz seed。识别器必须保证有界内存、无 panic，并能丢弃坏记录后重新同步。

RTCM3 转发专项用例：



* 单次 read 内一帧、多帧和半帧跨多次 read。

* `0xD3` 出现在合法帧负载内部时不能误切帧。

* 坏 CRC 后从下一字节重新同步，不吞掉后续合法帧。

* payload 超过 1023 字节或累计缓冲超过上限时拒绝。

* 一个 `SharedPacket` 被多个订阅者克隆时 payload 不重复复制。

* 富语义 parser 队列饱和时只丢分析样本，不能阻塞源站、fan-out 或 writer。

* relay 上游半帧、断流和 owner 切换时，下游行为符合故障策略，不能形成 relay 环。

### 18.7 NTRIP 兼容矩阵

必须用固定原始请求 fixture 做字节级响应测试：



| 请求                                        | 预期                                                                  |
| ----------------------------------------- | ------------------------------------------------------------------- |
| NTRIP1 `GET /mount` 无 User-Agent，HTTP/1.0 | 按 NTRIP1 数据流或兼容错误处理，不误判为普通 HTTP                                     |
| NTRIP1 `GET /`                            | `SOURCETABLE 200 OK`，`text/plain`，`Content-Length`，`ENDSOURCETABLE` |
| NTRIP1 `GET /` 无 HTTP 版本                  | 兼容为 HTTP/0.9 风格 NTRIP1                                              |
| NTRIP2 `GET /mount`                       | `HTTP/1.1 200 OK`，`gnss/data`，`Connection: close`，无 Content-Length  |
| NTRIP2 sourcetable                        | `HTTP/1.1 200 OK`，`gnss/sourcetable` 或配置的 `text/plain`              |
| NTRIP2 keep-alive sourcetable 后再次 GET     | 非流请求可复用；数据流绝不复用                                                     |
| NTRIP1 `GET /missing`                     | 返回 sourcetable 后关闭                                                  |
| NTRIP2 `GET /missing`                     | 404，不返回 sourcetable                                                 |
| NTRIP1 源站错误密码                             | `ERROR - Bad Password\r\n`                                          |
| NTRIP1 源站重复挂载点                            | `ERROR - Mount Point Taken or Invalid\r\n`                          |
| NTRIP2 源站错误认证                             | 401 + `WWW-Authenticate`                                            |
| POST chunked 源站                           | chunk 解码后进入同一 ProtocolDemux                                         |
| POST Content-Length 源站                    | 流式读取，不将整个流缓冲到内存                                                     |
| CL 与 chunked 同时出现                         | 400                                                                 |
| Header / 单行超限                             | 431 或关闭，且不留下 source/session                                         |
| NEAR 客户持续发送 GGA                           | 下行不断流，1 秒内最多一次空间查询                                                  |
| NEAR 客户发送坏 checksum / 非 GGA               | 丢弃对应输入，不关闭下行                                                        |

现场兼容测试至少覆盖 RTKLIB `str2str`、BKG Ntrip Client、Lefebure NTRIP Client、u-center NTRIP Client 和常见接收机内置源站。测试记录客户端版本、请求 Header、响应 Header、chunked/GGA 行为和断线重连次数；无法获得的商业客户端不受支持，应在此矩阵标为 “未验证”，不能口头宣称兼容。

### 18.8 STR 生命周期测试



* 源站 claim 成功后，etcd 存在 `runtime/sources/{mount}`，但状态必须是 `VALIDATING`。

* `VALIDATING` 源不得出现在 `GET /`、NEAR 索引或 Relay route 中。

* 准入成功后，owner 只能在自己的 generation/connection\_id 条件 Txn 下把状态改为 ACTIVE。

* 两个以上 Caster watch 同一 source record 后，本地 STR 中该挂载点的行一致。

* 旧 revision、旧 generation 或旧 connection\_id 的 watch 事件不能覆盖新本地表。

* NEAR 开启时，`CAS`、`NET` 之后的第一条 `STR` 必须是配置的 NEAR 名称。

* etcd 中不存在整张多挂载点 STR 文本，只有每挂载点 canonical records。

* `runtime/sources` 中不存在完整 `data_profile`、Source IP、设备序列号或逐包统计。

* CRC、未知字节、health 窗口或 parser 计数变化不会产生 etcd 写 / Watch。

* 管理 API 的单源详情和 health 根据 `caster_id` 回源 owner。

* 源站正常退出或 REJECTED 后，本地 STR 在 watch 传播后移除该行。

### 18.9 Etcd Watch 测试



* 新 Caster 先完成 users/mounts/config/source Range 快照，再从快照 revision + 1 开始 Watch。

* users/mounts 修改只产生对应前缀的增量事件，不触发全量用户或挂载点重新推送。

* blacklist 规则增删改只产生 `config/blacklist/` 增量事件，不要求重写整份 Caster Config。

* 一个 Caster 对每个 Watch 前缀只创建一个长期 watcher，不按 user/mount/session 创建子 watcher。

* `runtime/sources` 的 VALIDATING、ACTIVE、DRAINING、删除事件都只按 revision 增量应用。

* 收到旧 revision、旧 generation 或旧 connection\_id 的 Watch 事件时，本地 canonical record 不倒退。

* `compacted` 后停止使用对应缓存、重新 Range，并从新 revision +1 恢复 Watch。

* `online/users` Watch 只驱动管理视图和统计；用户连接配额仍通过 lock + 线性范围读取验证。

* 安全配置 Watch lag 超限时，新 Client/Source 准入暂停。

### 18.10 数据所有权与重连测试



* 非 session owner Caster 直接 PUT/DELETE 该 session 必须失败，只能写 owner command。

* 非 source owner Caster 不能修改 claim、runtime source 或 STR，只能通过 source command 请求断开。

* Caster 重启或 lease 重建后 `runtime_epoch` 必须变化，旧 epoch 的连接不能重新发布。

* Caster 与 etcd 恢复连接后先 Range 当前快照并替换本地缓存，再从 revision +1 Watch。

* 重连后旧本地 session/source 不会出现在新的 runtime/source 索引或 STR 中。

* 同一用户从另一 Caster 新建连接时产生新的 `session_id/runtime_epoch`，不能修改旧 owner 的记录。

* 节点异常退出后旧 session/source 在 lease TTL 内被自动删除，不产生永久幽灵记录。

* A 与 etcd 断连但未超过 deadline 时，已有客户端可继续；恢复后不换 epoch。

* A 超过 lease deadline 时，旧客户端 /source/relay 必须关闭；重新接入生成新 epoch 后，旧本地 socket 不能被写成在线 session。

* 如果旧 epoch 仍有未取消 task/socket，A 必须保持 FENCED/not-ready，不能开始新准入。

* A 的 source claim/STR 超过 lease deadline 后必须失效，重新同步不能把旧本地 source 重新发布为 ACTIVE。

* B 获得更大 source\_generation 后，A 的旧 STR/Relay publish 必须失败，新 STR 以 B 的记录为准。

### 18.11 自动滚动升级测试



* 任一 core 发起 upgrade 后，所有 core 看到同一个 operation 和 versions 状态。

* `locks/upgrade/global` 确保同一时刻只有一个节点处于升级 / 重启阶段。

* core 升级时 coordinator 失效，另一 core 能接管 operation 且不重复升级同一节点。

* 第一台健康或稳定性检查失败时 rollout 停止，后续节点不升级。

* `auto_rollback=true` 时失败节点切回旧 slot，并在恢复健康后写回滚状态。

* 新版本升级后使用新的 runtime\_epoch，旧 source/session 不会被恢复。

* 升级过程中 etcd quorum 至少保留，未满足容量阈值时拒绝开始。

* 未签名 artifact、Hash 不匹配、schema 不兼容或目标版本不在 manifest 中被拒绝。

### 18.12 外部 DNS/LB 测试



* 健康 core 通过 `caster.example.com` 可订阅，所有入口返回兼容的 global STR。

* 节点进入 draining 后 readiness=false，DNS/LB 不再分配新连接。

* `src-west.example.com` 只路由到 west region 的可用 source 节点。

* TLS 证书覆盖所有实际使用的主机名。

* PROXY Protocol 启用时，客户端 / 源站真实 IP 正确进入黑名单、quota、session 和 source record。

* 节点从 DNS 摘除时，已建立连接不声称会自动迁移；客户端必须重新连接。

* 普通多 A 记录不满足 “最近节点” 验收，只有 GeoDNS / 区域 VIP / 加权策略才能通过。

### 18.13 公网 Web/STR/ 地图 / 解析测试



* 匿名用户可以读取公共 STR JSON 和地图数据，但不会创建 NTRIP user session。

* 匿名请求 monitor ticket、WebSocket 或解析快照返回 401；已登录但 `web_enabled=false` 的用户同样拒绝。

* monitor ticket 只能针对公共 STR 中 ACTIVE 且可发布的 mountpoint，猜名称不能访问私有 / 失败源。

* 公网接口不返回密码、Token、Source IP、设备序列号和 internal relay endpoint。

* 多个已登录浏览器同时点击同一 Mountpoint 时，承接它们的同一 Web 管理节点只有一个本地 MonitorSession；不同管理节点可有各自实例。

* 点击远端 source 时由承接 Web 的管理节点建立 / 复用 Relay 并在本地解析，source owner 不因该页面启动 Web 解析线程。

* 第一 viewer 创建解析任务，后续 viewer 只增加 refcount。

* 最后一个 viewer 离开后默认立即关闭解析任务和仅由 monitor 使用的 Relay consumer；配置 grace 时不得超过其值。

* 达到 `max_monitor_sessions_per_node` 后返回 `MONITOR_BUSY` 和 Retry-After，不能创建第二个解析器。

* 达到每 IP WebSocket/viewer 限制时只拒绝该 IP。

* 慢 WebSocket 只收到丢弃 / 关闭，不影响解析器或其他 viewer。

* source generation 变化后旧 WebSocket 收到 `SOURCE_CHANGED`，旧 MonitorSession 关闭。

* RTCM2/BINEX/CMR 不显示虚构的卫星解析结果；NMEA-only 显示无效纠偏状态。

* 地图只能由公共 STR 记录生成；大规模数据使用 bbox/vector tile/pagination，不一次下载全部 10k 点。

* 公共 STR、地图和 monitor 快照的响应时间和资源占用纳入压测。



***

## 19. 核心不变量原型门

编码前先完成以下三个小型 etcd / 数据面原型，不要求管理 UI：

### 19.1 Source Fencing 原型

必须验证：



* A claim C63 后杀死 / 暂停 A，B 获得更高 source generation。

* A 恢复时旧 generation 的 publish、relay 请求、source command 全部失败。

* A 在 lease deadline 后恢复，FenceGuard 在 fan-out 前阻止旧数据。

* B 的 relay 只接受 generation/connection\_id 完全匹配的 source。

* 旧 A 的 socket 仍在接收源站数据时，A 不再向任何下游写出差分数据。

如果要求恶意节点或外部网络强制隔离，必须另设外部 ingress fencing；应用层测试不能把合作式假设伪装成绝对保证。

### 19.2 用户准入并发原型

必须验证：



* 同一用户最后一个配额同时被 Client 和 Source 争抢，只能成功一个。

* A 获得 user lock 后暂停超过 lease，B 获得新锁并完成准入；A 恢复提交时因 lock holder identity 不匹配而失败。

* Source claim 和 user session 在同一 Txn；任一部分失败都不能留下半状态。

* 集群中多个 Caster 并发准入时，session 总数不超过 effective max。

* IP quota 不统计 Source，且不会误伤 Source claim。

### 19.3 配置 revision 原型



* Caster 本地 ConfigSnapshot 落后时，新 Client/Source Txn 因 config mod\_revision 不匹配而失败。

* Caster 重新 watch 到当前 generation 后恢复准入。

* sourcetable、NEAR 等非安全配置可最终一致；安全准入不能依赖旧快照。

这三个原型的自动化测试通过后，再实现完整 Caster。



***

## 附录 A：原 Millipede 的可参考行为

原项目不参与代码复用，只保留以下协议和故障经验：



* NTRIP1 `SOURCE` 只给上传密码，不提供用户名。

* NTRIP2 `POST` 使用 Basic 用户名和密码。

* 原代码在 `ntripsrv.c` 中对 NTRIP1/2 有不同认证分支。

* 原项目客户端 `GET` 不校验挂载点密码；新系统按客户端认证开关设计。

* 原项目源站关闭时，普通订阅者立即断开，虚拟源客户端重新选站。

* 原项目把 STR 的 NMEA 字段位置用于本地 virtual 标记；新系统使用独立的系统 NEAR，不重载标准字段。

* 原项目的 RTCM framing、CRC24Q、1005/1006 位置和 MSM7 转换可作为数值参考。

* 原项目部分文本转换和 NEAR 算法可作为行为参考。

* 原项目的 libevent、job 队列、hand-written refcount、P2P syncer 不进入新设计。

附录中的内容只用于验证 NTRIP/RTCM 行为，不构成新系统兼容性要求。



***

## 附录 B：`E:/GitHub/NTRIPcaster` 参考结论

该 Python 项目主要提供 RTCM3 解析和 STR 修正参考：



* 源站上线时保存 `User-Agent`、协议版本、源 IP 和连接时间。

* 使用 `pyrtcm` 解析 RTCM。

* 1005/1006 提取 station ID、ECEF、经纬度和高度。

* 1033 提取 receiver、firmware、antenna 和序列号。

* 统计 RTCM 类型、GNSS 系统、载波、消息频率和 bitrate。

* 使用运行时解析结果生成或修正 STR 字段。

* Web 端提供总览、在线用户 / 挂载点、日志、地图和 RTCM 卫星可视化。

本方案采用的思路：



* 源站软件信息来自 HTTP Header，设备信息来自 RTCM 1033，两者分开记录。

* 源站流只通过一条 `ProtocolDemux` 解析，同时驱动校验、在线信息、STR 刷新和指标。

* 只有 RTCM3 使用 1033 和 1005/1006 规则；其他格式不能套用 RTCM3 必需消息。

* `observe/enforce` 是唯一策略开关，不再保留互相冲突的 `violation_action`。

* source key 只有在准入判断完成后才进入公开 STR 或 relay；enforce 要求全部启用规则通过，observe 允许带告警准入。

* MSM 版本、格式 / 纠偏 / 位置超时和 CRC 阈值由 Caster Config 控制。

* 只有 `enforce` 模式下的校验失败写入集群 cooldown；`observe` 只标记。

不在本方案中照搬的实现：



* 不复制其 Python 代码或数据库结构。

* 不使用每挂载点额外订阅线程重复解析数据。

* 不让运行时解析结果直接无限期覆盖持久配置。

* 不把单机内存字典作为集群在线状态来源。

* 不使用其单活跃 Web 解析挂载点限制；新系统允许多个受权限和并发上限约束的 monitor。

* 不让浏览器直接消费原始差分数据流。



***

## 附录 C：RTCM 转发与解析参考方案审查

采纳的原则：



* 增量 framing，不把一次 TCP read 当作一条 RTCM 记录。

* `Bytes` 表示完整记录，`Arc<Packet>` 在订阅者、relay 和 monitor 之间共享，不复制 payload。

* 每订阅者使用有界队列和字节 backlog，慢客户端只影响自己。

* 长期状态只保存 owned 的小对象；借读解析视图不能跨帧或跨 `await` 保存。

* relay 使用同一套 ProtocolDemux 和 fan-out，本地只保留一条上游连接。

* 富语义解析不阻塞转发；转换是独立新分配路径。

参考方案中不直接照搬的部分：



* MSM 类型范围是 1071–1137，不是 1071–1127；MSM 版本包含 1–7，不是只包含 1/3/4/5/7。

* 非 RTCM 字节不能无条件透传。必须根据 `observe/enforce`、挂载点期望格式和未知字节比例决定转发、标记或拒绝。

* “解析全部放旁路” 不满足源站准入要求。准入解析和基础计数必须在源数据路径完成；只有详细的卫星、C/N0 和图表分析可以抽样旁路。

* relay 不需要对每个转发帧完整语义解析；owner 维护公开 profile，edge 只做 framing、转发和按需抽样监视。

* `block_in_place` 不应作为通用重解析方案。CPU 密集任务应进入专用 blocking pool 或独立 worker，避免阻塞 Tokio worker。

* MSM7→MSM4/3 不是简单位段拷贝，第一版关闭转换；先实现过滤和准入策略。
