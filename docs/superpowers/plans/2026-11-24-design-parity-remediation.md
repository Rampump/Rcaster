# RCaster Design Parity Remediation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close the highest-impact gaps between the current RCaster implementation and `rust-rewrite-plan.md` without changing the deployed soak candidate.

**Architecture:** Keep the existing four-crate structure. Extend the protocol profile and admission policy first, then make STR and health consume the richer profile. Fix management APIs and NEAR only after the shared data model is stable.

**Tech Stack:** Rust 1.88, Tokio, serde, etcd 3.5, rustls, redb, existing test utilities.

**Spec:** `rust-rewrite-plan.md`

## Global Constraints

- Do not connect to, modify, restart, or query the four soak servers.
- Do not push any commit.
- Leave `README.md` uncommitted.
- Keep Rust MSRV at `1.88`.
- Add no dependency unless an existing dependency cannot solve the task.
- Preserve current config files. New fields must deserialize with defaults.
- Every behavior change follows RED → GREEN → REFACTOR.
- Commit locally after each independently verifiable task.

---

## File Structure

- `crates/rcaster-core/src/protocol.rs`: profile counters, admission violations and policy evaluation.
- `crates/rcaster-core/src/account.rs`: versioned Mountpoint STR metadata.
- `crates/rcaster-core/src/str_record.rs`: merge detected profile with Mountpoint defaults.
- `crates/rcaster-app/src/config.rs`: source validation, health and NEAR config file schema.
- `crates/rcaster-app/src/main.rs`: convert file config into runtime policy and thresholds.
- `crates/rcaster-service/src/source_admission.rs`: admission window state and deadline behavior.
- `crates/rcaster-service/src/ntrip_server.rs`: source admission timeout handling and stream activation.
- `crates/rcaster-service/src/hub.rs`: source health thresholds and warning codes.
- `crates/rcaster-service/src/health.rs`: Prometheus metrics.
- `crates/rcaster-service/src/near.rs`: bounded spatial candidate index.
- `crates/rcaster-service/src/admin.rs`: source filters and admin dashboard.
- `crates/rcaster-service/src/public_auth.rs`: monitor ticket source generation.
- `crates/rcaster-service/src/public.rs`: monitor generation-change handling.
- `crates/rcaster-service/src/account_store.rs`: standalone blacklist persistence.

### Task 1: Advanced Admission Profile and Immediate Rules

**Status:** Complete locally.

**Files:**
- Modify: `crates/rcaster-core/src/protocol.rs`
- Modify: `crates/rcaster-app/src/config.rs`
- Modify: `crates/rcaster-app/src/main.rs`
- Test: `crates/rcaster-core/tests/protocol_demux.rs`
- Test: `crates/rcaster-app/tests/standalone_config.rs`

**Interfaces:**
- Consumes: `ProtocolDemux::push`, `AdmissionPolicy::evaluate`.
- Produces: `DataProfile::{correction_frames, correction_formats, position_message_seen, msm_message_seen}`.
- Produces: `AdmissionViolation::{MixedCorrectionFormats, NoObservationOrSsr, BasePositionMissing, MsmRequiredMissing, CrcThresholdExceeded}`.
- Produces: `AdmissionPolicy::{min_correction_frames, max_crc_error_ratio, require_base_position, require_msm}`.

- [ ] **Step 1: Add failing profile-counter tests**

```rust
#[test]
fn profile_counts_correction_frames_and_mixed_formats() {
    let mut demux = ProtocolDemux::new();
    demux.push(&rtcm3_frame(1074));
    demux.push(&cmr_packet(0x94));
    let profile = demux.profile();
    assert_eq!(profile.correction_frames, 2);
    assert_eq!(profile.correction_formats.len(), 2);
}

#[test]
fn profile_tracks_position_and_msm_evidence() {
    let mut demux = ProtocolDemux::new();
    demux.push(&rtcm3_1005());
    demux.push(&rtcm3_frame(1074));
    assert!(demux.profile().position_message_seen);
    assert!(demux.profile().msm_message_seen);
}
```

- [ ] **Step 2: Run the profile tests and verify RED**

Run:

```powershell
cargo test -p rcaster-core --test protocol_demux profile_counts_correction_frames_and_mixed_formats --locked
```

Expected: compile failure because `correction_frames` does not exist.

- [ ] **Step 3: Add the profile fields and parser updates**

```rust
pub struct DataProfile {
    // existing fields...
    pub correction_frames: u64,
    pub correction_formats: BTreeSet<DataFormat>,
    pub position_message_seen: bool,
    pub msm_message_seen: bool,
}
```

`record_format_only` increments `correction_frames` and inserts the correction format. `record_rtcm3` does the same only for `is_correction_message(message_number)`. Parsing 1005/1006 sets `position_message_seen`; parsing an MSM message sets `msm_message_seen`.

- [ ] **Step 4: Run the profile tests and verify GREEN**

Run:

```powershell
cargo test -p rcaster-core --test protocol_demux profile_ --locked
```

Expected: both profile tests pass.

- [ ] **Step 5: Add failing policy tests**

```rust
#[test]
fn enforce_rejects_too_few_correction_frames() {
    let policy = AdmissionPolicy {
        min_valid_frames: 1,
        min_correction_frames: 2,
        ..AdmissionPolicy::default()
    };
    let profile = profile_with_frames(1, 3, 0);
    assert!(matches!(
        policy.evaluate(&profile),
        AdmissionProgress::Rejected { violations }
            if violations.contains(&AdmissionViolation::MissingCorrectionData)
    ));
}

#[test]
fn enforce_rejects_high_crc_ratio() {
    let policy = AdmissionPolicy {
        min_valid_frames: 3,
        max_crc_error_ratio: Some(0.1),
        ..AdmissionPolicy::default()
    };
    let profile = profile_with_frames(4, 4, 2);
    assert!(matches!(
        policy.evaluate(&profile),
        AdmissionProgress::Rejected { violations }
            if violations.contains(&AdmissionViolation::CrcThresholdExceeded)
    ));
}

#[test]
fn enforce_distinguishes_auxiliary_only_rtcm_from_nmea_only() {
    let policy = AdmissionPolicy {
        require_correction_data: true,
        ..AdmissionPolicy::default()
    };
    let rtcm = profile_with_primary_format(DataFormat::Rtcm3);
    let nmea = profile_with_primary_format(DataFormat::Nmea);
    assert!(violations(&policy, &rtcm).contains(&AdmissionViolation::NoObservationOrSsr));
    assert!(violations(&policy, &nmea).contains(&AdmissionViolation::MissingCorrectionData));
}
```

- [ ] **Step 6: Run the policy tests and verify RED**

Run:

```powershell
cargo test -p rcaster-core --test protocol_demux enforce_rejects_too_few_correction_frames --locked
```

Expected: compile failure because the new policy fields and violation do not exist.

- [ ] **Step 7: Implement immediate policy rules**

```rust
if self.require_correction_data && profile.correction_frames == 0 {
    violations.push(if profile.primary_format == Some(DataFormat::Rtcm3) {
        AdmissionViolation::NoObservationOrSsr
    } else {
        AdmissionViolation::MissingCorrectionData
    });
}
if profile.correction_frames > 0
    && profile.correction_frames < self.min_correction_frames
{
    violations.push(AdmissionViolation::MissingCorrectionData);
}
let integrity_attempts = profile.valid_frames.saturating_add(profile.crc_errors);
if profile.valid_frames >= self.min_valid_frames
    && integrity_attempts > 0
    && profile.crc_errors as f64 / integrity_attempts as f64
        > self.max_crc_error_ratio.unwrap_or(1.0)
{
    violations.push(AdmissionViolation::CrcThresholdExceeded);
}
if profile.correction_formats.len() > 1 {
    violations.push(AdmissionViolation::MixedCorrectionFormats);
}
if self.require_base_position
    && profile.primary_format == Some(DataFormat::Rtcm3)
    && !profile.position_message_seen
{
    violations.push(AdmissionViolation::BasePositionMissing);
}
if self.require_msm
    && profile.primary_format == Some(DataFormat::Rtcm3)
    && !profile.msm_message_seen
{
    violations.push(AdmissionViolation::MsmRequiredMissing);
}
```

- [ ] **Step 8: Run all core protocol tests and verify GREEN**

Run:

```powershell
cargo test -p rcaster-core --test protocol_demux --locked
```

Expected: 0 failures.

- [ ] **Step 9: Add config compatibility tests**

```rust
#[test]
fn source_validation_accepts_advanced_rtcm_rules() {
    let dir = tempdir();
    let path = write_config(
        &dir,
        r#"{
          "account_db":"accounts.redb",
          "source_admission":{
            "mode":"enforce",
            "allowed_formats":["rtcm3"],
            "require_correction_data":true,
            "min_valid_frames":3,
            "min_correction_frames":2,
            "max_crc_error_ratio":0.1,
            "require_base_position":true,
            "msm":{"require_any":true,"required_within_s":30},
            "max_bytes":65536,
            "max_frames":200,
            "timeout_ms":15000
          }
        }"#,
    );
    let config = StandaloneConfig::load(&path).unwrap();
    let admission = config.source_admission.unwrap();
    assert_eq!(admission.min_correction_frames, 2);
    assert_eq!(admission.max_crc_error_ratio, Some(0.1));
    assert!(admission.require_base_position);
    assert!(admission.msm.require_any);
}
```

- [ ] **Step 10: Implement config fields and runtime mapping**

Add defaults that accept old configs:

```rust
#[serde(default)]
pub min_correction_frames: u64,
#[serde(default)]
pub max_crc_error_ratio: Option<f64>,
#[serde(default)]
pub require_base_position: bool,
```

Add `require_any` and `required_within_s` to `SourceMsmConfigFile`. Validate ratios in `0.0..=1.0`; validate `required_within_s` only when `require_any=true`. Map the fields into `AdmissionPolicy` in `main.rs`.

- [ ] **Step 11: Run config and admission tests**

Run:

```powershell
cargo test -p rcaster-app --test standalone_config --locked
cargo test -p rcaster-service --test source_admission_window --locked
```

Expected: all pass.

- [ ] **Step 12: Commit locally**

```powershell
git add crates/rcaster-core/src/protocol.rs crates/rcaster-core/tests/protocol_demux.rs crates/rcaster-app/src/config.rs crates/rcaster-app/src/main.rs crates/rcaster-app/tests/standalone_config.rs
git commit -m "fix: enforce advanced source admission rules"
```

### Task 1A: End-to-End RTCM Upload to etcd STR

**Status:** Complete locally.

**Files:**
- Modify: `crates/rcaster-service/tests/ntrip_cluster.rs`

**Interfaces:**
- Consumes: NTRIP source upload, `SourceAdmissionWindow`, `ClusterRuntime::activate_source`.
- Produces: an integration test that reads the canonical etcd source record and verifies its STR.

- [x] **Step 1: Start local etcd, coordinator and Caster listener in the test**
- [x] **Step 2: Upload RTCM3 1005 and MSM4 1074 through NTRIP1 `SOURCE`**
- [x] **Step 3: Wait for `/runtime/sources/C66` to become `ACTIVE`**
- [x] **Step 4: Assert source generation, mountpoint and caster ownership**
- [x] **Step 5: Assert the etcd STR contains `RTCM 3.3`, message IDs `1005,1074` and non-zero position**
- [x] **Step 6: Run `cargo test -p rcaster-service --test ntrip_cluster --locked`**

The test is `uploaded_rtcm3_generates_active_etcd_str_record`.
The same test accepts `RCASTER_SERIAL_CAPTURE=<path>` to replace synthetic
RTCM3 with a temporary capture from the real local COM3 module.

The follow-on test
`second_caster_serves_cached_etcd_str_without_local_source` starts two local
Casters against the same etcd. Source Caster A uploads RTCM, Caster B converges
its `SourceWatcher` cache, and a client connected to B receives `STR;C66` from
B's locally generated source table. B never owns or hosts the source.

### Task 2: Observe Unknown Streams and Deadline Semantics

**Status:** Complete locally.

**Files:**
- Modify: `crates/rcaster-service/src/source_admission.rs`
- Modify: `crates/rcaster-service/src/ntrip_server.rs`
- Test: `crates/rcaster-service/tests/source_admission_window.rs`
- Test: `crates/rcaster-service/tests/ntrip_server.rs`

**Interfaces:**
- Consumes: `AdmissionPolicy.mode`, `DataProfile.valid_frames`.
- Produces: `SourceAdmissionWindow::finish` returns Accepted for an unknown stream in Observe mode.

- [x] **Step 1: Add a failing window test**

```rust
#[test]
fn observe_accepts_unknown_stream_after_window_closes() {
    let policy = AdmissionPolicy {
        mode: AdmissionMode::Observe,
        min_valid_frames: 1,
        ..AdmissionPolicy::default()
    };
    let mut window = SourceAdmissionWindow::new(policy, 4096, 20);
    assert_eq!(window.push(b"\xDE\xAD\xBE\xEF"), WindowOutcome::Pending);
    assert!(matches!(
        window.finish(),
        WindowOutcome::Accepted { warnings, .. }
            if warnings.contains(&AdmissionViolation::NoValidFrames)
    ));
}
```

- [x] **Step 2: Run the test and verify RED**

Run:

```powershell
cargo test -p rcaster-service --test source_admission_window observe_accepts_unknown_stream_after_window_closes --locked
```

Expected: rejected with `NoValidFrames`.

- [x] **Step 3: Implement the Observe fallback**

In `SourceAdmissionWindow::finish`, convert `Pending` to:

```rust
AdmissionProgress::Accepted {
    warnings: vec![AdmissionViolation::NoValidFrames],
}
```

only when the configured mode is Observe and no valid frame was seen. Keep Enforce as rejected.

- [x] **Step 4: Add a failing deadline test**

```rust
#[tokio::test]
async fn source_deadline_uses_finish_in_observe_mode() {
    // Start a source with no recognizable bytes and timeout_ms=50.
    // Assert the source becomes ACTIVE with primary_format=UNKNOWN.
}
```

- [x] **Step 5: Implement deadline finish**

Replace `timeout_at(...).await.map_err(TimedOut)?` for the admission phase with a match that calls `admission.finish(source, permit).await` on timeout. Keep the post-admission `read_timeout` behavior unchanged.

- [x] **Step 6: Run admission and NTRIP tests**

```powershell
cargo test -p rcaster-service --test source_admission_window --locked
cargo test -p rcaster-service --test ntrip_server --locked
```

- [x] **Step 7: Commit locally**

```powershell
git add crates/rcaster-service/src/source_admission.rs crates/rcaster-service/src/ntrip_server.rs crates/rcaster-service/tests/source_admission_window.rs crates/rcaster-service/tests/ntrip_server.rs
git commit -m "fix: keep unknown sources visible in observe mode"
```

### Task 3: Mountpoint STR Metadata and Merge

**Status:** Complete locally.

**Files:**
- Modify: `crates/rcaster-core/src/account.rs`
- Modify: `crates/rcaster-core/src/str_record.rs`
- Modify: `crates/rcaster-service/src/admin.rs`
- Modify: `crates/rcaster-service/src/ntrip_server.rs`
- Modify: `crates/rcaster-service/src/public.rs`
- Test: `crates/rcaster-core/tests/str_record.rs`
- Test: `crates/rcaster-service/tests/admin_api.rs`
- Test: `crates/rcaster-service/tests/public_api.rs`

**Interfaces:**
- Consumes: detected `DataProfile`.
- Produces: `MountpointStrConfig` and `StrRecord::from_profile_and_mountpoint`.

- [ ] **Step 1: Add failing STR merge tests**

```rust
#[test]
fn configured_str_fields_survive_when_profile_has_no_device_data() {
    let mount = mountpoint_with_str("Farm 1", "RTK-NET", "CHN", 31.2, 121.4, 30_000.0);
    let record = StrRecord::from_profile_and_mountpoint("C63", &DataProfile::default(), &mount);
    assert_eq!(record.identifier, "Farm 1");
    assert_eq!(record.network, "RTK-NET");
    assert_eq!(record.country, "CHN");
    assert_eq!(record.latitude, "31.2");
    assert_eq!(record.longitude, "121.4");
}

#[test]
fn detected_rtcm3_position_overrides_configured_position() {
    let mount = mountpoint_with_str("Farm 1", "RTK-NET", "CHN", 31.2, 121.4, 30_000.0);
    let profile = profile_with_position(30.0, 120.0);
    let record = StrRecord::from_profile_and_mountpoint("C63", &profile, &mount);
    assert_eq!(record.latitude, "30");
    assert_eq!(record.longitude, "120");
}
```

- [ ] **Step 2: Run tests and verify RED**

```powershell
cargo test -p rcaster-core --test str_record configured_str_fields_survive_when_profile_has_no_device_data --locked
```

- [ ] **Step 3: Add typed Mountpoint STR fields**

Add serializable optional fields:

```rust
pub struct MountpointStrConfig {
    pub identifier: Option<String>,
    pub format: Option<String>,
    pub carrier: Option<String>,
    pub navigation_system: Option<String>,
    pub network: Option<String>,
    pub country: Option<String>,
    pub solution: Option<String>,
}
```

Add `latitude: Option<f64>`, `longitude: Option<f64>`, `coverage_radius_m: Option<f64>` and `str_fields: Option<MountpointStrConfig>` to `Mountpoint` with `#[serde(default)]`.

- [ ] **Step 4: Implement merge precedence**

Use this precedence:

1. Detected RTCM3 position.
2. Configured Mountpoint position.
3. `0/0` only when neither exists.

Use configured identity fields as defaults; detected format, message details, generator, nav-system and bitrate override when known. Do not emit RTCM-specific fields for format-only sources.

- [ ] **Step 5: Extend admin mountpoint CRUD and public map**

Accept and return the new fields in mountpoint POST/PATCH/GET. Include `coverage_radius_m` in map feature properties and omit stations with no configured or detected position.

- [ ] **Step 6: Run STR, admin and public tests**

```powershell
cargo test -p rcaster-core --test str_record --locked
cargo test -p rcaster-service --test admin_api --locked
cargo test -p rcaster-service --test public_api --locked
```

- [ ] **Step 7: Commit locally**

```powershell
git add crates/rcaster-core/src/account.rs crates/rcaster-core/src/str_record.rs crates/rcaster-service/src/admin.rs crates/rcaster-service/src/ntrip_server.rs crates/rcaster-service/src/public.rs
git commit -m "fix: merge configured mountpoint STR metadata"
```

### Task 4: Source Health and Metrics

**Status:** Complete locally for the planned core health warnings and low-cardinality metrics.

**Files:**
- Modify: `crates/rcaster-service/src/config.rs`
- Modify: `crates/rcaster-service/src/hub.rs`
- Modify: `crates/rcaster-service/src/health.rs`
- Modify: `crates/rcaster-app/src/main.rs`
- Test: `crates/rcaster-service/tests/mountpoint_hub.rs`
- Test: `crates/rcaster-service/tests/health.rs`

**Interfaces:**
- Consumes: `SourceHealthConfig`, `DataProfile`, source state history.
- Produces: warning bits for observation stale, station change and position jump.
- Produces: low-cardinality Prometheus counters/gauges.

- [ ] **Step 1: Add failing health tests**

```rust
#[test]
fn source_health_warns_when_station_id_changes() {
    let hub = hub_with_source();
    publish_rtcm3_1005(&hub, 1001, 30.0, 120.0);
    publish_rtcm3_1005(&hub, 1002, 30.0, 120.0);
    let health = hub.source_health("C63").unwrap();
    assert_ne!(health.warning_codes & STATION_ID_CHANGED_WARNING, 0);
}

#[test]
fn source_health_warns_when_base_position_jumps() {
    let hub = hub_with_source();
    publish_rtcm3_1005(&hub, 1001, 30.0, 120.0);
    publish_rtcm3_1005(&hub, 1001, 30.01, 120.01);
    let health = hub.source_health("C63").unwrap();
    assert_ne!(health.warning_codes & BASE_POSITION_JUMP_WARNING, 0);
}
```

- [ ] **Step 2: Implement config-thresholded health**

Add defaults for `alert_clear_s`, `station_id_change_alert`, `base_position_jump_m`, `valid_frame_ratio_min`, `short_window_s` and `long_window_s`. Track last station ID, last position and contiguous warning/clear durations in source state.

- [ ] **Step 3: Add failing metrics tests**

```rust
assert!(metrics.contains("rcaster_sources_by_format{format=\"rtcm3\"}"));
assert!(metrics.contains("rcaster_source_rejections_total{reason=\"mixed_correction_formats\"}"));
assert!(metrics.contains("rcaster_monitor_busy_total"));
```

- [ ] **Step 4: Implement core low-cardinality metrics**

Add only fixed labels: format, validation result, rejection reason, relay state and monitor state. Do not label by username, mountpoint, session ID or IP.

- [ ] **Step 5: Run hub and health tests**

```powershell
cargo test -p rcaster-service --test mountpoint_hub --locked
cargo test -p rcaster-service --test health --locked
```

- [ ] **Step 6: Commit locally**

```powershell
git add crates/rcaster-service/src/config.rs crates/rcaster-service/src/hub.rs crates/rcaster-service/src/health.rs crates/rcaster-app/src/main.rs
git commit -m "fix: add source health warnings and metrics"
```

### Task 5: NEAR Spatial Index and Limits

**Status:** Complete locally. The spatial index, revision cache, GGA rate/jump
guards, query rate/concurrency limiter, position TTL and fixed-label metrics are
implemented. A live CRTK.NET test also models 1,302 public RTCM3 STR candidates.

**Files:**
- Modify: `crates/rcaster-service/src/near.rs`
- Modify: `crates/rcaster-service/src/ntrip_server.rs`
- Modify: `crates/rcaster-app/src/config.rs`
- Test: `crates/rcaster-service/tests/near.rs`
- Test: `crates/rcaster-service/tests/ntrip_server.rs`

**Interfaces:**
- Consumes: `NearSource` candidates.
- Produces: bounded spatial index refresh and query limiter.

- [x] **Step 1: Add failing near-query test**

```rust
#[test]
fn near_index_returns_same_source_as_full_scan() {
    let candidates = vec![source("A", 30.0, 120.0), source("B", 31.0, 121.0)];
    let index = NearIndex::new(&candidates);
    let query = GeoPosition::from_decimal(30.9, 120.9);
    assert_eq!(index.nearest(query, 100_000.0), full_scan_nearest(&candidates, query));
}
```

- [x] **Step 2: Add config fields**

Add `max_spatial_queries_per_second`, `max_query_concurrency`, `gga_max_input_per_second`, `gga_max_jump_speed_mps`, `position_ttl_s` and `position_ttl_action` with design-compatible defaults.

- [x] **Step 3: Implement a fixed-grid spatial index**

Use a latitude/longitude cell index rebuilt when STR revision changes. Query neighboring cells until the current minimum distance proves remaining cells cannot contain closer candidates.

- [x] **Step 4: Apply global and per-client limits**

Return no switch when a limit is hit and increment fixed-label metrics. Preserve current GGA drop behavior.

- [x] **Step 5: Run NEAR tests**

```powershell
cargo test -p rcaster-service --test near --locked
cargo test -p rcaster-service --test ntrip_server near_ --locked
```

- [x] **Step 6: Commit locally**

```powershell
git add crates/rcaster-service/src/near.rs crates/rcaster-service/src/ntrip_server.rs crates/rcaster-app/src/config.rs
git commit -m "fix: bound NEAR spatial queries"
```

### Task 6: Admin Filters, Monitor Generation and Dashboard

**Status:** Complete locally.

**Files:**
- Modify: `crates/rcaster-service/src/admin.rs`
- Modify: `crates/rcaster-service/src/public_auth.rs`
- Modify: `crates/rcaster-service/src/public.rs`
- Test: `crates/rcaster-service/tests/admin_api.rs`
- Test: `crates/rcaster-service/tests/public_api.rs`

**Interfaces:**
- Consumes: source route/profile and monitor ticket.
- Produces: `validation_result`, `expected_format_mismatch` and `msm_version` source filters.
- Produces: source-generation-bound `MonitorTicket`.

- [x] **Step 1: Add failing source-filter test**

```rust
assert_eq!(
    filtered_sources("?validation_result=warn&msm_version=4").len(),
    1
);
```

- [x] **Step 2: Add failing monitor-generation test**

```rust
let ticket = issue_ticket_for_generation(1001).await;
replace_source_with_generation(1002).await;
assert_source_changed_closes_websocket(ticket).await;
```

- [x] **Step 3: Implement filters and generation check**

Add `source_generation: u64` to `MonitorTicket`. Compare it on every monitor tick. Add the three missing source filters and include validation summary fields in list responses.

- [x] **Step 4: Hide write controls in service mode**

When `session.kind=service`, hide create/edit/delete controls and show a persistent read-only label. Keep server-side 403 enforcement.

- [x] **Step 5: Run admin and public tests**

```powershell
cargo test -p rcaster-service --test admin_api --locked
cargo test -p rcaster-service --test public_api --locked
```

- [x] **Step 6: Commit locally**

```powershell
git add crates/rcaster-service/src/admin.rs crates/rcaster-service/src/public_auth.rs crates/rcaster-service/src/public.rs
git commit -m "fix: tighten management and monitor semantics"
```

### Task 7: Standalone Blacklist

**Status:** Complete locally.

**Files:**
- Modify: `crates/rcaster-service/src/account_store.rs`
- Modify: `crates/rcaster-service/src/account_directory.rs`
- Modify: `crates/rcaster-service/src/admin.rs`
- Modify: `crates/rcaster-service/src/ntrip_server.rs`
- Test: `crates/rcaster-service/tests/account_store.rs`
- Test: `crates/rcaster-service/tests/admin_api.rs`

**Interfaces:**
- Consumes: `BlacklistRule`.
- Produces: standalone `list/put/delete` methods using a dedicated redb table.

- [x] **Step 1: Add failing persistence test**

```rust
#[test]
fn standalone_blacklist_round_trips_across_reopen() {
    let dir = tempdir();
    let path = dir.path().join("accounts.redb");
    let store = AccountStore::open(&path).unwrap();
    store.put_blacklist_rule(rule("blocked", "192.0.2.0/24", 0)).unwrap();
    drop(store);
    let store = AccountStore::open(&path).unwrap();
    assert_eq!(store.blacklist_rules().unwrap().len(), 1);
}
```

- [x] **Step 2: Implement dedicated table and directory methods**

Do not mix rules into the users or mountpoints table. Validate normalized CIDR uniqueness before write.

- [x] **Step 3: Wire standalone admin and admission**

When `runtime=None`, route blacklist CRUD to the Account Store and reload the local limiter immediately.

- [x] **Step 4: Run account and admin tests**

```powershell
cargo test -p rcaster-service --test account_store --locked
cargo test -p rcaster-service --test admin_api --locked
```

- [x] **Step 5: Commit locally**

```powershell
git add crates/rcaster-service/src/account_store.rs crates/rcaster-service/src/account_directory.rs crates/rcaster-service/src/admin.rs crates/rcaster-service/src/ntrip_server.rs
git commit -m "fix: persist blacklist rules in standalone mode"
```

### Task 8: Full Local Verification

**Status:** Complete locally.

**Files:**
- Modify: `docs/工作日记.md`
- Modify: `docs/superpowers/reports/2026-11-24-acceptance-matrix.md`

**Interfaces:**
- Consumes: all previous tasks.
- Produces: verified local status and handoff record.

- [x] **Step 1: Run formatting**

```powershell
cargo fmt --all -- --check
```

- [x] **Step 2: Run the full workspace**

```powershell
wsl.exe --cd /mnt/e/GitHub/Rcaster -d Debian --user rtk -- bash -lc "RTKLIB_SOURCE_DIR=/tmp/RTKLIB cargo test --workspace --all-features --locked"
```

- [x] **Step 3: Run Clippy**

```powershell
wsl.exe --cd /mnt/e/GitHub/Rcaster -d Debian --user rtk -- bash -lc "RTKLIB_SOURCE_DIR=/tmp/RTKLIB cargo clippy --workspace --all-features --all-targets --locked -- -D warnings"
```

- [x] **Step 4: Run release build**

```powershell
wsl.exe --cd /mnt/e/GitHub/Rcaster -d Debian --user rtk -- bash -lc "RTKLIB_SOURCE_DIR=/tmp/RTKLIB cargo build --release -p rcaster-app --bin rcaster --all-features --locked"
```

- [x] **Step 5: Update records**

Record changed behavior, remaining gaps and the fact that the soak candidate was not touched or redeployed.

- [x] **Step 6: Commit locally and do not push**

```powershell
git add docs/工作日记.md docs/superpowers/reports/2026-11-24-acceptance-matrix.md
git commit -m "docs: record design parity remediation"
```

## Self-Review

- Source admission, observe semantics, STR metadata, health, metrics, NEAR, admin filters, monitor generation and standalone blacklist each map to one task.
- Explicit design non-goals remain excluded.
- New config fields use defaults so existing files continue to load.
- The deployed soak candidate is never touched or redeployed.
