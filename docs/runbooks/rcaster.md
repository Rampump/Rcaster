# RCaster Operations Runbook

Replace `INSTANCE` with the Prometheus `instance` label and `MOUNT` with the
affected mountpoint.

## RCasterNotReady

1. Query `GET /health/ready` on `INSTANCE`.
2. Inspect `rcaster_cluster_dependency_healthy` to identify the failing
   dependency.
3. Check whether maintenance mode was intentionally enabled.
4. If readiness is false without maintenance, inspect the service log, etcd
   connectivity, config generation, blacklist Watch, source Watch, and account
   Watch.
5. Keep the node out of load balancer rotation until readiness remains true.

## RCasterDependencyUnhealthy

1. Use the `kind` label:
   - `lease`: verify Caster lease renewal and etcd quorum.
   - `config`: verify the Caster Config key and Watch.
   - `blacklist`: verify `config/blacklist/` Range and Watch.
   - `sources`: verify `runtime/sources/` Range and Watch.
   - `accounts`: verify `config/users/` and `config/mounts/` Range and Watch.
2. Restore etcd connectivity and quorum before forcing a process restart.
3. A Caster that cannot renew its lease must not remain in source-owner
   rotation.

## RCasterAuthLimiterFallback

1. Confirm etcd availability and latency from the affected node.
2. Check service logs around the first increase in
   `rcaster_auth_failure_fallback_total`.
3. Node-local failure limits remain active, but cross-Caster lockout is
   degraded. Investigate failed authentication spikes during this period.
4. Resolve the etcd failure; no process restart is required solely for this
   metric.

## RCasterTlsCertificateExpiring

1. Identify the listener certificate from node configuration.
2. Deploy a renewed certificate and private key through the normal atomic file
   update.
3. Wait for the reload interval and verify
   `rcaster_tls_certificate_expires_seconds` increases.
4. Confirm new TLS connections succeed. Existing sessions are not rewritten.

## RCasterClockNotSynchronized

1. Run `timedatectl` and verify `NTPSynchronized=yes`.
2. Check `chronyc tracking`, `ntpq -p` for NTPsec, or the node's configured
   NTP service. The metric falls back to the kernel `adjtimex` state when
   `chronyc` is unavailable.
3. Confirm UDP/TCP access to the configured time sources and inspect packet loss.
4. Restore time synchronization before accepting production traffic; lease
   deadlines use monotonic time, but audit and cross-node observation windows
   still require an accurate wall clock.

## RCasterClockSkewHigh

1. Compare each node's NTP source, stratum, and offset.
2. Correlate the offset spread with network latency and time-source outages.
3. Do not hide the spread by changing only one node's time manually; repair or
   re-point its NTP service and let it converge.
4. Keep the node out of production admission rotation if clocks remain outside
   the approved skew budget.

## RCasterSourceRejectionSpike

1. Query `/admin/v1/sources` and `/admin/v1/source-cooldowns`.
2. Group rejection reasons and affected mountpoints.
3. Check for a receiver sending the wrong format, NMEA-only streams, missing
   observation/SSR data, malformed frames, or reconnect storms.
4. Clear a cooldown only after the source configuration is corrected.

## RCasterCrcErrorSpike

1. Identify the source from admin source health and stream history.
2. Compare CRC, parity, and checksum errors with source bitrate and packet age.
3. Check the receiver, serial/TCP link, and any upstream proxy or relay.
4. Bad frames are dropped after ACTIVE admission; repeated corruption requires
   source-side remediation, not lowering parser validation.

## RCasterMemoryPressure

1. Confirm `rcaster_process_resident_memory_bytes` is rising rather than a
   one-time startup allocation.
2. Compare memory with active connections, subscribers, validating sources,
   parser queues, STR size, and socket buffer settings.
3. Capture heap/profile data before restarting if the process remains healthy.
4. Reduce node capacity or move traffic if memory approaches the deployment
   limit.

## RCasterFileDescriptorPressure

1. Confirm `rcaster_process_open_fds` is sustained and correlate it with
   connection count, relays, TLS handshakes, and redb/etcd files.
2. Check for reconnect storms, half-open clients, and failed relay cleanup.
3. Raise `LimitNOFILE` only when the configured `fd_budget` and validated host
   capacity justify it.
4. Restart only after capturing the FD count and connection state.

## RCasterEtcdBackupFailed

1. Run `systemctl status rcaster-etcd-backup.service` and inspect its journal.
2. Verify `/etc/rcaster/etcd-backup.env`, etcd client certificate permissions,
   endpoint reachability, quorum, and `/var/lib/rcaster/etcd-backups`.
3. Run the service manually and confirm a new private `rcaster-*.db` snapshot
   and successful retention.
4. Do not clear the failed state until a real snapshot has completed.

## RCasterEtcdBackupStale

1. Run `systemctl list-timers rcaster-etcd-backup.timer` and check that the timer
   is active with a next elapse no later than 15 minutes.
2. Inspect the latest snapshot modification time and
   `rcaster-etcd-backup.service` journal.
3. Confirm the node's systemd collector exports
   `node_systemd_timer_last_trigger_seconds`; without that collector this alert
   cannot be evaluated.
4. Restore scheduling and take a fresh snapshot before accepting the RPO.

## Etcd Backup Restore Drill

1. Take a backup without overwriting an existing file:

   ```bash
   rcaster etcd-backup \
     --etcd-endpoint https://etcd.internal:2379 \
     --output /backup/rcaster-$(date +%Y%m%dT%H%M%SZ).db \
     --etcd-ca etcd-ca.pem \
     --etcd-certificate backup-client.pem \
     --etcd-private-key backup-client-key.pem \
     --etcd-domain etcd.internal
   ```

2. Restore into a new data directory and alternate ports while the production
   cluster remains isolated:

   ```bash
   etcdutl snapshot restore /backup/rcaster-TIMESTAMP.db \
     --data-dir /var/lib/etcd-restore \
     --name rcaster-restored \
     --initial-cluster rcaster-restored=http://127.0.0.1:2480 \
     --initial-advertise-peer-urls http://127.0.0.1:2480
   ```

3. Start etcd on isolation ports and wait for quorum.
4. Run the recovery guard with the exact cluster ID:

   ```bash
   rcaster etcd-recovery-prepare \
     --etcd-endpoint http://127.0.0.1:2479 \
     --cluster-id production \
     --confirm production
   ```

5. Confirm the command reports the expected configuration generation and
   account counts. Verify `runtime/`, `online/`, and `locks/` are empty while
   `config/` remains.
6. Start one Caster with maintenance enabled, confirm readiness and TLS/etcd
   identity, then start the remaining Casters and allow sources/clients to
   reconnect.
7. Record actual RTO, data loss window, failed steps, and every manual repair.
   Recovered snapshots never restore old source claims, sessions, locks,
   admin sessions, monitor tickets, or upgrade state.

## Admin Dashboard Access

All four deployment nodes bind the administrative HTTPS listener to
`127.0.0.1:5758`. Do not expose that port publicly. Use an SSH tunnel from the
operator workstation:

```bash
ssh -N -L 15758:127.0.0.1:5758 root@NODE_ADDRESS
```

Open `https://127.0.0.1:15758/admin/` with a client certificate trusted by
`/etc/rcaster/pki/ca.crt`, then sign in with an enabled administrator account.
The service token can be used for read-only status checks from the node:

```bash
cfg=/etc/rcaster/caster.json
pki=/etc/rcaster/pki
token=$(jq -r '.cluster.coordinator_tls.service_token' "$cfg")
curl --cacert "$pki/ca.crt" \
  --cert "$pki/node.crt" \
  --key "$pki/node.key" \
  -H "Authorization: Bearer $token" \
  https://127.0.0.1:5758/admin/v1/cluster/status
```

`local_active_sources` is the count held by this node. `cluster_active_sources`
is the de-duplicated source count visible across the cluster. Service-token
access is read-only for token management: `/admin/v1/auth/csrf` and
`/admin/v1/api-tokens` intentionally return 403.

## Cross-Node Relay Soak

Use this drill after the Caster Config, account directory, TLS certificates,
and relay listener are healthy on at least two nodes.

1. Through the admin API, create a temporary NTRIP user and numeric
   mountpoints such as `SOAK000` through `SOAK029`. Set `advertise=false`.
2. Store the temporary credentials in a server-local `0600` environment file.
   Never commit that file.
3. Run a short smoke test from one server:

   ```bash
   cargo run -p rcaster-service --example cluster_soak -- \
     --endpoints caster-a:2102,caster-b:2102,caster-c:2102,caster-d:2102 \
     --sources 10 --subscribers 50 --duration-s 60 --rate-kib 1 \
     --mount-prefix SOAK --username soak-user --password secret \
     --ca-certificate /etc/rcaster/pki/ca.crt
   ```

4. Confirm:

   - every source becomes ACTIVE
   - admin `/admin/v1/sessions` reports sources plus subscribers
   - subscribers on nodes other than the source owner receive bytes
   - the generator FD and RSS counts stay bounded
   - the generator error log is empty
5. Run the long test as a transient systemd unit so SSH and workstation
   disconnects do not stop it. Put credentials in
   `/etc/rcaster/cluster-soak.env`, use `EnvironmentFile=`, and add
   `LimitNOFILE`, `MemoryMax`, `CPUQuota`, and a 259200-second duration.
6. At completion, stop the unit, preserve its JSON, verify that source and
   session counts return to zero, then delete the temporary mountpoints and
   user through the admin API.
7. Remove the credential environment file after the user is deleted.

Do not enable plaintext NTRIP on nodes solely for this drill. The generator
supports NTRIP2/TLS and validates the Caster CA.

## Standalone Backup Restore Drill

1. Keep `standalone_backup_dir` outside the account database filesystem if
   possible. The running Caster writes a timestamped directory containing
   `accounts.json` and `caster.json`.
2. Restore the account JSON into a new database while the service is stopped:

   ```bash
   rcaster import-accounts \
     --database /var/lib/rcaster/restore.accounts.redb \
     --input /backup/standalone-TIMESTAMP/accounts.json
   ```

3. Compare restored user and mountpoint counts with the backup report, then
   place the backed-up node configuration in the tested configuration path.
4. Start one Caster with maintenance enabled and the restored database/config.
5. Verify administrator login, one NTRIP2 source, one NTRIP1 source where
   configured, and one client subscription before clearing maintenance.
6. Preserve the failed database and config until the recovery review is
   complete. Never point a restored old database at an active cluster without
   reconciling account changes made after the backup.
