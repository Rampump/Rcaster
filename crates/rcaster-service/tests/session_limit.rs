use std::{
    collections::BTreeMap,
    net::{IpAddr, Ipv4Addr},
};

use rcaster_service::session_limit::{
    LocalSessionLimiter, SessionKind, SessionLimitConfig, SessionLimitError, SessionRequest,
    SessionUser,
};

#[test]
fn client_and_source_compete_for_the_same_user_slot() {
    let limiter =
        LocalSessionLimiter::new(config(1, BTreeMap::new(), BTreeMap::new())).expect("limiter");
    let client = request(SessionKind::Client);
    let source = request(SessionKind::Source);

    let _permit = limiter.try_acquire(client).expect("first slot");
    assert!(matches!(
        limiter.try_acquire(source),
        Err(SessionLimitError::Exceeded)
    ));
}

#[test]
fn dropping_a_permit_releases_user_and_ip_slots() {
    let limiter = LocalSessionLimiter::new(config(
        1,
        BTreeMap::new(),
        BTreeMap::from([("127.0.0.1".to_owned(), 1)]),
    ))
    .expect("limiter");
    let request = request(SessionKind::Client);

    let permit = limiter.try_acquire(request).expect("first slot");
    drop(permit);
    limiter.try_acquire(request).expect("released slot");
}

#[test]
fn username_limit_overrides_the_default() {
    let mut config = config(
        1,
        BTreeMap::from([("owner".to_owned(), 2)]),
        BTreeMap::new(),
    );
    config.default_max_ip_sessions = -1;
    let limiter = LocalSessionLimiter::new(config).expect("limiter");
    let request = request(SessionKind::Client);

    let _first = limiter.try_acquire(request).expect("first slot");
    let _second = limiter.try_acquire(request).expect("second slot");
    assert!(matches!(
        limiter.try_acquire(request),
        Err(SessionLimitError::Exceeded)
    ));
}

#[test]
fn zero_quota_cidr_is_a_preauth_deny() {
    let limiter = LocalSessionLimiter::new(config(
        -1,
        BTreeMap::new(),
        BTreeMap::from([("10.0.0.0/8".to_owned(), 0)]),
    ))
    .expect("limiter");

    assert!(limiter.is_ip_denied(IpAddr::V4(Ipv4Addr::new(10, 1, 2, 3))));
    assert!(!limiter.is_ip_denied(IpAddr::V4(Ipv4Addr::new(192, 0, 2, 1))));
}

#[test]
fn longest_prefix_ip_rule_wins() {
    let limiter = LocalSessionLimiter::new(config(
        -1,
        BTreeMap::new(),
        BTreeMap::from([("10.0.0.0/8".to_owned(), 0), ("10.1.0.0/16".to_owned(), 5)]),
    ))
    .expect("limiter");

    assert!(!limiter.is_ip_denied(IpAddr::V4(Ipv4Addr::new(10, 1, 2, 3))));
    assert!(limiter.is_ip_denied(IpAddr::V4(Ipv4Addr::new(10, 2, 2, 3))));
}

#[test]
fn ip_quota_counts_clients_but_not_sources() {
    let limiter =
        LocalSessionLimiter::new(config(-1, BTreeMap::new(), BTreeMap::new())).expect("limiter");
    let source = request(SessionKind::Source);
    let _source_permit = limiter.try_acquire(source).expect("source");
    let _client = limiter
        .try_acquire(request(SessionKind::Client))
        .expect("client");
    assert!(matches!(
        limiter.try_acquire(request(SessionKind::Client)),
        Err(SessionLimitError::Exceeded)
    ));
}

#[test]
fn rejects_invalid_quota_values_and_cidr_rules() {
    assert!(matches!(
        LocalSessionLimiter::new(SessionLimitConfig {
            default_max_sessions: -2,
            ..SessionLimitConfig::default()
        }),
        Err(SessionLimitError::InvalidConfig)
    ));
    assert!(matches!(
        LocalSessionLimiter::new(config(
            -1,
            BTreeMap::new(),
            BTreeMap::from([("not-a-cidr".to_owned(), 1)]),
        )),
        Err(SessionLimitError::InvalidConfig)
    ));
}

fn config(
    user_limit: i32,
    per_username: BTreeMap<String, i32>,
    per_ip: BTreeMap<String, i32>,
) -> SessionLimitConfig {
    SessionLimitConfig {
        default_max_sessions: user_limit,
        per_username,
        default_max_ip_sessions: 1,
        per_ip,
    }
}

fn request(kind: SessionKind) -> SessionRequest<'static> {
    SessionRequest {
        user: Some(SessionUser {
            user_id: "owner-id",
            username: "owner",
        }),
        kind,
        remote_ip: IpAddr::V4(Ipv4Addr::LOCALHOST),
    }
}
