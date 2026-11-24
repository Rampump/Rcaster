use std::{
    fs,
    path::{Path, PathBuf},
    process::Stdio,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use rcaster_app::accounts::AccountFile;
use rcaster_service::account_store::AccountStore;
use serde_json::{Value, json};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::TcpStream,
    process::{Child, Command},
    time::{sleep, timeout},
};

const RTCM3_1005: &[u8] = &[
    0xD3, 0x00, 0x13, 0x3E, 0xD0, 0x00, 0x02, 0x36, 0xFD, 0xB8, 0x0D, 0xDE, 0x08, 0x00, 0x5B, 0x2B,
    0xC1, 0x08, 0xA7, 0xB9, 0x8D, 0x3D, 0xD8, 0xAB, 0x37,
];

#[tokio::test]
#[ignore = "requires RCASTER_STR2STR_BIN"]
async fn rtklib_str2str_source_and_client_round_trip_rtcm3() {
    let str2str = std::env::var("RCASTER_STR2STR_BIN").expect("RCASTER_STR2STR_BIN");
    let directory = TempDirectory::new();
    let ports = available_ports(2);
    let caster_port = ports[0];
    let feed_port = ports[1];
    let database = directory.path().join("accounts.redb");
    let config = directory.path().join("caster.json");
    let received = directory.path().join("received.rtcm3");
    write_accounts(&database);
    write_config(&config, &database, caster_port);

    let mut caster = Command::new(env!("CARGO_BIN_EXE_rcaster"))
        .arg("serve")
        .arg("--config")
        .arg(&config)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .expect("start caster");
    wait_for_caster(&mut caster, caster_port).await;

    let mut source = Command::new(&str2str)
        .current_dir(directory.path())
        .arg("-in")
        .arg(format!("tcpsvr://:{feed_port}#rtcm3"))
        .arg("-out")
        .arg(format!("ntrips://:change-me@127.0.0.1:{caster_port}/C63"))
        .arg("-r")
        .arg("200")
        .arg("-s")
        .arg("3000")
        .arg("-t")
        .arg("1")
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .expect("start RTKLIB source");
    let mut feed = connect_with_retry(feed_port, Duration::from_secs(5)).await;
    let payload = RTCM3_1005.repeat(64);
    feed.write_all(&payload).await.expect("feed RTCM3");
    wait_for_mountpoint(caster_port).await;

    let mut client = Command::new(&str2str)
        .current_dir(directory.path())
        .arg("-in")
        .arg(format!("ntrip://127.0.0.1:{caster_port}/C63#rtcm3"))
        .arg("-out")
        .arg(&received)
        .arg("-r")
        .arg("200")
        .arg("-s")
        .arg("2000")
        .arg("-t")
        .arg("1")
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .expect("start RTKLIB client");

    for _ in 0..100 {
        feed.write_all(&payload).await.expect("feed RTCM3");
        sleep(Duration::from_millis(50)).await;
    }
    sleep(Duration::from_millis(500)).await;
    stop(&mut client).await;
    stop(&mut source).await;
    stop(&mut caster).await;

    let bytes = fs::read(&received).expect("read RTKLIB client output");
    assert!(
        bytes
            .windows(RTCM3_1005.len())
            .any(|window| window == RTCM3_1005),
        "RTKLIB client output did not contain the RTCM3 1005 frame"
    );
}

async fn wait_for_caster(caster: &mut Child, port: u16) {
    let stdout = caster.stdout.take().expect("caster stdout");
    let mut lines = BufReader::new(stdout).lines();
    let listener = timeout(Duration::from_secs(5), lines.next_line())
        .await
        .expect("caster startup timeout")
        .expect("read caster stdout")
        .expect("caster listener");
    assert_eq!(listener, format!("listening on 127.0.0.1:{port}"));
}

async fn wait_for_mountpoint(port: u16) {
    timeout(Duration::from_secs(5), async {
        loop {
            if let Ok(mut stream) = TcpStream::connect(("127.0.0.1", port)).await
                && stream.write_all(b"GET /\r\n\r\n").await.is_ok()
            {
                let mut response = String::new();
                if stream.read_to_string(&mut response).await.is_ok()
                    && response.contains("STR;C63;")
                {
                    return;
                }
            }
            sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("source did not become visible in sourcetable");
}

async fn connect_with_retry(port: u16, limit: Duration) -> TcpStream {
    let deadline = tokio::time::Instant::now() + limit;
    loop {
        if let Ok(stream) = TcpStream::connect(("127.0.0.1", port)).await {
            return stream;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "RTKLIB input did not listen on {port}"
        );
        sleep(Duration::from_millis(100)).await;
    }
}

async fn stop(child: &mut Child) {
    let _ = child.kill().await;
    let _ = child.wait().await;
}

fn write_accounts(path: &Path) {
    let source =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../../examples/standalone.accounts.json");
    let account_file = AccountFile::from_slice(&fs::read(source).expect("read example accounts"))
        .expect("parse example accounts");
    let snapshot = account_file.into_snapshot().expect("account snapshot");
    AccountStore::open(path)
        .expect("open account store")
        .replace(&snapshot)
        .expect("write accounts");
}

fn write_config(path: &Path, database: &Path, port: u16) {
    let source =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../../examples/standalone.caster.json");
    let mut config: Value = serde_json::from_slice(&fs::read(source).expect("read example config"))
        .expect("parse example config");
    config["bind"] = json!(format!("127.0.0.1:{port}"));
    config["account_db"] = json!(database);
    fs::write(
        path,
        serde_json::to_vec_pretty(&config).expect("encode config"),
    )
    .expect("write config");
}

fn available_ports(count: usize) -> Vec<u16> {
    let listeners = (0..count)
        .map(|_| std::net::TcpListener::bind("127.0.0.1:0").expect("bind temporary port"))
        .collect::<Vec<_>>();
    let ports = listeners
        .iter()
        .map(|listener| listener.local_addr().expect("temporary address").port())
        .collect();
    drop(listeners);
    ports
}

struct TempDirectory(PathBuf);

impl TempDirectory {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "rcaster-str2str-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        ));
        fs::create_dir_all(&path).expect("create temporary directory");
        Self(path)
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TempDirectory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}
