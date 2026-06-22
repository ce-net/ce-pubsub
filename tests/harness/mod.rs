//! Ephemeral CE node harness for ce-storage live integration tests.
//!
//! Spawns a real `ce` node in `--ephemeral --no-mine --no-mdns` mode on loopback, polls `/health`
//! until ready, reads the node-written `api.token`, and hands back a wired [`CeClient`]. The node and
//! its temp data dir are killed/removed on `Drop`. Tests call [`live_available`] first and skip
//! gracefully (logging why) when the release binary is absent, so the suite stays green where a node
//! genuinely cannot run.
//!
//! Deliberately does NOT touch the operator's live node on :8844 — every node here binds a fresh
//! port in the task-reserved ranges (api 18900-18999, p2p 14900-14999).

#![allow(dead_code)]

use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU16, Ordering};
use std::time::{Duration, Instant};

use ce_rs::CeClient;

const CE_BIN: &str = "/Users/07lead01/ce-net/.cargo-shared/release/ce";

static API_PORT: AtomicU16 = AtomicU16::new(18930);
static P2P_PORT: AtomicU16 = AtomicU16::new(14930);

// Start each test binary at a process-derived offset so two binaries running back-to-back don't pick
// the same port numbers (libp2p holds the UDP/QUIC port briefly after a node dies, which a pure TCP
// bind-check wouldn't see).
fn offset() -> u16 {
    (std::process::id() % 30) as u16
}

fn free_api_port() -> u16 {
    let _ = API_PORT
        .compare_exchange(18930, 18930 + offset(), Ordering::SeqCst, Ordering::SeqCst);
    free_port(&API_PORT, 18930, 18999, false)
}

fn free_p2p_port() -> u16 {
    let _ = P2P_PORT
        .compare_exchange(14930, 14930 + offset(), Ordering::SeqCst, Ordering::SeqCst);
    free_port(&P2P_PORT, 14930, 14999, true)
}

fn free_port(ctr: &AtomicU16, lo: u16, hi: u16, check_udp: bool) -> u16 {
    for _ in 0..(hi - lo + 1) {
        let mut p = ctr.fetch_add(1, Ordering::SeqCst);
        if p > hi {
            ctr.store(lo, Ordering::SeqCst);
            p = lo;
        }
        let tcp_ok = TcpListener::bind(("127.0.0.1", p)).is_ok();
        // For p2p ports, also confirm the UDP/QUIC port is free (libp2p binds both).
        let udp_ok = !check_udp || std::net::UdpSocket::bind(("127.0.0.1", p)).is_ok();
        if tcp_ok && udp_ok {
            return p;
        }
    }
    lo
}

/// Is a live node runnable here? `false` (with a logged reason) means tests should skip gracefully.
pub fn live_available() -> bool {
    if std::env::var("CE_NO_LIVE").is_ok() {
        eprintln!("[harness] CE_NO_LIVE set — skipping live tests");
        return false;
    }
    if !Path::new(CE_BIN).exists() {
        eprintln!("[harness] release ce binary not found at {CE_BIN} — skipping live tests");
        return false;
    }
    true
}

/// A running ephemeral node: process handle, data dir, and a wired authenticated client.
pub struct Node {
    child: Child,
    data_dir: PathBuf,
    pub api_port: u16,
    pub p2p_port: u16,
    pub node_id: String,
    pub dial: String,
    pub client: CeClient,
    /// The node's data dir, so a test can load its identity for capability minting.
    pub data_dir_path: PathBuf,
}

impl Node {
    pub async fn start(bootstrap: Option<&str>) -> anyhow::Result<Node> {
        let api_port = free_api_port();
        let p2p_port = free_p2p_port();
        let data_dir = std::env::temp_dir().join(format!(
            "ce-pubsub-test-{}-{}",
            std::process::id(),
            api_port
        ));
        std::fs::create_dir_all(&data_dir)?;

        let mut cmd = Command::new(CE_BIN);
        cmd.arg("--data-dir")
            .arg(&data_dir)
            .arg("start")
            .arg("--no-mine")
            .arg("--ephemeral")
            .arg("--no-mdns")
            .arg("--api-port")
            .arg(api_port.to_string())
            .arg("--port")
            .arg(p2p_port.to_string());
        if let Some(b) = bootstrap {
            cmd.arg("--bootstrap").arg(b);
        }
        cmd.env("CE_EXTERNAL_IP", "127.0.0.1");
        cmd.env("CE_NO_AUTOBOOTSTRAP", "1");
        cmd.stdout(Stdio::null()).stderr(Stdio::null());

        let child = cmd.spawn()?;
        let base = format!("http://127.0.0.1:{api_port}");

        let deadline = Instant::now() + Duration::from_secs(40);
        let probe = CeClient::with_token(&base, None);
        loop {
            if probe.health().await.unwrap_or(false) {
                break;
            }
            if Instant::now() > deadline {
                anyhow::bail!("node on api port {api_port} did not become healthy");
            }
            tokio::time::sleep(Duration::from_millis(150)).await;
        }

        let token = read_token(&data_dir, Duration::from_secs(5)).await;
        let client = CeClient::with_token(&base, token);
        let status = client.status().await?;
        let dial = fetch_dial_addr(&base, p2p_port).await.unwrap_or_default();

        Ok(Node {
            child,
            data_dir: data_dir.clone(),
            api_port,
            p2p_port,
            node_id: status.node_id,
            dial,
            client,
            data_dir_path: data_dir,
        })
    }

    pub fn dial_addr(&self) -> String {
        self.dial.clone()
    }
}

#[derive(serde::Deserialize)]
struct BootstrapResp {
    peers: Vec<String>,
}

async fn fetch_dial_addr(base: &str, p2p_port: u16) -> Option<String> {
    let url = format!("{base}/bootstrap");
    let resp: BootstrapResp = reqwest::get(&url).await.ok()?.json().await.ok()?;
    for p in &resp.peers {
        if p.contains("/ip4/127.0.0.1/") && p.contains("/p2p/") {
            return Some(p.clone());
        }
    }
    for p in &resp.peers {
        if let Some(idx) = p.rfind("/p2p/") {
            let pid = &p[idx + 5..];
            if !pid.is_empty() {
                return Some(format!("/ip4/127.0.0.1/tcp/{p2p_port}/p2p/{pid}"));
            }
        }
    }
    None
}

impl Drop for Node {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_dir_all(&self.data_dir);
    }
}

async fn read_token(data_dir: &Path, timeout: Duration) -> Option<String> {
    let path = data_dir.join("api.token");
    let deadline = Instant::now() + timeout;
    loop {
        if let Ok(s) = std::fs::read_to_string(&path) {
            let s = s.trim().to_string();
            if !s.is_empty() {
                return Some(s);
            }
        }
        if Instant::now() > deadline {
            return None;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}
