//! Test-only fake Base RPC endpoints for follow-mode transport tests.
//!
//! - [`FakeChain`]: a block clock that advances the head and records when each block appeared.
//! - [`FakeHttp`]: a minimal HTTP/1.1 JSON-RPC server (keep-alive) that can be healthy, hang
//!   (accept the request and never answer) or fail (HTTP 503).
//! - [`FakeWs`]: a WebSocket JSON-RPC server whose connections follow a script: answer normally,
//!   close with code 1013 "Internal Error" (as base-rpc.publicnode.com did on 2026-10-15), or go
//!   silent (no data, no pong, socket left open).

use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use alloy_primitives::{B256, U256};
use alloy_rpc_types_eth::{Block, BlockTransactions, Header};
use base_common_rpc_types::{BaseHeaderResponse, Transaction};
use futures::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::broadcast,
};
use tokio_tungstenite::tungstenite::{
    Message,
    protocol::{CloseFrame, frame::coding::CloseCode},
};

/// Base mainnet genesis timestamp offset: block `n` has timestamp `GEN + 2n`.
pub(super) const GEN: u64 = 1_686_789_347;

/// A block clock.
#[derive(Debug)]
pub(super) struct FakeChain {
    head: AtomicU64,
    produced: Mutex<HashMap<u64, Instant>>,
    tx: broadcast::Sender<u64>,
}

impl FakeChain {
    /// Starts a chain at `start` that makes a block every `block_time`.
    pub(super) fn spawn(start: u64, block_time: Duration) -> Arc<Self> {
        let (tx, _) = broadcast::channel(1024);
        let chain = Arc::new(Self {
            head: AtomicU64::new(start),
            produced: Mutex::new(HashMap::from([(start, Instant::now())])),
            tx,
        });
        let c = Arc::clone(&chain);
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(block_time);
            tick.tick().await;
            loop {
                tick.tick().await;
                let n = c.head.fetch_add(1, Ordering::SeqCst) + 1;
                c.produced.lock().unwrap().insert(n, Instant::now());
                let _ = c.tx.send(n);
            }
        });
        chain
    }

    pub(super) fn head(&self) -> u64 {
        self.head.load(Ordering::SeqCst)
    }

    /// When block `n` appeared.
    pub(super) fn produced_at(&self, n: u64) -> Option<Instant> {
        self.produced.lock().unwrap().get(&n).copied()
    }

    fn subscribe(&self) -> broadcast::Receiver<u64> {
        self.tx.subscribe()
    }
}

fn hash(n: u64) -> B256 {
    B256::from(U256::from(n).wrapping_add(U256::from(0xbabe_u64)))
}

fn header(n: u64) -> BaseHeaderResponse {
    let inner = alloy_consensus::Header {
        parent_hash: hash(n.saturating_sub(1)),
        number: n,
        timestamp: GEN + 2 * n,
        gas_limit: 400_000_000,
        base_fee_per_gas: Some(1),
        withdrawals_root: Some(B256::ZERO),
        blob_gas_used: Some(0),
        excess_blob_gas: Some(0),
        parent_beacon_block_root: Some(B256::ZERO),
        ..Default::default()
    };
    BaseHeaderResponse::new(Header { hash: hash(n), inner, total_difficulty: None, size: None })
}

fn block_json(n: u64) -> Value {
    let block: Block<Transaction, BaseHeaderResponse> = Block {
        header: header(n),
        uncles: vec![],
        transactions: BlockTransactions::Full(vec![]),
        withdrawals: Some(Default::default()),
    };
    serde_json::to_value(block).expect("block serializes")
}

/// JSON-RPC result for `method`/`params` against `chain` (`None` = unsupported method).
fn rpc_result(chain: &FakeChain, lag: u64, method: &str, params: &Value) -> Option<Value> {
    let head = chain.head().saturating_sub(lag);
    match method {
        "eth_chainId" => Some(json!("0x2105")),
        "eth_blockNumber" => Some(json!(format!("{head:#x}"))),
        "eth_getBlockByNumber" => {
            let tag = params.get(0).and_then(Value::as_str).unwrap_or("latest");
            let n = match tag {
                "latest" | "pending" => Some(head),
                "safe" => Some(head.saturating_sub(10)),
                "finalized" => Some(head.saturating_sub(20)),
                t => u64::from_str_radix(t.trim_start_matches("0x"), 16).ok(),
            };
            Some(match n {
                Some(n) if n <= head => block_json(n),
                _ => Value::Null,
            })
        }
        _ => None,
    }
}

fn rpc_response(chain: &FakeChain, lag: u64, req: &Value) -> Value {
    let id = req.get("id").cloned().unwrap_or(Value::Null);
    let method = req.get("method").and_then(Value::as_str).unwrap_or_default();
    let params = req.get("params").cloned().unwrap_or(Value::Null);
    rpc_result(chain, lag, method, &params).map_or_else(
        || json!({"jsonrpc": "2.0", "id": id, "error": {"code": -32601, "message": "method not found"}}),
        |result| json!({"jsonrpc": "2.0", "id": id, "result": result}),
    )
}

/// HTTP endpoint behaviour.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum HttpMode {
    Healthy,
    /// Accepts the request and never answers.
    Hang,
    /// Answers HTTP 503.
    Fail,
}

/// A minimal HTTP/1.1 JSON-RPC server.
#[derive(Debug, Clone)]
pub(super) struct FakeHttp {
    pub(super) addr: SocketAddr,
    mode: Arc<Mutex<HttpMode>>,
    calls: Arc<AtomicUsize>,
    payload_calls: Arc<AtomicUsize>,
    lag: Arc<AtomicU64>,
}

impl FakeHttp {
    pub(super) async fn spawn(chain: Arc<FakeChain>, mode: HttpMode) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let s = Self {
            addr: listener.local_addr().unwrap(),
            mode: Arc::new(Mutex::new(mode)),
            calls: Arc::new(AtomicUsize::new(0)),
            payload_calls: Arc::new(AtomicUsize::new(0)),
            lag: Arc::new(AtomicU64::new(0)),
        };
        let srv = s.clone();
        tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                let (srv, chain) = (srv.clone(), Arc::clone(&chain));
                tokio::spawn(async move { srv.serve(stream, chain).await });
            }
        });
        s
    }

    pub(super) fn url(&self) -> url::Url {
        url::Url::parse(&format!("http://{}", self.addr)).unwrap()
    }

    pub(super) fn set_mode(&self, mode: HttpMode) {
        *self.mode.lock().unwrap() = mode;
    }

    /// Serve the chain `blocks` behind its head (a lagging backend).
    pub(super) fn set_lag(&self, blocks: u64) {
        self.lag.store(blocks, Ordering::SeqCst);
    }

    /// `eth_getBlockByNumber` requests for a numbered block (payload fetches).
    pub(super) fn payload_calls(&self) -> usize {
        self.payload_calls.load(Ordering::SeqCst)
    }

    pub(super) fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }

    async fn serve(&self, mut stream: TcpStream, chain: Arc<FakeChain>) {
        let mut buf = Vec::new();
        loop {
            // headers
            let header_end = loop {
                if let Some(i) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                    break i + 4;
                }
                let mut chunk = [0u8; 8192];
                match stream.read(&mut chunk).await {
                    Ok(0) | Err(_) => return,
                    Ok(k) => buf.extend_from_slice(&chunk[..k]),
                }
            };
            let head = String::from_utf8_lossy(&buf[..header_end]).to_ascii_lowercase();
            let len = head
                .lines()
                .find_map(|l| l.strip_prefix("content-length:"))
                .and_then(|v| v.trim().parse::<usize>().ok())
                .unwrap_or(0);
            while buf.len() < header_end + len {
                let mut chunk = [0u8; 8192];
                match stream.read(&mut chunk).await {
                    Ok(0) | Err(_) => return,
                    Ok(k) => buf.extend_from_slice(&chunk[..k]),
                }
            }
            let body: Vec<u8> = buf.drain(..header_end + len).skip(header_end).collect();
            self.calls.fetch_add(1, Ordering::SeqCst);
            if body.windows(20).any(|w| w == b"eth_getBlockByNumber")
                && body.windows(3).any(|w| w == b"\"0x")
                && !body.windows(8).any(|w| w == b"\"latest\"")
                && !body.windows(6).any(|w| w == b"\"safe\"")
                && !body.windows(11).any(|w| w == b"\"finalized\"")
            {
                self.payload_calls.fetch_add(1, Ordering::SeqCst);
            }
            let mode = *self.mode.lock().unwrap();
            let (status, payload) = match mode {
                HttpMode::Hang => {
                    tokio::time::sleep(Duration::from_secs(3600)).await;
                    return;
                }
                HttpMode::Fail => ("503 Service Unavailable", "{}".to_owned()),
                HttpMode::Healthy => {
                    let req: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);
                    let resp = if let Value::Array(reqs) = &req {
                        Value::Array(
                            reqs.iter()
                                .map(|r| rpc_response(&chain, self.lag.load(Ordering::SeqCst), r))
                                .collect(),
                        )
                    } else {
                        rpc_response(&chain, self.lag.load(Ordering::SeqCst), &req)
                    };
                    ("200 OK", resp.to_string())
                }
            };
            let out = format!(
                "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{payload}",
                payload.len()
            );
            if stream.write_all(out.as_bytes()).await.is_err() {
                return;
            }
        }
    }
}

/// Script for one WebSocket connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum WsPlan {
    /// Answers requests and pushes heads.
    Normal,
    /// Normal until `after`, then closes with 1013 "Internal Error".
    CloseAt(Duration),
    /// Answers the subscription, then closes with 1013.
    CloseAfterSubscribe,
    /// Closes with 1013 on the first message of any kind.
    CloseOnFirstMessage,
    /// Normal until `after`, then silent: reads and writes nothing (pings go unanswered) and
    /// keeps the socket open.
    SilentAfter(Duration),
}

/// A scripted WebSocket JSON-RPC server.
#[derive(Debug, Clone)]
pub(super) struct FakeWs {
    pub(super) addr: SocketAddr,
    connections: Arc<AtomicUsize>,
}

impl FakeWs {
    /// Connection `i` follows `plans[i]`; later connections follow `then`.
    pub(super) async fn spawn(chain: Arc<FakeChain>, plans: Vec<WsPlan>, then: WsPlan) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let s = Self {
            addr: listener.local_addr().unwrap(),
            connections: Arc::new(AtomicUsize::new(0)),
        };
        let conns = Arc::clone(&s.connections);
        let plans = Arc::new(plans);
        tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                let i = conns.fetch_add(1, Ordering::SeqCst);
                let plan = plans.get(i).copied().unwrap_or(then);
                let chain = Arc::clone(&chain);
                tokio::spawn(async move { serve_ws(stream, chain, plan).await });
            }
        });
        s
    }

    pub(super) fn url(&self) -> url::Url {
        url::Url::parse(&format!("ws://{}", self.addr)).unwrap()
    }

    pub(super) fn connections(&self) -> usize {
        self.connections.load(Ordering::SeqCst)
    }
}

fn close_1013() -> Message {
    Message::Close(Some(CloseFrame { code: CloseCode::Again, reason: "Internal Error".into() }))
}

async fn serve_ws(stream: TcpStream, chain: Arc<FakeChain>, plan: WsPlan) {
    let Ok(mut ws) = tokio_tungstenite::accept_async(stream).await else { return };
    let started = tokio::time::Instant::now();
    let mut heads = chain.subscribe();
    let mut subscribed = false;
    let deadline = match plan {
        WsPlan::CloseAt(d) | WsPlan::SilentAfter(d) => Some(started + d),
        _ => None,
    };
    loop {
        let wake = async {
            match deadline {
                Some(d) => tokio::time::sleep_until(d).await,
                None => std::future::pending().await,
            }
        };
        tokio::select! {
            _ = wake => {
                if matches!(plan, WsPlan::SilentAfter(_)) {
                    // hold the socket open, never read or write again
                    tokio::time::sleep(Duration::from_secs(3600)).await;
                    drop(ws);
                    return;
                }
                let _ = ws.send(close_1013()).await;
                return;
            }
            n = heads.recv() => {
                let Ok(n) = n else { continue };
                if subscribed {
                    let note = json!({"jsonrpc": "2.0", "method": "eth_subscription",
                        "params": {"subscription": "0x1", "result": header(n)}});
                    if ws.send(Message::Text(note.to_string().into())).await.is_err() { return; }
                }
            }
            msg = ws.next() => {
                let Some(Ok(msg)) = msg else { return };
                let Message::Text(text) = msg else { continue };
                if plan == WsPlan::CloseOnFirstMessage {
                    let _ = ws.send(close_1013()).await;
                    return;
                }
                let req: Value = serde_json::from_str(&text).unwrap_or(Value::Null);
                let method = req.get("method").and_then(Value::as_str).unwrap_or_default();
                let resp = if method == "eth_subscribe" {
                    subscribed = true;
                    json!({"jsonrpc": "2.0", "id": req.get("id").cloned().unwrap_or(Value::Null), "result": "0x1"})
                } else if method == "eth_unsubscribe" {
                    json!({"jsonrpc": "2.0", "id": req.get("id").cloned().unwrap_or(Value::Null), "result": true})
                } else {
                    rpc_response(&chain, 0, &req)
                };
                if ws.send(Message::Text(resp.to_string().into())).await.is_err() { return; }
                if method == "eth_subscribe" && plan == WsPlan::CloseAfterSubscribe {
                    let _ = ws.send(close_1013()).await;
                    return;
                }
            }
        }
    }
}
