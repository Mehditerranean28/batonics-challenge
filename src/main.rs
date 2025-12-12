// src/main.rs
mod metrics;

use anyhow::{anyhow, Context, Result};
use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        Query, State,
    },
    http::StatusCode,
    response::IntoResponse,
    routing::get,
    Router,
};
use bytes::Bytes;
use clap::{Parser as ClapParser, Subcommand};
use dashmap::DashMap;
use memmap2::Mmap;
use serde::Serialize;
use serde_json::json;
use std::{
    cell::RefCell,
    fs::File,
    io::{BufReader, Read},
    net::SocketAddr,
    path::PathBuf,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, RwLock,
    },
    time::{Duration, Instant},
};
use tokio::{
    fs as tokio_fs,
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{mpsc, watch},
};
use tracing::{error, info};

use batonics_challenge::{
    book::{ApplyError, LevelPxQty, OrderBook, Side},
    parser::{Parser, SymbolId, SymbolIntern, WireMode},
    wire,
};
use crate::metrics::Metrics;

#[derive(ClapParser, Debug)]
#[command(name = "batonics-challenge", version)]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand, Debug)]
enum Cmd {
    Replay {
        #[arg(long, default_value = "0.0.0.0:9000")]
        bind: SocketAddr,
        #[arg(long)]
        file: PathBuf,
        #[arg(long, default_value_t = 1 << 20)]
        chunk: usize,
        #[arg(long, default_value_t = 0)]
        max_bps: u64,
    },
    Run {
        #[arg(long)]
        connect: Option<SocketAddr>,
        #[arg(long)]
        file: Option<PathBuf>,
        #[arg(long, default_value = "0.0.0.0:8080")]
        http_bind: SocketAddr,
        #[arg(long, default_value = "final_snapshot.json")]
        out: PathBuf,
        /// Depth=0 means full depth (all price levels)
        #[arg(long, default_value_t = 50)]
        depth: usize,
        /// Number of independent book shards (parallel symbol partitions).
        #[arg(long)]
        shards: Option<usize>,
        /// Publish snapshot interval (ms). Set 0 to disable time-based snapshots.
        #[arg(long, default_value_t = 250)]
        snapshot_interval_ms: u64,
        /// Publish snapshot every N messages per shard. Set 0 to disable count-based snapshots.
        #[arg(long, default_value_t = 250_000)]
        snapshot_every_n: u64,
        /// TCP reconnect backoff min (ms).
        #[arg(long, default_value_t = 50)]
        reconnect_min_ms: u64,
        /// TCP reconnect backoff max (ms).
        #[arg(long, default_value_t = 1000)]
        reconnect_max_ms: u64,
    },
}

#[derive(Clone)]
struct AppState {
    metrics: Arc<Metrics>,
    symbol_names: Arc<RwLock<Vec<String>>>,
    latest_levels: Arc<DashMap<SymbolId, SymbolLevels>>,
    bbo_tx: watch::Sender<Bytes>,
    bbo_rx: watch::Receiver<Bytes>,
    snap_tx: watch::Sender<Bytes>,
    snap_rx: watch::Receiver<Bytes>,
    ws_bbo_clients: Arc<AtomicUsize>,
    ws_snap_clients: Arc<AtomicUsize>,
    final_json: Arc<tokio::sync::RwLock<Bytes>>,
}

#[derive(Clone, Debug)]
struct SymbolLevels {
    bids: Vec<LevelPxQty>,
    asks: Vec<LevelPxQty>,
    seq: u64,
    ts_ns: u64,
}

#[derive(Debug)]
enum PubEvent {
    Bbo {
        symbol: SymbolId,
        seq: u64,
        ts_ns: u64,
        bid_px: Option<i64>,
        bid_qty: u64,
        ask_px: Option<i64>,
        ask_qty: u64,
    },
    Snapshot {
        symbol: SymbolId,
        seq: u64,
        ts_ns: u64,
        bids: Vec<LevelPxQty>,
        asks: Vec<LevelPxQty>,
    },
    Resync {
        seq: u64,
        ts_ns: u64,
    },
}

#[derive(Debug)]
enum ShardMsg {
    Mbo {
        symbol: SymbolId,
        op: batonics_challenge::book::Op,
        seq: u64,
        ts_ns: u64,
    },
    Mbp10 {
        symbol: SymbolId,
        seq: u64,
        ts_ns: u64,
        bids: Vec<LevelPxQty>,
        asks: Vec<LevelPxQty>,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(std::env::var("RUST_LOG").unwrap_or_else(|_| "info".to_string()))
        .init();

    let cli = Cli::parse();
    match cli.cmd {
        Cmd::Replay { bind, file, chunk, max_bps } => replay_server(bind, file, chunk, max_bps).await,
        Cmd::Run {
            connect,
            file,
            http_bind,
            out,
            depth,
            shards,
            snapshot_interval_ms,
            snapshot_every_n,
            reconnect_min_ms,
            reconnect_max_ms,
        } => {
            run_engine(
                connect,
                file,
                http_bind,
                out,
                depth,
                shards,
                snapshot_interval_ms,
                snapshot_every_n,
                reconnect_min_ms,
                reconnect_max_ms,
            )
            .await
        }
    }
}

async fn replay_server(bind: SocketAddr, file: PathBuf, chunk: usize, max_bps: u64) -> Result<()> {
    let f = File::open(&file).with_context(|| format!("open replay file {:?}", file))?;
    let mmap = unsafe { Mmap::map(&f)? };
    info!("replay: bind={bind} file={:?} bytes={}", file, mmap.len());

    let listener = TcpListener::bind(bind).await?;
    loop {
        let (mut sock, addr) = listener.accept().await?;
        let buf = mmap.as_ref();

        let res = async {
            let mut pos = 0usize;
            let mut sent_this_sec: u64 = 0;
            let mut window = Instant::now();

            while pos < buf.len() {
                let end = (pos + chunk).min(buf.len());
                sock.write_all(&buf[pos..end]).await?;
                let wrote = (end - pos) as u64;
                pos = end;

                if max_bps > 0 {
                    sent_this_sec += wrote;
                    let elapsed = window.elapsed();
                    if elapsed >= Duration::from_secs(1) {
                        sent_this_sec = 0;
                        window = Instant::now();
                    } else if sent_this_sec > max_bps {
                        let rem = Duration::from_secs(1).saturating_sub(elapsed);
                        tokio::time::sleep(rem).await;
                        sent_this_sec = 0;
                        window = Instant::now();
                    }
                }
            }

            sock.shutdown().await?;
            Ok::<(), anyhow::Error>(())
        }
        .await;

        if let Err(e) = res {
            error!("replay client {addr} error: {e:#}");
        }
    }
}

async fn process_file_ndjson(
    path: PathBuf,
    symbol_names: Arc<RwLock<Vec<String>>>,
    metrics: Arc<Metrics>,
    depth: usize,
) -> Result<hashbrown::HashMap<SymbolId, SymbolLevels>> {
    tokio::task::spawn_blocking(move || -> Result<_> {
        let f = File::open(&path).with_context(|| format!("open {:?}", path))?;
        let mut syms = SymbolIntern::new(symbol_names);

        let mut books: hashbrown::HashMap<SymbolId, OrderBook> = hashbrown::HashMap::new();
        let mut last_seq: hashbrown::HashMap<SymbolId, u64> = hashbrown::HashMap::new();
        let mut last_ts: hashbrown::HashMap<SymbolId, u64> = hashbrown::HashMap::new();

        Parser::ndjson_decode_reader(f, &mut syms, |p| {
            metrics.inc_total();
            last_ts.insert(p.symbol, p.ts_ns);

            if p.seq != 0 {
                let prev = last_seq.get(&p.symbol).copied().unwrap_or(0);
                if prev != 0 && p.seq != prev + 1 {
                    metrics.inc_seq_gap();
                    books.entry(p.symbol).or_insert_with(OrderBook::new).clear();
                }
                last_seq.insert(p.symbol, p.seq);
            }

            let b = books.entry(p.symbol).or_insert_with(OrderBook::new);
            let out = b.apply(p.op);
            if out.err != ApplyError::None {
                metrics.inc_parse_err();
                b.clear();
            }
        })?;

        let mut snaps = hashbrown::HashMap::new();
        for (&sym, b) in books.iter() {
            let bids = b.levels_depth(Side::Bid, depth);
            let asks = b.levels_depth(Side::Ask, depth);
            if bids.is_empty() && asks.is_empty() { continue; }
            snaps.insert(sym, SymbolLevels {
                bids,
                asks,
                seq: *last_seq.get(&sym).unwrap_or(&0),
                ts_ns: *last_ts.get(&sym).unwrap_or(&0),
            });
        }

        Ok(snaps)
    }).await?
}


async fn run_engine(
    connect: Option<SocketAddr>,
    file: Option<PathBuf>,
    http_bind: SocketAddr,
    out: PathBuf,
    depth: usize,
    shards: Option<usize>,
    snapshot_interval_ms: u64,
    snapshot_every_n: u64,
    reconnect_min_ms: u64,
    reconnect_max_ms: u64,
) -> Result<()> {
    if connect.is_none() && file.is_none() {
        return Err(anyhow!("need --connect OR --file"));
    }
    if connect.is_some() && file.is_some() {
        return Err(anyhow!("use only one of --connect/--file"));
    }

    // FILE MODE: fast offline path (no shards).
    if let Some(path) = file {
        let metrics = Arc::new(Metrics::new());
        let symbol_names: Arc<RwLock<Vec<String>>> = Arc::new(RwLock::new(Vec::new()));
        let latest_levels = Arc::new(DashMap::<SymbolId, SymbolLevels>::new());

        // sniff first bytes
        let mut f = File::open(&path).with_context(|| format!("open {:?}", path))?;
        let mut head = [0u8; 4096];
        let n = f.read(&mut head)?;
        let mode = Parser::detect_mode(&head[..n]);

        let snaps = match mode {
            WireMode::Dbn => process_file_dbn_fast(path.clone(), symbol_names.clone(), metrics.clone(), depth).await?,
            WireMode::Ndjson => process_file_ndjson(path.clone(), symbol_names.clone(), metrics.clone(), depth).await?,
            _ => return Err(anyhow!("file mode supports DBN or NDJSON; detected {mode:?}")),
        };

        for (sym, lvl) in snaps {
            latest_levels.insert(sym, lvl);
        }

        let final_text = build_final_json(symbol_names.clone(), latest_levels.clone()).await;
        tokio_fs::write(&out, final_text).await?;
        info!("wrote final snapshot to {:?}", out);
        return Ok(());
    }

    // TCP MODE: live engine with shards.
    let shard_n = shards.unwrap_or_else(|| {
        std::thread::available_parallelism()
            .map(|n| n.get().max(1))
            .unwrap_or(4)
    });

    info!("engine: shards={shard_n} depth={depth}");

    let metrics = Arc::new(Metrics::new());
    let symbol_names: Arc<RwLock<Vec<String>>> = Arc::new(RwLock::new(Vec::new()));
    let latest_levels = Arc::new(DashMap::<SymbolId, SymbolLevels>::new());
    let final_json =
        Arc::new(tokio::sync::RwLock::new(Bytes::from_static(b"{\"symbols\":{},\"type\":\"final\"}")));

    let (bbo_tx, bbo_rx) = watch::channel(Bytes::from_static(b""));
    let (snap_tx, snap_rx) = watch::channel(Bytes::from_static(b""));

    let state = AppState {
        metrics: metrics.clone(),
        symbol_names: symbol_names.clone(),
        latest_levels: latest_levels.clone(),
        bbo_tx,
        bbo_rx,
        snap_tx,
        snap_rx,
        ws_bbo_clients: Arc::new(AtomicUsize::new(0)),
        ws_snap_clients: Arc::new(AtomicUsize::new(0)),
        final_json: final_json.clone(),
    };

    let api = build_api(state.clone());
    let http_task = tokio::spawn(async move {
        info!("http: listening on {http_bind}");
        let listener = tokio::net::TcpListener::bind(http_bind).await?;
        axum::serve(listener, api).await?;
        Ok::<(), anyhow::Error>(())
    });

    let (pub_tx, pub_rx) = mpsc::channel::<PubEvent>(32_768);
    let pub_task = tokio::spawn(publisher_loop(state.clone(), pub_rx));

    let mut shard_txs = Vec::with_capacity(shard_n);
    for shard_id in 0..shard_n {
        let (tx, rx) = mpsc::channel::<ShardMsg>(32_768);
        shard_txs.push(tx);
        tokio::spawn(shard_loop(
            shard_id,
            rx,
            pub_tx.clone(),
            metrics.clone(),
            depth,
            snapshot_interval_ms,
            snapshot_every_n,
        ));
    }

    process_tcp_with_reconnect(
        connect.unwrap(),
        symbol_names.clone(),
        shard_txs,
        metrics.clone(),
        reconnect_min_ms,
        reconnect_max_ms,
    )
    .await?;

    pub_task.abort();
    http_task.abort();
    Ok(())
}

fn build_api(state: AppState) -> Router {
    Router::new()
        .route("/metrics", get(metrics_handler))
        .route("/symbols", get(symbols_handler))
        .route("/book", get(book_handler))
        .route("/ws/bbo", get(ws_bbo_handler))
        .route("/ws/snapshot", get(ws_snapshot_handler))
        .with_state(state)
}

async fn metrics_handler(State(st): State<AppState>) -> impl IntoResponse {
    (StatusCode::OK, st.metrics.prometheus_text())
}

async fn symbols_handler(State(st): State<AppState>) -> impl IntoResponse {
    #[derive(Serialize)]
    struct Sym {
        id: u32,
        name: String,
    }

    let names = st.symbol_names.read().unwrap();
    let out: Vec<Sym> = names
        .iter()
        .enumerate()
        .map(|(i, s)| Sym {
            id: i as u32,
            name: s.clone(),
        })
        .collect();
    (StatusCode::OK, axum::Json(out))
}

#[derive(serde::Deserialize)]
struct BookQuery {
    symbol: Option<String>,
}

async fn book_handler(State(st): State<AppState>, Query(q): Query<BookQuery>) -> impl IntoResponse {
    if q.symbol.is_none() {
        let b = st.final_json.read().await.clone();
        return (StatusCode::OK, b);
    }

    let sym = q.symbol.unwrap();
    let names = st.symbol_names.read().unwrap();
    let id = names.iter().position(|s| s == &sym).map(|i| i as u32);
    drop(names);

    if let Some(id) = id {
        if let Some(v) = st.latest_levels.get(&id) {
            let payload = json!({
                "type": "snapshot",
                "symbol": sym,
                "seq": v.seq,
                "ts_ns": v.ts_ns,
                "bids": v.bids,
                "asks": v.asks
            })
            .to_string();
            return (StatusCode::OK, Bytes::from(payload));
        }
    }

    (StatusCode::NOT_FOUND, Bytes::from_static(b"{}"))
}

async fn ws_bbo_handler(ws: WebSocketUpgrade, State(st): State<AppState>) -> impl IntoResponse {
    ws.on_upgrade(move |socket| ws_watch_loop(socket, st.bbo_rx.clone(), st.ws_bbo_clients.clone()))
}

async fn ws_snapshot_handler(ws: WebSocketUpgrade, State(st): State<AppState>) -> impl IntoResponse {
    ws.on_upgrade(move |socket| ws_watch_loop(socket, st.snap_rx.clone(), st.ws_snap_clients.clone()))
}

async fn ws_watch_loop(
    mut socket: WebSocket,
    mut rx: watch::Receiver<Bytes>,
    clients: Arc<AtomicUsize>,
) {
    clients.fetch_add(1, Ordering::Relaxed);
    let cur = rx.borrow().clone();
    if !cur.is_empty() {
        let _ = socket.send(Message::Binary(cur.to_vec())).await;
    }

    loop {
        tokio::select! {
            r = rx.changed() => {
                if r.is_err() { break; }
                let msg = rx.borrow().clone();
                if msg.is_empty() { continue; }
                if socket.send(Message::Binary(msg.to_vec())).await.is_err() { break; }
            }
            _ = tokio::time::sleep(Duration::from_secs(15)) => {
                if socket.send(Message::Ping(vec![])).await.is_err() { break; }
            }
        }
    }

    clients.fetch_sub(1, Ordering::Relaxed);
}

async fn publisher_loop(st: AppState, mut rx: mpsc::Receiver<PubEvent>) -> Result<()> {
    while let Some(ev) = rx.recv().await {
        match ev {
            PubEvent::Bbo { symbol, seq, ts_ns, bid_px, bid_qty, ask_px, ask_qty } => {
                if st.ws_bbo_clients.load(Ordering::Relaxed) != 0 {
                    let frame = wire::encode_bbo(symbol, seq, ts_ns, bid_px, bid_qty, ask_px, ask_qty);
                    let _ = st.bbo_tx.send_replace(frame);
                }
                st.metrics.inc_pub_bbo();
            }
            PubEvent::Snapshot { symbol, seq, ts_ns, bids, asks } => {
                st.latest_levels.insert(
                    symbol,
                    SymbolLevels { bids: bids.clone(), asks: asks.clone(), seq, ts_ns },
                );
                if st.ws_snap_clients.load(Ordering::Relaxed) != 0 {
                    let frame = wire::encode_snapshot(symbol, seq, ts_ns, &bids, &asks);
                    let _ = st.snap_tx.send_replace(frame);
                }
                st.metrics.inc_pub_snap();
            }
            PubEvent::Resync { seq, ts_ns } => {
                if st.ws_snap_clients.load(Ordering::Relaxed) != 0 {
                    let frame = wire::encode_resync(seq, ts_ns);
                    let _ = st.snap_tx.send_replace(frame);
                }
                if st.ws_bbo_clients.load(Ordering::Relaxed) != 0 {
                    let frame = wire::encode_resync(seq, ts_ns);
                    let _ = st.bbo_tx.send_replace(frame);
                }
            }
        }
    }
    Ok(())
}

async fn shard_loop(
    shard_id: usize,
    mut rx: mpsc::Receiver<ShardMsg>,
    pub_tx: mpsc::Sender<PubEvent>,
    metrics: Arc<Metrics>,
    depth: usize,
    snapshot_interval_ms: u64,
    snapshot_every_n: u64,
) {
    #[derive(Default)]
    struct SymState {
        book: OrderBook,
        last_seq: u64,
        seen_mbp10: bool,
    }

    let mut st: hashbrown::HashMap<SymbolId, SymState> = hashbrown::HashMap::new();
    let mut msg_count: u64 = 0;
    let mut last_snap = Instant::now();

    let snap_interval = if snapshot_interval_ms == 0 {
        None
    } else {
        Some(Duration::from_millis(snapshot_interval_ms))
    };

    info!("shard[{shard_id}] started");

    while let Some(m) = rx.recv().await {
        msg_count += 1;

        match m {
            ShardMsg::Mbp10 { symbol, seq, ts_ns, mut bids, mut asks } => {
                let e = st.entry(symbol).or_insert_with(|| {
                    let mut b = OrderBook::new();
                    b.reserve_orders(250_000);
                    SymState { book: b, ..Default::default() }
                });

                if seq != 0 && e.last_seq != 0 && seq != e.last_seq + 1 {
                    metrics.inc_seq_gap();
                    e.book.clear();
                    e.seen_mbp10 = false;
                    let _ = pub_tx.try_send(PubEvent::Resync { seq, ts_ns });
                }

                e.last_seq = seq;
                e.seen_mbp10 = true;
                e.book.clear(); // MBP10 authoritative; do not mix.

                if depth != 0 {
                    bids.truncate(depth);
                    asks.truncate(depth);
                }

                let (bid_px, bid_qty) = bids.first().map(|l| (Some(l.px), l.qty)).unwrap_or((None, 0));
                let (ask_px, ask_qty) = asks.first().map(|l| (Some(l.px), l.qty)).unwrap_or((None, 0));
                let _ = pub_tx.try_send(PubEvent::Bbo { symbol, seq, ts_ns, bid_px, bid_qty, ask_px, ask_qty });

                let _ = pub_tx.try_send(PubEvent::Snapshot { symbol, seq, ts_ns, bids, asks });
                continue;
            }

            ShardMsg::Mbo { symbol, op, seq, ts_ns } => {
                let e = st.entry(symbol).or_insert_with(|| {
                    let mut b = OrderBook::new();
                    b.reserve_orders(250_000);
                    SymState { book: b, ..Default::default() }
                });

                if e.seen_mbp10 {
                    // Once MBP10 exists for a symbol, ignore MBO to avoid double-counting.
                    continue;
                }

                if seq != 0 {
                    if e.last_seq != 0 && seq != e.last_seq + 1 {
                        metrics.inc_seq_gap();
                        e.book.clear();
                        e.seen_mbp10 = false;
                        let _ = pub_tx.try_send(PubEvent::Resync { seq, ts_ns });
                    }
                    e.last_seq = seq;
                }

                let t0 = Instant::now();
                let out = e.book.apply(op);
                metrics.record_engine(t0.elapsed());
                metrics.inc_total();

                if out.err != ApplyError::None {
                    metrics.inc_parse_err();
                    e.book.clear();
                    e.seen_mbp10 = false;
                    let _ = pub_tx.try_send(PubEvent::Resync { seq, ts_ns });
                    continue;
                }

                if out.bbo_changed {
                    let b = e.book.bbo();
                    let _ = pub_tx.try_send(PubEvent::Bbo {
                        symbol,
                        seq,
                        ts_ns,
                        bid_px: b.bid_px,
                        bid_qty: b.bid_qty,
                        ask_px: b.ask_px,
                        ask_qty: b.ask_qty,
                    });
                }

                let mut do_snap = false;
                if snapshot_every_n != 0 && (msg_count % snapshot_every_n == 0) {
                    do_snap = true;
                }
                if let Some(iv) = snap_interval {
                    if last_snap.elapsed() >= iv {
                        do_snap = true;
                    }
                }

                if do_snap {
                    last_snap = Instant::now();
                    let bids = e.book.levels_depth(Side::Bid, depth);
                    let asks = e.book.levels_depth(Side::Ask, depth);
                    let _ = pub_tx.try_send(PubEvent::Snapshot { symbol, seq, ts_ns, bids, asks });
                }
            }
        }
    }

    info!("shard[{shard_id}] stopped");
}

async fn process_file_dbn_fast(
    path: PathBuf,
    symbol_names: Arc<RwLock<Vec<String>>>,
    metrics: Arc<Metrics>,
    depth: usize,
) -> Result<hashbrown::HashMap<SymbolId, SymbolLevels>> {
    tokio::task::spawn_blocking(move || -> Result<hashbrown::HashMap<SymbolId, SymbolLevels>> {
        let f = File::open(&path).with_context(|| format!("open file {:?}", path))?;
        let mmap = unsafe { Mmap::map(&f)? };
        let bytes = mmap.as_ref();

        let mode = Parser::detect_mode(bytes);
        info!("input(file): {:?} mode={mode:?} bytes={}", path, bytes.len());
        if !matches!(mode, WireMode::Dbn) {
            return Err(anyhow!("file mode supports DBN only"));
        }

        let mut syms = SymbolIntern::new(symbol_names);

        struct FileState {
            books: hashbrown::HashMap<SymbolId, OrderBook>,
            last_seq: hashbrown::HashMap<SymbolId, u64>,
            last_ts: hashbrown::HashMap<SymbolId, u64>,
            snaps: hashbrown::HashMap<SymbolId, SymbolLevels>,
        }

        let st = RefCell::new(FileState {
            books: hashbrown::HashMap::new(),
            last_seq: hashbrown::HashMap::new(),
            last_ts: hashbrown::HashMap::new(),
            snaps: hashbrown::HashMap::new(),
        });

        Parser::dbn_decode_bytes(
            bytes,
            &mut syms,
            |p| {
                metrics.inc_total();

                let mut s = st.borrow_mut();
                s.last_ts.insert(p.symbol, p.ts_ns);

                if p.seq != 0 {
                    let prev = s.last_seq.get(&p.symbol).copied().unwrap_or(0);
                    if prev != 0 && p.seq != prev + 1 {
                        metrics.inc_seq_gap();
                        s.books.entry(p.symbol).or_insert_with(|| {
                            let mut b = OrderBook::new();
                            b.reserve_orders(250_000);
                            b
                        }).clear();
                    }
                    s.last_seq.insert(p.symbol, p.seq);
                }

                let book = s.books.entry(p.symbol).or_insert_with(|| {
                    let mut b = OrderBook::new();
                    b.reserve_orders(250_000);
                    b
                });

                let out = book.apply(p.op);
                if out.err != ApplyError::None {
                    metrics.inc_parse_err();
                    book.clear();
                }
            },
            |symbol, seq, ts_ns, mut bids, mut asks| {
                if depth != 0 {
                    bids.truncate(depth);
                    asks.truncate(depth);
                }
                let mut s = st.borrow_mut();
                s.last_seq.insert(symbol, seq);
                s.last_ts.insert(symbol, ts_ns);

                let replace = match s.snaps.get(&symbol) {
                    None => true,
                    Some(cur) => seq >= cur.seq,
                };
                if replace {
                    s.snaps.insert(
                        symbol,
                        SymbolLevels { bids, asks, seq, ts_ns },
                    );
                }
            },
        )?;

        let mut s = st.into_inner();

        for (sym, book) in s.books.iter() {
            if s.snaps.contains_key(sym) {
                continue;
            }
            let bids = book.levels_depth(Side::Bid, depth);
            let asks = book.levels_depth(Side::Ask, depth);
            if bids.is_empty() && asks.is_empty() {
                continue;
            }
            let seq = *s.last_seq.get(sym).unwrap_or(&0);
            let ts_ns = *s.last_ts.get(sym).unwrap_or(&0);
            s.snaps.insert(*sym, SymbolLevels { bids, asks, seq, ts_ns });
        }

        Ok(s.snaps)
    })
    .await?
}

async fn process_tcp_with_reconnect(
    connect: SocketAddr,
    symbol_names: Arc<RwLock<Vec<String>>>,
    shard_txs: Vec<mpsc::Sender<ShardMsg>>,
    metrics: Arc<Metrics>,
    min_ms: u64,
    max_ms: u64,
) -> Result<()> {
    let shard_n = shard_txs.len().max(1);
    let mut backoff = min_ms.max(1);
    let max_ms = max_ms.max(backoff);

    loop {
        let connect = connect;
        let symbol_names = symbol_names.clone();
        let shard_txs = shard_txs.clone();
        let metrics = metrics.clone();

        let res = tokio::task::spawn_blocking(move || -> Result<()> {
            let sock = std::net::TcpStream::connect(connect).with_context(|| format!("connect {connect}"))?;
            sock.set_nodelay(true).ok();

            let mut rd = BufReader::with_capacity(1 << 20, sock);

            // Read 4 bytes for mode check; then chain back into reader.
            let mut hdr = [0u8; 4];
            rd.read_exact(&mut hdr)?;
            let mode = Parser::detect_mode(&hdr);
            if !matches!(mode, WireMode::Dbn) {
                return Err(anyhow!("tcp: expected DBN stream, detected {mode:?}"));
            }

            let cur = std::io::Cursor::new(hdr.to_vec());
            let mut chained = std::io::Read::chain(cur, rd);

            let mut syms = SymbolIntern::new(symbol_names);

            Parser::dbn_decode_reader(
                &mut chained,
                &mut syms,
                |p| {
                    let shard = (p.symbol as usize) % shard_n;
                    let _ = shard_txs[shard].blocking_send(ShardMsg::Mbo {
                        symbol: p.symbol,
                        op: p.op,
                        seq: p.seq,
                        ts_ns: p.ts_ns,
                    });
                },
                |symbol, seq, ts_ns, bids, asks| {
                    let shard = (symbol as usize) % shard_n;
                    let _ = shard_txs[shard].blocking_send(ShardMsg::Mbp10 {
                        symbol,
                        seq,
                        ts_ns,
                        bids,
                        asks,
                    });
                },
            )?;

            Ok(())
        })
        .await;

        match res {
            Ok(Ok(())) => {
                backoff = min_ms.max(1);
                info!("input(tcp): stream ended cleanly; reconnecting");
            }
            Ok(Err(e)) => {
                error!("input(tcp): error: {e:#}");
            }
            Err(e) => {
                error!("input(tcp): join error: {e:#}");
            }
        }

        tokio::time::sleep(Duration::from_millis(backoff)).await;
        backoff = (backoff * 2).min(max_ms);
        metrics.inc_seq_gap();
    }
}

async fn build_final_json(
    symbol_names: Arc<RwLock<Vec<String>>>,
    latest: Arc<DashMap<SymbolId, SymbolLevels>>,
) -> String {
    let ids = SymbolIntern::iter_ids_lex(&symbol_names);
    let names = symbol_names.read().unwrap();

    let mut symbols = serde_json::Map::new();
    for id in ids {
        if let Some(v) = latest.get(&id) {
            let name = names.get(id as usize).cloned().unwrap_or_default();
            symbols.insert(
                name,
                json!({
                    "seq": v.seq,
                    "ts_ns": v.ts_ns,
                    "bids": v.bids,
                    "asks": v.asks
                }),
            );
        }
    }

    json!({ "type": "final", "symbols": symbols }).to_string()
}
