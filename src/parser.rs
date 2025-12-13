// src/parser.rs
use crate::book::{LevelPxQty, Op, Side};
use anyhow::Result;
use serde::Deserialize;
use std::collections::HashMap;
use std::io::{BufRead, BufReader, Cursor, Read};
use std::sync::{Arc, RwLock};

pub type SymbolId = u32;

#[derive(Clone, Copy, Debug)]
pub enum WireMode {
    Dbn,
    LenPref,
    Ndjson,
    Lines,
}

#[derive(Clone, Debug)]
pub struct Parsed {
    pub symbol: SymbolId,
    pub op: Op,
    pub seq: u64,
    pub ts_ns: u64,
}

pub struct SymbolIntern {
    names: Arc<RwLock<Vec<String>>>,
    name_to_id: HashMap<String, SymbolId>,
    instrument_to_id: HashMap<u32, SymbolId>,
}

impl SymbolIntern {
    pub fn new(names: Arc<RwLock<Vec<String>>>) -> Self {
        let mut name_to_id = HashMap::new();
        let mut instrument_to_id = HashMap::new();

        // Seed maps from existing names to prevent duplicates on reconnect
        {
            let r = names.read().unwrap();
            for (i, n) in r.iter().enumerate() {
                let id = i as u32;
                name_to_id.insert(n.clone(), id);
                if let Some(rest) = n.strip_prefix("inst:") {
                    if let Ok(inst) = rest.parse::<u32>() {
                        instrument_to_id.insert(inst, id);
                    }
                }
            }
        }

        Self { names, name_to_id, instrument_to_id }
    }

    pub fn intern_name(&mut self, name: &str) -> SymbolId {
        if let Some(&id) = self.name_to_id.get(name) {
            return id;
        }

        // Defensive dedupe: protect against duplicate inserts if multiple interners
        // ever exist (reconnect cutovers, tests, future refactors).
        // This is cold-path (first-seen symbol) so O(n) scan is acceptable.
        if let Some(id) = {
            let r = self.names.read().unwrap();
            r.iter().position(|s| s == name).map(|i| i as u32)
        } {
            self.name_to_id.insert(name.to_owned(), id);
            return id;
        }

        let mut w = self.names.write().unwrap();
        if let Some(id) = w.iter().position(|s| s == name).map(|i| i as u32) {
            self.name_to_id.insert(name.to_owned(), id);
            return id;
        }

        let id = w.len() as u32;
        w.push(name.to_owned());
        self.name_to_id.insert(name.to_owned(), id);
        id
    }

    pub fn intern_instrument_id(&mut self, instrument_id: u32) -> SymbolId {
        if let Some(&id) = self.instrument_to_id.get(&instrument_id) {
            return id;
        }
        let name = format!("inst:{instrument_id}");
        let id = self.intern_name(&name);
        self.instrument_to_id.insert(instrument_id, id);
        id
    }

    pub fn iter_ids_lex(names: &Arc<RwLock<Vec<String>>>) -> Vec<SymbolId> {
        let r = names.read().unwrap();
        let mut ids: Vec<SymbolId> = (0..r.len() as u32).collect();
        ids.sort_by(|&a, &b| r[a as usize].cmp(&r[b as usize]));
        ids
    }
}

pub struct Parser;

#[derive(Deserialize)]
#[serde(tag = "type")]
enum NdjsonOp {
    #[serde(rename = "add")]
    Add {
        symbol: String,
        side: String,
        oid: u64,
        px: i64,
        qty: u64,
        seq: u64,
        ts_ns: u64,
    },
    #[serde(rename = "cancel")]
    Cancel {
        symbol: String,
        oid: u64,
        qty: u64,
        seq: u64,
        ts_ns: u64,
    },
    #[serde(rename = "fill")]
    Fill {
        symbol: String,
        oid: u64,
        qty: u64,
        seq: u64,
        ts_ns: u64,
    },
    #[serde(rename = "modify")]
    Modify {
        symbol: String,
        oid: u64,
        px: Option<i64>,
        qty: Option<u64>,
        side: Option<String>,
        seq: u64,
        ts_ns: u64,
    },
    #[serde(rename = "clear")]
    Clear { symbol: String, seq: u64, ts_ns: u64 },
}

impl Parser {
    pub fn detect_mode(bytes: &[u8]) -> WireMode {
        if bytes.len() >= 4 && &bytes[..3] == b"DBN" && bytes[3] >= 1 {
            return WireMode::Dbn;
        }

        let mut i = 0;
        while i < bytes.len() && matches!(bytes[i], b' ' | b'\n' | b'\r' | b'\t') {
            i += 1;
        }
        if i < bytes.len() && bytes[i] == b'{' {
            return WireMode::Ndjson;
        }

        WireMode::LenPref
    }

    pub fn ndjson_decode_reader<R: Read>(
        r: R,
        syms: &mut SymbolIntern,
        mut on_mbo: impl FnMut(Parsed),
    ) -> Result<()> {
        let mut br = BufReader::new(r);
        let mut line = String::new();

        loop {
            line.clear(); // clear first to fix borrow checker issue
            if br.read_line(&mut line)? == 0 {
                break;
            }

            let s = line.trim();
            if s.is_empty() {
                continue;
            }

            let op: NdjsonOp = serde_json::from_str(s)?;

            match op {
                NdjsonOp::Add { symbol, side, oid, px, qty, seq, ts_ns } => {
                    let sym = syms.intern_name(&symbol);
                    let side = match side.as_str() {
                        "bid" | "Bid" => Side::Bid,
                        "ask" | "Ask" => Side::Ask,
                        _ => continue,
                    };
                    on_mbo(Parsed {
                        symbol: sym,
                        op: Op::Add { order_id: oid, side, price: px, qty },
                        seq,
                        ts_ns,
                    });
                }
                NdjsonOp::Cancel { symbol, oid, qty, seq, ts_ns } => {
                    let sym = syms.intern_name(&symbol);
                    on_mbo(Parsed {
                        symbol: sym,
                        op: Op::Cancel { order_id: oid, qty },
                        seq,
                        ts_ns,
                    });
                }
                NdjsonOp::Fill { symbol, oid, qty, seq, ts_ns } => {
                    let sym = syms.intern_name(&symbol);
                    on_mbo(Parsed {
                        symbol: sym,
                        op: Op::Fill { order_id: oid, qty },
                        seq,
                        ts_ns,
                    });
                }
                NdjsonOp::Modify { symbol, oid, px, qty, side, seq, ts_ns } => {
                    let sym = syms.intern_name(&symbol);
                    let new_side = side.and_then(|s| match s.as_str() {
                        "bid" | "Bid" => Some(Side::Bid),
                        "ask" | "Ask" => Some(Side::Ask),
                        _ => None,
                    });
                    on_mbo(Parsed {
                        symbol: sym,
                        op: Op::Modify { order_id: oid, new_price: px, new_qty: qty, new_side },
                        seq,
                        ts_ns,
                    });
                }
                NdjsonOp::Clear { symbol, seq, ts_ns } => {
                    let sym = syms.intern_name(&symbol);
                    on_mbo(Parsed { symbol: sym, op: Op::Clear, seq, ts_ns });
                }
            }
        }

        Ok(())
    }

    pub fn dbn_decode_reader<R: Read>(
        r: R,
        syms: &mut SymbolIntern,
        mut on_mbo: impl FnMut(Parsed),
        mut on_mbp10: impl FnMut(SymbolId, u64, u64, Vec<LevelPxQty>, Vec<LevelPxQty>),
    ) -> Result<()> {
        use dbn::decode::dbn::Decoder;
        use dbn::decode::DecodeRecordRef;
        use dbn::{RecordRefEnum, VersionUpgradePolicy};

        let mut dec = Decoder::with_upgrade_policy(r, VersionUpgradePolicy::UpgradeToV3)?;

        while let Some(rec) = dec.decode_record_ref()? {
            let Ok(e) = rec.as_enum() else { continue };

            match e {
                RecordRefEnum::Mbo(m) => {
                    let symbol = syms.intern_instrument_id(m.hd.instrument_id);
                    let seq = m.sequence as u64;
                    let ts_ns = m.hd.ts_event;

                    let side = match m.side as u8 {
                        b'B' => Side::Bid,
                        b'A' => Side::Ask,
                        _ => continue,
                    };

                    let op = match m.action as u8 {
                        b'A' => Op::Add {
                            order_id: m.order_id,
                            side,
                            price: m.price,
                            qty: m.size as u64,
                        },
                        b'C' => Op::Cancel {
                            order_id: m.order_id,
                            qty: m.size as u64,
                        },
                        b'M' => Op::Modify {
                            order_id: m.order_id,
                            new_price: Some(m.price),
                            new_qty: Some(m.size as u64),
                            new_side: Some(side),
                        },
                        b'T' => Op::Fill {
                            order_id: m.order_id,
                            qty: m.size as u64,
                        },
                        b'R' | b'Z' => Op::Clear,
                        _ => continue,
                    };

                    on_mbo(Parsed {
                        symbol,
                        op,
                        seq,
                        ts_ns,
                    });
                }

                RecordRefEnum::Mbp10(m) => {
                    let symbol = syms.intern_instrument_id(m.hd.instrument_id);
                    let seq = m.sequence as u64;
                    let ts_ns = m.hd.ts_event;

                    let mut bids = Vec::with_capacity(10);
                    let mut asks = Vec::with_capacity(10);

                    for lvl in m.levels.iter() {
                        if lvl.bid_sz > 0 && lvl.bid_px != 0 {
                            bids.push(LevelPxQty {
                                px: lvl.bid_px,
                                qty: lvl.bid_sz as u64,
                            });
                        }
                        if lvl.ask_sz > 0 && lvl.ask_px != 0 {
                            asks.push(LevelPxQty {
                                px: lvl.ask_px,
                                qty: lvl.ask_sz as u64,
                            });
                        }
                    }

                    on_mbp10(symbol, seq, ts_ns, bids, asks);
                }

                _ => {}
            }
        }

        Ok(())
    }

    pub fn dbn_decode_bytes(
        bytes: &[u8],
        syms: &mut SymbolIntern,
        on_mbo: impl FnMut(Parsed),
        on_mbp10: impl FnMut(SymbolId, u64, u64, Vec<LevelPxQty>, Vec<LevelPxQty>),
    ) -> Result<()> {
        Self::dbn_decode_reader(Cursor::new(bytes), syms, on_mbo, on_mbp10)
    }

    pub fn parse_lenpref_payload(_payload: &[u8], _syms: &mut SymbolIntern) -> Result<Parsed> {
        anyhow::bail!("lenpref parsing not implemented")
    }
}
