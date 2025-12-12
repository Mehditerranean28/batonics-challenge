// src/wire.rs
use bytes::Bytes;
use itoa::Buffer;

use crate::book::LevelPxQty;
use crate::parser::SymbolId;

#[inline(always)]
fn push_str(out: &mut Vec<u8>, s: &str) {
    out.extend_from_slice(s.as_bytes());
}

#[inline(always)]
fn push_u64(out: &mut Vec<u8>, buf: &mut Buffer, v: u64) {
    push_str(out, buf.format(v));
}

#[inline(always)]
fn push_i64(out: &mut Vec<u8>, buf: &mut Buffer, v: i64) {
    push_str(out, buf.format(v));
}

#[inline(always)]
fn push_opt_i64(out: &mut Vec<u8>, buf: &mut Buffer, v: Option<i64>) {
    match v {
        Some(x) => push_i64(out, buf, x),
        None => push_str(out, "null"),
    }
}

pub fn encode_bbo(
    symbol: SymbolId,
    seq: u64,
    ts_ns: u64,
    bid_px: Option<i64>,
    bid_qty: u64,
    ask_px: Option<i64>,
    ask_qty: u64,
) -> Bytes {
    let mut out = Vec::with_capacity(160);
    let mut b = Buffer::new();

    push_str(&mut out, "{\"type\":\"bbo\",\"symbol\":");
    push_u64(&mut out, &mut b, symbol as u64);

    push_str(&mut out, ",\"seq\":");
    push_u64(&mut out, &mut b, seq);

    push_str(&mut out, ",\"ts_ns\":");
    push_u64(&mut out, &mut b, ts_ns);

    push_str(&mut out, ",\"bid_px\":");
    push_opt_i64(&mut out, &mut b, bid_px);

    push_str(&mut out, ",\"bid_qty\":");
    push_u64(&mut out, &mut b, bid_qty);

    push_str(&mut out, ",\"ask_px\":");
    push_opt_i64(&mut out, &mut b, ask_px);

    push_str(&mut out, ",\"ask_qty\":");
    push_u64(&mut out, &mut b, ask_qty);

    out.push(b'}');
    Bytes::from(out)
}

pub fn encode_snapshot(
    symbol: SymbolId,
    seq: u64,
    ts_ns: u64,
    bids: &[LevelPxQty],
    asks: &[LevelPxQty],
) -> Bytes {
    let mut out = Vec::with_capacity(96 + (bids.len() + asks.len()) * 28);
    let mut b = Buffer::new();

    push_str(&mut out, "{\"type\":\"snapshot\",\"symbol\":");
    push_u64(&mut out, &mut b, symbol as u64);

    push_str(&mut out, ",\"seq\":");
    push_u64(&mut out, &mut b, seq);

    push_str(&mut out, ",\"ts_ns\":");
    push_u64(&mut out, &mut b, ts_ns);

    push_str(&mut out, ",\"bids\":[");
    for (i, lv) in bids.iter().enumerate() {
        if i != 0 {
            out.push(b',');
        }
        push_str(&mut out, "{\"px\":");
        push_i64(&mut out, &mut b, lv.px);
        push_str(&mut out, ",\"qty\":");
        push_u64(&mut out, &mut b, lv.qty);
        out.push(b'}');
    }

    push_str(&mut out, "],\"asks\":[");
    for (i, lv) in asks.iter().enumerate() {
        if i != 0 {
            out.push(b',');
        }
        push_str(&mut out, "{\"px\":");
        push_i64(&mut out, &mut b, lv.px);
        push_str(&mut out, ",\"qty\":");
        push_u64(&mut out, &mut b, lv.qty);
        out.push(b'}');
    }

    push_str(&mut out, "]}");
    Bytes::from(out)
}

pub fn encode_resync(seq: u64, ts_ns: u64) -> Bytes {
    let mut out = Vec::with_capacity(64);
    let mut b = Buffer::new();

    push_str(&mut out, "{\"type\":\"resync\",\"seq\":");
    push_u64(&mut out, &mut b, seq);

    push_str(&mut out, ",\"ts_ns\":");
    push_u64(&mut out, &mut b, ts_ns);

    out.push(b'}');
    Bytes::from(out)
}
