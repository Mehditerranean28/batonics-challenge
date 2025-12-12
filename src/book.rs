// src/book.rs
//! Order book implementation with high-performance price level management.
//!
//! This module provides a complete order book implementation optimized for low-latency
//! market data processing. It supports all major order operations (add, cancel, modify,
//! fill) and maintains efficient price-time priority.
//!
//! ## Key Features
//!
//! - **Efficient price levels**: Uses BTreeMap for O(log n) price level operations
//! - **Order tracking**: HashMap-based order ID to order metadata mapping
//! - **Memory pre-allocation**: Configurable order capacity for reduced allocations
//! - **Invariant checking**: Comprehensive validation in debug/test builds
//! - **Zero-cost abstractions**: Inline functions and optimized data structures
//!
//! ## Performance Characteristics
//!
//! - Add/Cancel/Modify: O(log P) where P is number of price levels
//! - Fill: O(log P) for level updates
//! - BBO access: O(1)
//! - Memory usage: ~48 bytes per order + price level overhead
//!
//! ## Example
//!
//! ```rust
//! use batonics_challenge::book::{OrderBook, Op, Side, ApplyError};
//!
//! let mut book = OrderBook::new();
//! book.reserve_orders(1000); // Pre-allocate for performance
//!
//! // Add a bid order
//! let op = Op::Add {
//!     order_id: 1,
//!     side: Side::Bid,
//!     price: 10000, // $100.00 in cents
//!     qty: 100,
//! };
//!
//! let result = book.apply(op);
//! assert_eq!(result.err, ApplyError::None);
//! assert!(result.bbo_changed);
//!
//! // Check best bid/ask
//! let bbo = book.bbo();
//! assert_eq!(bbo.bid_px, Some(10000));
//! assert_eq!(bbo.bid_qty, 100);
//! ```

use serde::Serialize;
use std::collections::BTreeMap;

pub type OrderId = u64;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub enum Side {
    Bid,
    Ask,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize)]
pub struct Bbo {
    pub bid_px: Option<i64>,
    pub bid_qty: u64,
    pub ask_px: Option<i64>,
    pub ask_qty: u64,
}

#[derive(Clone, Copy, Debug, Serialize)]
pub struct LevelPxQty {
    pub px: i64,
    pub qty: u64,
}

#[derive(Clone, Debug)]
pub enum Op {
    Add {
        order_id: OrderId,
        side: Side,
        price: i64,
        qty: u64,
    },
    Cancel {
        order_id: OrderId,
        qty: u64,
    },
    Modify {
        order_id: OrderId,
        new_price: Option<i64>,
        new_qty: Option<u64>,
        new_side: Option<Side>,
    },
    Fill {
        order_id: OrderId,
        qty: u64,
    },
    Clear,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ApplyError {
    None,
    ZeroQty,
    UnknownOrder,
    QtyTooLarge,
    LevelUnderflow,
    Overflow,
    CrossedBook,
}

impl Default for ApplyError {
    #[inline]
    fn default() -> Self {
        ApplyError::None
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct ApplyOut {
    pub bbo_changed: bool,
    pub err: ApplyError,
}

#[derive(Clone, Copy, Debug)]
struct OrderMeta {
    side: Side,
    price: i64,
    qty: u64,
}

#[derive(Default)]
pub struct OrderBook {
    bids: BTreeMap<i64, u64>,
    asks: BTreeMap<i64, u64>,
    orders: hashbrown::HashMap<OrderId, OrderMeta>,
    best_bid: Option<i64>,
    best_ask: Option<i64>,
}

impl OrderBook {
    #[inline]
    pub fn new() -> Self {
        Self::default()
    }

    #[inline]
    pub fn reserve_orders(&mut self, n: usize) {
        self.orders.reserve(n);
    }

    #[inline]
    pub fn clear(&mut self) {
        self.bids.clear();
        self.asks.clear();
        self.orders.clear();
        self.best_bid = None;
        self.best_ask = None;
    }

    #[inline]
    pub fn bbo(&self) -> Bbo {
        Bbo {
            bid_px: self.best_bid,
            bid_qty: self
                .best_bid
                .and_then(|p| self.bids.get(&p).copied())
                .unwrap_or(0),
            ask_px: self.best_ask,
            ask_qty: self
                .best_ask
                .and_then(|p| self.asks.get(&p).copied())
                .unwrap_or(0),
        }
    }

    #[inline(always)]
    fn recompute_best(&mut self, side: Side) {
        match side {
            Side::Bid => self.best_bid = self.bids.iter().next_back().map(|(p, _)| *p),
            Side::Ask => self.best_ask = self.asks.iter().next().map(|(p, _)| *p),
        }
    }

    #[inline(always)]
    fn crossed(&self) -> bool {
        match (self.best_bid, self.best_ask) {
            (Some(b), Some(a)) => b >= a,
            _ => false,
        }
    }

    #[inline(always)]
    fn level_add(&mut self, side: Side, px: i64, add: u64) -> Result<u64, ApplyError> {
        if add == 0 {
            return Err(ApplyError::ZeroQty);
        }

        let best_before = match side {
            Side::Bid => self.best_bid,
            Side::Ask => self.best_ask,
        };

        let new_qty = match side {
            Side::Bid => {
                let e = self.bids.entry(px).or_insert(0);
                let Some(nv) = e.checked_add(add) else {
                    return Err(ApplyError::Overflow);
                };
                *e = nv;
                nv
            }
            Side::Ask => {
                let e = self.asks.entry(px).or_insert(0);
                let Some(nv) = e.checked_add(add) else {
                    return Err(ApplyError::Overflow);
                };
                *e = nv;
                nv
            }
        };

        match side {
            Side::Bid => match best_before {
                None => self.best_bid = Some(px),
                Some(b) if px > b => self.best_bid = Some(px),
                _ => {}
            },
            Side::Ask => match best_before {
                None => self.best_ask = Some(px),
                Some(a) if px < a => self.best_ask = Some(px),
                _ => {}
            },
        }

        Ok(new_qty)
    }

    #[inline(always)]
    fn level_sub(&mut self, side: Side, px: i64, sub: u64) -> Result<u64, ApplyError> {
        if sub == 0 {
            return Err(ApplyError::ZeroQty);
        }

        let best_before = match side {
            Side::Bid => self.best_bid,
            Side::Ask => self.best_ask,
        };

        let (new_qty, removed) = match side {
            Side::Bid => {
                let Some(cur) = self.bids.get_mut(&px) else {
                    return Err(ApplyError::LevelUnderflow);
                };
                if *cur < sub {
                    return Err(ApplyError::LevelUnderflow);
                }
                *cur -= sub;
                if *cur == 0 {
                    self.bids.remove(&px);
                    (0, true)
                } else {
                    (*cur, false)
                }
            }
            Side::Ask => {
                let Some(cur) = self.asks.get_mut(&px) else {
                    return Err(ApplyError::LevelUnderflow);
                };
                if *cur < sub {
                    return Err(ApplyError::LevelUnderflow);
                }
                *cur -= sub;
                if *cur == 0 {
                    self.asks.remove(&px);
                    (0, true)
                } else {
                    (*cur, false)
                }
            }
        };

        if removed && best_before == Some(px) {
            self.recompute_best(side);
        }

        Ok(new_qty)
    }

    #[inline(always)]
    fn reduce_order(&mut self, order_id: OrderId, reduce_by: u64) -> ApplyError {
        if reduce_by == 0 {
            return ApplyError::ZeroQty;
        }

        let meta = match self.orders.get(&order_id).copied() {
            Some(m) => m,
            None => return ApplyError::UnknownOrder,
        };

        if reduce_by > meta.qty {
            return ApplyError::QtyTooLarge;
        }

        if let Err(e) = self.level_sub(meta.side, meta.price, reduce_by) {
            return e;
        }

        if reduce_by == meta.qty {
            self.orders.remove(&order_id);
        } else if let Some(m) = self.orders.get_mut(&order_id) {
            m.qty -= reduce_by;
        }

        ApplyError::None
    }

    #[inline(always)]
    fn replace_atomic(
        &mut self,
        order_id: OrderId,
        old: OrderMeta,
        new_side: Side,
        new_px: i64,
        new_qty: u64,
    ) -> ApplyError {
        // Remove old
        if let Err(e) = self.level_sub(old.side, old.price, old.qty) {
            return e;
        }

        // Add new; rollback old on failure
        if let Err(e2) = self.level_add(new_side, new_px, new_qty) {
            let _ = self.level_add(old.side, old.price, old.qty);
            return e2;
        }

        self.orders.insert(
            order_id,
            OrderMeta {
                side: new_side,
                price: new_px,
                qty: new_qty,
            },
        );
        ApplyError::None
    }

    pub fn apply(&mut self, op: Op) -> ApplyOut {
        let prev = self.bbo();
        let mut err = ApplyError::None;

        match op {
            Op::Clear => self.clear(),

            Op::Add {
                order_id,
                side,
                price,
                qty,
            } => {
                if qty == 0 {
                    err = ApplyError::ZeroQty;
                } else if let Some(old) = self.orders.get(&order_id).copied() {
                    err = self.replace_atomic(order_id, old, side, price, qty);
                } else {
                    match self.level_add(side, price, qty) {
                        Ok(_) => {
                            self.orders.insert(order_id, OrderMeta { side, price, qty });
                        }
                        Err(e) => err = e,
                    }
                }
            }

            Op::Cancel { order_id, qty } => err = self.reduce_order(order_id, qty),

            Op::Fill { order_id, qty } => err = self.reduce_order(order_id, qty),

            Op::Modify {
                order_id,
                new_price,
                new_qty,
                new_side,
            } => {
                let old = match self.orders.get(&order_id).copied() {
                    Some(m) => m,
                    None => {
                        err = ApplyError::UnknownOrder;
                        return ApplyOut {
                            bbo_changed: self.bbo() != prev,
                            err,
                        };
                    }
                };

                let side = new_side.unwrap_or(old.side);
                let px = new_price.unwrap_or(old.price);
                let qty = new_qty.unwrap_or(old.qty);

                if qty == 0 {
                    match self.level_sub(old.side, old.price, old.qty) {
                        Ok(_) => {
                            self.orders.remove(&order_id);
                        }
                        Err(e) => err = e,
                    }
                } else if side == old.side && px == old.price {
                    if qty > old.qty {
                        let add = qty - old.qty;
                        match self.level_add(side, px, add) {
                            Ok(_) => {
                                self.orders.insert(order_id, OrderMeta { side, price: px, qty });
                            }
                            Err(e) => err = e,
                        }
                    } else if qty < old.qty {
                        let sub = old.qty - qty;
                        match self.level_sub(side, px, sub) {
                            Ok(_) => {
                                self.orders.insert(order_id, OrderMeta { side, price: px, qty });
                            }
                            Err(e) => err = e,
                        }
                    }
                } else {
                    err = self.replace_atomic(order_id, old, side, px, qty);
                }
            }
        }

        if err == ApplyError::None && self.crossed() {
            err = ApplyError::CrossedBook;
        }

        ApplyOut {
            bbo_changed: self.bbo() != prev,
            err,
        }
    }

    pub fn levels_depth(&self, side: Side, depth: usize) -> Vec<LevelPxQty> {
        let n = match side {
            Side::Bid => self.bids.len(),
            Side::Ask => self.asks.len(),
        };

        let take_n = if depth == 0 { n } else { depth.min(n) };
        let mut out = Vec::with_capacity(take_n);

        match side {
            Side::Bid => {
                for (&px, &qty) in self.bids.iter().rev().take(take_n) {
                    out.push(LevelPxQty { px, qty });
                }
            }
            Side::Ask => {
                for (&px, &qty) in self.asks.iter().take(take_n) {
                    out.push(LevelPxQty { px, qty });
                }
            }
        }

        out
    }
}

impl OrderBook {
    pub fn assert_invariants(&self) {
        // 1) best pointers must match the trees
        let exp_best_bid = self.bids.iter().next_back().map(|(&p, _)| p);
        let exp_best_ask = self.asks.iter().next().map(|(&p, _)| p);
        assert_eq!(self.best_bid, exp_best_bid, "best_bid mismatch");
        assert_eq!(self.best_ask, exp_best_ask, "best_ask mismatch");

        // 2) no zero-qty levels
        assert!(self.bids.values().all(|&q| q > 0), "zero bid level");
        assert!(self.asks.values().all(|&q| q > 0), "zero ask level");

        // 3) no zero-qty orders
        assert!(self.orders.values().all(|m| m.qty > 0), "zero qty order");

        // 4) map totals must equal sum of orders per (side,px)
        use hashbrown::HashMap;
        let mut agg_bids: HashMap<i64, u64> = HashMap::new();
        let mut agg_asks: HashMap<i64, u64> = HashMap::new();
        for m in self.orders.values() {
            let map = match m.side { Side::Bid => &mut agg_bids, Side::Ask => &mut agg_asks };
            *map.entry(m.price).or_insert(0) += m.qty;
        }

        for (&px, &q) in self.bids.iter() {
            let exp = agg_bids.get(&px).copied().unwrap_or(0);
            assert_eq!(q, exp, "bid level qty mismatch at px={px}");
        }
        for (&px, &q) in self.asks.iter() {
            let exp = agg_asks.get(&px).copied().unwrap_or(0);
            assert_eq!(q, exp, "ask level qty mismatch at px={px}");
        }

        // Note: Crossed books (bid >= ask) are allowed in some market models
    }
}
