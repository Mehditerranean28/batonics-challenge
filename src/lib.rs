//! # Batonics Challenge - High-Performance Order Book Engine
//!
//! This crate provides a high-performance market data processing engine capable of handling
//! real-time order book updates from various wire formats (DBN, NDJSON). It implements
//! a multi-threaded architecture with sharding for scalability.
//!
//! ## Architecture
//!
//! The engine consists of several key components:
//! - **Parser**: Handles decoding of market data from various wire formats
//! - **OrderBook**: Maintains order book state with efficient price level management
//! - **Wire Protocol**: Encodes market data for WebSocket distribution
//! - **Metrics**: Provides comprehensive performance monitoring
//! 
//! ## Features
//!
//! - **Multi-format support**: DBN, NDJSON, and other wire formats
//! - **Sharded processing**: Parallel symbol processing across multiple threads
//! - **Real-time WebSocket API**: Live market data distribution
//! - **Comprehensive metrics**: Prometheus-compatible performance monitoring
//! - **High performance**: Optimized for low-latency, high-throughput scenarios
//!
//! ## Example
//!
//! ```rust,no_run
//! use batonics_challenge::{
//!     book::{OrderBook, Op, Side, ApplyError},
//!     parser::{Parser, SymbolIntern},
//!     wire,
//! };
//!
//! // Create and manipulate an order book
//! let mut book = OrderBook::new();
//! book.reserve_orders(1000);
//!
//! let op = Op::Add {
//!     order_id: 1,
//!     side: Side::Bid,
//!     price: 10000, // $100.00 in cents
//!     qty: 100,
//! };
//!
//! let result = book.apply(op);
//! assert_eq!(result.err, ApplyError::None);
//! ```
pub mod book;
pub mod parser;
pub mod wire;
