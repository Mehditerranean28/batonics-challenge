use batonics_challenge::book::{Op, OrderBook, Side};
use proptest::prelude::*;

proptest! {
    #[test]
    fn book_invariants_hold(ops in prop::collection::vec(any_op(), 1..10000)) {
        let mut b = OrderBook::new();
        b.reserve_orders(200_000);

        for op in ops {
            b.apply(op);
        }
        b.assert_invariants();
    }
}

fn any_op() -> impl Strategy<Value = Op> {
    prop_oneof![
        (any_side(), 1u64..50_000u64, -2_000_000i64..2_000_000i64, 0u64..5_000u64)
            .prop_map(|(side, oid, px, qty)| Op::Add{ side, order_id: oid, price: px, qty }),

        (1u64..50_000u64, 1u64..5_000u64)
            .prop_map(|(oid, qty)| Op::Cancel{ order_id: oid, qty }),

        (1u64..50_000u64, 1u64..5_000u64)
            .prop_map(|(oid, qty)| Op::Fill{ order_id: oid, qty }),

        (
            1u64..50_000u64,
            prop::option::of(-2_000_000i64..2_000_000i64),
            prop::option::of(0u64..5_000u64),
            prop::option::of(any_side())
        )
        .prop_map(|(oid, px, qty, side)| Op::Modify{ order_id: oid, new_price: px, new_qty: qty, new_side: side }),

        Just(Op::Clear),
    ]
}

fn any_side() -> impl Strategy<Value = Side> {
    prop_oneof![Just(Side::Bid), Just(Side::Ask)]
}
