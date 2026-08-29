//! The obviously-correct order book.
//!
//! Price-time priority: orders rest at a price level in arrival order, and the
//! front of the queue at the best price is what an aggressor matches against.

use std::collections::{BTreeMap, HashMap, VecDeque};

use wire::Side;

/// Why a book operation was refused.
///
/// These are not "shouldn't happen" conditions to be unwrapped away. On the
/// handler side they mean the feed described a book change that does not apply
/// to the book we have — which is exactly the divergence this milestone exists
/// to detect — so every one of them is surfaced rather than swallowed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BookError {
    /// An `AddOrder` arrived for an id already resting.
    DuplicateOrderId(u64),
    /// A modify or delete named an order that is not on the book.
    UnknownOrderId(u64),
    /// A `Reduce` tried to raise the quantity. Reduce keeps queue priority, so
    /// letting it increase size would hand out priority the order never earned.
    ReduceWouldIncrease { order_id: u64, from: u32, to: u32 },
    /// A resting order was left with zero quantity. Removal is a `DeleteOrder`.
    ZeroQuantity(u64),
}

impl std::fmt::Display for BookError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::DuplicateOrderId(id) => write!(f, "order {id} is already on the book"),
            Self::UnknownOrderId(id) => write!(f, "order {id} is not on the book"),
            Self::ReduceWouldIncrease { order_id, from, to } => write!(
                f,
                "order {order_id}: reduce from {from} to {to} would increase quantity"
            ),
            Self::ZeroQuantity(id) => write!(f, "order {id} would be left with zero quantity"),
        }
    }
}

impl std::error::Error for BookError {}

/// An order resting on the book.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RestingOrder {
    pub order_id: u64,
    pub price: i64,
    pub quantity: u32,
    pub side: Side,
}

/// One aggregated price level.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Level {
    pub price: i64,
    pub quantity: u64,
    pub order_count: u32,
}

/// One symbol's book.
#[derive(Debug, Default)]
pub struct ReferenceBook {
    orders: HashMap<u64, RestingOrder>,
    /// Iterated in reverse: the highest bid is the best bid.
    bids: BTreeMap<i64, VecDeque<u64>>,
    /// Iterated forward: the lowest ask is the best ask.
    asks: BTreeMap<i64, VecDeque<u64>>,
}

impl ReferenceBook {
    pub fn new() -> Self {
        Self::default()
    }

    fn side_mut(&mut self, side: Side) -> &mut BTreeMap<i64, VecDeque<u64>> {
        match side {
            Side::Bid => &mut self.bids,
            Side::Ask => &mut self.asks,
        }
    }

    fn side(&self, side: Side) -> &BTreeMap<i64, VecDeque<u64>> {
        match side {
            Side::Bid => &self.bids,
            Side::Ask => &self.asks,
        }
    }

    /// Rests a new order at the back of its price level.
    pub fn add(
        &mut self,
        order_id: u64,
        side: Side,
        price: i64,
        quantity: u32,
    ) -> Result<(), BookError> {
        if quantity == 0 {
            return Err(BookError::ZeroQuantity(order_id));
        }
        if self.orders.contains_key(&order_id) {
            return Err(BookError::DuplicateOrderId(order_id));
        }
        self.orders.insert(
            order_id,
            RestingOrder {
                order_id,
                price,
                quantity,
                side,
            },
        );
        self.side_mut(side)
            .entry(price)
            .or_default()
            .push_back(order_id);
        Ok(())
    }

    /// Removes an order and returns what it was.
    pub fn delete(&mut self, order_id: u64) -> Result<RestingOrder, BookError> {
        let order = self
            .orders
            .remove(&order_id)
            .ok_or(BookError::UnknownOrderId(order_id))?;
        Self::unlink(self.side_mut(order.side), order.price, order_id);
        Ok(order)
    }

    fn unlink(levels: &mut BTreeMap<i64, VecDeque<u64>>, price: i64, order_id: u64) {
        if let Some(queue) = levels.get_mut(&price) {
            if let Some(pos) = queue.iter().position(|id| *id == order_id) {
                queue.remove(pos);
            }
            if queue.is_empty() {
                levels.remove(&price);
            }
        }
    }

    /// Lowers an order's quantity, keeping its place in the queue.
    pub fn reduce(&mut self, order_id: u64, new_quantity: u32) -> Result<(), BookError> {
        let order = self
            .orders
            .get_mut(&order_id)
            .ok_or(BookError::UnknownOrderId(order_id))?;
        if new_quantity == 0 {
            return Err(BookError::ZeroQuantity(order_id));
        }
        if new_quantity > order.quantity {
            return Err(BookError::ReduceWouldIncrease {
                order_id,
                from: order.quantity,
                to: new_quantity,
            });
        }
        order.quantity = new_quantity;
        Ok(())
    }

    /// Changes price and/or quantity, sending the order to the back of its
    /// level. This is the losing-priority case, which is why it is a distinct
    /// operation rather than something inferred from the numbers changing.
    pub fn replace(
        &mut self,
        order_id: u64,
        new_price: i64,
        new_quantity: u32,
    ) -> Result<(), BookError> {
        if new_quantity == 0 {
            return Err(BookError::ZeroQuantity(order_id));
        }
        let order = *self
            .orders
            .get(&order_id)
            .ok_or(BookError::UnknownOrderId(order_id))?;
        Self::unlink(self.side_mut(order.side), order.price, order_id);
        let updated = RestingOrder {
            price: new_price,
            quantity: new_quantity,
            ..order
        };
        self.orders.insert(order_id, updated);
        self.side_mut(order.side)
            .entry(new_price)
            .or_default()
            .push_back(order_id);
        Ok(())
    }

    pub fn get(&self, order_id: u64) -> Option<&RestingOrder> {
        self.orders.get(&order_id)
    }

    pub fn len(&self) -> usize {
        self.orders.len()
    }

    pub fn is_empty(&self) -> bool {
        self.orders.is_empty()
    }

    /// Removes every order, keeping allocated capacity.
    pub fn clear(&mut self) {
        self.orders.clear();
        self.bids.clear();
        self.asks.clear();
    }

    /// The best price on a side, and the id at the front of its queue — the
    /// order an aggressor would match against first.
    pub fn front(&self, side: Side) -> Option<(i64, u64)> {
        let levels = self.side(side);
        let (price, queue) = match side {
            Side::Bid => levels.iter().next_back()?,
            Side::Ask => levels.iter().next()?,
        };
        Some((*price, *queue.front()?))
    }

    /// Aggregated levels, best first. `depth` of 0 means every level.
    pub fn levels(&self, side: Side, depth: usize) -> Vec<Level> {
        let levels = self.side(side);
        let iter: Box<dyn Iterator<Item = (&i64, &VecDeque<u64>)>> = match side {
            Side::Bid => Box::new(levels.iter().rev()),
            Side::Ask => Box::new(levels.iter()),
        };
        let mut out = Vec::new();
        for (price, queue) in iter {
            if depth != 0 && out.len() == depth {
                break;
            }
            let quantity: u64 = queue
                .iter()
                .filter_map(|id| self.orders.get(id))
                .map(|o| u64::from(o.quantity))
                .sum();
            out.push(Level {
                price: *price,
                quantity,
                order_count: u32::try_from(queue.len()).unwrap_or(u32::MAX),
            });
        }
        out
    }

    /// Every resting order on one side, in the order an aggressor would match
    /// them: best price first, and within a price, oldest first.
    ///
    /// This is the order a snapshot must be written in. A consumer that re-adds
    /// them in this order reproduces price-time priority exactly, which is the
    /// whole reason `Snapshot` carries orders rather than aggregated levels —
    /// an aggregate says how much rests at a price but not which order is at the
    /// front of the queue.
    pub fn orders_in_queue_order(&self, side: Side) -> Vec<RestingOrder> {
        let levels = self.side(side);
        let iter: Box<dyn Iterator<Item = (&i64, &VecDeque<u64>)>> = match side {
            Side::Bid => Box::new(levels.iter().rev()),
            Side::Ask => Box::new(levels.iter()),
        };
        let mut out = Vec::with_capacity(self.orders.len());
        for (_price, queue) in iter {
            for id in queue {
                if let Some(order) = self.orders.get(id) {
                    out.push(*order);
                }
            }
        }
        out
    }

    pub fn best_bid(&self) -> Option<Level> {
        self.levels(Side::Bid, 1).into_iter().next()
    }

    pub fn best_ask(&self) -> Option<Level> {
        self.levels(Side::Ask, 1).into_iter().next()
    }

    /// Panics in debug builds if the two indexes have drifted apart.
    ///
    /// The order map and the price levels are redundant representations of the
    /// same state, so a bug in one shows up as disagreement with the other. The
    /// tests call this after every operation.
    pub fn check_invariants(&self) -> Result<(), String> {
        let mut counted = 0usize;
        for (side, levels) in [(Side::Bid, &self.bids), (Side::Ask, &self.asks)] {
            for (price, queue) in levels {
                if queue.is_empty() {
                    return Err(format!("empty level left behind at price {price}"));
                }
                for id in queue {
                    counted += 1;
                    match self.orders.get(id) {
                        None => return Err(format!("level {price} holds unknown order {id}")),
                        Some(o) if o.price != *price => {
                            return Err(format!(
                                "order {id} is filed at {price} but thinks it is at {}",
                                o.price
                            ))
                        }
                        Some(o) if o.side != side => {
                            return Err(format!("order {id} is filed on the wrong side"))
                        }
                        Some(o) if o.quantity == 0 => {
                            return Err(format!("order {id} rests with zero quantity"))
                        }
                        Some(_) => {}
                    }
                }
            }
        }
        if counted != self.orders.len() {
            return Err(format!(
                "{} orders in the map but {counted} filed on levels",
                self.orders.len()
            ));
        }
        Ok(())
    }
}

/// Every symbol's book, keyed by the `symbolId` on the wire.
///
/// `BTreeMap` rather than `HashMap` so iteration order is the symbol id order,
/// which keeps the digest deterministic across processes without sorting.
#[derive(Debug, Default)]
pub struct Books {
    books: BTreeMap<u16, ReferenceBook>,
}

impl Books {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn get_or_create(&mut self, symbol_id: u16) -> &mut ReferenceBook {
        self.books.entry(symbol_id).or_default()
    }

    pub fn get(&self, symbol_id: u16) -> Option<&ReferenceBook> {
        self.books.get(&symbol_id)
    }

    /// Empties one symbol, keeping its allocated capacity.
    ///
    /// A snapshot is the whole book as of a sequence, not an increment, so
    /// applying one starts by discarding whatever was there. Keeping the
    /// capacity matters because recovery happens while the feed is still
    /// arriving: this is not the moment to hand memory back and ask for it
    /// again.
    /// Empties every book, keeping allocated capacity.
    ///
    /// A snapshot *cycle* replaces the whole set, not one symbol: a symbol that
    /// has gone away since the last cycle simply stops appearing, and clearing
    /// only the symbols the cycle mentions would leave it resting forever.
    pub fn clear_all(&mut self) {
        for book in self.books.values_mut() {
            book.clear();
        }
    }

    pub fn clear_symbol(&mut self, symbol_id: u16) {
        if let Some(book) = self.books.get_mut(&symbol_id) {
            book.clear();
        }
    }

    pub fn iter(&self) -> impl Iterator<Item = (&u16, &ReferenceBook)> {
        self.books.iter()
    }

    pub fn total_orders(&self) -> usize {
        self.books.values().map(ReferenceBook::len).sum()
    }

    pub fn check_invariants(&self) -> Result<(), String> {
        for (symbol, book) in &self.books {
            book.check_invariants()
                .map_err(|e| format!("symbol {symbol}: {e}"))?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn book() -> ReferenceBook {
        ReferenceBook::new()
    }

    #[test]
    fn orders_rest_in_arrival_order_at_a_price() {
        let mut b = book();
        b.add(1, Side::Bid, 100, 10).unwrap();
        b.add(2, Side::Bid, 100, 20).unwrap();
        b.add(3, Side::Bid, 100, 30).unwrap();
        assert_eq!(b.front(Side::Bid), Some((100, 1)));
        b.delete(1).unwrap();
        assert_eq!(b.front(Side::Bid), Some((100, 2)));
        b.check_invariants().unwrap();
    }

    #[test]
    fn the_best_bid_is_the_highest_and_the_best_ask_the_lowest() {
        let mut b = book();
        b.add(1, Side::Bid, 100, 10).unwrap();
        b.add(2, Side::Bid, 101, 10).unwrap();
        b.add(3, Side::Ask, 105, 10).unwrap();
        b.add(4, Side::Ask, 104, 10).unwrap();
        assert_eq!(b.best_bid().unwrap().price, 101);
        assert_eq!(b.best_ask().unwrap().price, 104);
    }

    #[test]
    fn reduce_keeps_queue_priority_and_replace_loses_it() {
        let mut b = book();
        b.add(1, Side::Bid, 100, 10).unwrap();
        b.add(2, Side::Bid, 100, 10).unwrap();

        b.reduce(1, 5).unwrap();
        assert_eq!(b.front(Side::Bid), Some((100, 1)), "reduce keeps the front");
        assert_eq!(b.get(1).unwrap().quantity, 5);

        b.replace(1, 100, 8).unwrap();
        assert_eq!(
            b.front(Side::Bid),
            Some((100, 2)),
            "replace sends the order to the back"
        );
        b.check_invariants().unwrap();
    }

    #[test]
    fn replace_across_price_levels_moves_the_order() {
        let mut b = book();
        b.add(1, Side::Bid, 100, 10).unwrap();
        b.replace(1, 99, 10).unwrap();
        assert_eq!(b.best_bid().unwrap().price, 99);
        assert_eq!(b.levels(Side::Bid, 0).len(), 1, "the old level is gone");
        b.check_invariants().unwrap();
    }

    #[test]
    fn levels_aggregate_quantity_and_count() {
        let mut b = book();
        b.add(1, Side::Ask, 105, 10).unwrap();
        b.add(2, Side::Ask, 105, 15).unwrap();
        b.add(3, Side::Ask, 106, 5).unwrap();
        let levels = b.levels(Side::Ask, 0);
        assert_eq!(
            levels,
            vec![
                Level {
                    price: 105,
                    quantity: 25,
                    order_count: 2
                },
                Level {
                    price: 106,
                    quantity: 5,
                    order_count: 1
                },
            ]
        );
        assert_eq!(b.levels(Side::Ask, 1).len(), 1, "depth is respected");
    }

    #[test]
    fn a_feed_that_contradicts_the_book_is_an_error_not_a_silent_no_op() {
        let mut b = book();
        b.add(1, Side::Bid, 100, 10).unwrap();
        assert_eq!(
            b.add(1, Side::Bid, 100, 10),
            Err(BookError::DuplicateOrderId(1))
        );
        assert_eq!(b.delete(99), Err(BookError::UnknownOrderId(99)));
        assert_eq!(b.reduce(99, 1), Err(BookError::UnknownOrderId(99)));
        assert_eq!(
            b.reduce(1, 20),
            Err(BookError::ReduceWouldIncrease {
                order_id: 1,
                from: 10,
                to: 20
            })
        );
        assert_eq!(b.add(2, Side::Bid, 100, 0), Err(BookError::ZeroQuantity(2)));
        assert_eq!(b.reduce(1, 0), Err(BookError::ZeroQuantity(1)));
    }

    #[test]
    fn emptying_a_level_removes_it() {
        let mut b = book();
        b.add(1, Side::Bid, 100, 10).unwrap();
        b.delete(1).unwrap();
        assert!(b.is_empty());
        assert_eq!(b.best_bid(), None);
        assert_eq!(b.front(Side::Bid), None);
        b.check_invariants().unwrap();
    }
}
