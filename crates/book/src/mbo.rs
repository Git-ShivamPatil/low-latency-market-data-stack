//! The fast book: market-by-order, with market-by-price as a view over it.
//!
//! # Why one structure and two views
//!
//! The milestone asks for an MBP view and an MBO view. They are built here as
//! one store with two accessors rather than two books, for a reason that is not
//! about saving code: **MBP cannot be maintained on its own from this feed.**
//! `DeleteOrder` carries an order id, a symbol and a side — no price. A
//! price-aggregated book has no way to know which level to decrement. It needs
//! the per-order detail, which means it needs the MBO store anyway.
//!
//! So the per-level aggregates live alongside the order lists and are updated in
//! the same operation. They cannot drift apart, because there is no separate
//! thing to drift.
//!
//! # The three structures, and why each one
//!
//! **Levels: a dense array indexed by tick offset.** Not a `BTreeMap`. See
//! [`crate::tick`] — this is the decision the latency target rests on, and it has
//! to be made before the code is written rather than tuned into later.
//!
//! **Orders: a slab.** Nodes are allocated once, at construction, and handed out
//! from a free list threaded through the unused ones. An order arriving and
//! leaving is two index writes, not a malloc and a free.
//!
//! **Order ids: an open-addressed map.** A `HashMap` allocates on growth and
//! chases a pointer per bucket. This is a flat array of `(id, node)` with linear
//! probing, sized once at a low load factor so probe chains stay short.
//!
//! Deletion uses **backward-shift**, not tombstones. Tombstones are simpler and
//! wrong here: a book that processes millions of adds and cancels would fill the
//! table with them and degrade to a linear scan, on a path that is supposed to
//! be a single probe. Backward-shift keeps the table exactly as full as it has
//! live entries.
//!
//! # Allocation
//!
//! Everything is sized at construction. No operation on this book allocates —
//! that is the claim, and `crates/alloc-guard` is what checks it.

use wire::Side;

use crate::reference::{BookError, Level, RestingOrder};
use crate::tick::{Slot, TickIndex};

const NIL: u32 = u32::MAX;

#[derive(Debug, Clone, Copy)]
struct Node {
    order_id: u64,
    price: i64,
    quantity: u32,
    side: Side,
    /// Index of the level this order rests at.
    slot: u32,
    /// Intrusive per-level FIFO. `next` is towards the back of the queue.
    next: u32,
    prev: u32,
}

impl Default for Node {
    fn default() -> Self {
        Self {
            order_id: 0,
            price: 0,
            quantity: 0,
            side: Side::Bid,
            slot: NIL,
            next: NIL,
            prev: NIL,
        }
    }
}

/// One aggregated price level, and the head and tail of its order queue.
#[derive(Debug, Clone, Copy)]
struct LevelCell {
    quantity: u64,
    orders: u32,
    head: u32,
    tail: u32,
}

impl Default for LevelCell {
    fn default() -> Self {
        Self {
            quantity: 0,
            orders: 0,
            head: NIL,
            tail: NIL,
        }
    }
}

impl LevelCell {
    fn is_empty(&self) -> bool {
        self.orders == 0
    }
}

#[derive(Debug, Clone, Copy)]
struct MapEntry {
    order_id: u64,
    node: u32,
}

impl Default for MapEntry {
    fn default() -> Self {
        Self {
            order_id: 0,
            node: NIL,
        }
    }
}

/// How the book was sized. Everything is allocated once from this.
#[derive(Debug, Clone, Copy)]
pub struct MboCapacity {
    /// Price levels per side.
    pub levels: usize,
    /// Simultaneously resting orders.
    pub orders: usize,
    /// Price at the centre of the initial window.
    pub reference_price: i64,
    pub tick: i64,
}

impl MboCapacity {
    pub fn new(reference_price: i64, tick: i64) -> Self {
        Self {
            levels: 4096,
            orders: 1 << 16,
            reference_price,
            tick,
        }
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct MboStats {
    /// Times the price window had to slide.
    pub rebases: u64,
    /// Probes beyond the first, summed. High relative to lookups means the map
    /// is too full or the hash is poor.
    pub extra_probes: u64,
    pub lookups: u64,
    /// Deepest the slab has been.
    pub peak_orders: usize,
}

pub struct MboBook {
    index: TickIndex,
    bids: Vec<LevelCell>,
    asks: Vec<LevelCell>,
    nodes: Vec<Node>,
    /// Head of the free list threaded through unused nodes via `next`.
    free_head: u32,
    map: Vec<MapEntry>,
    map_mask: usize,
    live: usize,
    /// Highest occupied bid slot, lowest occupied ask slot.
    best_bid: Option<usize>,
    best_ask: Option<usize>,
    /// Occupied levels per side, maintained incrementally.
    ///
    /// Only the digest needs this, and only because its encoding writes a
    /// length before the levels. Counting on demand would mean a second walk of
    /// a 4096-slot array per side per symbol, at every checkpoint.
    bid_levels: u32,
    ask_levels: u32,
    stats: MboStats,
}

impl std::fmt::Debug for MboBook {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MboBook")
            .field("live", &self.live)
            .field("levels", &self.index.capacity())
            .field("slab", &self.nodes.len())
            .field("map", &self.map.len())
            .finish()
    }
}

impl MboBook {
    pub fn new(cap: MboCapacity) -> Self {
        let levels = cap.levels.max(4);
        let orders = cap.orders.max(4);
        // A quarter full at most. Linear probing degrades sharply past about
        // half, and the memory is cheap: 16 bytes an entry.
        let map_len = (orders * 4).next_power_of_two();

        let mut nodes = vec![Node::default(); orders];
        // Thread the free list through every node, front to back.
        for (i, node) in nodes.iter_mut().enumerate() {
            node.next = if i + 1 < orders { (i + 1) as u32 } else { NIL };
        }

        Self {
            index: TickIndex::centred_on(cap.reference_price, cap.tick, levels),
            bids: vec![LevelCell::default(); levels],
            asks: vec![LevelCell::default(); levels],
            nodes,
            free_head: 0,
            map: vec![MapEntry::default(); map_len],
            map_mask: map_len - 1,
            live: 0,
            best_bid: None,
            best_ask: None,
            bid_levels: 0,
            ask_levels: 0,
            stats: MboStats::default(),
        }
    }

    pub fn stats(&self) -> MboStats {
        self.stats
    }

    pub fn len(&self) -> usize {
        self.live
    }

    pub fn is_empty(&self) -> bool {
        self.live == 0
    }

    pub fn capacity(&self) -> usize {
        self.nodes.len()
    }

    /// Empties the book, keeping every allocation.
    ///
    /// Used when a snapshot replaces the book wholesale. Rebuilding by dropping
    /// and recreating would allocate — during a recovery, which is exactly when
    /// the process is already behind.
    pub fn clear(&mut self) {
        for cell in self.bids.iter_mut().chain(self.asks.iter_mut()) {
            *cell = LevelCell::default();
        }
        let n = self.nodes.len();
        for (i, node) in self.nodes.iter_mut().enumerate() {
            *node = Node {
                next: if i + 1 < n { (i + 1) as u32 } else { NIL },
                ..Node::default()
            };
        }
        for entry in &mut self.map {
            *entry = MapEntry::default();
        }
        self.free_head = 0;
        self.live = 0;
        self.best_bid = None;
        self.best_ask = None;
        self.bid_levels = 0;
        self.ask_levels = 0;
    }

    /// The slot `price` occupies right now, or `None` if it is outside the
    /// window or off the grid. Read-only: it never rebases.
    pub fn slot_of(&self, price: i64) -> Option<usize> {
        match self.index.slot(price) {
            Slot::At(i) => Some(i),
            _ => None,
        }
    }

    /// Occupied levels on a side, capped at `depth` when `depth` is non-zero.
    pub fn level_count(&self, side: Side, depth: usize) -> usize {
        let n = match side {
            Side::Bid => self.bid_levels as usize,
            Side::Ask => self.ask_levels as usize,
        };
        if depth == 0 {
            n
        } else {
            n.min(depth)
        }
    }

    #[inline]
    fn level_opened(&mut self, side: Side) {
        match side {
            Side::Bid => self.bid_levels += 1,
            Side::Ask => self.ask_levels += 1,
        }
    }

    #[inline]
    fn level_closed(&mut self, side: Side) {
        match side {
            Side::Bid => self.bid_levels -= 1,
            Side::Ask => self.ask_levels -= 1,
        }
    }

    // ---- the order-id map ------------------------------------------------

    #[inline]
    fn hash(&self, order_id: u64) -> usize {
        // Order ids in this system are dense and increasing, so the low bits
        // alone would cluster badly under linear probing. One multiply-xor of
        // SplitMix64's finaliser spreads them.
        let mut z = order_id;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        ((z ^ (z >> 31)) as usize) & self.map_mask
    }

    #[inline]
    fn map_find(&mut self, order_id: u64) -> Option<usize> {
        self.stats.lookups += 1;
        let mut i = self.hash(order_id);
        let mut probes = 0u64;
        loop {
            let entry = self.map[i];
            if entry.node == NIL {
                self.stats.extra_probes += probes;
                return None;
            }
            if entry.order_id == order_id {
                self.stats.extra_probes += probes;
                return Some(i);
            }
            i = (i + 1) & self.map_mask;
            probes += 1;
            if probes as usize > self.map.len() {
                // Cannot happen while the load factor is enforced, but a full
                // table would otherwise spin forever.
                self.stats.extra_probes += probes;
                return None;
            }
        }
    }

    fn map_insert(&mut self, order_id: u64, node: u32) {
        let mut i = self.hash(order_id);
        while self.map[i].node != NIL {
            i = (i + 1) & self.map_mask;
        }
        self.map[i] = MapEntry { order_id, node };
    }

    /// Removes the entry at `hole`, closing the probe chain behind it.
    ///
    /// Backward-shift deletion. Tombstones would be fewer lines and would
    /// degrade this table into a linear scan over a long session, on the exact
    /// lookup the latency target depends on.
    fn map_remove_at(&mut self, hole: usize) {
        let mut hole = hole;
        self.map[hole] = MapEntry::default();
        let mut j = hole;
        loop {
            j = (j + 1) & self.map_mask;
            let entry = self.map[j];
            if entry.node == NIL {
                return;
            }
            let home = self.hash(entry.order_id);
            // Can `entry` move back to `hole` without breaking its own probe
            // chain? Only if its home slot is not cyclically inside `(hole, j]`.
            let stays = if hole <= j {
                home > hole && home <= j
            } else {
                home > hole || home <= j
            };
            if !stays {
                self.map[hole] = entry;
                self.map[j] = MapEntry::default();
                hole = j;
            }
        }
    }

    // ---- levels ----------------------------------------------------------

    #[inline]
    fn side_cells(&mut self, side: Side) -> &mut Vec<LevelCell> {
        match side {
            Side::Bid => &mut self.bids,
            Side::Ask => &mut self.asks,
        }
    }

    #[inline]
    fn side_cells_ref(&self, side: Side) -> &Vec<LevelCell> {
        match side {
            Side::Bid => &self.bids,
            Side::Ask => &self.asks,
        }
    }

    /// The lowest and highest occupied slots across **both** sides.
    ///
    /// Written as two scans over the union rather than as a loop over the two
    /// arrays, because the loop version is wrong in a way that is very easy to
    /// miss: restarting the index at 0 for the second side leaves the running
    /// maximum holding that side's highest slot, not the highest of both. A
    /// rebase computed from that shifts an occupied level on the other side
    /// straight off the end of the window — and the guard that is supposed to
    /// refuse such a shift is checking against the same wrong number, so it
    /// agrees. The differential test caught it as a level count drifting by one
    /// after 118,000 operations.
    fn occupied_extent(&self) -> (Option<usize>, Option<usize>) {
        let cap = self.index.capacity();
        let occupied = |i: usize| !self.bids[i].is_empty() || !self.asks[i].is_empty();
        (
            (0..cap).find(|&i| occupied(i)),
            (0..cap).rev().find(|&i| occupied(i)),
        )
    }

    /// Slides the price window so `price` fits, moving everything with it.
    fn rebase(&mut self, order_id: u64, price: i64) -> Result<(), BookError> {
        let (lo, hi) = self.occupied_extent();
        let keep = match (lo, hi) {
            (Some(l), Some(h)) => Some((self.index.price_at(l), self.index.price_at(h))),
            _ => None,
        };
        let Some((moved, shift)) = self.index.rebase_for(price, keep) else {
            // Only reachable when the span is too wide: `slot_for` has already
            // rejected off-grid prices. Refusing is the only honest answer — the
            // alternative is a book that silently forgets its far side, which
            // nothing downstream would notice.
            return Err(BookError::PriceOutOfRange { order_id, price });
        };
        debug_assert!(
            moved.shift_preserves(shift, lo, hi),
            "rebase_for returned a placement that loses an occupied level"
        );

        // Move occupied cells in place. Direction matters: shifting down means
        // copying front to back, up means back to front, or entries overwrite
        // each other.
        let cap = self.index.capacity();
        for side in [Side::Bid, Side::Ask] {
            let cells = self.side_cells(side);
            if shift > 0 {
                let s = shift as usize;
                for i in 0..cap {
                    cells[i] = if i + s < cap {
                        cells[i + s]
                    } else {
                        LevelCell::default()
                    };
                }
            } else if shift < 0 {
                let s = (-shift) as usize;
                for i in (0..cap).rev() {
                    cells[i] = if i >= s {
                        cells[i - s]
                    } else {
                        LevelCell::default()
                    };
                }
            }
        }

        // Every node records its slot, so they all move too.
        if shift != 0 {
            for node in &mut self.nodes {
                if node.slot != NIL {
                    let moved_slot = node.slot as i64 - shift;
                    node.slot = moved_slot as u32;
                }
            }
            self.best_bid = self.best_bid.map(|b| (b as i64 - shift) as usize);
            self.best_ask = self.best_ask.map(|a| (a as i64 - shift) as usize);
        }

        self.index = moved;
        self.stats.rebases += 1;
        Ok(())
    }

    /// The slot for `price`, rebasing if necessary.
    fn slot_for(&mut self, order_id: u64, price: i64) -> Result<usize, BookError> {
        match self.index.slot(price) {
            Slot::At(i) => Ok(i),
            Slot::OffGrid => Err(BookError::PriceOffGrid { order_id, price }),
            Slot::Outside => {
                self.rebase(order_id, price)?;
                match self.index.slot(price) {
                    Slot::At(i) => Ok(i),
                    _ => Err(BookError::PriceOutOfRange { order_id, price }),
                }
            }
        }
    }

    fn note_occupied(&mut self, side: Side, slot: usize) {
        match side {
            Side::Bid => {
                if self.best_bid.is_none_or(|b| slot > b) {
                    self.best_bid = Some(slot);
                }
            }
            Side::Ask => {
                if self.best_ask.is_none_or(|a| slot < a) {
                    self.best_ask = Some(slot);
                }
            }
        }
    }

    /// Rescans for the best price on a side after its best level emptied.
    ///
    /// A dense array scan, which is why it is affordable: the levels next to the
    /// touch are in the same cache lines, so the usual case touches one or two.
    fn rescan_best(&mut self, side: Side) {
        let cap = self.index.capacity();
        match side {
            Side::Bid => {
                let start = self.best_bid.unwrap_or(cap.saturating_sub(1));
                self.best_bid = (0..=start).rev().find(|&i| !self.bids[i].is_empty());
            }
            Side::Ask => {
                let start = self.best_ask.unwrap_or(0);
                self.best_ask = (start..cap).find(|&i| !self.asks[i].is_empty());
            }
        }
    }

    // ---- the intrusive per-level queue ------------------------------------

    fn link_back(&mut self, side: Side, slot: usize, node: u32) {
        let cells = self.side_cells(side);
        let tail = cells[slot].tail;
        if tail == NIL {
            cells[slot].head = node;
            cells[slot].tail = node;
            self.nodes[node as usize].prev = NIL;
            self.nodes[node as usize].next = NIL;
        } else {
            cells[slot].tail = node;
            self.nodes[tail as usize].next = node;
            self.nodes[node as usize].prev = tail;
            self.nodes[node as usize].next = NIL;
        }
    }

    fn unlink(&mut self, side: Side, slot: usize, node: u32) {
        let (prev, next) = {
            let n = &self.nodes[node as usize];
            (n.prev, n.next)
        };
        if prev != NIL {
            self.nodes[prev as usize].next = next;
        }
        if next != NIL {
            self.nodes[next as usize].prev = prev;
        }
        let cells = self.side_cells(side);
        if cells[slot].head == node {
            cells[slot].head = next;
        }
        if cells[slot].tail == node {
            cells[slot].tail = prev;
        }
        self.nodes[node as usize].prev = NIL;
        self.nodes[node as usize].next = NIL;
    }

    // ---- the slab --------------------------------------------------------

    fn take_node(&mut self, order_id: u64) -> Result<u32, BookError> {
        if self.free_head == NIL {
            return Err(BookError::SlabFull {
                order_id,
                capacity: self.nodes.len(),
            });
        }
        let node = self.free_head;
        self.free_head = self.nodes[node as usize].next;
        Ok(node)
    }

    fn give_node(&mut self, node: u32) {
        self.nodes[node as usize] = Node {
            next: self.free_head,
            ..Node::default()
        };
        self.free_head = node;
    }

    // ---- the operations --------------------------------------------------

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
        if self.map_find(order_id).is_some() {
            return Err(BookError::DuplicateOrderId(order_id));
        }
        let slot = self.slot_for(order_id, price)?;
        let node = self.take_node(order_id)?;

        self.nodes[node as usize] = Node {
            order_id,
            price,
            quantity,
            side,
            slot: slot as u32,
            next: NIL,
            prev: NIL,
        };
        let was_empty = self.side_cells_ref(side)[slot].is_empty();
        self.link_back(side, slot, node);
        let cells = self.side_cells(side);
        cells[slot].quantity += u64::from(quantity);
        cells[slot].orders += 1;
        if was_empty {
            self.level_opened(side);
        }
        self.map_insert(order_id, node);
        self.live += 1;
        self.stats.peak_orders = self.stats.peak_orders.max(self.live);
        self.note_occupied(side, slot);
        Ok(())
    }

    /// Removes an order and returns what it was.
    pub fn delete(&mut self, order_id: u64) -> Result<RestingOrder, BookError> {
        let Some(map_i) = self.map_find(order_id) else {
            return Err(BookError::UnknownOrderId(order_id));
        };
        let node = self.map[map_i].node;
        let n = self.nodes[node as usize];
        let slot = n.slot as usize;

        self.unlink(n.side, slot, node);
        let cells = self.side_cells(n.side);
        cells[slot].quantity -= u64::from(n.quantity);
        cells[slot].orders -= 1;
        let emptied = cells[slot].is_empty();

        self.map_remove_at(map_i);
        self.give_node(node);
        self.live -= 1;

        if emptied {
            self.level_closed(n.side);
            match n.side {
                Side::Bid if self.best_bid == Some(slot) => self.rescan_best(Side::Bid),
                Side::Ask if self.best_ask == Some(slot) => self.rescan_best(Side::Ask),
                _ => {}
            }
        }
        Ok(RestingOrder {
            order_id: n.order_id,
            price: n.price,
            quantity: n.quantity,
            side: n.side,
        })
    }

    /// Lowers an order's quantity, keeping its place in the queue.
    pub fn reduce(&mut self, order_id: u64, new_quantity: u32) -> Result<(), BookError> {
        if new_quantity == 0 {
            return Err(BookError::ZeroQuantity(order_id));
        }
        let Some(map_i) = self.map_find(order_id) else {
            return Err(BookError::UnknownOrderId(order_id));
        };
        let node = self.map[map_i].node as usize;
        let (old, side, slot) = {
            let n = &self.nodes[node];
            (n.quantity, n.side, n.slot as usize)
        };
        if new_quantity > old {
            return Err(BookError::ReduceWouldIncrease {
                order_id,
                from: old,
                to: new_quantity,
            });
        }
        self.nodes[node].quantity = new_quantity;
        let cells = self.side_cells(side);
        cells[slot].quantity -= u64::from(old - new_quantity);
        Ok(())
    }

    /// Changes price and/or quantity, sending the order to the back of its level.
    pub fn replace(
        &mut self,
        order_id: u64,
        new_price: i64,
        new_quantity: u32,
    ) -> Result<(), BookError> {
        if new_quantity == 0 {
            return Err(BookError::ZeroQuantity(order_id));
        }
        let Some(map_i) = self.map_find(order_id) else {
            return Err(BookError::UnknownOrderId(order_id));
        };
        let node = self.map[map_i].node;
        let n = self.nodes[node as usize];

        // Resolve the destination BEFORE detaching, so a refusal leaves the book
        // exactly as it was rather than half-moved.
        let new_slot = self.slot_for(order_id, new_price)?;
        // Read the old slot only *after* that: `slot_for` may have rebased, and
        // a rebase renumbers every recorded slot. A copy taken before the call
        // would be off by the shift, and would corrupt a level that this order
        // never rested at.
        let old_slot = self.nodes[node as usize].slot as usize;

        self.unlink(n.side, old_slot, node);
        {
            let cells = self.side_cells(n.side);
            cells[old_slot].quantity -= u64::from(n.quantity);
            cells[old_slot].orders -= 1;
        }
        let emptied = self.side_cells_ref(n.side)[old_slot].is_empty();
        if emptied {
            self.level_closed(n.side);
        }

        // Checked *after* the old level was decremented, which is what makes a
        // same-price replace net out to no change rather than a double count.
        let filled = self.side_cells_ref(n.side)[new_slot].is_empty();
        self.nodes[node as usize].price = new_price;
        self.nodes[node as usize].quantity = new_quantity;
        self.nodes[node as usize].slot = new_slot as u32;
        self.link_back(n.side, new_slot, node);
        {
            let cells = self.side_cells(n.side);
            cells[new_slot].quantity += u64::from(new_quantity);
            cells[new_slot].orders += 1;
        }
        if filled {
            self.level_opened(n.side);
        }
        self.note_occupied(n.side, new_slot);
        if emptied {
            match n.side {
                Side::Bid if self.best_bid == Some(old_slot) => self.rescan_best(Side::Bid),
                Side::Ask if self.best_ask == Some(old_slot) => self.rescan_best(Side::Ask),
                _ => {}
            }
        }
        Ok(())
    }

    // ---- reading it ------------------------------------------------------

    pub fn get(&self, order_id: u64) -> Option<RestingOrder> {
        // A read-only probe, so it does not touch the stats the mutating path
        // maintains.
        let mut i = self.hash(order_id);
        for _ in 0..=self.map.len() {
            let e = self.map[i];
            if e.node == NIL {
                return None;
            }
            if e.order_id == order_id {
                let n = self.nodes[e.node as usize];
                return Some(RestingOrder {
                    order_id: n.order_id,
                    price: n.price,
                    quantity: n.quantity,
                    side: n.side,
                });
            }
            i = (i + 1) & self.map_mask;
        }
        None
    }

    /// The best price on a side, and the order at the front of its queue.
    pub fn front(&self, side: Side) -> Option<(i64, u64)> {
        let slot = match side {
            Side::Bid => self.best_bid?,
            Side::Ask => self.best_ask?,
        };
        let cell = self.side_cells_ref(side)[slot];
        if cell.head == NIL {
            return None;
        }
        Some((
            self.index.price_at(slot),
            self.nodes[cell.head as usize].order_id,
        ))
    }

    /// Walks the aggregated levels, best first, without allocating.
    ///
    /// A callback rather than an iterator or a `Vec`: the `Vec` is what the
    /// reference book does and what the digest used to do, and it allocated once
    /// per call. The digest runs on the hot path when a checkpoint lands.
    ///
    /// Stops when `f` returns `false`, or after `depth` levels if `depth` is
    /// non-zero.
    pub fn for_each_level(&self, side: Side, depth: usize, mut f: impl FnMut(Level) -> bool) {
        let cells = self.side_cells_ref(side);
        let cap = self.index.capacity();
        let mut seen = 0usize;
        match side {
            Side::Bid => {
                let Some(start) = self.best_bid else { return };
                for i in (0..=start.min(cap - 1)).rev() {
                    if cells[i].is_empty() {
                        continue;
                    }
                    if depth != 0 && seen == depth {
                        return;
                    }
                    seen += 1;
                    if !f(Level {
                        price: self.index.price_at(i),
                        quantity: cells[i].quantity,
                        order_count: cells[i].orders,
                    }) {
                        return;
                    }
                }
            }
            Side::Ask => {
                let Some(start) = self.best_ask else { return };
                for (i, cell) in cells.iter().enumerate().take(cap).skip(start) {
                    if cell.is_empty() {
                        continue;
                    }
                    if depth != 0 && seen == depth {
                        return;
                    }
                    seen += 1;
                    if !f(Level {
                        price: self.index.price_at(i),
                        quantity: cell.quantity,
                        order_count: cell.orders,
                    }) {
                        return;
                    }
                }
            }
        }
    }

    pub fn best_bid(&self) -> Option<Level> {
        let mut out = None;
        self.for_each_level(Side::Bid, 1, |l| {
            out = Some(l);
            false
        });
        out
    }

    pub fn best_ask(&self) -> Option<Level> {
        let mut out = None;
        self.for_each_level(Side::Ask, 1, |l| {
            out = Some(l);
            false
        });
        out
    }

    /// Walks every resting order on a side in queue order: best price first, and
    /// within a price, oldest first.
    ///
    /// This is the order a snapshot has to be written in.
    pub fn for_each_order(&self, side: Side, mut f: impl FnMut(RestingOrder) -> bool) {
        let cells = self.side_cells_ref(side);
        let cap = self.index.capacity();
        // Walked with an explicit index rather than a `Box<dyn Iterator>` over
        // one of two directions. The boxed version reads better and allocates,
        // on a path that is part of the claim this milestone exists to make.
        let (mut i, descending) = match side {
            Side::Bid => match self.best_bid {
                Some(start) => (start.min(cap - 1), true),
                None => return,
            },
            Side::Ask => match self.best_ask {
                Some(start) => (start, false),
                None => return,
            },
        };
        loop {
            let mut node = cells[i].head;
            while node != NIL {
                let n = self.nodes[node as usize];
                if !f(RestingOrder {
                    order_id: n.order_id,
                    price: n.price,
                    quantity: n.quantity,
                    side: n.side,
                }) {
                    return;
                }
                node = n.next;
            }
            if descending {
                if i == 0 {
                    return;
                }
                i -= 1;
            } else {
                if i + 1 >= cap {
                    return;
                }
                i += 1;
            }
        }
    }

    /// Cross-checks the three structures against each other.
    ///
    /// The slab, the map and the level lists are redundant views of the same
    /// state, so a bug in one shows up as disagreement with the others. The
    /// tests call this after every operation; nothing on the hot path does.
    pub fn check_invariants(&self) -> Result<(), String> {
        let mut counted = 0usize;
        for (side, cells) in [(Side::Bid, &self.bids), (Side::Ask, &self.asks)] {
            for (i, cell) in cells.iter().enumerate() {
                let mut node = cell.head;
                let mut in_level = 0u32;
                let mut qty = 0u64;
                let mut prev = NIL;
                while node != NIL {
                    let n = self.nodes[node as usize];
                    if n.slot as usize != i {
                        return Err(format!(
                            "order {} is in level {i} but records slot {}",
                            n.order_id, n.slot
                        ));
                    }
                    if n.side != side {
                        return Err(format!("order {} is on the wrong side", n.order_id));
                    }
                    if n.prev != prev {
                        return Err(format!(
                            "order {} has prev {} but the walk came from {prev}",
                            n.order_id, n.prev
                        ));
                    }
                    if n.quantity == 0 {
                        return Err(format!("order {} rests with zero quantity", n.order_id));
                    }
                    if self.get(n.order_id).is_none() {
                        return Err(format!("order {} is not in the map", n.order_id));
                    }
                    in_level += 1;
                    qty += u64::from(n.quantity);
                    prev = node;
                    node = n.next;
                    if in_level as usize > self.nodes.len() {
                        return Err(format!("level {i} list is cyclic"));
                    }
                }
                if cell.tail != prev {
                    return Err(format!(
                        "level {i} tail is {} but the walk ended at {prev}",
                        cell.tail
                    ));
                }
                if in_level != cell.orders {
                    return Err(format!(
                        "level {i} says {} orders but holds {in_level}",
                        cell.orders
                    ));
                }
                if qty != cell.quantity {
                    return Err(format!(
                        "level {i} says {} quantity but holds {qty}",
                        cell.quantity
                    ));
                }
                counted += in_level as usize;
            }
            let occupied = cells.iter().filter(|c| !c.is_empty()).count();
            let claimed = self.level_count(side, 0);
            if occupied != claimed {
                return Err(format!(
                    "{side:?} side says {claimed} occupied levels but holds {occupied}"
                ));
            }
        }
        if counted != self.live {
            return Err(format!("{} live orders but {counted} on levels", self.live));
        }

        // The cached touch must actually be the touch. A stale best price is the
        // characteristic failure of every incremental cache like this one, and
        // downstream it looks like a book that is quoting confidently and wrong.
        let cap = self.index.capacity();
        let real_bid = (0..cap).rev().find(|&i| !self.bids[i].is_empty());
        if self.best_bid != real_bid {
            return Err(format!(
                "best bid is cached as {:?} but the highest occupied bid is {real_bid:?}",
                self.best_bid
            ));
        }
        let real_ask = (0..cap).find(|&i| !self.asks[i].is_empty());
        if self.best_ask != real_ask {
            return Err(format!(
                "best ask is cached as {:?} but the lowest occupied ask is {real_ask:?}",
                self.best_ask
            ));
        }

        let map_entries = self.map.iter().filter(|e| e.node != NIL).count();
        if map_entries != self.live {
            return Err(format!(
                "{} live orders but {map_entries} map entries; backward-shift deletion \
                 left the table wrong",
                self.live
            ));
        }
        Ok(())
    }
}

impl crate::view::OrderBook for MboBook {
    fn add(
        &mut self,
        order_id: u64,
        side: Side,
        price: i64,
        quantity: u32,
    ) -> Result<(), BookError> {
        MboBook::add(self, order_id, side, price, quantity)
    }

    fn delete(&mut self, order_id: u64) -> Result<RestingOrder, BookError> {
        MboBook::delete(self, order_id)
    }

    fn reduce(&mut self, order_id: u64, new_quantity: u32) -> Result<(), BookError> {
        MboBook::reduce(self, order_id, new_quantity)
    }

    fn replace(
        &mut self,
        order_id: u64,
        new_price: i64,
        new_quantity: u32,
    ) -> Result<(), BookError> {
        MboBook::replace(self, order_id, new_price, new_quantity)
    }

    fn get(&self, order_id: u64) -> Option<RestingOrder> {
        MboBook::get(self, order_id)
    }

    fn len(&self) -> usize {
        self.live
    }

    fn level_count(&self, side: Side, depth: usize) -> usize {
        MboBook::level_count(self, side, depth)
    }

    fn for_each_level(&self, side: Side, depth: usize, f: &mut dyn FnMut(Level)) {
        MboBook::for_each_level(self, side, depth, |l| {
            f(l);
            true
        });
    }

    fn for_each_order(&self, side: Side, f: &mut dyn FnMut(RestingOrder) -> bool) {
        MboBook::for_each_order(self, side, f);
    }

    fn clear(&mut self) {
        MboBook::clear(self);
    }

    fn check_invariants(&self) -> Result<(), String> {
        MboBook::check_invariants(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn book() -> MboBook {
        // Small on purpose: a 64-slot window and a 16-order slab put the
        // capacity edges within reach of a test instead of a million messages
        // away.
        MboBook::new(MboCapacity {
            levels: 64,
            orders: 16,
            reference_price: 1_000_000,
            tick: 100,
        })
    }

    fn levels(b: &MboBook, side: Side, depth: usize) -> Vec<Level> {
        let mut out = Vec::new();
        b.for_each_level(side, depth, |l| {
            out.push(l);
            true
        });
        out
    }

    fn queue(b: &MboBook, side: Side) -> Vec<u64> {
        let mut out = Vec::new();
        b.for_each_order(side, |o| {
            out.push(o.order_id);
            true
        });
        out
    }

    #[test]
    fn orders_rest_in_arrival_order_at_a_price() {
        let mut b = book();
        b.add(1, Side::Bid, 1_000_000, 10).unwrap();
        b.add(2, Side::Bid, 1_000_000, 20).unwrap();
        b.add(3, Side::Bid, 1_000_000, 30).unwrap();
        assert_eq!(queue(&b, Side::Bid), vec![1, 2, 3]);
        assert_eq!(b.front(Side::Bid), Some((1_000_000, 1)));
        b.check_invariants().unwrap();
    }

    #[test]
    fn the_best_bid_is_the_highest_and_the_best_ask_is_the_lowest() {
        let mut b = book();
        b.add(1, Side::Bid, 999_900, 10).unwrap();
        b.add(2, Side::Bid, 1_000_000, 10).unwrap();
        b.add(3, Side::Ask, 1_000_200, 10).unwrap();
        b.add(4, Side::Ask, 1_000_100, 10).unwrap();
        assert_eq!(b.best_bid().unwrap().price, 1_000_000);
        assert_eq!(b.best_ask().unwrap().price, 1_000_100);
        b.check_invariants().unwrap();
    }

    #[test]
    fn levels_aggregate_quantity_and_count() {
        let mut b = book();
        b.add(1, Side::Bid, 1_000_000, 10).unwrap();
        b.add(2, Side::Bid, 1_000_000, 5).unwrap();
        b.add(3, Side::Bid, 999_900, 7).unwrap();
        let ls = levels(&b, Side::Bid, 0);
        assert_eq!(ls.len(), 2);
        assert_eq!(
            ls[0],
            Level {
                price: 1_000_000,
                quantity: 15,
                order_count: 2
            }
        );
        assert_eq!(
            ls[1],
            Level {
                price: 999_900,
                quantity: 7,
                order_count: 1
            }
        );
        assert_eq!(b.level_count(Side::Bid, 0), 2);
        assert_eq!(b.level_count(Side::Bid, 1), 1);
    }

    #[test]
    fn deleting_the_only_order_at_the_touch_moves_the_best_price_down() {
        // The rescan path. A stale cached best is the failure mode this whole
        // structure is most exposed to.
        let mut b = book();
        b.add(1, Side::Bid, 1_000_000, 10).unwrap();
        b.add(2, Side::Bid, 999_900, 10).unwrap();
        b.delete(1).unwrap();
        assert_eq!(b.best_bid().unwrap().price, 999_900);
        b.check_invariants().unwrap();
        b.delete(2).unwrap();
        assert_eq!(b.best_bid(), None);
        assert!(b.is_empty());
        b.check_invariants().unwrap();
    }

    #[test]
    fn reduce_keeps_queue_position_and_replace_loses_it() {
        let mut b = book();
        b.add(1, Side::Bid, 1_000_000, 10).unwrap();
        b.add(2, Side::Bid, 1_000_000, 10).unwrap();

        b.reduce(1, 4).unwrap();
        assert_eq!(queue(&b, Side::Bid), vec![1, 2], "reduce keeps priority");
        assert_eq!(levels(&b, Side::Bid, 0)[0].quantity, 14);

        b.replace(1, 1_000_000, 9).unwrap();
        assert_eq!(queue(&b, Side::Bid), vec![2, 1], "replace goes to the back");
        assert_eq!(levels(&b, Side::Bid, 0)[0].quantity, 19);
        b.check_invariants().unwrap();
    }

    #[test]
    fn a_same_price_replace_does_not_double_count_the_level() {
        // The level empties and refills within one operation. Counting the two
        // halves independently would leave the occupied-level total wrong, and
        // nothing but the digest would ever notice.
        let mut b = book();
        b.add(1, Side::Bid, 1_000_000, 10).unwrap();
        b.replace(1, 1_000_000, 12).unwrap();
        assert_eq!(b.level_count(Side::Bid, 0), 1);
        b.check_invariants().unwrap();
    }

    #[test]
    fn a_replace_across_prices_moves_the_level_totals() {
        let mut b = book();
        b.add(1, Side::Bid, 1_000_000, 10).unwrap();
        b.add(2, Side::Bid, 999_900, 3).unwrap();
        b.replace(1, 999_800, 4).unwrap();
        assert_eq!(b.best_bid().unwrap().price, 999_900);
        assert_eq!(levels(&b, Side::Bid, 0).len(), 2);
        assert_eq!(b.get(1).unwrap().price, 999_800);
        b.check_invariants().unwrap();
    }

    #[test]
    fn the_malformed_feed_cases_match_the_reference_book() {
        let mut b = book();
        b.add(1, Side::Bid, 1_000_000, 10).unwrap();
        assert_eq!(
            b.add(1, Side::Bid, 1_000_000, 5),
            Err(BookError::DuplicateOrderId(1))
        );
        assert_eq!(b.delete(99), Err(BookError::UnknownOrderId(99)));
        assert_eq!(b.reduce(99, 1), Err(BookError::UnknownOrderId(99)));
        assert_eq!(b.reduce(1, 0), Err(BookError::ZeroQuantity(1)));
        assert_eq!(
            b.reduce(1, 11),
            Err(BookError::ReduceWouldIncrease {
                order_id: 1,
                from: 10,
                to: 11
            })
        );
        assert_eq!(
            b.add(2, Side::Bid, 1_000_000, 0),
            Err(BookError::ZeroQuantity(2))
        );
        b.check_invariants().unwrap();
    }

    #[test]
    fn a_price_off_the_tick_grid_is_refused() {
        let mut b = book();
        assert_eq!(
            b.add(1, Side::Bid, 1_000_050, 10),
            Err(BookError::PriceOffGrid {
                order_id: 1,
                price: 1_000_050
            })
        );
        assert!(b.is_empty());
        b.check_invariants().unwrap();
    }

    #[test]
    fn the_slab_reports_being_full_rather_than_growing() {
        let mut b = book(); // 16 orders
        for i in 0..16u64 {
            b.add(i, Side::Bid, 1_000_000, 1).unwrap();
        }
        assert_eq!(
            b.add(16, Side::Bid, 1_000_000, 1),
            Err(BookError::SlabFull {
                order_id: 16,
                capacity: 16
            })
        );
        b.check_invariants().unwrap();
        // And the slab is reusable: freeing one makes room for one.
        b.delete(0).unwrap();
        b.add(16, Side::Bid, 1_000_000, 1).unwrap();
        b.check_invariants().unwrap();
    }

    #[test]
    fn the_window_slides_onto_a_far_price_and_brings_the_book_with_it() {
        let mut b = book();
        b.add(1, Side::Bid, 1_000_000, 10).unwrap();
        b.add(2, Side::Bid, 999_900, 5).unwrap();
        assert_eq!(b.stats().rebases, 0);

        // A 64-slot window of 100 centred on 1_000_000 covers 996_800..=1_003_100,
        // so this is outside it — but the occupied prices are close enough that
        // one slide fits everything.
        let outside = 1_004_000;
        assert_eq!(
            b.slot_of(outside),
            None,
            "the test price must start outside"
        );
        b.add(3, Side::Ask, outside, 7).unwrap();
        assert_eq!(b.stats().rebases, 1, "the window had to move");

        // Everything that was on the book is still on it, at its own price.
        assert_eq!(b.get(1).unwrap().price, 1_000_000);
        assert_eq!(b.get(2).unwrap().price, 999_900);
        assert_eq!(b.get(3).unwrap().price, outside);
        assert_eq!(b.best_bid().unwrap().price, 1_000_000);
        assert_eq!(b.best_ask().unwrap().price, outside);
        b.check_invariants().unwrap();
    }

    #[test]
    fn a_rebase_that_would_drop_a_live_level_is_refused_not_performed() {
        // The property that stops the book quietly forgetting its far side.
        let mut b = book(); // 64 slots of 100 = a 6,400-wide window
        b.add(1, Side::Bid, 1_000_000, 10).unwrap();
        let far = 2_000_000;
        assert_eq!(
            b.add(2, Side::Bid, far, 10),
            Err(BookError::PriceOutOfRange {
                order_id: 2,
                price: far
            })
        );
        // Refused, not half-applied.
        assert_eq!(b.len(), 1);
        assert_eq!(b.get(1).unwrap().price, 1_000_000);
        assert_eq!(b.stats().rebases, 0);
        b.check_invariants().unwrap();
    }

    #[test]
    fn a_replace_refused_for_range_leaves_the_order_where_it_was() {
        let mut b = book();
        b.add(1, Side::Bid, 1_000_000, 10).unwrap();
        b.add(2, Side::Bid, 999_900, 10).unwrap();
        assert!(b.replace(1, 9_000_000, 5).is_err());
        assert_eq!(
            b.get(1).unwrap(),
            RestingOrder {
                order_id: 1,
                price: 1_000_000,
                quantity: 10,
                side: Side::Bid,
            }
        );
        assert_eq!(queue(&b, Side::Bid), vec![1, 2]);
        b.check_invariants().unwrap();
    }

    #[test]
    fn clear_empties_the_book_and_the_slab_is_fully_reusable_afterwards() {
        // Recovery does this: a snapshot replaces the book wholesale, and the
        // free list has to come back intact or the next 16 adds fail.
        let mut b = book();
        for i in 0..16u64 {
            b.add(i, Side::Bid, 1_000_000 - (i as i64) * 100, 1)
                .unwrap();
        }
        b.clear();
        assert!(b.is_empty());
        assert_eq!(b.best_bid(), None);
        assert_eq!(b.level_count(Side::Bid, 0), 0);
        b.check_invariants().unwrap();
        for i in 0..16u64 {
            b.add(i, Side::Bid, 1_000_000, 1).unwrap();
        }
        b.check_invariants().unwrap();
    }

    #[test]
    fn the_order_map_survives_heavy_churn_without_degrading() {
        // Backward-shift deletion is the reason this passes. With tombstones the
        // table would end up mostly dead entries and the probe count would climb
        // without bound.
        let mut b = book();
        let mut next = 0u64;
        for round in 0..500u64 {
            for _ in 0..8 {
                b.add(next, Side::Bid, 1_000_000, 1).unwrap();
                next += 1;
            }
            for id in (next - 8)..next {
                b.delete(id).unwrap();
            }
            if round % 100 == 0 {
                b.check_invariants().unwrap();
            }
        }
        assert!(b.is_empty());
        let s = b.stats();
        // 4x sized table, so a healthy table averages well under one extra probe.
        assert!(
            s.extra_probes < s.lookups,
            "probe chains grew: {} extra probes over {} lookups",
            s.extra_probes,
            s.lookups
        );
        b.check_invariants().unwrap();
    }

    #[test]
    fn colliding_ids_are_all_findable_after_the_middle_one_is_removed() {
        // The exact case backward-shift deletion exists for: three ids that
        // probe to the same home slot, with the middle of the chain removed.
        // Naively clearing the slot would strand the third.
        let mut b = MboBook::new(MboCapacity {
            levels: 8,
            orders: 1024,
            reference_price: 1_000_000,
            tick: 100,
        });
        // Find three ids that share a home slot in this table.
        let mut colliding = Vec::new();
        let home = b.hash(1);
        for id in 1..2_000_000u64 {
            if b.hash(id) == home {
                colliding.push(id);
                if colliding.len() == 3 {
                    break;
                }
            }
        }
        assert_eq!(colliding.len(), 3, "expected to find a 3-way collision");

        for id in &colliding {
            b.add(*id, Side::Bid, 1_000_000, 1).unwrap();
        }
        b.delete(colliding[1]).unwrap();
        assert!(b.get(colliding[0]).is_some());
        assert!(b.get(colliding[1]).is_none());
        assert!(
            b.get(colliding[2]).is_some(),
            "the tail of the probe chain was stranded by the deletion"
        );
        b.check_invariants().unwrap();
    }
}
